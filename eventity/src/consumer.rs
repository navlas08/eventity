use crate::BatchOptions;
use crate::postgres::InboxRecord;
use crate::{EventityError, EventityPg, IntegrationEvent, RabbitMQ, TransactionalEventHandler};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::ShortString;
use lapin::{Channel, Consumer};
use serde_json::value::RawValue;
use sqlx::types::uuid;
use std::{str::FromStr, sync::Arc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

async fn prepare_consumer<E: IntegrationEvent>(
    broker: &RabbitMQ,
    prefetch: u16,
) -> Result<(Consumer, Channel), EventityError> {
    let exchange = ShortString::from(E::exchange());
    let queue_name = ShortString::from(E::queue());
    broker.declare_exchange(&exchange).await?;
    let dead_exchange: ShortString = format!("{}.dead", E::exchange()).into();
    broker
        .declare_dead_letter_topology(dead_exchange.clone(), queue_name.clone())
        .await?;
    let queue = broker.declare_queue(&queue_name, &dead_exchange).await?;
    broker.bind_queue(queue, exchange).await?;
    broker.declare_consumer(queue_name, prefetch).await
}

pub(crate) async fn prepare_consumer_with_timeout<E: IntegrationEvent>(
    broker: &RabbitMQ,
    prefetch: u16,
) -> Result<(Consumer, Channel), EventityError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        prepare_consumer::<E>(broker, prefetch),
    )
    .await
    .map_err(|_| EventityError::TopologyTimeout)?
}

pub(crate) async fn run_consumer<E, H>(
    consumer: Consumer,
    channel: Channel,
    store: Arc<EventityPg>,
    handler: Arc<H>,
    cancel: CancellationToken,
    limit: usize,
    batching: BatchOptions,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    let result = run_consumer_deliveries::<E, H>(
        consumer,
        channel.clone(),
        store,
        handler,
        cancel,
        limit,
        batching,
    )
    .await;
    // Closing also requeues deliveries that were prefetched but never started.
    // Do this on errors as well as shutdown so abandoned consumers cannot retain
    // unacknowledged messages on a connection shared with other subscriptions.
    let close = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        channel.close(200, "consumer stopped".into()),
    )
    .await;
    result?;
    close.map_err(|_| EventityError::Worker("timed out closing consumer channel".into()))??;
    Ok(())
}

async fn run_consumer_deliveries<E, H>(
    mut consumer: Consumer,
    channel: Channel,
    store: Arc<EventityPg>,
    handler: Arc<H>,
    cancel: CancellationToken,
    limit: usize,
    batching: BatchOptions,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            delivery = consumer.next(), if tasks.len() < limit => match delivery {
                Some(Ok(delivery)) => {
                    let (deliveries, stream_error) = collect_batch(&mut consumer, delivery, batching, &cancel).await;
                    let (store, handler) = (store.clone(), handler.clone());
                    tasks.spawn(async move {
                        process_and_settle::<E, H>(&store, &handler, deliveries, limit == 1).await
                    });
                    if let Some(error) = stream_error {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            result = channel.wait_for_recovery(error) => result?,
                        }
                    }
                }
                Some(Err(error)) => match tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = channel.wait_for_recovery(error) => result,
                } {
                    Ok(()) => continue,
                    Err(error) => return Err(error.into()),
                },
                None => return Err(EventityError::Worker("RabbitMQ consumer stream closed".into())),
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                result.map_err(|error| EventityError::Join(error.to_string()))??;
            }
        }
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        channel.basic_cancel(
            consumer.tag(),
            lapin::options::BasicCancelOptions::default(),
        ),
    )
    .await
    .map_err(|_| EventityError::Worker("timed out cancelling consumer".into()))??;
    while let Some(result) = tasks.join_next().await {
        result.map_err(|error| EventityError::Join(error.to_string()))??;
    }
    Ok(())
}

/// Keep a fixed deadline from the first delivery so a slow stream cannot hold
/// a transaction batch indefinitely. No transaction is opened while collecting.
async fn collect_batch(
    consumer: &mut Consumer,
    first: Delivery,
    options: BatchOptions,
    cancel: &CancellationToken,
) -> (Vec<Delivery>, Option<lapin::Error>) {
    let limit = usize::from(options.max_messages.get());
    let mut deliveries = Vec::with_capacity(limit.min(32));
    deliveries.push(first);
    let deadline = tokio::time::Instant::now() + options.max_wait;
    while deliveries.len() < limit {
        let next = match consumer.next().now_or_never() {
            Some(next) => next,
            None if options.max_wait.is_zero() => break,
            None => tokio::select! {
                _ = cancel.cancelled() => break,
                result = tokio::time::timeout_at(deadline, consumer.next()) => match result {
                    Ok(next) => next,
                    Err(_) => break,
                },
            },
        };
        match next {
            Some(Ok(delivery)) => deliveries.push(delivery),
            Some(Err(error)) => return (deliveries, Some(error)),
            None => break,
        }
    }
    (deliveries, None)
}

/// Settle individually: `multiple: true` could acknowledge unfinished work
/// belonging to another concurrently executing batch on this channel.
async fn settle(
    delivery: &Delivery,
    result: Result<(), EventityError>,
) -> Result<(), EventityError> {
    match result {
        Ok(()) => delivery
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(Into::into),
        Err(error) => {
            let requeue = matches!(error, EventityError::Postgres(_));
            if requeue {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            delivery
                .nack(BasicNackOptions {
                    requeue,
                    ..Default::default()
                })
                .await?;
            tracing::error!(%error, requeue, "event processing failed");
            Ok(())
        }
    }
}

async fn process_and_settle<E, H>(
    store: &EventityPg,
    handler: &H,
    deliveries: Vec<Delivery>,
    exclusive: bool,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    tracing::debug!(target: "eventity::consumer", consumer_messages = deliveries.len() as u64,
        consumer_batches = 1_u64, "processing delivery batch");
    if deliveries.len() > 1 {
        match process_batch::<E, H>(store, handler, &deliveries).await {
            Ok(()) => {
                // With one active batch, every earlier delivery has already
                // settled. A cumulative ack cannot cover unfinished work.
                // Concurrent batches must retain individual acknowledgements.
                if exclusive {
                    deliveries
                        .last()
                        .expect("nonempty batch")
                        .ack(BasicAckOptions { multiple: true })
                        .await?;
                    return Ok(());
                }
                let mut acknowledgements: FuturesUnordered<_> = deliveries
                    .iter()
                    .map(|delivery| delivery.ack(BasicAckOptions::default()))
                    .collect();
                while let Some(result) = acknowledgements.next().await {
                    result?;
                }
                return Ok(());
            }
            Err(EventityError::Postgres(error)) => {
                // Back off once per batch, not once per message.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                for delivery in &deliveries {
                    delivery
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                }
                tracing::error!(%error, "batch database operation failed; requeued");
                return Ok(());
            }
            Err(error) => tracing::warn!(%error, "batch rolled back; isolating failed deliveries"),
        }
    }
    for delivery in &deliveries {
        settle(
            delivery,
            process_delivery::<E, H>(store, handler, delivery).await,
        )
        .await?;
    }
    Ok(())
}

fn message_id(delivery: &Delivery) -> Result<uuid::Uuid, EventityError> {
    let id = delivery
        .properties
        .message_id()
        .as_ref()
        .ok_or(EventityError::MissingMessageId)?;
    uuid::Uuid::from_str(id.as_str())
        .map_err(|error| EventityError::InvalidMessageId(error.to_string()))
}

async fn process_batch<E, H>(
    store: &EventityPg,
    handler: &H,
    deliveries: &[Delivery],
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    let mut events = Vec::with_capacity(deliveries.len());
    let mut records = Vec::with_capacity(deliveries.len());
    for delivery in deliveries {
        let id = message_id(delivery)?;
        let payload: &RawValue = serde_json::from_slice(&delivery.data)?;
        let event: E = serde_json::from_str(payload.get())?;
        events.push((id, event));
        records.push((id, payload));
    }
    let mut tx = store.get_transaction().await?;
    let mut inserted = store
        .create_inbox_batch(&mut tx, E::queue(), &records)
        .await?;
    let mut outgoing = Vec::new();
    for (id, event) in events {
        // Also suppress duplicate IDs appearing within this batch.
        if inserted.remove(&id) {
            let messages = handler
                .handle_in_transaction(event, &mut tx)
                .await
                .map_err(|error| EventityError::Handler(format!("{error:?}")))?;
            outgoing.extend(messages.into_records()?);
        }
    }
    let inserted = store.create_outbox_records(&mut tx, outgoing).await?;
    tx.commit().await?;
    if inserted > 0 {
        store.notify_outbox();
    }
    Ok(())
}

async fn process_delivery<E, H>(
    store: &EventityPg,
    handler: &H,
    delivery: &lapin::message::Delivery,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    let payload: &RawValue = serde_json::from_slice(&delivery.data)?;
    let event: E = serde_json::from_str(payload.get())?;
    let event_id = message_id(delivery)?;
    let mut tx = store.get_transaction().await?;
    let inserted = store
        .create_inbox_record(
            &mut tx,
            InboxRecord {
                id: event_id,
                consumer: E::queue().to_owned(),
                event_type: E::queue().to_owned(),
                payload,
            },
        )
        .await?;
    if inserted == 0 {
        tx.rollback().await?;
        return Ok(());
    }
    let outgoing = handler
        .handle_in_transaction(event, &mut tx)
        .await
        .map_err(|error| EventityError::Handler(format!("{error:?}")))?;
    let records = outgoing.into_records()?;
    let inserted = store.create_outbox_records(&mut tx, records).await?;
    tx.commit().await?;
    if inserted > 0 {
        store.notify_outbox();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventHandler;
    use crate::rabbitmq::Message;
    use crate::test_support::Resources;
    use async_trait::async_trait;
    use lapin::BasicProperties;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    #[derive(Serialize, Deserialize)]
    struct TestEvent {
        id: u64,
        fail: bool,
    }
    impl IntegrationEvent for TestEvent {
        type Error = String;
        fn queue() -> &'static str {
            "test-consumer"
        }
        fn exchange() -> &'static str {
            "unused"
        }
        fn aggregate() -> &'static str {
            "test"
        }
        fn aggregate_id(&self) -> String {
            self.id.to_string()
        }
    }
    struct TransactionalHandler;
    #[async_trait]
    impl EventHandler<TestEvent> for TransactionalHandler {
        async fn handle(&self, _: TestEvent) -> crate::EHandlerResult<String> {
            panic!("transactional hook should be called")
        }
        async fn handle_transactional(
            &self,
            event: TestEvent,
            tx: &mut sqlx::PgTransaction<'_>,
        ) -> crate::EHandlerResult<String> {
            sqlx::query("INSERT INTO effects (id) VALUES ($1)")
                .bind(event.id as i64)
                .execute(&mut **tx)
                .await
                .map_err(|e| e.to_string())?;
            if event.fail {
                return Err("rollback".into());
            }
            Ok(crate::OutgoingMessages::none())
        }
    }
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn inbox_deduplicates_and_rolls_back_application_changes() {
        let r = Resources::new().await;
        sqlx::query("CREATE TABLE effects (id bigint)")
            .execute(&r.pool)
            .await
            .unwrap();
        for fail in [false, true] {
            r.broker
                .publish(Message {
                    event_id: uuid::Uuid::now_v7(),
                    exchange_name: r.schema.clone().into(),
                    payload: serde_json::json!({"id": 1, "fail": fail}),
                })
                .await
                .unwrap();
            let message = r
                .channel
                .basic_get(
                    r.schema.clone().into(),
                    lapin::options::BasicGetOptions { no_ack: true },
                )
                .await
                .unwrap()
                .unwrap();
            for _ in 0..2 {
                let result = process_delivery::<TestEvent, _>(
                    &r.store,
                    &TransactionalHandler,
                    &message.delivery,
                )
                .await;
                assert_eq!(result.is_err(), fail);
            }
        }
        let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM effects")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        let inbox: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!((effects, inbox), (1, 1));
        r.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn publisher_reuses_channel_and_replaces_closed_channel() {
        let r = Resources::new().await;
        let exchange: ShortString = r.schema.clone().into();
        let first = r.broker.publisher_channel(&exchange).await.unwrap();
        let next = r.broker.publisher_channel(&exchange).await.unwrap();
        assert_eq!(first.id(), next.id());
        assert!(first.status().confirm());
        first
            .close(200, "test channel replacement".into())
            .await
            .unwrap();
        r.broker
            .publish(Message {
                event_id: uuid::Uuid::now_v7(),
                exchange_name: exchange,
                payload: serde_json::json!({"ok": true}),
            })
            .await
            .unwrap();
        let message = r
            .channel
            .basic_get(
                r.schema.clone().into(),
                lapin::options::BasicGetOptions { no_ack: true },
            )
            .await
            .unwrap();
        assert!(message.is_some());
        r.cleanup().await;
    }
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn publisher_pool_recovers_each_cached_channel_independently() {
        let r = Resources::new().await;
        let broker = RabbitMQ::new_with_publisher_connections(
            std::env::var("AMQP_URL").unwrap(),
            std::num::NonZeroU16::new(2).unwrap(),
        )
        .await
        .unwrap();
        let exchange: ShortString = r.schema.clone().into();
        let first = broker.publisher_channel(&exchange).await.unwrap();
        let second = broker.publisher_channel(&exchange).await.unwrap();
        first
            .close(200, "test pool replacement".into())
            .await
            .unwrap();
        assert!(second.status().connected());
        for id in 0..4 {
            broker
                .publish(Message {
                    event_id: uuid::Uuid::now_v7(),
                    exchange_name: exchange.clone(),
                    payload: serde_json::json!({"id": id}),
                })
                .await
                .unwrap();
        }
        for id in 0..4 {
            let message = r
                .channel
                .basic_get(
                    exchange.clone(),
                    lapin::options::BasicGetOptions { no_ack: true },
                )
                .await
                .unwrap()
                .unwrap();
            let payload: Value = serde_json::from_slice(&message.delivery.data).unwrap();
            assert_eq!(payload["id"], id);
        }
        assert!(second.status().connected());
        r.cleanup().await;
    }
    struct BlockingHandler {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl EventHandler<TestEvent> for BlockingHandler {
        async fn handle(&self, _: TestEvent) -> crate::EHandlerResult<String> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(crate::OutgoingMessages::none())
        }
    }
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn shutdown_drains_handler_and_requeues_prefetched_deliveries() {
        let r = Resources::new().await;
        for id in 0..8 {
            r.broker
                .publish(Message {
                    event_id: uuid::Uuid::now_v7(),
                    exchange_name: r.schema.clone().into(),
                    payload: serde_json::json!({"id": id, "fail": false}),
                })
                .await
                .unwrap();
        }
        let (consumer, channel) = r
            .broker
            .declare_consumer(r.schema.clone().into(), 8)
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let handler = Arc::new(BlockingHandler {
            entered: entered.clone(),
            release: release.clone(),
        });
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(run_consumer::<TestEvent, _>(
            consumer,
            channel,
            Arc::new(EventityPg::new(r.pool.clone())),
            handler,
            cancel.clone(),
            1,
            BatchOptions::default(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        cancel.cancel();
        // Give the worker a chance to cancel its subscription before releasing
        // the active handler; prefetched deliveries must stay unprocessed.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let inbox: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!(inbox, 1);
        let queue = r
            .channel
            .queue_declare(
                r.schema.clone().into(),
                lapin::options::QueueDeclareOptions::durable(),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(queue.message_count(), 7);
        let mut message = r
            .channel
            .basic_get(
                r.schema.clone().into(),
                lapin::options::BasicGetOptions { no_ack: true },
            )
            .await
            .unwrap()
            .unwrap();
        message.delivery.properties = BasicProperties::default();
        assert!(matches!(
            process_delivery::<TestEvent, _>(&r.store, &TransactionalHandler, &message.delivery)
                .await,
            Err(EventityError::MissingMessageId)
        ));
        r.cleanup().await;
    }
    async fn deliveries(r: &Resources, events: &[(uuid::Uuid, u64, bool)]) -> Vec<Delivery> {
        for &(event_id, id, fail) in events {
            r.broker
                .publish(Message {
                    event_id,
                    exchange_name: r.schema.clone().into(),
                    payload: serde_json::json!({"id": id, "fail": fail}),
                })
                .await
                .unwrap();
        }
        let mut deliveries = Vec::new();
        for _ in events {
            let message = r
                .channel
                .basic_get(r.schema.clone().into(), Default::default())
                .await
                .unwrap()
                .unwrap();
            deliveries.push(message.delivery);
        }
        deliveries
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn batch_deduplicates_within_and_across_concurrent_transactions() {
        let r = Resources::new().await;
        sqlx::query("CREATE TABLE effects (id bigint)")
            .execute(&r.pool)
            .await
            .unwrap();
        let first = uuid::Uuid::now_v7();
        let events = deliveries(
            &r,
            &[
                (first, 1, false),
                (first, 999, false),
                (uuid::Uuid::now_v7(), 2, false),
            ],
        )
        .await;
        let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                process_batch::<TestEvent, _>(&r.store, &TransactionalHandler, &events),
                process_batch::<TestEvent, _>(&r.store, &TransactionalHandler, &events),
            )
        })
        .await
        .unwrap();
        left.unwrap();
        right.unwrap();
        let effects: Vec<i64> = sqlx::query_scalar("SELECT id FROM effects ORDER BY id")
            .fetch_all(&r.pool)
            .await
            .unwrap();
        assert_eq!(effects, [1, 2]);
        let payload: Value = sqlx::query_scalar("SELECT payload FROM inbox WHERE id = $1")
            .bind(first)
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!(payload["id"], 1);
        for delivery in events {
            delivery.ack(Default::default()).await.unwrap();
        }
        r.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn failed_batch_rolls_back_and_isolates_poison_message() {
        let r = Resources::new().await;
        sqlx::query("CREATE TABLE effects (id bigint)")
            .execute(&r.pool)
            .await
            .unwrap();
        let events = deliveries(
            &r,
            &[
                (uuid::Uuid::now_v7(), 1, false),
                (uuid::Uuid::now_v7(), 2, true),
                (uuid::Uuid::now_v7(), 3, false),
            ],
        )
        .await;
        assert!(
            process_batch::<TestEvent, _>(&r.store, &TransactionalHandler, &events)
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM effects")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        process_and_settle::<TestEvent, _>(&r.store, &TransactionalHandler, events, false)
            .await
            .unwrap();
        let effects: Vec<i64> = sqlx::query_scalar("SELECT id FROM effects ORDER BY id")
            .fetch_all(&r.pool)
            .await
            .unwrap();
        assert_eq!(effects, [1, 3]);
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert!(
            r.channel
                .basic_get(r.schema.clone().into(), Default::default())
                .await
                .unwrap()
                .is_none()
        );
        r.cleanup().await;
    }
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn partial_batch_deadline_and_exclusive_ack_preserve_later_deliveries() {
        struct NativeHandler;
        impl TransactionalEventHandler<TestEvent> for NativeHandler {
            async fn handle_in_transaction(
                &self,
                _: TestEvent,
                _: &mut sqlx::PgTransaction<'_>,
            ) -> crate::EHandlerResult<String> {
                Ok(crate::OutgoingMessages::none())
            }
        }
        let r = Resources::new().await;
        let (mut consumer, channel) = r
            .broker
            .declare_consumer(r.schema.clone().into(), 8)
            .await
            .unwrap();
        for id in 0..3 {
            r.broker
                .publish(Message {
                    event_id: uuid::Uuid::now_v7(),
                    exchange_name: r.schema.clone().into(),
                    payload: serde_json::json!({"id": id, "fail": false}),
                })
                .await
                .unwrap();
        }
        let first = consumer.next().await.unwrap().unwrap();
        let wait = std::time::Duration::from_millis(20);
        let started = tokio::time::Instant::now();
        let (batch, error) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            collect_batch(
                &mut consumer,
                first,
                BatchOptions {
                    max_messages: std::num::NonZeroU16::new(4).unwrap(),
                    max_wait: wait,
                },
                &CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
        assert!(error.is_none());
        assert_eq!(batch.len(), 3);
        assert!(started.elapsed() >= wait);
        // These deliveries are already outstanding when the earlier batch
        // commits. Its cumulative ack must not acknowledge them.
        for id in 3..5 {
            r.broker
                .publish(Message {
                    event_id: uuid::Uuid::now_v7(),
                    exchange_name: r.schema.clone().into(),
                    payload: serde_json::json!({"id": id, "fail": false}),
                })
                .await
                .unwrap();
            consumer.next().await.unwrap().unwrap();
        }
        process_and_settle::<TestEvent, _>(&r.store, &NativeHandler, batch, true)
            .await
            .unwrap();
        channel.close(200, "test requeue".into()).await.unwrap();
        let queue = r
            .channel
            .queue_declare(
                r.schema.clone().into(),
                lapin::options::QueueDeclareOptions::durable(),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(queue.message_count(), 2);
        let inbox: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox")
            .fetch_one(&r.pool)
            .await
            .unwrap();
        assert_eq!(inbox, 3);
        r.cleanup().await;
    }
}
