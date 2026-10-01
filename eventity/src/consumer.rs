use crate::postgres::InboxRecord;
use crate::{EventHandler, EventityError, EventityPg, IntegrationEvent, RabbitMQ};
use futures_lite::StreamExt;
use futures_util::FutureExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use lapin::types::ShortString;
use lapin::{Channel, Consumer};
use serde_json::Value;
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
    batch_size: usize,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: EventHandler<E>,
{
    let result = run_consumer_deliveries::<E, H>(
        consumer,
        channel.clone(),
        store,
        handler,
        cancel,
        limit,
        batch_size,
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
    batch_size: usize,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: EventHandler<E>,
{
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            delivery = consumer.next(), if tasks.len() < limit => match delivery {
                Some(Ok(delivery)) => {
                    let mut deliveries = Vec::with_capacity(batch_size.min(32));
                    deliveries.push(delivery);
                    // Poll only ready deliveries: no latency penalty at low load.
                    // Consumer errors encountered here are handled after this batch
                    // is scheduled, so an already-read delivery is never discarded.
                    let mut stream_error = None;
                    while deliveries.len() < batch_size {
                        match consumer.next().now_or_never() {
                            Some(Some(Ok(delivery))) => deliveries.push(delivery),
                            Some(Some(Err(error))) => { stream_error = Some(error); break; }
                            _ => break,
                        }
                    }
                    let (store, handler) = (store.clone(), handler.clone());
                    tasks.spawn(async move {
                        process_and_settle::<E, H>(&store, &handler, deliveries).await
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
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: EventHandler<E>,
{
    if deliveries.len() > 1 {
        match process_batch::<E, H>(store, handler, &deliveries).await {
            Ok(()) => {
                for delivery in &deliveries {
                    settle(delivery, Ok(())).await?;
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
    H: EventHandler<E>,
{
    let mut events = Vec::with_capacity(deliveries.len());
    let mut records = Vec::with_capacity(deliveries.len());
    for delivery in deliveries {
        let id = message_id(delivery)?;
        let payload: Value = serde_json::from_slice(&delivery.data)?;
        let event = E::deserialize(&payload)?;
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
                .handle_transactional(event, &mut tx)
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
    H: EventHandler<E>,
{
    let payload: Value = serde_json::from_slice(&delivery.data)?;
    let event = E::deserialize(&payload)?;
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
        .handle_transactional(event, &mut tx)
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
    use crate::rabbitmq::Message;
    use crate::test_support::Resources;
    use async_trait::async_trait;
    use lapin::BasicProperties;
    use serde::{Deserialize, Serialize};

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
            1,
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
}
