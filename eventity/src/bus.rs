use crate::errors::EventityError;
use crate::postgres::InboxRecord;
use crate::registry::HandlerRegistry;
use crate::{EventHandler, EventityPg, IntegrationEvent, Request, RequestHandler};
use async_trait::async_trait;
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind, Queue,
};
use serde_json::Value;
use sqlx::types::uuid;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

macro_rules! recover_channel_call {
    ($channel:expr, $future:expr) => {{
        let result: lapin::Result<_> = match $future.await {
            Ok(value) => Ok(value),
            Err(error) => match $channel.wait_for_recovery(error).await {
                Ok(()) => $future.await,
                Err(error) => Err(error),
            },
        };
        result
    }};
}

/// Lapin-backed RabbitMQ connection used by a message bus.
///
/// Connections enable Lapin's automatic recovery. Exchanges, queues, bindings,
/// and consumers are declared by registered event subscriptions and replayed
/// by Lapin after a recoverable connection failure. The connection is shared by
/// all bus workers; create one instance and pass it to the builder.
pub struct RabbitMQ {
    connection: Connection,
}

impl RabbitMQ {
    /// Opens a RabbitMQ connection and enables Lapin automatic recovery.
    ///
    /// The initial connection attempt times out after 15 seconds. A later
    /// recoverable disconnect is handled by Lapin's Reconnect and topology
    /// recovery mechanism.
    pub async fn new(connection_string: impl AsRef<str>) -> Result<Self, EventityError> {
        let connection = Self::connect(connection_string.as_ref()).await?;
        Ok(Self { connection })
    }

    async fn connect(uri: &str) -> Result<Connection, EventityError> {
        Ok(tokio::time::timeout(
            std::time::Duration::from_secs(15),
            Connection::connect(uri, ConnectionProperties::default().enable_auto_recover()),
        )
        .await
        .map_err(|_| EventityError::ConnectionTimeout)??)
    }

    async fn create_channel(&self) -> Result<Channel, EventityError> {
        Ok(self.connection.create_channel().await?)
    }
    async fn declare_exchange(&self, name: &ShortString) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.exchange_declare(
                name.clone(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    async fn declare_queue(
        &self,
        name: &ShortString,
        dead_letter_exchange: &ShortString,
    ) -> Result<Queue, EventityError> {
        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dead_letter_exchange.to_string().into()),
        );
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(name.to_string().into()),
        );
        let channel = self.create_channel().await?;
        Ok(recover_channel_call!(
            channel,
            channel.queue_declare(
                name.clone(),
                QueueDeclareOptions::durable(),
                arguments.clone(),
            )
        )?)
    }
    async fn declare_dead_letter_topology(
        &self,
        exchange: ShortString,
        queue_name: ShortString,
    ) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.exchange_declare(
                exchange.clone(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
        )?;
        let dead_queue: ShortString = format!("{queue_name}.dead").into();
        let queue = recover_channel_call!(
            channel,
            channel.queue_declare(
                dead_queue.clone(),
                QueueDeclareOptions::durable(),
                FieldTable::default(),
            )
        )?;
        recover_channel_call!(
            channel,
            channel.queue_bind(
                queue.name().clone(),
                exchange.clone(),
                queue_name.clone(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    async fn declare_consumer(
        &self,
        queue: ShortString,
        prefetch: u16,
    ) -> Result<(Consumer, Channel), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.basic_qos(prefetch, BasicQosOptions::default())
        )?;
        let consumer = recover_channel_call!(
            channel,
            channel.basic_consume(
                queue.clone(),
                "eventity_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok((consumer, channel))
    }
    async fn bind_queue(&self, queue: Queue, exchange: ShortString) -> Result<(), EventityError> {
        let channel = self.create_channel().await?;
        recover_channel_call!(
            channel,
            channel.queue_bind(
                queue.name().clone(),
                exchange.clone(),
                "".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        )?;
        Ok(())
    }
    pub(crate) async fn publish(&self, message: Message) -> Result<(), EventityError> {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let channel = self.create_channel().await?;
            recover_channel_call!(
                channel,
                channel.exchange_declare(
                    message.exchange_name.clone(),
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default(),
                )
            )?;
            recover_channel_call!(
                channel,
                channel.confirm_select(ConfirmSelectOptions::default())
            )?;
            let confirmation = recover_channel_call!(
                channel,
                channel.basic_publish(
                    message.exchange_name.clone(),
                    "".into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    &serde_json::to_vec(&message.payload)?,
                    BasicProperties::default()
                        .with_message_id(message.event_id.to_string().into())
                        .with_delivery_mode(2),
                )
            )?;
            let confirmation = match confirmation.await {
                Ok(confirmation) => confirmation,
                Err(error) => {
                    channel.wait_for_recovery(error).await?;
                    return Err(EventityError::PublishNotConfirmed);
                }
            };
            match confirmation {
                lapin::Confirmation::Ack(None) => Ok(()),
                lapin::Confirmation::Ack(Some(_)) => Err(EventityError::UnroutableMessage),
                lapin::Confirmation::Nack(_) => Err(EventityError::PublishRejected),
                lapin::Confirmation::NotRequested => Err(EventityError::PublishNotConfirmed),
            }
        })
        .await
        .map_err(|_| EventityError::PublishTimeout)?
    }
}

pub(crate) struct Message {
    pub(crate) event_id: uuid::Uuid,
    pub(crate) exchange_name: ShortString,
    pub(crate) payload: Value,
}

/// Typestate marker used until a RabbitMQ connection is attached to the builder.
pub struct NoBroker;
/// Typestate marker used until a PostgreSQL store is attached to the builder.
pub struct NoStore;

/// Configures a message bus before starting its background workers.
///
/// `broker` and `store` change the builder's type state. Consequently,
/// [`MessageBusBuilder::build`] is available only after both have been set.
/// Register request and event handlers before building; a duplicate request
/// type or event queue reports as a configuration error by `build`.
pub struct MessageBusBuilder<B = NoBroker, S = NoStore> {
    broker: B,
    store: S,
    handlers: HandlerRegistry,
    subscriptions: Vec<Box<dyn Subscription>>,
    event_queues: HashSet<&'static str>,
    configuration_error: Option<EventityError>,
}

impl MessageBusBuilder<NoBroker, NoStore> {
    /// Creates an empty builder. Both dependencies and handlers are added next.
    pub fn new() -> Self {
        Self {
            broker: NoBroker,
            store: NoStore,
            handlers: HandlerRegistry::default(),
            subscriptions: Vec::new(),
            event_queues: HashSet::new(),
            configuration_error: None,
        }
    }
}
impl Default for MessageBusBuilder<NoBroker, NoStore> {
    fn default() -> Self {
        Self::new()
    }
}
impl<S> MessageBusBuilder<NoBroker, S> {
    /// Supplies the RabbitMQ connection and advances the builder's type state.
    pub fn broker(self, broker: Arc<RabbitMQ>) -> MessageBusBuilder<Arc<RabbitMQ>, S> {
        MessageBusBuilder {
            broker,
            store: self.store,
            handlers: self.handlers,
            subscriptions: self.subscriptions,
            event_queues: self.event_queues,
            configuration_error: self.configuration_error,
        }
    }
}
impl<B> MessageBusBuilder<B, NoStore> {
    /// Supplies the PostgreSQL store and advances the builder's type state.
    pub fn store(self, store: Arc<EventityPg>) -> MessageBusBuilder<B, Arc<EventityPg>> {
        MessageBusBuilder {
            broker: self.broker,
            store,
            handlers: self.handlers,
            subscriptions: self.subscriptions,
            event_queues: self.event_queues,
            configuration_error: self.configuration_error,
        }
    }
}
impl<B, S> MessageBusBuilder<B, S> {
    /// Registers the handler for request type `R`.
    ///
    /// Registration happens before the bus starts. Only one handler may be
    /// registered for a request type; duplicates make [`Self::build`] return an
    /// error.
    pub fn request_handler<R: Request, H: RequestHandler<R>>(mut self, handler: H) -> Self {
        if !self.handlers.register::<R, H>(handler) {
            self.configuration_error = Some(EventityError::DuplicateRequestHandler(
                std::any::type_name::<R>(),
            ));
        }
        self
    }
    /// Registers an event handler with a concurrency/prefetch of one.
    ///
    /// The queue is created as durable and configured with a dead-letter queue.
    /// Each queue can be registered only once on a bus.
    pub fn event_handler<E: IntegrationEvent, H: EventHandler<E>>(self, handler: H) -> Self {
        self.event_handler_with_concurrency::<E, H>(handler, std::num::NonZeroU16::MIN)
    }
    /// Registers an event handler with the requested concurrent delivery limit.
    ///
    /// The non-zero concurrency value is also used as the RabbitMQ channel's
    /// prefetch count, so the broker does not send more unacknowledged messages
    /// than this worker can process concurrently.
    pub fn event_handler_with_concurrency<E: IntegrationEvent, H: EventHandler<E>>(
        mut self,
        handler: H,
        concurrency: std::num::NonZeroU16,
    ) -> Self {
        if !self.event_queues.insert(E::queue()) {
            self.configuration_error = Some(EventityError::DuplicateEventQueue(E::queue()));
            return self;
        }
        self.subscriptions.push(Box::new(EventSubscription::<E, H> {
            handler: Arc::new(handler),
            concurrency,
            _event: std::marker::PhantomData,
        }));
        self
    }
}
impl MessageBusBuilder<Arc<RabbitMQ>, Arc<EventityPg>> {
    /// Declares event topology, starts consumers and the outbox publisher, and
    /// returns a running bus.
    ///
    /// This method exists only when both RabbitMQ and PostgreSQL have been
    /// supplied. All handlers must be registered before this call. It returns
    /// an error if a handler is duplicated or the initial topology setup fails.
    pub async fn build(self) -> Result<RunningBus, EventityError> {
        let MessageBusBuilder {
            broker,
            store,
            handlers,
            subscriptions,
            configuration_error,
            ..
        } = self;
        if let Some(error) = configuration_error {
            return Err(error);
        }
        let cancel = CancellationToken::new();
        let mut workers: Vec<JoinHandle<Result<(), EventityError>>> = Vec::new();
        for subscription in subscriptions {
            match subscription
                .start(broker.clone(), store.clone(), cancel.clone())
                .await
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    cancel.cancel();
                    for worker in workers {
                        let _ = worker.await;
                    }
                    return Err(error);
                }
            }
        }
        let outbox = store
            .start_outbox_worker_with_cancel(broker.clone(), cancel.clone())
            .await?;
        workers.push(outbox);
        Ok(RunningBus {
            bus: MessageBus { store, handlers },
            cancel,
            workers,
        })
    }
}

#[async_trait]
trait Subscription: Send + Sync {
    async fn start(
        &self,
        broker: Arc<RabbitMQ>,
        store: Arc<EventityPg>,
        cancel: CancellationToken,
    ) -> Result<JoinHandle<Result<(), EventityError>>, EventityError>;
}
struct EventSubscription<E, H> {
    handler: Arc<H>,
    concurrency: std::num::NonZeroU16,
    _event: std::marker::PhantomData<fn(E)>,
}
#[async_trait]
impl<E, H> Subscription for EventSubscription<E, H>
where
    E: IntegrationEvent,
    H: EventHandler<E>,
{
    async fn start(
        &self,
        broker: Arc<RabbitMQ>,
        store: Arc<EventityPg>,
        cancel: CancellationToken,
    ) -> Result<JoinHandle<Result<(), EventityError>>, EventityError> {
        let concurrency = self.concurrency.get();
        let consumer = prepare_consumer_with_timeout::<E>(&broker, concurrency).await?;
        let handler = self.handler.clone();
        Ok(tokio::spawn(async move {
            let mut consumer = Some(consumer);
            let mut retry_delay = std::time::Duration::from_millis(500);
            loop {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let Some((current_consumer, channel)) = consumer.take() else {
                    return Err(EventityError::Worker("consumer was not initialized".into()));
                };
                match run_consumer::<E, H>(
                    current_consumer,
                    channel,
                    store.clone(),
                    handler.clone(),
                    cancel.clone(),
                    usize::from(concurrency),
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        tracing::error!(%error, "event consumer stopped; reconnecting");
                        retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                    }
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(retry_delay) => {}
                }
                match prepare_consumer_with_timeout::<E>(&broker, concurrency).await {
                    Ok(next) => {
                        consumer = Some(next);
                    }
                    Err(error) => {
                        tracing::error!(%error, ?retry_delay, "could not reconnect event consumer");
                        retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                        continue;
                    }
                }
            }
        }))
    }
}

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

async fn prepare_consumer_with_timeout<E: IntegrationEvent>(
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

async fn run_consumer<E, H>(
    mut consumer: Consumer,
    channel: Channel,
    store: Arc<EventityPg>,
    handler: Arc<H>,
    cancel: CancellationToken,
    limit: usize,
) -> Result<(), EventityError>
where
    E: IntegrationEvent,
    H: EventHandler<E>,
{
    let mut tasks = JoinSet::new();
    let concurrency = Arc::new(tokio::sync::Semaphore::new(limit));
    // A semaphore token is consumed for each delivery and replenished once per
    // second. This caps starts at `limit` per second without blocking the
    // consumer on the number of JoinSet entries.
    const MAX_MESSAGES_PER_SECOND: usize = 160;
    let rate = Arc::new(tokio::sync::Semaphore::new(MAX_MESSAGES_PER_SECOND));
    let mut rate_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    rate_interval.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = rate_interval.tick() => {
                // Discard unused tokens so idle time cannot accumulate into a
                // burst larger than the per-second limit.
                let available = rate.available_permits();
                if available > 0 {
                    if let Ok(permit) = rate.clone().try_acquire_many_owned(available as u32) {
                        permit.forget();
                    }
                }
                rate.add_permits(MAX_MESSAGES_PER_SECOND);
            }
            delivery = consumer.next() => match delivery {
                Some(Ok(delivery)) => {
                    let (store, handler, cancel, rate, concurrency) =
                        (store.clone(), handler.clone(), cancel.clone(), rate.clone(), concurrency.clone());
                    tasks.spawn(async move {
                        let rate_permit = tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            permit = rate.acquire_owned() => permit.map_err(|_| EventityError::Worker("rate limiter closed".into()))?,
                        };
                        rate_permit.forget();
                        let concurrency_permit = tokio::select! {
                            _ = cancel.cancelled() => return Ok(()),
                            permit = concurrency.acquire_owned() => permit.map_err(|_| EventityError::Worker("concurrency limiter closed".into()))?,
                        };
                        let _concurrency_permit = concurrency_permit;
                        match process_delivery::<E, H>(&store, &handler, &delivery).await {
                            Ok(()) => delivery.ack(BasicAckOptions::default()).await.map(|_| ()).map_err(EventityError::from),
                            Err(error) => {
                                delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await?;
                                tracing::error!(%error, "event processing failed; message moved to the dead-letter queue");
                                Ok(())
                            }
                        }
                    });
                }
                Some(Err(error)) => match channel.wait_for_recovery(error).await {
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
    while let Some(result) = tasks.join_next().await {
        result.map_err(|error| EventityError::Join(error.to_string()))??;
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
    let event: E = serde_json::from_value(payload.clone())?;
    let event_id = match delivery.properties.message_id() {
        Some(id) => uuid::Uuid::from_str(id.as_str())
            .map_err(|error| EventityError::InvalidMessageId(error.to_string()))?,
        None => uuid::Uuid::now_v7(),
    };
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
        .handle(event)
        .await
        .map_err(|error| EventityError::Handler(format!("{error:?}")))?;
    let records = outgoing.into_records()?;
    store.create_outbox_records(&mut tx, records).await?;
    tx.commit().await?;
    Ok(())
}

/// Request invocation handle returned by a running bus.
///
/// Get it through [`MessageBus::builder`] and [`MessageBusBuilder::build`].
/// The `RunningBus` dereferences to this type, so callers can invoke requests
/// directly on the running handle.
pub struct MessageBus {
    store: Arc<EventityPg>,
    handlers: HandlerRegistry,
}
impl MessageBus {
    /// Runs the registered handler for `request` and returns its output.
    ///
    /// Handler database changes and outgoing outbox records are committed in a
    /// single PostgreSQL transaction. If the handler or database operation
    /// fails, the transaction rolls back. An unregistered request returns
    /// [`InvokeError::HandlerNotRegistered`].
    pub async fn invoke<R: Request>(&self, request: R) -> Result<R::Output, InvokeError<R::Error>> {
        let handler = self
            .handlers
            .get_handler::<R>()
            .ok_or(InvokeError::HandlerNotRegistered)?;
        let mut tx = self
            .store
            .get_transaction()
            .await
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        let (output, outgoing) = handler
            .handle(request, &mut tx)
            .await
            .map_err(InvokeError::Handler)?;
        let records = outgoing
            .into_records()
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        self.store
            .create_outbox_records(&mut tx, records)
            .await
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        tx.commit()
            .await
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        Ok(output)
    }
    /// Starts a builder for a new message bus.
    pub fn builder() -> MessageBusBuilder {
        MessageBusBuilder::new()
    }
}
/// Error returned by [`MessageBus::invoke`].
///
/// The generic parameter preserves the request handler's own error type.
#[derive(Debug, thiserror::Error)]
pub enum InvokeError<E> {
    /// No handler was registered for the invoked request type.
    #[error("request handler is not registered")]
    HandlerNotRegistered,
    /// The registered application handler returned an error.
    #[error("request handler failed: {0:?}")]
    Handler(E),
    /// Eventity failed while starting or committing the database transaction.
    #[error("message bus failed: {0}")]
    Bus(EventityError),
}

/// A started message bus with request invocation and managed background workers.
///
/// Dropping this handle requests cancellation. Prefer [`RunningBus::shutdown`]
/// when the application needs to await worker termination and receive worker
/// errors. Use [`RunningBus::wait`] when the bus should run until a worker exits.
pub struct RunningBus {
    bus: MessageBus,
    cancel: CancellationToken,
    workers: Vec<JoinHandle<Result<(), EventityError>>>,
}
impl std::ops::Deref for RunningBus {
    type Target = MessageBus;
    fn deref(&self) -> &MessageBus {
        &self.bus
    }
}
impl RunningBus {
    /// Waits for a background worker to finish, cancels the remaining workers,
    /// then joins them all.
    ///
    /// Since consumers and the outbox publisher normally run continuously, this
    /// typically waits until a worker fails or exits.
    pub async fn wait(mut self) -> Result<(), EventityError> {
        let mut tasks = JoinSet::new();
        for worker in self.workers.drain(..) {
            tasks.spawn(worker);
        }
        let result = match tasks.join_next().await {
            Some(Ok(Ok(result))) => result,
            Some(Ok(Err(error))) | Some(Err(error)) => Err(EventityError::Join(error.to_string())),
            None => Ok(()),
        };
        self.cancel.cancel();
        let mut first_error = result.err();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    first_error.get_or_insert(error);
                }
                Ok(Err(error)) | Err(error) => {
                    first_error.get_or_insert_with(|| EventityError::Join(error.to_string()));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
    /// Gracefully requests cancellation and waits for every background worker.
    pub async fn shutdown(mut self) -> Result<(), EventityError> {
        self.cancel.cancel();
        join_workers(&mut self.workers).await
    }
}
async fn join_workers(
    workers: &mut Vec<JoinHandle<Result<(), EventityError>>>,
) -> Result<(), EventityError> {
    let mut first_error = None;
    for worker in workers.drain(..) {
        let result = worker
            .await
            .map_err(|error| EventityError::Join(error.to_string()))
            .and_then(|result| result);
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

impl Drop for RunningBus {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
