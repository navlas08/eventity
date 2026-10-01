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
/// by Lapin after a recoverable connection failure. Publishing uses a separate
/// connection so broker flow control cannot block consumer acknowledgements.
/// Create one instance and share it with the bus builder.
pub struct RabbitMQ {
    connection: Connection,
    publishing_connection: Connection,
    publisher: tokio::sync::Mutex<Option<Publisher>>,
}

struct Publisher {
    channel: Channel,
    exchanges: HashSet<ShortString>,
}

impl RabbitMQ {
    /// Opens consumer and publisher connections with Lapin automatic recovery.
    ///
    /// Each initial connection attempt times out after 15 seconds. A later
    /// recoverable disconnect is handled by Lapin's Reconnect and topology
    /// recovery mechanism.
    pub async fn new(connection_string: impl AsRef<str>) -> Result<Self, EventityError> {
        let connection = Self::connect(connection_string.as_ref()).await?;
        // Broker flow control on publishing must not block consumer acks.
        let publishing_connection = Self::connect(connection_string.as_ref()).await?;
        Ok(Self {
            connection,
            publishing_connection,
            publisher: tokio::sync::Mutex::new(None),
        })
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
            let channel = self.publisher_channel(&message.exchange_name).await?;
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

    // Serialize initialization only. Publishing and waiting for confirms must
    // happen outside this lock so that confirms can be pipelined.
    async fn publisher_channel(&self, exchange: &ShortString) -> Result<Channel, EventityError> {
        let mut publisher = self.publisher.lock().await;
        if publisher
            .as_ref()
            .is_some_and(|p| !p.channel.status().connected() && !p.channel.status().reconnecting())
        {
            *publisher = None;
        }
        if publisher.is_none() {
            let channel = self.publishing_connection.create_channel().await?;
            recover_channel_call!(
                channel,
                channel.confirm_select(ConfirmSelectOptions::default())
            )?;
            *publisher = Some(Publisher {
                channel,
                exchanges: HashSet::new(),
            });
        }
        let publisher = publisher.as_mut().expect("publisher initialized above");
        if !publisher.exchanges.contains(exchange) {
            recover_channel_call!(
                publisher.channel,
                publisher.channel.exchange_declare(
                    exchange.clone(),
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default()
                )
            )?;
            publisher.exchanges.insert(exchange.clone());
        }
        Ok(publisher.channel.clone())
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

/// Independent limits for handler execution and broker delivery buffering.
#[derive(Clone, Copy, Debug)]
pub struct ConsumerOptions {
    /// Maximum number of handler tasks executing concurrently.
    pub concurrency: std::num::NonZeroU16,
    /// Maximum unacknowledged broker deliveries. Values above concurrency
    /// buffer deliveries while handlers run and hide broker round-trip latency.
    pub prefetch: std::num::NonZeroU16,
}
impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            concurrency: std::num::NonZeroU16::MIN,
            prefetch: std::num::NonZeroU16::MIN,
        }
    }
}

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
        self,
        handler: H,
        concurrency: std::num::NonZeroU16,
    ) -> Self {
        self.event_handler_with_options::<E, H>(
            handler,
            ConsumerOptions {
                concurrency,
                prefetch: concurrency,
            },
        )
    }
    /// Registers a handler with independent execution and prefetch limits.
    /// Concurrent handlers may complete out of order; use concurrency one
    /// when processing order is required. Prefetch below concurrency limits
    /// the effective parallelism to the prefetch value.
    pub fn event_handler_with_options<E: IntegrationEvent, H: EventHandler<E>>(
        mut self,
        handler: H,
        options: ConsumerOptions,
    ) -> Self {
        if !self.event_queues.insert(E::queue()) {
            self.configuration_error = Some(EventityError::DuplicateEventQueue(E::queue()));
            return self;
        }
        self.subscriptions.push(Box::new(EventSubscription::<E, H> {
            handler: Arc::new(handler),
            options,
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
    options: ConsumerOptions,
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
        let concurrency = self.options.concurrency.get();
        let prefetch = self.options.prefetch.get();
        let consumer = prepare_consumer_with_timeout::<E>(&broker, prefetch).await?;
        let handler = self.handler.clone();
        Ok(tokio::spawn(async move {
            let mut consumer = Some(consumer);
            let mut retry_delay = std::time::Duration::from_millis(500);
            loop {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let (current_consumer, channel) = match consumer.take() {
                    Some(current) => current,
                    None => match tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        result = prepare_consumer_with_timeout::<E>(&broker, prefetch) => result,
                    } {
                        Ok(next) => next,
                        Err(error) => {
                            tracing::error!(%error, ?retry_delay, "could not reconnect event consumer");
                            tokio::select! {
                                _ = cancel.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(retry_delay) => {},
                            }
                            retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                            continue;
                        }
                    },
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
    consumer: Consumer,
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
    let result =
        run_consumer_deliveries::<E, H>(consumer, channel.clone(), store, handler, cancel, limit)
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
                    let (store, handler) = (store.clone(), handler.clone());
                    tasks.spawn(async move {
                        match process_delivery::<E, H>(&store, &handler, &delivery).await {
                            Ok(()) => delivery.ack(BasicAckOptions::default()).await.map(|_| ()).map_err(EventityError::from),
                            Err(error) => {
                                let requeue = matches!(error, EventityError::Postgres(_));
                                if requeue {
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                }
                                delivery.nack(BasicNackOptions { requeue, ..Default::default() }).await?;
                                tracing::error!(%error, requeue, "event processing failed");
                                Ok(())
                            }
                        }
                    });
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
        None => return Err(EventityError::MissingMessageId),
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
        let inserted = self
            .store
            .create_outbox_records(&mut tx, records)
            .await
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        tx.commit()
            .await
            .map_err(EventityError::from)
            .map_err(InvokeError::Bus)?;
        if inserted > 0 {
            self.store.notify_outbox();
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Resources;
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
