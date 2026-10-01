use crate::consumer::{prepare_consumer_with_timeout, run_consumer};
use crate::errors::EventityError;
use crate::registry::HandlerRegistry;
use crate::{
    EventityPg, IntegrationEvent, RabbitMQ, Request, RequestHandler, TransactionalEventHandler,
};
use async_trait::async_trait;
use std::{collections::HashSet, sync::Arc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

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

/// Bounds transaction grouping and the optional time spent filling a batch.
#[derive(Clone, Copy, Debug)]
pub struct BatchOptions {
    /// Maximum deliveries sharing one transaction. Defaults to one.
    pub max_messages: std::num::NonZeroU16,
    /// Maximum time from the first delivery to dispatching a partial batch.
    /// Zero processes ready deliveries immediately. A short wait can amortize
    /// inbox writes when broker deliveries arrive in small bursts.
    pub max_wait: std::time::Duration,
}
impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_messages: std::num::NonZeroU16::MIN,
            max_wait: std::time::Duration::ZERO,
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
    pub fn event_handler<E: IntegrationEvent, H: TransactionalEventHandler<E>>(
        self,
        handler: H,
    ) -> Self {
        self.event_handler_with_concurrency::<E, H>(handler, std::num::NonZeroU16::MIN)
    }
    /// Registers an event handler with the requested concurrent delivery limit.
    ///
    /// The non-zero concurrency value is also used as the RabbitMQ channel's
    /// prefetch count, so the broker does not send more unacknowledged messages
    /// than this worker can process concurrently.
    pub fn event_handler_with_concurrency<E: IntegrationEvent, H: TransactionalEventHandler<E>>(
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
    pub fn event_handler_with_options<E: IntegrationEvent, H: TransactionalEventHandler<E>>(
        self,
        handler: H,
        options: ConsumerOptions,
    ) -> Self {
        self.event_handler_with_batching::<E, H>(handler, options, std::num::NonZeroU16::MIN)
    }

    /// Registers a handler that can share a transaction across ready deliveries.
    ///
    /// Handlers run sequentially within each batch; `options.concurrency` bounds
    /// concurrent batches. There is no delay waiting for a batch to fill.
    /// Inbox records and all application/outbox writes commit together. If a
    /// handler fails, the batch rolls back and deliveries are retried individually
    /// to isolate the failure. External effects must tolerate this replay.
    /// Acknowledgements are sent only after commit. Use batch size one for an
    /// independent transaction per delivery (the default).
    pub fn event_handler_with_batching<E: IntegrationEvent, H: TransactionalEventHandler<E>>(
        self,
        handler: H,
        options: ConsumerOptions,
        batch_size: std::num::NonZeroU16,
    ) -> Self {
        self.event_handler_with_batch_options::<E, H>(
            handler,
            options,
            BatchOptions {
                max_messages: batch_size,
                ..Default::default()
            },
        )
    }

    /// Registers a handler with bounded transaction batches and an optional
    /// collection deadline. Transaction and replay semantics are the same as
    /// [`Self::event_handler_with_batching`]. Waiting applies only to partial
    /// batches, measured from the first delivery; full batches run immediately.
    pub fn event_handler_with_batch_options<
        E: IntegrationEvent,
        H: TransactionalEventHandler<E>,
    >(
        mut self,
        handler: H,
        options: ConsumerOptions,
        batching: BatchOptions,
    ) -> Self {
        if !self.event_queues.insert(E::queue()) {
            self.configuration_error = Some(EventityError::DuplicateEventQueue(E::queue()));
            return self;
        }
        self.subscriptions.push(Box::new(EventSubscription::<E, H> {
            handler: Arc::new(handler),
            options,
            batching,
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
    batching: BatchOptions,
    _event: std::marker::PhantomData<fn(E)>,
}
#[async_trait]
impl<E, H> Subscription for EventSubscription<E, H>
where
    E: IntegrationEvent,
    H: TransactionalEventHandler<E>,
{
    async fn start(
        &self,
        broker: Arc<RabbitMQ>,
        store: Arc<EventityPg>,
        cancel: CancellationToken,
    ) -> Result<JoinHandle<Result<(), EventityError>>, EventityError> {
        let batching = self.batching;
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
                    batching,
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
