use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Events to be written to the transactional outbox.
///
/// Build a list with [`OutgoingMessages::add`] and chain [`OutgoingMessages::and`],
/// or return [`OutgoingMessages::none`] when a handler has no events to emit.
#[derive(Default)]
pub struct OutgoingMessages {
    events: Vec<Box<dyn ErasedIntegrationEvent>>,
}

impl OutgoingMessages {
    /// Starts an outgoing event list with one event.
    pub fn add<IE: IntegrationEvent>(event: IE) -> Self {
        OutgoingMessages {
            events: vec![Box::new(event)],
        }
    }

    /// Appends an event to this list.
    pub fn and<IE: IntegrationEvent>(mut self, event: IE) -> Self {
        self.events.push(Box::new(event));
        self
    }

    /// Creates an empty outgoing event list.
    pub fn none() -> Self {
        OutgoingMessages::default()
    }

    pub(crate) fn into_records(
        self,
    ) -> Result<Vec<crate::postgres::OutboxRecord>, serde_json::Error> {
        self.events
            .into_iter()
            .map(|event| {
                let payload = event.erased_to_value()?;
                Ok(crate::postgres::OutboxRecord {
                    event_id: sqlx::types::uuid::Uuid::now_v7(),
                    aggregate_type: event.erased_aggregate().into(),
                    aggregate_id: event.erased_aggregate_id(),
                    event_type: event.erased_queue().into(),
                    exchange: event.erased_exchange().into(),
                    payload,
                })
            })
            .collect()
    }
}

/// An event type that can be serialized to the outbox and consumed from RabbitMQ.
///
/// Implement this trait directly or derive it with `#[derive(eventity::Event)]`.
/// The derive expects an `id` or `aggregate_id` field (or a field marked with
/// `#[aggregate_id]`) and an `#[event(error = YourError)]` attribute. Queue names
/// and exchange names determine the RabbitMQ topology; keep them stable across
/// deployments. The aggregate identifier is stored with the outbox record to
/// preserve per-aggregate publishing order.
pub trait IntegrationEvent: Send + Sync + DeserializeOwned + Serialize + 'static {
    /// Error type returned by handlers for this event.
    type Error: Send + std::fmt::Debug + std::fmt::Display + 'static;

    /// Durable RabbitMQ queue name for this event subscription.
    fn queue() -> &'static str;
    /// Logical aggregate type used to order outbox publication.
    fn aggregate() -> &'static str;
    /// Identifier of the aggregate instance that produced this event.
    fn aggregate_id(&self) -> String;
    /// RabbitMQ exchange to which this event is published.
    fn exchange() -> &'static str;
}

trait ErasedIntegrationEvent: Send + Sync + 'static {
    fn erased_queue(&self) -> &'static str;
    fn erased_aggregate(&self) -> &'static str;
    fn erased_aggregate_id(&self) -> String;
    fn erased_exchange(&self) -> &'static str;

    // Serialize an event once into the JSON value stored in the outbox.
    fn erased_to_value(&self) -> Result<Value, serde_json::Error>;
}

impl<T: IntegrationEvent> ErasedIntegrationEvent for T {
    fn erased_queue(&self) -> &'static str {
        T::queue()
    }

    fn erased_aggregate(&self) -> &'static str {
        T::aggregate()
    }

    fn erased_aggregate_id(&self) -> String {
        T::aggregate_id(self)
    }

    fn erased_exchange(&self) -> &'static str {
        T::exchange()
    }

    fn erased_to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

/// Result returned by an [`EventHandler`].
///
/// Returning outgoing events commits them to the outbox in the same transaction
/// as inbox deduplication. Returning an error rolls back that transaction and
/// causes the delivery to be dead-lettered.
pub type EHandlerResult<Error> = Result<OutgoingMessages, Error>;

/// Processes deliveries from the queue belonging to an [`IntegrationEvent`].
///
/// Event handlers should be idempotent with respect to external side effects:
/// Eventity transactionally deduplicates database processing but cannot roll
/// back network calls or other effects outside PostgreSQL.
#[async_trait]
pub trait EventHandler<IE: IntegrationEvent>: Send + Sync + 'static {
    /// Handles one decoded event and returns any events to emit.
    async fn handle(&self, event: IE) -> EHandlerResult<IE::Error>;

    /// Handles an event using the inbox/outbox transaction. Override this method
    /// to commit application database changes atomically with deduplication.
    /// The default delegates to [`Self::handle`] for existing handlers.
    async fn handle_transactional(
        &self,
        event: IE,
        _tx: &mut sqlx::PgTransaction<'_>,
    ) -> EHandlerResult<IE::Error> {
        self.handle(event).await
    }
}
