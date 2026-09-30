use lapin::Error as LapinError;
use thiserror::Error;

/// Errors returned while configuring or running Eventity infrastructure.
#[derive(Debug, Error)]
pub enum EventityError {
    /// Lapin reported a RabbitMQ connection, channel, or protocol error.
    #[error("rabbitmq error")]
    Rabbitmq(#[from] LapinError),
    /// SQLx reported a PostgreSQL error.
    #[error("postgres error")]
    Postgres(#[from] sqlx::Error),
    /// An event could not be serialized or deserialized as JSON.
    #[error("JSON processing failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A delivery had a non-UUID RabbitMQ `message_id` property.
    #[error("RabbitMQ message_id is not a valid UUID: {0}")]
    InvalidMessageId(String),
    /// RabbitMQ did not confirm a publish before the publish timeout.
    #[error("timed out waiting for RabbitMQ publisher confirmation")]
    PublishTimeout,
    /// The initial RabbitMQ connection attempt exceeded its timeout.
    #[error("timed out connecting to RabbitMQ")]
    ConnectionTimeout,
    /// Initial event topology setup exceeded its timeout.
    #[error("timed out declaring RabbitMQ consumer topology")]
    TopologyTimeout,
    /// The mandatory RabbitMQ publish had no bound destination queue.
    #[error("RabbitMQ did not route the published event to any queue")]
    UnroutableMessage,
    /// RabbitMQ negatively acknowledged a publisher confirm.
    #[error("RabbitMQ negatively acknowledged the published event")]
    PublishRejected,
    /// Publisher confirmation was unavailable or failed during recovery.
    #[error("RabbitMQ publisher confirms were not enabled")]
    PublishNotConfirmed,
    /// An event handler returned an application error.
    #[error("event handler failed: {0}")]
    Handler(String),
    /// A Tokio background task failed to join.
    #[error("background task failed: {0}")]
    Join(String),
    /// A background worker stopped unexpectedly.
    #[error("background worker failed: {0}")]
    Worker(String),
    /// More than one handler was registered for the same request type.
    #[error("request handler registered more than once for {0}")]
    DuplicateRequestHandler(&'static str),
    /// More than one event handler was registered for the same queue.
    #[error("event queue registered more than once: {0}")]
    DuplicateEventQueue(&'static str),
}
