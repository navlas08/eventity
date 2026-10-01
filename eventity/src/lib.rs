//! Transactional request handling and event delivery for Tokio applications.
//!
//! Eventity combines PostgreSQL transactions with an outbox/inbox and RabbitMQ:
//! request handlers commit their database changes and outgoing events together;
//! background workers publish the outbox and consume subscribed event queues.
//!
//! # Setup
//!
//! Create the RabbitMQ connection and PostgreSQL pool, run [`EventityPg::migrate`],
//! then register all handlers before calling [`MessageBusBuilder::build`]. The
//! builder's type state makes it impossible to call `build` until both
//! dependencies have been supplied.
//!
//! ```rust,ignore
//! let running = MessageBus::builder()
//!     .broker(Arc::new(RabbitMQ::new(amqp_uri).await?))
//!     .store(Arc::new(EventityPg::new(pool)))
//!     .request_handler(CreateOrderHandler)
//!     .event_handler(OrderCreatedHandler)
//!     .build()
//!     .await?;
//!
//! let order_id = running.invoke(CreateOrder { /* ... */ }).await?;
//! running.shutdown().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! `invoke` runs the request handler in a PostgreSQL transaction and inserts
//! its outgoing events into the outbox before committing. Outbox publication
//! happens asynchronously. Event handlers run concurrently up to their
//! configured prefetch limit; inbox deduplication and their outgoing events are
//! committed transactionally. A failed event handler is rejected to the
//! queue's dead-letter queue.

#![warn(missing_docs)]

mod bus;
mod errors;

pub use errors::EventityError;
mod event;
mod postgres;
mod registry;
mod request;

pub use eventity_macro::Event;
pub use eventity_macro::Request;

pub use event::EHandlerResult;
pub use event::EventHandler;
pub use event::IntegrationEvent;

pub use request::HandlerResult;
pub use request::Request;
pub use request::RequestHandler;

pub use bus::RabbitMQ;
pub use bus::{ConsumerOptions, InvokeError, MessageBus, MessageBusBuilder, RunningBus};

pub use event::OutgoingMessages;

pub use postgres::{EventityPg, OutboxOptions};

#[cfg(test)]
mod test_support;
