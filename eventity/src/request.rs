use crate::OutgoingMessages;
use async_trait::async_trait;
use sqlx::PgTransaction;

/// A command or query that can be handled by [`RequestHandler`].
///
/// Use `#[derive(eventity::Request)]` with `#[request(output = ..., error = ...)]`
/// to implement the associated types. Request values must be owned and safe to
/// move between Tokio tasks.
pub trait Request: Send + Sync + 'static {
    /// Value returned to the caller after the handler transaction commits.
    type Output: Send + 'static;
    /// Application error returned by the registered handler.
    type Error: Send + 'static;
}

/// Successful handler output together with events to insert into the outbox.
///
/// Returning an error rolls back the handler's PostgreSQL transaction and does
/// not enqueue any outgoing events.
pub type HandlerResult<Output, Error> = Result<(Output, OutgoingMessages), Error>;

/// Handles a request inside the transaction used by [`crate::MessageBus::invoke`].
///
/// The handler should use the supplied transaction for database work that must
/// commit atomically with the outgoing events returned in [`HandlerResult`].
#[async_trait]
pub trait RequestHandler<R: Request>: Send + Sync + 'static {
    /// Processes a request and returns its response and zero or more events.
    async fn handle(
        &self,
        request: R,
        tx: &mut PgTransaction<'_>,
    ) -> HandlerResult<R::Output, R::Error>;
}
