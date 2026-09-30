use crate::bus::Message;
use lapin::types::ShortString;
use serde_json::Value;
use sqlx::types::uuid;
use sqlx::{FromRow, PgTransaction, Postgres, QueryBuilder};
pub(crate) use sqlx::{PgPool, query};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn restore_outbox_payload(payload: Value) -> Value {
    let Value::Array(values) = payload else {
        return payload;
    };

    let Some(bytes) = values
        .iter()
        .map(|value| value.as_u64().and_then(|value| u8::try_from(value).ok()))
        .collect::<Option<Vec<_>>>()
    else {
        return Value::Array(values);
    };

    serde_json::from_slice(&bytes).unwrap_or(Value::Array(values))
}

pub(crate) struct OutboxRecord {
    pub event_id: uuid::Uuid,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub exchange: String,
    pub payload: Value,
}

#[derive(FromRow)]
pub(crate) struct InboxRecord {
    pub(crate) id: uuid::Uuid,
    pub(crate) consumer: String,
    pub(crate) event_type: String,
    pub(crate) payload: Value,
}

#[derive(FromRow)]
struct PendingOutboxRecord {
    id: i64,
    event_id: uuid::Uuid,
    exchange: String,
    payload: Value,
    attempt_count: i32,
}

/// PostgreSQL persistence for request transactions, inbox deduplication, and outbox delivery.
///
/// Construct this with the application's SQLx `PgPool`, run
/// [`EventityPg::migrate`], and share it with the bus and request handlers.
pub struct EventityPg {
    pool: PgPool,
}

impl EventityPg {
    /// Wraps an existing SQLx PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates the final `outbox` and `inbox` schema under a PostgreSQL advisory lock.
    ///
    /// This is an initialization step, not an in-place upgrade mechanism:
    /// `CREATE TABLE IF NOT EXISTS` does not modify existing tables. Drop legacy
    /// Eventity tables before calling this method when adopting the current
    /// schema. It is safe for multiple application instances to call during
    /// startup; the advisory lock serializes the DDL.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(0x4556_5449_i64)
            .execute(&mut *tx)
            .await?;
        sqlx::raw_sql(include_str!("../migrations/outbox.sql"))
            .execute(&mut *tx)
            .await?;
        sqlx::raw_sql(include_str!("../migrations/inbox.sql"))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(())
    }

    /// Deletes completed outbox and inbox records older than `cutoff`.
    ///
    /// Returns `(outbox_rows_deleted, inbox_rows_deleted)`. Choose a cutoff long
    /// enough that no old broker delivery can still be replayed: deleting inbox
    /// rows allows a redelivered event with the same ID to be processed again.
    /// Pending outbox rows are never removed by this method.
    pub async fn purge_processed_before(
        &self,
        cutoff: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
    ) -> Result<(u64, u64), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let outbox = query("DELETE FROM outbox WHERE processed_at < $1")
            .bind(cutoff)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let inbox = query("DELETE FROM inbox WHERE processed_at < $1")
            .bind(cutoff)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok((outbox, inbox))
    }

    // Gets a transaction from the connection pool
    pub(crate) async fn get_transaction(&self) -> Result<PgTransaction<'_>, sqlx::Error> {
        self.pool.begin().await
    }

    /// Creates a new outbox record
    /// Returns the number of rows affected
    pub(crate) async fn create_outbox_records(
        &self,
        tx: &mut PgTransaction<'_>,
        records: impl IntoIterator<Item = OutboxRecord>,
    ) -> Result<u64, sqlx::Error> {
        let mut records = records.into_iter();
        let mut inserted = 0;
        loop {
            let batch: Vec<_> = records.by_ref().take(1_000).collect();
            if batch.is_empty() {
                return Ok(inserted);
            }
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, exchange) ",
            );
            query_builder.push_values(batch, |mut b, record| {
                b.push_bind(record.event_id)
                    .push_bind(record.aggregate_type)
                    .push_bind(record.aggregate_id)
                    .push_bind(record.event_type)
                    .push_bind(sqlx::types::Json(record.payload))
                    .push_bind(record.exchange);
            });
            inserted += query_builder
                .build()
                .execute(&mut **tx)
                .await?
                .rows_affected();
        }
    }

    /// Create a new inbox record
    /// Returns the number of rows affected
    pub(crate) async fn create_inbox_record(
        &self,
        tx: &mut PgTransaction<'_>,
        record: InboxRecord,
    ) -> Result<u64, sqlx::Error> {
        Ok(query(
            "INSERT INTO inbox (id, consumer, event_type, payload) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (consumer, id) DO NOTHING",
        )
        .bind(record.id)
        .bind(record.consumer)
        .bind(record.event_type)
        .bind(sqlx::types::Json(record.payload))
        .execute(&mut **tx)
        .await?
        .rows_affected())
    }

    pub(crate) async fn start_outbox_worker_with_cancel(
        self: &Arc<Self>,
        broker: Arc<crate::RabbitMQ>,
        cancel: CancellationToken,
    ) -> Result<
        tokio::task::JoinHandle<Result<(), crate::errors::EventityError>>,
        crate::errors::EventityError,
    > {
        let pg = Arc::clone(self);
        Ok(tokio::spawn(async move {
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(500));
            let mut retry_delay = std::time::Duration::from_millis(500);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = poll.tick() => tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        result = pg.process_pending_outbox(&broker) => match result {
                            Ok(()) => retry_delay = std::time::Duration::from_millis(500),
                            Err(error) => {
                                tracing::error!(%error, ?retry_delay, "outbox processing failed; retrying");
                                tokio::select! {
                                    _ = cancel.cancelled() => return Ok(()),
                                    _ = tokio::time::sleep(retry_delay) => {}
                                }
                                retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                            }
                        }
                    },
                }
            }
        }))
    }

    async fn process_pending_outbox(
        &self,
        broker: &crate::RabbitMQ,
    ) -> Result<(), crate::errors::EventityError> {
        loop {
            if !self.process_next_outbox(broker).await? {
                return Ok(());
            }
        }
    }

    async fn process_next_outbox(
        &self,
        broker: &crate::RabbitMQ,
    ) -> Result<bool, crate::errors::EventityError> {
        let mut tx = self.pool.begin().await?;
        let event = sqlx::query_as::<_, PendingOutboxRecord>(
            r#"
    SELECT o.id, o.event_id, o.exchange, o.payload, o.attempt_count
    FROM outbox AS o
    WHERE o.processed_at IS NULL
      AND o.next_attempt_at <= now()
      AND NOT EXISTS (
          SELECT 1 FROM outbox AS earlier
          WHERE earlier.aggregate_type = o.aggregate_type
            AND earlier.aggregate_id = o.aggregate_id
            AND earlier.id < o.id
            AND earlier.processed_at IS NULL
      )
    ORDER BY o.next_attempt_at, o.id
    LIMIT 1
    FOR UPDATE OF o SKIP LOCKED
    "#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(record) = event {
            let attempt_count = record.attempt_count.saturating_add(1);
            let message = Message {
                event_id: record.event_id,
                exchange_name: ShortString::from(record.exchange),
                payload: restore_outbox_payload(record.payload),
            };

            if let Err(error) = broker.publish(message).await {
                let retry_seconds = 1_i64 << attempt_count.clamp(1, 8);
                sqlx::query(
                    "UPDATE outbox SET attempt_count = $1, next_attempt_at = now() + ($2 * interval '1 second'), last_error = $3 WHERE id = $4",
                )
                .bind(attempt_count)
                .bind(retry_seconds)
                .bind(error.to_string())
                .bind(record.id)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                tracing::error!(outbox_id = record.id, attempt_count, retry_seconds, %error, "outbox publish failed; scheduled retry");
                return Ok(true);
            }

            sqlx::query("UPDATE outbox SET processed_at = now(), last_error = NULL WHERE id = $1")
                .bind(record.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(true);
        }
        tx.rollback().await?;
        Ok(false)
    }
}
