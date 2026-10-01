use futures_util::{StreamExt, stream::FuturesUnordered};
use lapin::types::ShortString;
use serde_json::Value;
use sqlx::types::uuid;
use sqlx::{FromRow, PgTransaction};
pub(crate) use sqlx::{PgPool, query};
use std::sync::Arc;
use std::{num::NonZeroU16, time::Duration};
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

// PostgreSQL already stores valid JSON. Forward its text representation without
// building and serializing a JSON DOM. Only legacy byte-array payloads need repair.
fn encoded_outbox_payload(payload: &str) -> Result<std::borrow::Cow<'_, [u8]>, serde_json::Error> {
    if payload.trim_start().starts_with('[') {
        let value = restore_outbox_payload(serde_json::from_str(payload)?);
        return serde_json::to_vec(&value).map(std::borrow::Cow::Owned);
    }
    Ok(std::borrow::Cow::Borrowed(payload.as_bytes()))
}

pub(crate) struct OutboxRecord {
    pub event_id: uuid::Uuid,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub exchange: String,
    pub payload: Value,
}

pub(crate) struct InboxRecord<'a> {
    pub(crate) id: uuid::Uuid,
    pub(crate) consumer: String,
    pub(crate) event_type: String,
    pub(crate) payload: &'a serde_json::value::RawValue,
}

#[derive(FromRow)]
struct PendingOutboxRecord {
    id: i64,
    event_id: uuid::Uuid,
    exchange: String,
    payload: String,
    attempt_count: i32,
}

/// Bounded outbox publishing and idle polling settings.
#[derive(Clone, Debug)]
pub struct OutboxOptions {
    /// Maximum messages fetched and published concurrently per transaction.
    /// Only the earliest pending row of each aggregate is eligible.
    pub batch_size: NonZeroU16,
    /// Fallback polling for commits from other processes and scheduled retries.
    pub poll_interval: Duration,
}
impl Default for OutboxOptions {
    fn default() -> Self {
        Self {
            batch_size: NonZeroU16::new(256).unwrap(),
            poll_interval: Duration::from_millis(500),
        }
    }
}

/// PostgreSQL persistence for request transactions, inbox deduplication, and outbox delivery.
///
/// Construct this with the application's SQLx `PgPool`, run
/// [`EventityPg::migrate`], and share it with the bus and request handlers.
pub struct EventityPg {
    pool: PgPool,
    outbox_options: OutboxOptions,
    outbox_ready: tokio::sync::Notify,
    outbox_concurrency: NonZeroU16,
}

impl EventityPg {
    /// Wraps an existing SQLx PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            outbox_options: OutboxOptions::default(),
            outbox_ready: tokio::sync::Notify::new(),
            outbox_concurrency: NonZeroU16::MIN,
        }
    }

    /// Configures outbox batching before sharing the store with a bus.
    pub fn with_outbox_options(mut self, options: OutboxOptions) -> Self {
        self.outbox_options = options;
        self
    }

    /// Runs independent outbox batches concurrently. Each worker holds one
    /// database connection while publishing. Aggregate ordering is still enforced
    /// by row locks; size the pool for workers plus consumers and application work.
    pub fn with_outbox_concurrency(mut self, concurrency: NonZeroU16) -> Self {
        self.outbox_concurrency = concurrency;
        self
    }

    pub(crate) fn notify_outbox(&self) {
        self.outbox_ready.notify_waiters();
        self.outbox_ready.notify_one();
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
            let mut ids = Vec::with_capacity(batch.len());
            let mut aggregate_types = Vec::with_capacity(batch.len());
            let mut aggregate_ids = Vec::with_capacity(batch.len());
            let mut event_types = Vec::with_capacity(batch.len());
            let mut payloads = Vec::with_capacity(batch.len());
            let mut exchanges = Vec::with_capacity(batch.len());
            for record in batch {
                ids.push(record.event_id);
                aggregate_types.push(record.aggregate_type);
                aggregate_ids.push(record.aggregate_id);
                event_types.push(record.event_type);
                payloads.push(sqlx::types::Json(record.payload));
                exchanges.push(record.exchange);
            }
            inserted += query(
                "INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, exchange) \
                 SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::jsonb[], $6::text[])"
            )
            .bind(&ids)
            .bind(&aggregate_types)
            .bind(&aggregate_ids)
            .bind(&event_types)
            .bind(&payloads)
            .bind(&exchanges)
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
        record: InboxRecord<'_>,
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

    /// Insert in UUID order so overlapping batches acquire unique-index locks
    /// consistently. RETURNING distinguishes new events from redeliveries.
    pub(crate) async fn create_inbox_batch(
        &self,
        tx: &mut PgTransaction<'_>,
        consumer: &str,
        records: &[(uuid::Uuid, &serde_json::value::RawValue)],
    ) -> Result<std::collections::HashSet<uuid::Uuid>, sqlx::Error> {
        let mut ordered: Vec<_> = records.iter().collect();
        // Stable sorting retains the first payload for duplicate IDs, matching
        // the first handler invocation below and single-delivery deduplication.
        ordered.sort_by_key(|(id, _)| *id);
        ordered.dedup_by_key(|(id, _)| *id);
        let mut inserted = std::collections::HashSet::with_capacity(records.len());
        for chunk in ordered.chunks(1000) {
            let ids: Vec<_> = chunk.iter().map(|(id, _)| *id).collect();
            let payloads: Vec<_> = chunk
                .iter()
                .map(|(_, payload)| sqlx::types::Json(payload))
                .collect();
            let new_ids = sqlx::query_scalar::<_, uuid::Uuid>(
                "INSERT INTO inbox (id, consumer, event_type, payload) \
                 SELECT id, $3, $3, payload FROM UNNEST($1::uuid[], $2::jsonb[]) AS batch(id, payload) \
                 ORDER BY id \
                 ON CONFLICT (consumer, id) DO NOTHING RETURNING id",
            )
            .bind(&ids)
            .bind(&payloads)
            .bind(consumer)
            .fetch_all(&mut **tx)
            .await?;
            inserted.extend(new_ids);
        }
        Ok(inserted)
    }

    pub(crate) async fn start_outbox_worker_with_cancel(
        self: &Arc<Self>,
        broker: Arc<crate::RabbitMQ>,
        cancel: CancellationToken,
    ) -> Result<
        tokio::task::JoinHandle<Result<(), crate::errors::EventityError>>,
        crate::errors::EventityError,
    > {
        let store = Arc::clone(self);
        Ok(tokio::spawn(async move {
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..store.outbox_concurrency.get() {
                let (store, broker, cancel) = (store.clone(), broker.clone(), cancel.clone());
                workers.spawn(async move { store.run_outbox_worker(&broker, cancel).await });
            }
            let mut first_error = None;
            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    cancel.cancel();
                    first_error.get_or_insert(crate::EventityError::Join(error.to_string()));
                }
            }
            first_error.map_or(Ok(()), Err)
        }))
    }

    async fn run_outbox_worker(&self, broker: &crate::RabbitMQ, cancel: CancellationToken) {
        let mut retry_delay = Duration::from_millis(500);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            // Finish a claimed batch before shutdown: dropping publication
            // futures can leave broker acceptance ambiguous.
            match self.process_next_outbox(broker).await {
                Ok(true) => {
                    retry_delay = Duration::from_millis(500);
                    continue;
                }
                Ok(false) => {
                    retry_delay = Duration::from_millis(500);
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = self.outbox_ready.notified() => {},
                        _ = tokio::time::sleep(self.outbox_options.poll_interval) => {},
                    }
                }
                Err(error) => {
                    tracing::error!(%error, ?retry_delay, "outbox processing failed; retrying");
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(retry_delay) => {},
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn process_next_outbox(
        &self,
        broker: &crate::RabbitMQ,
    ) -> Result<bool, crate::errors::EventityError> {
        let started = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;
        let events = sqlx::query_as::<_, PendingOutboxRecord>(
            r#"
    SELECT o.id, o.event_id, o.exchange, o.payload::text AS payload, o.attempt_count
    FROM outbox AS o
    WHERE o.processed_at IS NULL
      AND o.next_attempt_at <= now()
      AND NOT EXISTS (
          SELECT 1 FROM outbox AS earlier
          WHERE earlier.aggregate_type = o.aggregate_type
            AND earlier.aggregate_id = o.aggregate_id
            AND earlier.id < o.id
            AND earlier.processed_at IS NULL
          -- Keep this an indexed correlated lookup. An anti-join can choose
          -- a quadratic materialized scan after statistics report no pending
          -- rows and a new burst arrives. OFFSET 0 prevents that rewrite.
          OFFSET 0
      )
    ORDER BY o.next_attempt_at, o.id
    LIMIT $1
    FOR UPDATE OF o SKIP LOCKED
    "#,
        )
        .bind(i64::from(self.outbox_options.batch_size.get()))
        .fetch_all(&mut *tx)
        .await?;

        if events.is_empty() {
            tx.rollback().await?;
            return Ok(false);
        }
        let claimed_at = std::time::Instant::now();
        let batch_len = events.len();
        // The row locks remain held until all outcomes are persisted. Other
        // workers skip these heads and cannot overtake them within an aggregate.
        let mut publishes: FuturesUnordered<_> = events
            .into_iter()
            .map(|record| async move {
                let result = async {
                    let payload = encoded_outbox_payload(&record.payload)?;
                    broker
                        .publish_raw(
                            record.event_id,
                            &ShortString::from(record.exchange),
                            &payload,
                        )
                        .await
                }
                .await;
                (record.id, record.attempt_count.saturating_add(1), result)
            })
            .collect();
        let mut completed = Vec::new();
        let mut failed_ids = Vec::new();
        let mut attempts = Vec::new();
        let mut delays = Vec::new();
        let mut errors = Vec::new();
        while let Some((id, attempt_count, result)) = publishes.next().await {
            match result {
                Ok(()) => completed.push(id),
                Err(error) => {
                    let retry_seconds = 1_i64 << attempt_count.clamp(1, 8);
                    tracing::error!(outbox_id = id, attempt_count, retry_seconds, %error, "outbox publish failed; scheduled retry");
                    failed_ids.push(id);
                    attempts.push(attempt_count);
                    delays.push(retry_seconds);
                    errors.push(error.to_string());
                }
            }
        }
        let published_at = std::time::Instant::now();
        if !completed.is_empty() {
            sqlx::query(
                "UPDATE outbox SET processed_at = now(), last_error = NULL WHERE id = ANY($1)",
            )
            .bind(&completed)
            .execute(&mut *tx)
            .await?;
        }
        if !failed_ids.is_empty() {
            sqlx::query(
                "UPDATE outbox AS o SET attempt_count = f.attempt, next_attempt_at = now() + (f.delay * interval '1 second'), last_error = f.error \
                 FROM UNNEST($1::bigint[], $2::integer[], $3::bigint[], $4::text[]) AS f(id, attempt, delay, error) WHERE o.id = f.id"
            ).bind(&failed_ids).bind(&attempts).bind(&delays).bind(&errors)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        tracing::debug!(target: "eventity::outbox", messages = batch_len as u64,
            claim_us = claimed_at.duration_since(started).as_micros() as u64,
            publish_us = published_at.duration_since(claimed_at).as_micros() as u64,
            commit_us = published_at.elapsed().as_micros() as u64,
            "outbox batch completed");
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Resources;

    #[test]
    fn restores_legacy_payload_without_changing_json_objects() {
        let object = serde_json::json!({"id": 1});
        let bytes = serde_json::to_value(serde_json::to_vec(&object).unwrap()).unwrap();
        assert_eq!(restore_outbox_payload(bytes), object);
        assert_eq!(restore_outbox_payload(object.clone()), object);
        let array = serde_json::json!(["normal", "array"]);
        assert_eq!(restore_outbox_payload(array.clone()), array);
    }

    async fn seed(r: &Resources, aggregates: &[&str]) {
        let mut tx = r.pool.begin().await.unwrap();
        r.store
            .create_outbox_records(
                &mut tx,
                aggregates
                    .iter()
                    .enumerate()
                    .map(|(n, aggregate)| OutboxRecord {
                        event_id: uuid::Uuid::now_v7(),
                        aggregate_type: "test".into(),
                        aggregate_id: (*aggregate).into(),
                        event_type: "test".into(),
                        exchange: r.schema.clone(),
                        payload: serde_json::json!({"n": n}),
                    }),
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn batches_independent_aggregates_and_does_not_overtake_locked_heads() {
        let r = Resources::new().await;
        seed(&r, &["a", "a", "b", "c"]).await;
        let mut lock = r.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM outbox WHERE id = 1 FOR UPDATE")
            .execute(&mut *lock)
            .await
            .unwrap();
        assert!(r.store.process_next_outbox(&r.broker).await.unwrap());
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM outbox WHERE processed_at IS NOT NULL ORDER BY id")
                .fetch_all(&r.pool)
                .await
                .unwrap();
        assert_eq!(ids, vec![3, 4]);
        assert!(!r.store.process_next_outbox(&r.broker).await.unwrap());
        lock.rollback().await.unwrap();
        // Competing workers cannot publish the same head concurrently.
        let (a, b) = tokio::join!(
            r.store.process_next_outbox(&r.broker),
            r.store.process_next_outbox(&r.broker)
        );
        a.unwrap();
        b.unwrap();
        while r.store.process_next_outbox(&r.broker).await.unwrap() {}
        let queue = r
            .channel
            .queue_declare(
                r.schema.clone().into(),
                lapin::options::QueueDeclareOptions::durable(),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(queue.message_count(), 4);
        let mut delivered = Vec::new();
        for _ in 0..4 {
            let message = r
                .channel
                .basic_get(
                    r.schema.clone().into(),
                    lapin::options::BasicGetOptions { no_ack: true },
                )
                .await
                .unwrap()
                .unwrap();
            delivered.push(
                serde_json::from_slice::<Value>(&message.delivery.data).unwrap()["n"]
                    .as_u64()
                    .unwrap(),
            );
        }
        assert!(delivered.iter().position(|n| *n == 0) < delivered.iter().position(|n| *n == 1));
        r.cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn unroutable_head_retries_without_blocking_other_aggregates() {
        let r = Resources::new().await;
        seed(&r, &["a", "a", "b"]).await;
        let missing = format!("{}_unrouted", r.schema);
        sqlx::query("UPDATE outbox SET exchange = $1 WHERE id = 1")
            .bind(&missing)
            .execute(&r.pool)
            .await
            .unwrap();
        assert!(r.store.process_next_outbox(&r.broker).await.unwrap());
        let states: Vec<(i64, i32, bool)> = sqlx::query_as(
            "SELECT id, attempt_count, processed_at IS NOT NULL FROM outbox ORDER BY id",
        )
        .fetch_all(&r.pool)
        .await
        .unwrap();
        assert_eq!(states, vec![(1, 1, false), (2, 0, false), (3, 0, true)]);
        assert!(!r.store.process_next_outbox(&r.broker).await.unwrap());
        sqlx::query("UPDATE outbox SET exchange = $1, next_attempt_at = now() WHERE id = 1")
            .bind(&r.schema)
            .execute(&r.pool)
            .await
            .unwrap();
        assert!(r.store.process_next_outbox(&r.broker).await.unwrap());
        assert!(r.store.process_next_outbox(&r.broker).await.unwrap());
        assert!(!r.store.process_next_outbox(&r.broker).await.unwrap());
        r.channel
            .exchange_delete(missing.into(), Default::default())
            .await
            .unwrap();
        r.cleanup().await;
    }
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL and RabbitMQ; set DATABASE_URL and AMQP_URL"]
    async fn respects_configured_batch_limit_after_empty_outbox_analysis() {
        let mut r = Resources::new().await;
        r.store = EventityPg::new(r.pool.clone()).with_outbox_options(OutboxOptions {
            batch_size: NonZeroU16::new(2).unwrap(),
            ..Default::default()
        });
        seed(&r, &["old"]).await;
        sqlx::query("UPDATE outbox SET processed_at = now()")
            .execute(&r.pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE outbox")
            .execute(&r.pool)
            .await
            .unwrap();
        seed(&r, &["a", "b", "c", "d", "e"]).await;
        for expected_pending in [3_i64, 1, 0] {
            assert!(r.store.process_next_outbox(&r.broker).await.unwrap());
            let pending: i64 =
                sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
                    .fetch_one(&r.pool)
                    .await
                    .unwrap();
            assert_eq!(pending, expected_pending);
        }
        assert!(!r.store.process_next_outbox(&r.broker).await.unwrap());
        r.cleanup().await;
    }
}
