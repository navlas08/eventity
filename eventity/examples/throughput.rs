//! Run with: cargo run --release -p eventity --example throughput -- 10000 64
//! Uses a unique PostgreSQL schema and RabbitMQ topology, removed on success.
#[path = "throughput/metrics.rs"]
mod metrics;
use async_trait::async_trait;
use eventity::{
    BatchOptions, ConsumerOptions, EventityPg, IntegrationEvent, MessageBus, OutboxOptions,
    OutgoingMessages, RabbitMQ, Request, RequestHandler, TransactionalEventHandler,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgTransaction, postgres::PgPoolOptions};
use std::{
    num::NonZeroU16,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

static ROUTE: OnceLock<String> = OnceLock::new();
#[derive(Serialize, Deserialize)]
struct Event {
    id: u64,
    data: String,
}
impl IntegrationEvent for Event {
    type Error = std::convert::Infallible;
    fn queue() -> &'static str {
        ROUTE.get().unwrap()
    }
    fn exchange() -> &'static str {
        Self::queue()
    }
    fn aggregate() -> &'static str {
        "benchmark"
    }
    fn aggregate_id(&self) -> String {
        self.id.to_string()
    }
}
struct Handler;
impl TransactionalEventHandler<Event> for Handler {
    async fn handle_in_transaction(
        &self,
        _: Event,
        _: &mut PgTransaction<'_>,
    ) -> eventity::EHandlerResult<std::convert::Infallible> {
        Ok(OutgoingMessages::none())
    }
}
struct Seed {
    start: u64,
    end: u64,
}
impl Request for Seed {
    type Output = ();
    type Error = std::convert::Infallible;
}
#[async_trait]
impl RequestHandler<Seed> for Handler {
    async fn handle(
        &self,
        seed: Seed,
        _: &mut PgTransaction<'_>,
    ) -> eventity::HandlerResult<(), std::convert::Infallible> {
        let mut events = OutgoingMessages::none();
        for id in seed.start..seed.end {
            events = events.and(Event {
                id,
                data: "x".repeat(128),
            });
        }
        Ok(((), events))
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "throughput [messages=10000] [concurrency=64] [outbox_batch=256] \
[prefetch=concurrency] [warmup=0] [consumer_batch=1] [outbox_workers=1] \
[publisher_connections=1] [end-to-end|consumer] [batch_wait_us=0]\n\
Set DATABASE_URL and AMQP_URL. Consumer mode excludes queue prefill.\n\
Both modes count committed inbox rows; see docs/performance.md."
        );
        return Ok(());
    }
    let metrics = Arc::new(metrics::Metrics::default());
    tracing::subscriber::set_global_default(metrics.clone())?;
    let count: u64 = std::env::args().nth(1).unwrap_or("10000".into()).parse()?;
    let concurrency: NonZeroU16 = std::env::args().nth(2).unwrap_or("64".into()).parse()?;
    let batch_size: NonZeroU16 = std::env::args().nth(3).unwrap_or("256".into()).parse()?;
    let prefetch: NonZeroU16 = std::env::args()
        .nth(4)
        .unwrap_or(concurrency.to_string())
        .parse()?;
    let warmup: u64 = std::env::args().nth(5).unwrap_or("0".into()).parse()?;
    let consumer_batch: NonZeroU16 = std::env::args().nth(6).unwrap_or("1".into()).parse()?;
    let outbox_workers: NonZeroU16 = std::env::args().nth(7).unwrap_or("1".into()).parse()?;
    let publisher_connections: NonZeroU16 =
        std::env::args().nth(8).unwrap_or("1".into()).parse()?;
    let mode = std::env::args().nth(9).unwrap_or("end-to-end".into());
    if !matches!(mode.as_str(), "end-to-end" | "consumer") {
        return Err("mode must be end-to-end or consumer".into());
    }
    let batch_wait_us: u64 = std::env::args().nth(10).unwrap_or("0".into()).parse()?;
    if count == 0
        || count
            .checked_add(warmup)
            .is_none_or(|total| total > i64::MAX as u64)
    {
        return Err("messages must be positive and messages + warmup must fit in i64".into());
    }
    let schema = format!("eventity_bench_{}", uuid::Uuid::now_v7().simple());
    ROUTE.set(schema.clone()).unwrap();
    let db = std::env::var("DATABASE_URL")
        .unwrap_or("postgres://eventity:eventity@localhost/eventity".into());
    let amqp = std::env::var("AMQP_URL").unwrap_or("amqp://guest:guest@localhost:5672/%2f".into());
    let admin = PgPoolOptions::new().max_connections(2).connect(&db).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await?;
    let search_path = format!("SET search_path TO {schema}");
    let pool = PgPoolOptions::new()
        .max_connections(u32::from(concurrency.get()) + u32::from(outbox_workers.get()) + 4)
        .after_connect(move |conn, _| {
            let sql = search_path.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(sql)).execute(conn).await?;
                Ok(())
            })
        })
        .connect(&db)
        .await?;
    let store = Arc::new(
        EventityPg::new(pool.clone())
            .with_outbox_options(OutboxOptions {
                batch_size,
                ..Default::default()
            })
            .with_outbox_concurrency(outbox_workers),
    );
    store.migrate().await?;
    let broker =
        Arc::new(RabbitMQ::new_with_publisher_connections(&amqp, publisher_connections).await?);
    let build_bus = || {
        MessageBus::builder()
            .broker(broker.clone())
            .store(store.clone())
            .request_handler(Handler)
            .event_handler_with_batch_options::<Event, _>(
                Handler,
                ConsumerOptions {
                    concurrency,
                    prefetch,
                },
                BatchOptions {
                    max_messages: consumer_batch,
                    max_wait: Duration::from_micros(batch_wait_us),
                },
            )
            .build()
    };
    let mut bus = build_bus().await?;
    if warmup > 0 {
        for first in (0..warmup).step_by(1000) {
            bus.invoke(Seed {
                start: first,
                end: (first + 1000).min(warmup),
            })
            .await?;
        }
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                let (received, pending): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM inbox), (SELECT count(*) FROM outbox WHERE processed_at IS NULL)").fetch_one(&pool).await?;
                if received == warmup as i64 && pending == 0 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, sqlx::Error>(())
        }).await??;
        sqlx::query("ANALYZE outbox").execute(&pool).await?;
        sqlx::query("ANALYZE inbox").execute(&pool).await?;
    }
    if mode == "consumer" {
        bus.shutdown().await?;
        bus = MessageBus::builder()
            .broker(broker.clone())
            .store(store.clone())
            .request_handler(Handler)
            .build()
            .await?;
    }
    metrics.reset();
    let mut start = Instant::now();
    for first in (0..count).step_by(1000) {
        bus.invoke(Seed {
            start: warmup + first,
            end: warmup + (first + 1000).min(count),
        })
        .await?;
    }
    if mode == "end-to-end" {
        println!("enqueue_s={:.3}", start.elapsed().as_secs_f64());
    }
    if mode == "consumer" {
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                let pending: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
                        .fetch_one(&pool)
                        .await?;
                if pending == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, sqlx::Error>(())
        })
        .await??;
        bus.shutdown().await?;
        println!(
            "prefill_s={:.3} (excluded from consumer-only measurement)",
            start.elapsed().as_secs_f64()
        );
        metrics.reset();
        start = Instant::now();
        bus = build_bus().await?;
    }
    let mut published_at = None;
    tokio::time::timeout(Duration::from_secs(300), async {
        loop {
            let received: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox")
                .fetch_one(&pool)
                .await?;
            let sent: i64 =
                sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NOT NULL")
                    .fetch_one(&pool)
                    .await?;
            if sent == (count + warmup) as i64 && published_at.is_none() {
                published_at = Some(start.elapsed());
            }
            if received == (count + warmup) as i64 && sent == (count + warmup) as i64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, sqlx::Error>(())
    })
    .await??;
    let elapsed = start.elapsed();
    println!(
        "mode={mode} messages={count} handler_concurrency={concurrency} payload_data_bytes=128 elapsed_s={:.3} messages_per_s={:.0}",
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );
    if mode == "end-to-end" {
        println!(
            "outbox_complete_s={:.3}",
            published_at.unwrap().as_secs_f64()
        );
    }
    println!(
        "batch_size={batch_size} prefetch={prefetch} warmup={warmup} consumer_batch={consumer_batch} outbox_workers={outbox_workers} publisher_connections={publisher_connections} batch_wait_us={batch_wait_us}"
    );
    bus.shutdown().await?;
    metrics.report();
    let connection =
        lapin::Connection::connect(&amqp, lapin::ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    for name in [schema.clone(), format!("{schema}.dead")] {
        channel
            .queue_delete(name.clone().into(), Default::default())
            .await?;
        channel
            .exchange_delete(name.into(), Default::default())
            .await?;
    }
    connection.close(200, "benchmark complete".into()).await?;
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await?;
    Ok(())
}
