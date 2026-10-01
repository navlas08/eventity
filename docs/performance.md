# Throughput and transaction batching

Eventity exceeded 50,000 **consumer messages/sec** in the local benchmark below.
This measures RabbitMQ delivery through committed PostgreSQL inbox rows. It does
not mean every application, payload, queue type, or end-to-end workload achieves
that rate. The handler in this benchmark performs no business work.

## Consumer configuration

```rust,ignore
use eventity::{BatchOptions, ConsumerOptions};
use std::{num::NonZeroU16, time::Duration};

let bus = MessageBus::builder()
    .broker(broker)
    .store(store)
    .event_handler_with_batch_options::<OrderCreated, _>(
        OrderCreatedHandler,
        ConsumerOptions {
            concurrency: NonZeroU16::MIN,
            prefetch: NonZeroU16::new(8192).unwrap(),
        },
        BatchOptions {
            max_messages: NonZeroU16::new(512).unwrap(),
            max_wait: Duration::from_millis(1),
        },
    )
    .build()
    .await?;
```

These are benchmark settings, not new defaults. Defaults retain one delivery per
transaction and one executing handler. Start with a smaller prefetch for large
messages: it bounds unacknowledged deliveries, not bytes. Concurrency controls
active transactions; handlers within each transaction execute sequentially.
More concurrency helps slow handlers but can increase database contention.

Batch collection ends when full or after `max_wait` from the first delivery.
Zero wait drains only immediately available deliveries. No database transaction
is held while collecting. Prefetch smaller than the batch size prevents full
batches. With concurrency one, successful batches use one cumulative broker
acknowledgement after commit. Concurrent batches use individual, pipelined
acknowledgements so later completion cannot acknowledge unfinished earlier work.

Inbox inserts, application changes made through the supplied transaction, and
outgoing events commit together. A handler failure rolls the batch back and
retries its deliveries individually to isolate the failed event. External side
effects can therefore repeat; keep them idempotent. Database failures requeue the
batch with backoff. RabbitMQ delivery remains at least once, with transactional
inbox deduplication. Outbox per-aggregate publication ordering is preserved;
concurrent consumer completion order is not guaranteed.

## Native async handlers

New handlers can implement `TransactionalEventHandler` with an ordinary
`async fn handle_in_transaction(&self, event, tx)`. This avoids the boxed futures
introduced by `async_trait` in the legacy handler path. Use the supplied
`&mut sqlx::PgTransaction<'_>` for application database writes. Existing
`EventHandler` implementations continue to work through an adapter; implement
one interface per handler type.

The consumer borrows JSON directly from the delivery buffer for inbox storage.
Bulk inbox and outbox writes use fixed SQL statements with PostgreSQL `UNNEST`
arrays, avoiding a different prepared statement for each batch length. Inbox
IDs are inserted in sorted order to avoid opposite unique-index lock ordering;
duplicates retain their first payload.

## Publisher tuning

`EventityPg::with_outbox_concurrency` controls independent outbox workers.
`OutboxOptions::batch_size` limits each worker's in-flight publications. Budget
PostgreSQL connections for active consumer batches, outbox workers, and request
handlers. More workers are useful across independent aggregates; a single hot
aggregate remains sequential for ordering correctness.

`RabbitMQ::new_with_publisher_connections(uri, count)` optionally spreads
publication across multiple cached confirm-enabled connections. Default is one
publisher connection plus a separate consumer connection. Extra connections
increase broker/client resources and did not substantially improve this local
workload. Publisher confirms, mandatory routing, persistent messages, and inbox
commits remain enabled.

## Reproducing the benchmark

Use isolated PostgreSQL and RabbitMQ instances. The example creates unique
schemas and queue/exchange names, removing them on success. A failed run leaves
its resources for inspection. It never truncates application tables.

```sh
export DATABASE_URL=postgres://eventity:eventity@localhost:55432/eventity
export AMQP_URL=amqp://guest:guest@localhost:55672/%2f
cargo run --release -p eventity --example throughput -- \
  200000 1 4096 8192 10000 512 4 1 consumer 1000
```

Arguments in order: measured messages, consumer concurrency, outbox batch size,
prefetch, warmup messages, consumer batch size, outbox workers, publisher
connections, mode (`consumer` or `end-to-end`), batch wait in microseconds.
`--help` also lists defaults.

Consumer mode fills the durable queue before starting its timer. It includes
consumer startup and waits for committed inbox rows, excluding queue prefill.
End-to-end mode times request enqueueing, PostgreSQL outbox writes, confirmed
RabbitMQ publication, consumer processing, and committed inbox rows. Both use
128-byte string data plus JSON metadata, a distinct aggregate per message,
1,000 outgoing events per request transaction, and four Tokio worker threads.
These rates are not comparable to transport-only or non-durable dispatch rates.

Local measurements used an Intel i5-12400F (6 cores/12 threads), about 32 GB RAM,
PostgreSQL 18.6 and RabbitMQ 4.3.6 in local Docker containers, with PostgreSQL
`fsync` and `synchronous_commit` enabled and classic durable RabbitMQ queues.
Other desktop workloads were active, so results vary. No comparison benchmark
against Wolverine was run. Its [durability documentation](https://wolverinefx.net/guide/durability/)
provides a useful reference for amortizing persistence through batching, but
transport dispatch figures alone do not establish equivalent durability or work.

| Consumer configuration | Messages | Messages/sec |
| --- | ---: | ---: |
| Concurrency 8, batch 512, 1 ms wait, individual acknowledgements | 200,000 | 44,291 |
| Concurrency 1, batch 512, 1 ms wait, individual acknowledgements | 200,000 | 48,680 |
| Concurrency 1, batch 512, 1 ms wait, cumulative batch acknowledgements | 200,000 | 67,711 |
| Final implementation, same consumer settings | 1,000,000 | 57,318 |

The 200,000-message run took 2.954 seconds, with 403 batches averaging 496.3
messages; prefill took 3.254 seconds. The final million-message run took 17.447
seconds, with 1,991 batches averaging 502.3 messages; prefill took 17.960 seconds.
Both consumer rates exclude prefill. An earlier million-message run measured
56,176/sec, also above 50,000/sec.

With the same 200,000-message configuration, **end-to-end throughput was 41,784
messages/sec** (4.787 seconds). Four publisher connections instead of one
measured 38,711/sec (5.166 seconds), so additional connections did not improve
this run. End-to-end 50,000/sec has not been demonstrated on this machine.

The example reports average consumer batch size and aggregate outbox worker
claim/publish/commit durations. Worker durations overlap and must not be treated
as wall-clock latency. Consumer batch counts include processing attempts, not
just unique successful events.

## Validation

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --include-ignored
```

Integration tests require the isolated service URLs above. They cover inbox
rollback/deduplication, overlapping batches, poison-message isolation, partial
batch deadlines, cumulative acknowledgement boundaries, graceful shutdown,
publisher channel replacement, outbox ordering, and unroutable-message retries.
