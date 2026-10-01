use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Default)]
pub struct Metrics {
    messages: AtomicU64,
    claim_us: AtomicU64,
    publish_us: AtomicU64,
    commit_us: AtomicU64,
    consumer_messages: AtomicU64,
    consumer_batches: AtomicU64,
}
impl Metrics {
    pub fn reset(&self) {
        for counter in [
            &self.messages,
            &self.claim_us,
            &self.publish_us,
            &self.commit_us,
            &self.consumer_messages,
            &self.consumer_batches,
        ] {
            counter.store(0, Relaxed);
        }
    }
    pub fn report(&self) {
        println!(
            "consumer_batches={} average_batch_size={:.1}",
            self.consumer_batches.load(Relaxed),
            self.consumer_messages.load(Relaxed) as f64
                / self.consumer_batches.load(Relaxed).max(1) as f64
        );
        println!(
            "outbox_worker_totals messages={} claim_ms={} publish_ms={} commit_ms={} (overlapping worker times, not wall-clock durations)",
            self.messages.load(Relaxed),
            self.claim_us.load(Relaxed) / 1000,
            self.publish_us.load(Relaxed) / 1000,
            self.commit_us.load(Relaxed) / 1000
        );
    }
}
impl Visit for &Metrics {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    fn record_u64(&mut self, field: &Field, value: u64) {
        let counter = match field.name() {
            "messages" => &self.messages,
            "claim_us" => &self.claim_us,
            "publish_us" => &self.publish_us,
            "commit_us" => &self.commit_us,
            "consumer_messages" => &self.consumer_messages,
            "consumer_batches" => &self.consumer_batches,
            _ => return,
        };
        counter.fetch_add(value, Relaxed);
    }
}
impl Subscriber for Metrics {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        matches!(metadata.target(), "eventity::outbox" | "eventity::consumer")
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        event.record(&mut &*self);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
