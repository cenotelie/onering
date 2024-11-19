use std::{cell::Cell, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use disruptor_rs::{DisruptorBuilder, EventHandler, EventProcessorExecutor, EventProducer, ExecutorHandle};

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 256;
/// The number of messages
pub const SCALE_MSG_COUNT: usize = 1_000_000;
/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;
/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;

#[derive(Default)]
struct MyHandler {
    next: Cell<usize>,
}

impl EventHandler<usize> for MyHandler {
    fn on_event(&self, event: &usize, _sequence: i64, _end_of_batch: bool) {
        self.next.set(*event);
    }

    fn on_start(&self) {}

    fn on_shutdown(&self) {}
}

fn disruptor_rs_spsc() {
    let (executor, mut producer) = DisruptorBuilder::with_ring_buffer::<usize>(SCALE_QUEUE_SIZE)
        .with_busy_spin_waiting_strategy()
        .with_single_producer_sequencer()
        .with_barrier(|scope| {
            scope.handle_events(MyHandler::default());
        })
        .build();
    let handle = executor.spawn();

    for i in 0..SCALE_MSG_COUNT {
        producer.write([i], |slot, _seq, item| {
            *slot = *item;
        });
    }
    producer.drain();

    handle.join();
}

fn disruptor_rs_spmc() {
    let (executor, mut producer) = DisruptorBuilder::with_ring_buffer::<usize>(SCALE_QUEUE_SIZE)
        .with_busy_spin_waiting_strategy()
        .with_single_producer_sequencer()
        .with_barrier(|scope| {
            for _ in 0..SCALE_CONSUMERS {
                scope.handle_events(MyHandler::default());
            }
        })
        .build();
    let handle = executor.spawn();

    for i in 0..SCALE_MSG_COUNT {
        producer.write([i], |slot, _seq, item| {
            *slot = *item;
        });
    }
    producer.drain();

    handle.join();
}

pub fn bench_disruptor_rs(c: &mut Criterion) {
    let mut group = c.benchmark_group("disruptor_rs");
    group.throughput(Throughput::Elements(SCALE_MSG_COUNT as u64));
    group.bench_function("disruptor_rs_spsc", |b| b.iter(disruptor_rs_spsc));
    group.bench_function("disruptor_rs_spmc", |b| b.iter(disruptor_rs_spmc));
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_disruptor_rs
);
criterion_main!(benches);
