use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_utils::Backoff;
use disruptor::queue::{Consumer, ConsumerMode, RingBuffer, SingleProducer};

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 256;
/// The number of messages
pub const SCALE_MSG_COUNT: usize = 1_000_000;
/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;
/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;

fn queue_spsc() {
    let ring = Arc::new(RingBuffer::<usize>::new(SCALE_QUEUE_SIZE));
    let mut consumer = Consumer::new_for_ring(ring.clone(), ConsumerMode::Blocking);
    let mut producer = SingleProducer::new(ring);

    let consumer = std::thread::spawn({
        move || {
            let mut next = 0;
            while next != SCALE_MSG_COUNT {
                match consumer.try_recv() {
                    Ok(items) => {
                        for &item in items {
                            assert_eq!(next, item);
                            next += 1;
                        }
                    }
                    Err(_e) => {
                        let backoff = Backoff::new();
                        backoff.snooze();
                    }
                }
            }
        }
    });

    for i in 0..SCALE_MSG_COUNT {
        while producer.try_push(i).is_err() {}
    }

    consumer.join().unwrap();
}

pub fn bench_crossbeam(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue");
    group.throughput(Throughput::Elements(SCALE_MSG_COUNT as u64));
    group.bench_function("queue_spsc", |b| b.iter(queue_spsc));
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_crossbeam
);
criterion_main!(benches);
