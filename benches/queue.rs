use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_utils::Backoff;
use onering::errors::TryRecvError;
use onering::queue::{Consumer, ConsumerMode, RingBuffer, SingleProducer};

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 256;
/// The number of messages
pub const SCALE_MSG_COUNT: usize = 1_000_000;
/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;
/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;

fn queue_spsc() {
    let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(SCALE_QUEUE_SIZE, 16));
    let mut consumer = Consumer::new(ring.clone(), ConsumerMode::Blocking).unwrap();
    let mut producer = SingleProducer::new(ring);

    let consumer = std::thread::spawn({
        move || {
            let mut next = 0;
            while next != SCALE_MSG_COUNT {
                let waiting = consumer.get_number_of_items();
                if waiting < 20 && next + waiting < SCALE_MSG_COUNT {
                    let backoff = Backoff::new();
                    backoff.spin();
                    continue;
                }
                match consumer.try_recv() {
                    Ok(items) => {
                        for &item in items {
                            assert_eq!(item, next);
                            next += 1;
                        }
                        // wait a little bit for the next batch
                        let backoff = Backoff::new();
                        backoff.snooze();
                    }
                    Err(e) => {
                        if matches!(e, TryRecvError::Empty) {
                            // wait a little bit
                            let backoff = Backoff::new();
                            backoff.snooze();
                        }
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

fn queue_spmc() {
    let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(SCALE_QUEUE_SIZE, 16));
    let mut consumers = (0..SCALE_CONSUMERS)
        .map(|_| Consumer::new(ring.clone(), ConsumerMode::Blocking).unwrap())
        .collect::<Vec<_>>();
    let mut producer = SingleProducer::new(ring);

    let consumer_threads = (0..SCALE_CONSUMERS)
        .map(|_| {
            let mut consumer = consumers.pop().unwrap();
            std::thread::spawn({
                move || {
                    let mut count = 0;
                    while count < SCALE_MSG_COUNT {
                        let waiting = consumer.get_number_of_items();
                        if waiting < 20 && count + waiting < SCALE_MSG_COUNT {
                            let backoff = Backoff::new();
                            backoff.spin();
                            continue;
                        }
                        if let Ok(items) = consumer.try_recv() {
                            count += items.len();
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for item in 0..SCALE_MSG_COUNT {
        while producer.try_push(item).is_err() {}
    }

    for consumer in consumer_threads {
        consumer.join().unwrap();
    }
}

pub fn bench_disruptor_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue");
    group.throughput(Throughput::Elements(SCALE_MSG_COUNT as u64));
    group.bench_function("queue_spsc", |b| b.iter(queue_spsc));
    group.bench_function("queue_spmc", |b| b.iter(queue_spmc));
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_disruptor_queue
);
criterion_main!(benches);
