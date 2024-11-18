use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 256;
/// The number of messages
pub const SCALE_MSG_COUNT: usize = 1_000_000;
/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;
/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;

fn crossbeam_spsc() {
    let (sender, receiver) = crossbeam::channel::bounded(SCALE_QUEUE_SIZE);

    let consumer = std::thread::spawn({
        move || {
            for i in 0..SCALE_MSG_COUNT {
                loop {
                    if let Ok(value) = receiver.recv() {
                        assert_eq!(i, value);
                        break;
                    }
                }
            }
        }
    });

    for i in 0..SCALE_MSG_COUNT {
        while sender.send(i).is_err() {}
    }

    consumer.join().unwrap();
}

pub fn bench_crossbeam(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam");
    group.throughput(Throughput::Elements(SCALE_MSG_COUNT as u64));
    group.bench_function("crossbeam_spsc", |b| b.iter(crossbeam_spsc));
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
