/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

use crate::tests::{SCALE_CONSUMERS, SCALE_MSG_COUNT, SCALE_PRODUCERS, SCALE_QUEUE_SIZE};

#[test]
fn channel_1p_1c() {
    let (sender, receiver) = crossbeam::channel::bounded(SCALE_QUEUE_SIZE);

    let producer = std::thread::spawn({
        move || {
            let start = std::time::Instant::now();
            for i in 0..SCALE_MSG_COUNT {
                while sender.send(i).is_err() {}
            }
            start
        }
    });
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
            std::time::Instant::now()
        }
    });

    let start = producer.join().unwrap();
    let end = consumer.join().unwrap();
    let duration = end.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("channel_1p_1c queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, throughput={throughput}");
}

#[test]
fn channel_mp_sc() {
    let (sender, receiver) = crossbeam::channel::bounded(SCALE_QUEUE_SIZE);

    let producers = (0..SCALE_PRODUCERS)
        .map(|p| {
            let sender = sender.clone();
            std::thread::spawn(move || {
                let start = std::time::Instant::now();
                for i in 0..(SCALE_MSG_COUNT / SCALE_PRODUCERS) {
                    while sender.send((p * SCALE_MSG_COUNT / SCALE_PRODUCERS) + i).is_err() {}
                }
                start
            })
        })
        .collect::<Vec<_>>();

    let consumer = {
        std::thread::spawn(move || {
            let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
            for _ in 0..SCALE_MSG_COUNT {
                loop {
                    if let Ok(value) = receiver.recv() {
                        output.push(value);
                        break;
                    }
                }
            }
            (output, std::time::Instant::now())
        })
    };

    let start = producers.into_iter().map(|handle| handle.join().unwrap()).min().unwrap();

    let (mut outputs, end) = consumer.join().unwrap();

    outputs.sort_unstable();
    outputs.dedup();
    assert_eq!(SCALE_MSG_COUNT, outputs.len());
    for (i, v) in outputs.into_iter().enumerate() {
        assert_eq!(i, v);
    }

    let duration = end.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = ((SCALE_MSG_COUNT * SCALE_PRODUCERS) as f64) / duration;
    println!("channel_mp_sc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, producers={SCALE_PRODUCERS}, consumers=1, throughput={throughput}");
}

#[test]
fn channel_sp_mc() {
    let (sender, receiver) = crossbeam::channel::bounded(SCALE_QUEUE_SIZE);

    let consumers = (0..SCALE_CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            std::thread::spawn(move || {
                let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
                for _ in 0..SCALE_MSG_COUNT {
                    match receiver.recv() {
                        Ok(value) => {
                            output.push(value);
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                (output, std::time::Instant::now())
            })
        })
        .collect::<Vec<_>>();

    let producer = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for i in 0..SCALE_MSG_COUNT {
            while sender.send(i).is_err() {}
        }
        start
    });

    let start = producer.join().unwrap();
    let mut outputs = Vec::new();
    let mut end = None;
    for handle in consumers {
        let (mut partial_output, partial_end) = handle.join().unwrap();
        end = match end {
            None => Some(partial_end),
            Some(end) => Some(end.max(partial_end)),
        };
        outputs.append(&mut partial_output);
    }

    outputs.sort_unstable();
    outputs.dedup();
    assert_eq!(SCALE_MSG_COUNT, outputs.len());
    for (i, v) in outputs.into_iter().enumerate() {
        assert_eq!(i, v);
    }

    let duration = end.unwrap().duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("channel_sp_mc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, producers=1, consumers={SCALE_CONSUMERS}, throughput={throughput}");
}
