/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

use crate::errors::TryRecvError;
use crate::tests::{SCALE_CONSUMERS, SCALE_MSG_COUNT, SCALE_PRODUCERS, SCALE_QUEUE_SIZE};
use crate::{Receiver, Sender};

#[test]
fn channel_1p_1c() {
    let (mut sender, mut receiver) = crate::channels::channel_spsc::<usize>(SCALE_QUEUE_SIZE);

    let producer = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for i in 0..SCALE_MSG_COUNT {
            while sender.try_send(i).is_err() {}
        }
        start
    });
    let consumer = std::thread::spawn(move || {
        for i in 0..SCALE_MSG_COUNT {
            loop {
                if let Ok(value) = receiver.try_recv() {
                    assert_eq!(i, value);
                    break;
                }
            }
        }
        std::time::Instant::now()
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
    let mut receiver = crate::channels::channel_mpsc::<usize>(SCALE_QUEUE_SIZE);

    let producers = (0..SCALE_PRODUCERS)
        .map(|p| {
            let mut sender = receiver.add_sender();
            std::thread::spawn(move || {
                let start = std::time::Instant::now();
                for i in 0..(SCALE_MSG_COUNT / SCALE_PRODUCERS) {
                    while sender.try_send((p * SCALE_MSG_COUNT / SCALE_PRODUCERS) + i).is_err() {}
                }
                start
            })
        })
        .collect::<Vec<_>>();

    let consumer = std::thread::spawn(move || {
        let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
        for _i in 0..SCALE_MSG_COUNT {
            loop {
                if let Ok(value) = receiver.try_recv() {
                    output.push(value);
                    break;
                }
            }
        }
        (output, std::time::Instant::now())
    });

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
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("channel_mp_sc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, producers={SCALE_PRODUCERS}, consumers=1, throughput={throughput}");
}

#[test]
fn channel_sp_mc() {
    let mut sender = crate::channels::channel_spmc::<usize>(SCALE_QUEUE_SIZE);

    let consumers = (0..SCALE_CONSUMERS)
        .map(|_| {
            let mut receiver = sender.add_receiver();
            std::thread::spawn(move || {
                let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
                'outer: for _ in 0..SCALE_MSG_COUNT {
                    loop {
                        match receiver.try_recv() {
                            Ok(value) => {
                                output.push(value);
                                break;
                            }
                            Err(TryRecvError::Disconnected) => {
                                break 'outer;
                            }
                            Err(TryRecvError::Empty) => {}
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
            while sender.try_send(i).is_err() {}
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

#[test]
fn test_channel_pipeline_3c() {
    let (mut sender0, mut receiver1) = crate::channels::channel_spsc::<usize>(SCALE_QUEUE_SIZE);
    let (mut sender1, mut receiver2) = crate::channels::channel_spsc::<usize>(SCALE_QUEUE_SIZE);
    let (mut sender2, mut receiver3) = crate::channels::channel_spsc::<usize>(SCALE_QUEUE_SIZE);

    let producer = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for i in 0..SCALE_MSG_COUNT {
            while sender0.try_send(i).is_err() {}
        }
        start
    });
    let intermediate1 = std::thread::spawn(move || {
        for _ in 0..SCALE_MSG_COUNT {
            let item = receiver1.recv().unwrap();
            sender1.send(item).unwrap();
        }
    });
    let intermediate2 = std::thread::spawn(move || {
        for _ in 0..SCALE_MSG_COUNT {
            let item = receiver2.recv().unwrap();
            sender2.send(item).unwrap();
        }
    });
    let consumer = std::thread::spawn(move || {
        for i in 0..SCALE_MSG_COUNT {
            loop {
                if let Ok(value) = receiver3.try_recv() {
                    assert_eq!(i, value);
                    break;
                }
            }
        }
        std::time::Instant::now()
    });

    let start = producer.join().unwrap();
    intermediate1.join().unwrap();
    intermediate2.join().unwrap();
    let end = consumer.join().unwrap();
    let duration = end.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("test_channel_pipeline_3c queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, throughput={throughput}");
}

#[test]
fn test_channel_diamond() {
    let mut dispatcher = crate::channels::channel_spmc::<usize>(SCALE_QUEUE_SIZE);
    let mut dispatcher_receiver1 = dispatcher.add_receiver();
    let mut dispatcher_receiver2 = dispatcher.add_receiver();
    let mut aggregator = crate::channels::channel_mpsc::<usize>(SCALE_QUEUE_SIZE);

    let producer = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for i in 0..SCALE_MSG_COUNT {
            while dispatcher.try_send(i).is_err() {}
        }
        start
    });
    let intermediate1 = std::thread::spawn({
        let mut sender = aggregator.add_sender();
        move || {
            for _ in 0..SCALE_MSG_COUNT {
                let Ok(item) = dispatcher_receiver1.recv() else {
                    return;
                };
                sender.send(item).unwrap();
            }
        }
    });
    let intermediate2 = std::thread::spawn({
        let mut sender = aggregator.add_sender();
        move || {
            for _ in 0..SCALE_MSG_COUNT {
                let Ok(item) = dispatcher_receiver2.recv() else {
                    return;
                };
                sender.send(item).unwrap();
            }
        }
    });
    let consumer = std::thread::spawn(move || {
        let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
        for _i in 0..SCALE_MSG_COUNT {
            loop {
                if let Ok(value) = aggregator.try_recv() {
                    output.push(value);
                    break;
                }
            }
        }
        (output, std::time::Instant::now())
    });

    let start = producer.join().unwrap();
    intermediate1.join().unwrap();
    intermediate2.join().unwrap();
    let (mut outputs, end) = consumer.join().unwrap();

    outputs.sort_unstable();
    outputs.dedup();
    assert_eq!(SCALE_MSG_COUNT, outputs.len());
    for (i, v) in outputs.into_iter().enumerate() {
        assert_eq!(i, v);
    }

    let duration = end.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("test_channel_diamond queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, throughput={throughput}");
}
