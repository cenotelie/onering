/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

use alloc::sync::Arc;

use crossbeam_utils::Backoff;

use crate::errors::TryRecvError;
use crate::queue::{Consumer, ConsumerMode, RingBuffer, SingleProducer};
use crate::tests::{SCALE_CONSUMERS, SCALE_MSG_COUNT, SCALE_PRODUCERS, SCALE_QUEUE_SIZE};

#[test]
#[allow(clippy::cast_precision_loss)]
fn queue_spsc() {
    let ring = Arc::new(RingBuffer::<usize>::new(SCALE_QUEUE_SIZE));
    let mut sender = SingleProducer::new(ring.clone());
    let mut receiver = Consumer::new_for_ring(ring.clone(), ConsumerMode::Blocking);

    let consumer = std::thread::spawn(move || {
        let mut outputs = Vec::with_capacity(SCALE_MSG_COUNT);
        loop {
            match receiver.try_recv() {
                Ok(access) => {
                    for &item in access {
                        outputs.push(item);
                    }
                }
                Err(TryRecvError::Lagging(_)) => {
                    panic!("consumer should not lag");
                }
                Err(TryRecvError::Disconnected) => {
                    break;
                }
                Err(TryRecvError::Empty) => {
                    let backoff = Backoff::new();
                    backoff.snooze();
                }
            }
        }
        let end = std::time::Instant::now();
        outputs.sort_unstable();
        outputs.dedup();
        assert_eq!(SCALE_MSG_COUNT, outputs.len());
        for (i, v) in outputs.into_iter().enumerate() {
            assert_eq!(i, v);
        }
        end
    });

    let producer = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        for i in 0..SCALE_MSG_COUNT {
            while sender.try_push(i).is_err() {
                let backoff = Backoff::new();
                backoff.spin();
            }
        }
        start
    });

    let start = producer.join().unwrap();
    let end = consumer.join().unwrap();
    let duration = end.duration_since(start).as_secs_f64();
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!(
        "queue_spsc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, throughput={:.03}M",
        throughput / 1_000_000.0
    );
}

// #[test]
// #[allow(clippy::cast_precision_loss)]
// fn queue_mp_sc() {
//     let sender = RingBuffer::<usize>::new_multi_producers(SCALE_QUEUE_SIZE);
//     let mut receiver = sender.ring.add_consumer(sender.publication_barrier(), None);

//     let producers = (0..SCALE_PRODUCERS)
//         .map(|p| {
//             let mut sender = sender.clone();
//             std::thread::spawn(move || {
//                 let start = std::time::Instant::now();
//                 for i in 0..(SCALE_MSG_COUNT / SCALE_PRODUCERS) {
//                     while sender.try_push((p * SCALE_MSG_COUNT / SCALE_PRODUCERS) + i).is_err() {}
//                 }
//                 start
//             })
//         })
//         .collect::<Vec<_>>();

//     let consumer = {
//         std::thread::spawn(move || {
//             let mut output = Vec::with_capacity(SCALE_MSG_COUNT);
//             let mut batch_size = 0;
//             let mut batch_count = 0;
//             while output.len() < SCALE_MSG_COUNT {
//                 if let Ok(items) = receiver.try_recv() {
//                     for &value in items.iter() {
//                         output.push(value);
//                     }
//                     batch_size += items.len();
//                     batch_count += 1;
//                 }
//                 // wait a bit
//                 let backoff = Backoff::new();
//                 backoff.snooze();
//             }
//             (std::time::Instant::now(), output, batch_size as f64 / f64::from(batch_count))
//         })
//     };

//     let start = producers.into_iter().map(|handle| handle.join().unwrap()).min().unwrap();

//     let (end, mut outputs, mean_batch_size) = consumer.join().unwrap();

//     outputs.sort_unstable();
//     outputs.dedup();
//     assert_eq!(SCALE_MSG_COUNT, outputs.len());
//     for (i, v) in outputs.into_iter().enumerate() {
//         assert_eq!(i, v);
//     }

//     let duration = end.duration_since(start).as_secs_f64();
//     let throughput = (SCALE_MSG_COUNT as f64) / duration;
//     println!("queue_mp_sc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, producers={SCALE_PRODUCERS}, consumers=1, mean_batch_size={mean_batch_size:.02}, throughput={:.03}M", throughput / 1_000_000.0);
// }

// #[test]
// #[allow(clippy::cast_precision_loss)]
// fn queue_sp_mc() {
//     let mut sender = RingBuffer::<usize>::new_single_producer(SCALE_QUEUE_SIZE);
//     let mut receivers = (0..SCALE_CONSUMERS)
//         .map(|_| sender.ring.add_consumer(sender.publication_barrier(), None))
//         .collect::<Vec<_>>();

//     let producer = std::thread::spawn({
//         move || {
//             let mut tries = 0_usize;
//             let mut items = 0..SCALE_MSG_COUNT;
//             let mut sent = 0;
//             let start = std::time::Instant::now();
//             while sent < SCALE_MSG_COUNT {
//                 tries += 1;
//                 if let Ok(count) = sender.try_push_iterator(&mut items) {
//                     sent += count;
//                 }
//             }
//             let end = std::time::Instant::now();
//             let error_rate = ((tries - SCALE_MSG_COUNT) * 100) as f64 / tries as f64;
//             println!(
//                 "producer: error rate={error_rate:.2}%  finished in {}ms",
//                 (end - start).as_millis()
//             );
//             start
//         }
//     });

//     let consumers = (0..SCALE_CONSUMERS)
//         .map(|_| {
//             let mut receiver = receivers.pop().unwrap();
//             std::thread::spawn(move || {
//                 let mut count = 0;
//                 let mut batch_size = 0;
//                 let mut batch_count = 0;
//                 let mut buffer = vec![0; SCALE_QUEUE_SIZE];
//                 while count < SCALE_MSG_COUNT {
//                     let waiting = receiver.get_number_of_items();
//                     if waiting < 20 && count + waiting < SCALE_MSG_COUNT {
//                         let backoff = Backoff::new();
//                         backoff.spin();
//                         continue;
//                     }
//                     if let Ok(len) = receiver.try_recv_copies(&mut buffer) {
//                         // for (index, value) in buffer[..len].iter().enumerate() {
//                         //     assert_eq!(count + index, *value);
//                         // }
//                         count += len;
//                         batch_size += len;
//                         batch_count += 1;
//                     }
//                 }
//                 let end = std::time::Instant::now();
//                 println!("consumer: mean batch size {:.02}", batch_size as f64 / f64::from(batch_count));
//                 end
//             })
//         })
//         .collect::<Vec<_>>();

//     let start = producer.join().unwrap();
//     let mut end = None;
//     for handle in consumers {
//         let partial_end = handle.join().unwrap();
//         end = match end {
//             None => Some(partial_end),
//             Some(end) => Some(end.max(partial_end)),
//         };
//     }

//     let duration = end.unwrap().duration_since(start).as_secs_f64();
//     let throughput = (SCALE_MSG_COUNT as f64) / duration;
//     println!("queue_sp_mc queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, producers=1, consumers={SCALE_CONSUMERS}, throughput={:.03}M", throughput / 1_000_000.0);
// }
