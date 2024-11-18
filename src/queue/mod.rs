/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Disruptor-inspired queue

mod barriers;
mod consumers;
mod producers;
mod ring;

use alloc::sync::Arc;

pub use barriers::{MultiBarrier, SingleBarrier, UserOutput};
pub use consumers::{Consumer, ConsumerAccess, ConsumerMode};
pub use producers::{ConcurrentProducer, SingleProducer};
pub use ring::RingBuffer;

/// The position of an item in the queue.
/// This also uniquely identifies the item within the queue and is usedby queue users to keep track of what items are still expected or consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Sequence(isize);

impl Default for Sequence {
    #[inline]
    fn default() -> Self {
        Self(-1) // sequence start at -1, meaning no item
    }
}

#[test]
fn test_sequence_default() {
    assert_eq!(Sequence::default().0, -1);
}

impl From<isize> for Sequence {
    #[inline]
    fn from(value: isize) -> Self {
        Self(value)
    }
}

impl From<usize> for Sequence {
    #[inline]
    #[allow(clippy::cast_possible_wrap)]
    fn from(value: usize) -> Self {
        Self(value as isize)
    }
}

impl Sequence {
    /// Gets whether this sequence represents a valid item
    #[must_use]
    #[inline]
    pub fn is_valid_item(self) -> bool {
        self.0 >= 0
    }

    /// Gets the value of the sequence that can be used as an index within the buffer
    #[must_use]
    #[inline]
    pub fn as_index(self) -> usize {
        debug_assert!(self.0 >= 0);
        self.0.unsigned_abs()
    }
}

/// The user of a queue, be it a producer or a consumer
pub trait QueueUser {
    /// The type of the queue items
    type Item;

    /// Gets the queue itself
    fn queue(&self) -> &Arc<RingBuffer<Self::Item>>;

    /// Gets the output that can be awaited on for items that are available *after* this user
    fn output(&self) -> &Arc<UserOutput>;
}

#[cfg(test)]
mod tests_send_sync {
    use alloc::sync::Arc;

    use super::{ConcurrentProducer, Consumer, ConsumerMode, RingBuffer, SingleProducer};

    pub fn assert_send<T: Send>(_thing: &T) {}
    pub fn assert_sync<T: Sync>(_thing: &T) {}

    #[test]
    fn ring_is_send_sync() {
        let ring = RingBuffer::<usize>::new(4);
        assert_send(&ring);
        assert_sync(&ring);
    }

    #[test]
    fn producers_are_send_sync() {
        let ring = Arc::new(RingBuffer::<usize>::new(4));
        let producer_single = SingleProducer::new(ring.clone());
        assert_send(&producer_single);
        assert_sync(&producer_single);
        let producer_concurrent = ConcurrentProducer::new(ring.clone());
        assert_send(&producer_concurrent);
        assert_sync(&producer_concurrent);
    }

    #[test]
    fn consumer_is_send_sync() {
        let ring = Arc::new(RingBuffer::<usize>::new(4));
        let producer_single = SingleProducer::new(ring.clone());
        let consumer = Consumer::new_awaiting_on(&producer_single, ConsumerMode::default());
        assert_send(&consumer);
        assert_sync(&consumer);
    }
}

#[cfg(test)]
mod tests_concurrency_stress {
    use alloc::sync::Arc;

    use super::{ConcurrentProducer, Consumer, ConsumerMode, RingBuffer, SingleProducer};
    use crate::errors::TryRecvError;

    /// The size of the queue to use
    const SCALE_QUEUE_SIZE: usize = 256;
    /// The number of messages
    const SCALE_MSG_COUNT: usize = 1_000_000;
    /// The number of producers in a multiple producers, singe consumer test
    const SCALE_PRODUCERS: usize = 5;
    /// The number of consumers in a multiple producers, singe consumer test
    const SCALE_CONSUMERS: usize = 5;

    #[test]
    fn spsc() {
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
                    Err(TryRecvError::Empty) => {}
                }
            }
            outputs.sort_unstable();
            outputs.dedup();
            assert_eq!(SCALE_MSG_COUNT, outputs.len());
            for (i, v) in outputs.into_iter().enumerate() {
                assert_eq!(i, v);
            }
        });

        let producer = std::thread::spawn(move || {
            let mut _tries = 0;
            for i in 0..SCALE_MSG_COUNT {
                _tries += 1;
                while sender.try_push(i).is_err() {
                    _tries += 1;
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn spmc() {
        let ring = Arc::new(RingBuffer::<usize>::new(SCALE_QUEUE_SIZE));
        let mut sender = SingleProducer::new(ring.clone());
        let mut receivers = (0..SCALE_CONSUMERS)
            .map(|_| Consumer::new_for_ring(ring.clone(), ConsumerMode::Blocking))
            .collect::<Vec<_>>();

        let consumers = (0..SCALE_CONSUMERS)
            .map(|_| {
                let mut receiver = receivers.pop().unwrap();
                std::thread::spawn(move || {
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
                            Err(TryRecvError::Empty) => {}
                        }
                    }
                    outputs.sort_unstable();
                    outputs.dedup();
                    assert_eq!(SCALE_MSG_COUNT, outputs.len());
                    for (i, v) in outputs.into_iter().enumerate() {
                        assert_eq!(i, v);
                    }
                })
            })
            .collect::<Vec<_>>();

        let producer = std::thread::spawn(move || {
            let mut _tries = 0;
            for i in 0..SCALE_MSG_COUNT {
                _tries += 1;
                while sender.try_push(i).is_err() {
                    _tries += 1;
                }
            }
        });

        producer.join().unwrap();
        for consumer in consumers {
            consumer.join().unwrap();
        }
    }

    #[test]
    fn mpmc() {
        let ring = Arc::new(RingBuffer::<usize>::new(SCALE_QUEUE_SIZE));
        let mut senders = (0..SCALE_PRODUCERS)
            .map(|_| ConcurrentProducer::new(ring.clone()))
            .collect::<Vec<_>>();
        let mut receivers = (0..SCALE_CONSUMERS)
            .map(|_| Consumer::new_for_ring(ring.clone(), ConsumerMode::Blocking))
            .collect::<Vec<_>>();

        let consumers = (0..SCALE_CONSUMERS)
            .map(|_| {
                let mut receiver = receivers.pop().unwrap();
                std::thread::spawn(move || {
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
                            Err(TryRecvError::Empty) => {}
                        }
                    }
                    outputs.sort_unstable();
                    outputs.dedup();
                    assert_eq!(SCALE_MSG_COUNT, outputs.len());
                    for (i, v) in outputs.into_iter().enumerate() {
                        assert_eq!(i, v);
                    }
                })
            })
            .collect::<Vec<_>>();

        let producers = (0..SCALE_PRODUCERS)
            .map(|p| {
                let mut sender = senders.pop().unwrap();
                std::thread::spawn(move || {
                    for i in 0..(SCALE_MSG_COUNT / SCALE_PRODUCERS) {
                        while sender.try_push((p * SCALE_MSG_COUNT / SCALE_PRODUCERS) + i).is_err() {}
                        println!("pushed {}", (p * SCALE_MSG_COUNT / SCALE_PRODUCERS) + i);
                    }
                })
            })
            .collect::<Vec<_>>();

        for producer in producers {
            producer.join().unwrap();
        }
        for consumer in consumers {
            consumer.join().unwrap();
        }
    }
}
