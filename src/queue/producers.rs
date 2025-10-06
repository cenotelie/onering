/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! The producers for the queue

use alloc::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

use super::barriers::{Output, OwnedOutput, SharedOutput};
use super::ring::RingBuffer;
use super::{QueueUser, Sequence};
use crate::errors::TrySendError;
use crate::queue::{SpinWaitStrategy, WaitStrategy};
use crate::utils::Phantom;

/// A single producer that will be the only producer for a queue
#[derive(Debug)]
pub struct SingleProducer<T> {
    /// The value shared by all producers, used to keep track of connected producers
    _shared_next: Arc<CachePadded<AtomicUsize>>,
    /// The identifier of the next message to be inserted in the queue
    next: usize,
    /// The owned output used to signal when an item is published
    publish: Arc<OwnedOutput>,
    /// The cache for the consumer's last published sequence
    cache_last_seq: Sequence,
    /// The ring itself
    pub(crate) ring: Arc<RingBuffer<T, OwnedOutput>>,
}

impl<T> QueueUser for SingleProducer<T> {
    type Item = T;
    type UserOutput = OwnedOutput;
    type ProducerOutput = OwnedOutput;

    #[inline]
    fn queue(&self) -> &Arc<RingBuffer<Self::Item, Self::ProducerOutput>> {
        &self.ring
    }

    #[inline]
    fn output(&self) -> &Arc<Self::UserOutput> {
        &self.publish
    }
}

impl<T> SingleProducer<T> {
    /// Creates the producer for a ring
    #[must_use]
    pub fn new(ring: Arc<RingBuffer<T, OwnedOutput>>) -> Self {
        assert_eq!(
            Arc::strong_count(&ring.producers_shared),
            1,
            "another producer is attached to the ring"
        );
        Self {
            _shared_next: ring.producers_shared.clone(),
            next: 0,
            publish: ring.producers_barrier.get_dependency().clone(),
            cache_last_seq: Sequence::default(),
            ring,
        }
    }

    /// Gets the number of items in the queue
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let last_seq = self.ring.get_next_after_all_consumers(Sequence::from(self.next));
        if last_seq.is_valid_item() {
            self.next - last_seq.as_index() - 1
        } else {
            self.next
        }
    }

    /// Attempts to push a single item onto the queue
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    pub fn try_push(&mut self, item: T) -> Result<(), TrySendError<T>> {
        let last_seq = self
            .ring
            .get_next_after_all_consumers_with_cache(Sequence::from(self.next), &mut self.cache_last_seq);
        let current_count = if last_seq.is_valid_item() {
            self.next - last_seq.as_index() - 1
        } else {
            self.next
        };
        if current_count >= self.ring.capacity() {
            // buffer is full
            if self.ring.get_connected_consumers() == 0 {
                return Err(TrySendError::Disconnected(item));
            }
            return Err(TrySendError::Full(item));
        }
        // write
        self.ring.write_slot(self.next, item);
        self.publish.commit(Sequence::from(self.next));
        self.next += 1;
        Ok(())
    }

    /// Attempts to push multiple items onto the queues when `T` is `Copy`
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    pub fn try_push_copies(&mut self, items: &[T]) -> Result<usize, TrySendError<()>>
    where
        T: Copy,
    {
        let last_seq = self
            .ring
            .get_next_after_all_consumers_with_cache(Sequence::from(self.next), &mut self.cache_last_seq);
        let current_count = if last_seq.is_valid_item() {
            self.next - last_seq.as_index() - 1
        } else {
            self.next
        };
        if current_count >= self.ring.capacity() {
            // buffer is full
            if self.ring.get_connected_consumers() == 0 {
                return Err(TrySendError::Disconnected(()));
            }
            return Err(TrySendError::Full(()));
        }
        // compute the number of items to write
        let available = self.ring.capacity() - current_count;
        let to_write = available.min(items.len());
        if to_write == 0 {
            return Err(TrySendError::NoData);
        }
        // write
        for (i, item) in items[..to_write].iter().enumerate() {
            self.ring.write_slot(self.next + i, *item);
        }
        self.publish.commit(Sequence::from(self.next + to_write - 1));
        self.next += to_write;
        Ok(to_write)
    }
}

#[cfg(test)]
mod tests_single {
    use alloc::sync::Arc;

    use super::SingleProducer;
    use crate::errors::TrySendError;
    use crate::queue::Sequence;
    use crate::queue::barriers::{Output, OwnedOutput};
    use crate::queue::ring::RingBuffer;

    #[test]
    fn nb_of_items_no_consumer() {
        let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(4, 16));
        let mut producer = SingleProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        producer.next = 1;
        assert_eq!(producer.get_number_of_items(), 1);
        producer.next = 2;
        assert_eq!(producer.get_number_of_items(), 2);
        producer.next = 3;
        assert_eq!(producer.get_number_of_items(), 3);
        producer.next = 4;
        assert_eq!(producer.get_number_of_items(), 4);
    }

    #[test]
    fn nb_of_items_with_consumer() {
        let ring = RingBuffer::<usize, _>::new_single_producer(4, 16);
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let mut producer = SingleProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        producer.next = 1;
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(0_isize));
        assert_eq!(producer.get_number_of_items(), 0);
        producer.next = 2;
        assert_eq!(producer.get_number_of_items(), 1);
        producer.next = 3;
        assert_eq!(producer.get_number_of_items(), 2);
        producer.next = 4;
        assert_eq!(producer.get_number_of_items(), 3);
        consumer_output.commit(Sequence::from(1_isize));
        assert_eq!(producer.get_number_of_items(), 2);
        consumer_output.commit(Sequence::from(2_isize));
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(3_isize));
        assert_eq!(producer.get_number_of_items(), 0);
    }

    #[test]
    fn try_push_until_full() {
        let ring = RingBuffer::<usize, _>::new_single_producer(4, 16);
        let mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output).unwrap();
        let ring = Arc::new(ring);
        let mut producer = SingleProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        assert_eq!(producer.try_push(0), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 2);
        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(2_usize));
        assert_eq!(producer.get_number_of_items(), 3);
        assert_eq!(producer.try_push(3), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(3_usize));
        assert_eq!(producer.get_number_of_items(), 4);
        assert_eq!(producer.try_push(4), Err(TrySendError::Full(4)));
        drop(mock_consumer_shared);
        assert_eq!(producer.try_push(4), Err(TrySendError::Disconnected(4)));
    }

    #[test]
    fn try_push_on_full() {
        let ring = RingBuffer::<usize, _>::new_single_producer(4, 16);
        let mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output).unwrap();
        let ring = Arc::new(ring);
        let mut producer = SingleProducer::new(ring);

        producer.next = 4;
        assert_eq!(producer.get_number_of_items(), 4);
        let r = producer.try_push(0);
        assert_eq!(r, Err(TrySendError::Full(0)));

        drop(mock_consumer_shared);
        let r = producer.try_push(0);
        assert_eq!(r, Err(TrySendError::Disconnected(0)));
    }

    #[test]
    fn try_push_with_consumer() {
        let ring = RingBuffer::<usize, _>::new_single_producer(4, 16);
        let _mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let mut producer = SingleProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        assert_eq!(producer.try_push(0), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 2);

        consumer_output.commit(Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 0);

        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(2_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(3), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(3_usize));
        assert_eq!(producer.get_number_of_items(), 2);
        assert_eq!(producer.try_push(4), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(4_usize));
        assert_eq!(producer.get_number_of_items(), 3);
        assert_eq!(producer.try_push(5), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(5_usize));
        assert_eq!(producer.get_number_of_items(), 4);
    }
}

/// A producer for a queue that can be concurrent with other (concurrent) producers
#[derive(Debug)]
pub struct ConcurrentProducer<T, WS: WaitStrategy = SpinWaitStrategy> {
    _use_rs: Phantom<WS>,
    /// The identifier of the next message to be inserted in the queue
    shared_next: Arc<CachePadded<AtomicUsize>>,
    /// The owned output used to signal when an item is published
    publish: Arc<SharedOutput>,
    /// The cache for the consumer's last published sequence
    cache_last_seq: Sequence,
    /// The ring itself
    pub(crate) ring: Arc<RingBuffer<T, SharedOutput>>,
}

impl<T, WS: WaitStrategy> Clone for ConcurrentProducer<T, WS> {
    fn clone(&self) -> Self {
        Self {
            _use_rs: Phantom::default(),
            shared_next: self.shared_next.clone(),
            publish: self.publish.clone(),
            cache_last_seq: self.cache_last_seq,
            ring: self.ring.clone(),
        }
    }
}

impl<T, WS: WaitStrategy> QueueUser for ConcurrentProducer<T, WS> {
    type Item = T;
    type UserOutput = SharedOutput;
    type ProducerOutput = SharedOutput;

    #[inline]
    fn queue(&self) -> &Arc<RingBuffer<Self::Item, Self::ProducerOutput>> {
        &self.ring
    }

    #[inline]
    fn output(&self) -> &Arc<SharedOutput> {
        &self.publish
    }
}

impl<T> ConcurrentProducer<T> {
    /// Creates a producer for a ring
    #[must_use]
    pub fn new(ring: Arc<RingBuffer<T, SharedOutput>>) -> ConcurrentProducer<T> {
        Self {
            _use_rs: Phantom::default(),
            shared_next: ring.producers_shared.clone(),
            publish: ring.producers_barrier.get_dependency().clone(),
            cache_last_seq: Sequence::default(),
            ring,
        }
    }
}

impl<T, WS: WaitStrategy> ConcurrentProducer<T, WS> {
    /// Creates a producer for a ring
    #[must_use]
    pub fn new_with_strategy(ring: Arc<RingBuffer<T, SharedOutput>>) -> ConcurrentProducer<T, WS> {
        Self {
            _use_rs: Phantom::default(),
            shared_next: ring.producers_shared.clone(),
            publish: ring.producers_barrier.get_dependency().clone(),
            cache_last_seq: Sequence::default(),
            ring,
        }
    }

    /// Gets the number of items in the queue
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        self.ring.get_number_of_items()
    }

    /// Attempts to push a single item onto the queue
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    pub fn try_push(&mut self, item: T) -> Result<(), TrySendError<T>> {
        let backoff = WS::default();
        let mut next = self.shared_next.load(Ordering::Acquire);
        let last_seq = self
            .ring
            .get_next_after_all_consumers_with_cache(Sequence::from(next), &mut self.cache_last_seq);

        loop {
            let current_count = if last_seq.is_valid_item() {
                let last_seq_index = last_seq.as_index();
                if last_seq_index >= next {
                    // this producer is waaaay late
                    next = self.shared_next.load(Ordering::Acquire);
                    continue;
                }
                next - last_seq_index - 1
            } else {
                next
            };
            if current_count >= self.ring.capacity() {
                // buffer is full
                if self.ring.get_connected_consumers() == 0 {
                    return Err(TrySendError::Disconnected(item));
                }
                return Err(TrySendError::Full(item));
            }

            // try to acquire
            if let Err(real_next) = self
                .shared_next
                .compare_exchange(next, next + 1, Ordering::AcqRel, Ordering::Relaxed)
            {
                next = real_next;
                backoff.wait(); // wait a bit
                continue;
            }

            // write
            self.ring.write_slot(next, item);

            // publish
            #[allow(clippy::cast_possible_wrap)]
            let next_signed = next as isize;
            while self
                .publish
                .try_commit(Sequence::from(next_signed - 1), Sequence::from(next_signed))
                .is_err()
            {
                // wait for other producers to write and publish
            }
            return Ok(());
        }
    }

    /// Attempts to push multiple items onto the queues when `T` is `Copy`
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    #[allow(clippy::cast_possible_wrap)]
    pub fn try_push_copies(&mut self, items: &[T]) -> Result<usize, TrySendError<()>>
    where
        T: Copy,
    {
        let backoff = WS::default();
        let mut next = self.shared_next.load(Ordering::Acquire);
        let last_seq = self
            .ring
            .get_next_after_all_consumers_with_cache(Sequence::from(next), &mut self.cache_last_seq);

        loop {
            let current_count = if last_seq.is_valid_item() {
                let last_seq_index = last_seq.as_index();
                if last_seq_index >= next {
                    // this producer is waaaay late
                    next = self.shared_next.load(Ordering::Acquire);
                    continue;
                }
                next - last_seq_index - 1
            } else {
                next
            };
            if current_count >= self.ring.capacity() {
                // buffer is full
                if self.ring.get_connected_consumers() == 0 {
                    return Err(TrySendError::Disconnected(()));
                }
                return Err(TrySendError::Full(()));
            }

            // compute the number of items to write
            let available = self.ring.capacity() - current_count;
            let to_write = available.min(items.len());
            if to_write == 0 {
                return Err(TrySendError::NoData);
            }

            // try to acquire
            if let Err(real_next) =
                self.shared_next
                    .compare_exchange(next, next + to_write, Ordering::AcqRel, Ordering::Relaxed)
            {
                next = real_next;
                backoff.wait(); // wait a bit
                continue;
            }

            // write
            for (i, item) in items[..to_write].iter().enumerate() {
                self.ring.write_slot(next + i, *item);
            }

            // publish
            let next_signed = next as isize;
            while self
                .publish
                .try_commit(
                    Sequence::from(next_signed - 1),
                    Sequence::from(next_signed + to_write as isize - 1),
                )
                .is_err()
            {
                // wait for other producers to write and publish
            }
            return Ok(to_write);
        }
    }
}

#[cfg(test)]
mod tests_concurrent {
    use alloc::sync::Arc;
    use core::sync::atomic::Ordering;

    use super::ConcurrentProducer;
    use crate::errors::TrySendError;
    use crate::queue::Sequence;
    use crate::queue::barriers::{Output, OwnedOutput};
    use crate::queue::ring::RingBuffer;

    #[test]
    fn nb_of_items_no_consumer() {
        let ring = Arc::new(RingBuffer::<usize, _>::new_multi_producer(4, 16));
        let producer = ConcurrentProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        producer.shared_next.store(1, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 1);
        producer.shared_next.store(2, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 2);
        producer.shared_next.store(3, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 3);
        producer.shared_next.store(4, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 4);
    }

    #[test]
    fn nb_of_items_with_consumer() {
        let ring = RingBuffer::<usize, _>::new_multi_producer(4, 16);
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let producer = ConcurrentProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        producer.shared_next.store(1, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(0_isize));
        assert_eq!(producer.get_number_of_items(), 0);
        producer.shared_next.store(2, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 1);
        producer.shared_next.store(3, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 2);
        producer.shared_next.store(4, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 3);
        consumer_output.commit(Sequence::from(1_isize));
        assert_eq!(producer.get_number_of_items(), 2);
        consumer_output.commit(Sequence::from(2_isize));
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(3_isize));
        assert_eq!(producer.get_number_of_items(), 0);
    }

    #[test]
    fn try_push_until_full() {
        let ring = RingBuffer::<usize, _>::new_multi_producer(4, 16);
        let mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let mut producer = ConcurrentProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        assert_eq!(producer.try_push(0), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 2);
        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(2_usize));
        assert_eq!(producer.get_number_of_items(), 3);
        assert_eq!(producer.try_push(3), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(3_usize));
        assert_eq!(producer.get_number_of_items(), 4);
        assert_eq!(producer.try_push(4), Err(TrySendError::Full(4)));
        drop(mock_consumer_shared);
        assert_eq!(producer.try_push(4), Err(TrySendError::Disconnected(4)));
    }

    #[test]
    fn try_push_on_full() {
        let ring = RingBuffer::<usize, _>::new_multi_producer(4, 16);
        let mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let mut producer = ConcurrentProducer::new(ring);

        producer.shared_next.store(4, Ordering::Relaxed);
        assert_eq!(producer.get_number_of_items(), 4);
        let r = producer.try_push(0);
        assert_eq!(r, Err(TrySendError::Full(0)));

        drop(mock_consumer_shared);
        let r = producer.try_push(0);
        assert_eq!(r, Err(TrySendError::Disconnected(0)));
    }

    #[test]
    fn try_push_with_consumer() {
        let ring = RingBuffer::<usize, _>::new_multi_producer(4, 16);
        let _mock_consumer_shared = ring.consumers_shared.clone();
        let consumer_output = Arc::new(OwnedOutput::new(-1_isize));
        ring.register_consumer_output(consumer_output.clone()).unwrap();
        let ring = Arc::new(ring);
        let mut producer = ConcurrentProducer::new(ring);

        assert_eq!(producer.get_number_of_items(), 0);
        assert_eq!(producer.try_push(0), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 2);

        consumer_output.commit(Sequence::from(0_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        consumer_output.commit(Sequence::from(1_usize));
        assert_eq!(producer.get_number_of_items(), 0);

        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(2_usize));
        assert_eq!(producer.get_number_of_items(), 1);
        assert_eq!(producer.try_push(3), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(3_usize));
        assert_eq!(producer.get_number_of_items(), 2);
        assert_eq!(producer.try_push(4), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(4_usize));
        assert_eq!(producer.get_number_of_items(), 3);
        assert_eq!(producer.try_push(5), Ok(()));
        assert_eq!(producer.publish.published(), Sequence::from(5_usize));
        assert_eq!(producer.get_number_of_items(), 4);
    }
}
