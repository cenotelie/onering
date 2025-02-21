/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! The ring buffer for the queue

use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::ops::Range;
use core::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;

use super::barriers::{Barrier, MultiBarrier, Output, OwnedOutput, SharedOutput, SingleBarrier};
use super::Sequence;
use crate::errors::TooManyConsumers;

/// A circular queue to be accessed by producer(s) and consumers
#[derive(Debug)]
pub struct RingBuffer<T, PO: Output + 'static> {
    /// The buffer containing the items themselves
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// The mask to use for getting an index with the buffer
    pub(crate) mask: usize,
    /// The value shared by all producers, used to keep track of connected producers
    pub(crate) producers_shared: Arc<CachePadded<AtomicUsize>>,
    /// The value shared by all consumers, used to keep track of connected consumers
    pub(crate) consumers_shared: Arc<usize>,
    /// The barrier enabling awaiting on the producer(s)
    pub(crate) producers_barrier: SingleBarrier<PO>,
    /// The barriers associated to consumers so that the queue can know when an item has been used by all consumers
    consumers_barrier: MultiBarrier<OwnedOutput>,
}

/// SAFETY: The implementation guards the access to elements, this is fine for as long as `T` is itself `Sync`
unsafe impl<T, PO: Output + 'static> Sync for RingBuffer<T, PO> where T: Sync {}

impl<T, PO: Output + 'static> Drop for RingBuffer<T, PO> {
    fn drop(&mut self) {
        if core::mem::needs_drop::<T>() {
            // we need to drop all the items in the buffer
            let published = self.producers_barrier.next(Sequence::default());
            if !published.is_valid_item() {
                return;
            }
            let count = self.buffer.len().min(published.as_index() + 1);
            for slot in &mut self.buffer[..count] {
                unsafe {
                    slot.get_mut().assume_init_drop();
                }
            }
        }
    }
}

impl<T> RingBuffer<T, OwnedOutput> {
    /// Creates a new ring buffer that can only have a single producer
    #[must_use]
    pub fn new_single_producer(queue_size: usize, max_consumers: usize) -> Self {
        Self::new(queue_size, OwnedOutput::default(), max_consumers)
    }
}

impl<T> RingBuffer<T, SharedOutput> {
    /// Creates a new ring buffer that supports multiple producers
    #[must_use]
    pub fn new_multi_producer(queue_size: usize, max_consumers: usize) -> Self {
        Self::new(queue_size, SharedOutput::default(), max_consumers)
    }
}

impl<T, PO: Output + 'static> RingBuffer<T, PO> {
    /// Creates a new ring buffer
    #[must_use]
    fn new(queue_size: usize, producers_output: PO, max_consumers: usize) -> Self {
        assert!(queue_size.is_power_of_two(), "size must be power of two");
        let buffer = (0..queue_size)
            .map(|_i| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Box<[_]>>();
        Self {
            buffer,
            mask: queue_size - 1,
            producers_shared: Arc::new(CachePadded::new(AtomicUsize::new(0))),
            consumers_shared: Arc::new(0),
            producers_barrier: SingleBarrier::await_on(&Arc::new(producers_output)),
            consumers_barrier: MultiBarrier::new(max_consumers),
        }
    }

    /// Gets the capacity of the ring
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Gets an access to a slice of slots from the backing buffer
    #[inline]
    pub(crate) fn get_slots(&self, range: Range<usize>) -> &[T] {
        debug_assert!(range.end <= self.buffer.len());
        unsafe {
            (core::ptr::from_ref(self.buffer.get_unchecked(range)) as *const [T]) // assume init
                .as_ref()
                .unwrap()
        }
    }

    /// Overwrites an item to a slot
    #[inline]
    pub(crate) fn write_slot(&self, index: usize, item: T) {
        debug_assert!(index & self.mask < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index & self.mask).get() };
        if core::mem::needs_drop::<T>() && index >= self.buffer.len() {
            // drop the previous value
            unsafe {
                slot.as_mut().unwrap().assume_init_drop();
            }
        }
        unsafe {
            slot.write_volatile(MaybeUninit::new(item));
        }
    }

    /// Register the output of a new consumer so that it can be correctly awaited on by the producers
    pub(crate) fn register_consumer_output(&self, output: Arc<OwnedOutput>) -> Result<(), TooManyConsumers> {
        self.consumers_barrier
            .add_dependency(output)
            .map_err(|_| TooManyConsumers(self.consumers_barrier.max_dependencies))
    }

    /// Unregisters the output of a consumer
    pub(crate) fn unregister_consumer_output(&self, output: &Arc<OwnedOutput>) {
        self.consumers_barrier.remove_dependency(output);
    }

    /// Gets the number of connected producers
    #[must_use]
    #[inline]
    pub fn get_connected_producers(&self) -> usize {
        Arc::strong_count(&self.producers_shared) - 1
    }

    /// Gets the number of connected consumers
    #[must_use]
    #[inline]
    pub fn get_connected_consumers(&self) -> usize {
        Arc::strong_count(&self.consumers_shared) - 1
    }

    /// Gets the next item that was seen by all consumers
    ///
    /// # Safety
    ///
    /// This is safe for as long as no other thread is adding a consumer at the same time.
    #[must_use]
    #[inline]
    pub(crate) fn get_next_after_all_consumers(&self, observer: Sequence) -> Sequence {
        self.consumers_barrier.next(observer)
    }

    /// Gets the next item that was seen by all consumers
    ///
    /// # Safety
    ///
    /// This is safe for as long as no other thread is adding a consumer at the same time.
    #[must_use]
    pub(crate) fn get_next_after_all_consumers_with_cache(&self, observer: Sequence, cache: &mut Sequence) -> Sequence {
        let current_count = if cache.is_valid_item() {
            observer.as_index() - cache.as_index() - 1
        } else {
            observer.as_index()
        };
        if current_count >= self.capacity() {
            *cache = self.consumers_barrier.next(observer);
        }
        *cache
    }
}

impl<T> RingBuffer<T, SharedOutput> {
    /// Gets the number of items in the queue
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let mut next = self.producers_shared.load(Ordering::Acquire);
        let last_seq = self.get_next_after_all_consumers(Sequence::from(next));
        loop {
            if last_seq.is_valid_item() {
                let last_seq_index = last_seq.as_index();
                if last_seq_index < next {
                    return next - last_seq_index - 1;
                }
                // this producer is waaaay late, reload
                next = self.producers_shared.load(Ordering::Acquire);
            } else {
                return next;
            }
        }
    }
}

#[cfg(test)]
mod tests_init {
    use super::RingBuffer;

    #[test]
    fn size_power_of_two() {
        let _ring = RingBuffer::<usize, _>::new_single_producer(16, 16);
    }

    #[test]
    #[should_panic(expected = "size must be power of two")]
    fn panic_on_non_power_of_two() {
        let _ring = RingBuffer::<usize, _>::new_single_producer(3, 16);
    }
}

#[cfg(test)]
mod tests_drop {
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::RingBuffer;
    use crate::queue::{Output, Sequence};

    struct DropCallback(Box<dyn Fn()>);

    impl Drop for DropCallback {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    #[test]
    fn on_empty() {
        let _ring = RingBuffer::<DropCallback, _>::new_single_producer(16, 16);
    }

    fn test_with_count(queue_size: usize, to_fill: usize, published: Option<Sequence>) {
        assert!(to_fill <= queue_size);
        if let Some(published) = published {
            let published_index = published.as_index();
            assert!(published_index + 1 >= to_fill); // the cursor must be at least the number of filled slots
            if published_index >= queue_size {
                assert!(to_fill == queue_size);
            }
        } else {
            assert_eq!(to_fill, 0);
        }

        // create and fill the ring
        let mut ring = RingBuffer::<DropCallback, _>::new_single_producer(queue_size, 16);
        let drop_count = Arc::new(AtomicUsize::new(0));
        for i in 0..to_fill {
            ring.buffer[i].get_mut().write(DropCallback(Box::new({
                let drop_count = drop_count.clone();
                move || {
                    drop_count.fetch_add(1, Ordering::SeqCst);
                }
            })));
        }
        if let Some(published) = published {
            ring.producers_barrier.get_dependency().commit(published);
        }

        // drop the ring
        drop(ring);
        assert_eq!(drop_count.load(Ordering::SeqCst), to_fill);
    }

    #[test]
    fn on_first_lap() {
        test_with_count(4, 0, None);
        test_with_count(4, 1, Some(Sequence::from(0_isize)));
        test_with_count(4, 2, Some(Sequence::from(1_isize)));
        test_with_count(4, 3, Some(Sequence::from(2_isize)));
        test_with_count(4, 4, Some(Sequence::from(3_isize)));
    }

    #[test]
    fn on_second_lap() {
        test_with_count(4, 4, Some(Sequence::from(4_isize)));
        test_with_count(4, 4, Some(Sequence::from(5_isize)));
        test_with_count(4, 4, Some(Sequence::from(6_isize)));
        test_with_count(4, 4, Some(Sequence::from(7_isize)));
        test_with_count(4, 4, Some(Sequence::from(8_isize)));
    }

    #[test]
    fn drop_on_write() {
        let ring = RingBuffer::<DropCallback, _>::new_single_producer(4, 16);
        let drop_count = Arc::new(AtomicUsize::new(0));

        // fill the buffer
        for i in 0..4 {
            ring.write_slot(
                i,
                DropCallback(Box::new({
                    let drop_count = drop_count.clone();
                    move || {
                        drop_count.fetch_add(1, Ordering::SeqCst);
                    }
                })),
            );
        }
        ring.producers_barrier.get_dependency().commit(Sequence::from(0_usize));
        assert_eq!(drop_count.load(Ordering::SeqCst), 0);

        // overwrite index 0
        ring.write_slot(4, DropCallback(Box::new(|| {})));
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    }
}
