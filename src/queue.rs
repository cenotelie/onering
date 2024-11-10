/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Disruptor-inspired queue

use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::fmt::Debug;
use core::mem::MaybeUninit;
use core::ops::{Deref, Range};
use core::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

use crossbeam_utils::{Backoff, CachePadded};

use crate::errors::{TryRecvError, TrySendError};

/// A barrier to be waited on by either a producer or a consumer
#[derive(Debug)]
pub struct Barrier {
    /// The wrapped cusor
    cursor: CachePadded<AtomicIsize>,
}

impl Default for Barrier {
    fn default() -> Self {
        Self {
            cursor: CachePadded::new(AtomicIsize::new(-1)),
        }
    }
}

impl Barrier {
    /// Gets the published value using `Relaxed`
    #[must_use]
    #[inline]
    pub fn published(&self) -> isize {
        self.cursor.load(Ordering::Acquire)
    }

    /// Write to the barrier to publish an event
    #[inline]
    #[allow(clippy::cast_possible_wrap)]
    pub fn write(&self, value: usize) {
        self.cursor.store(value as isize, Ordering::Release);
    }
}

/// The user of a queue
pub trait QueueUser {
    /// Gets the barrier that can be awaited on for items that are available *after* this user
    fn publication_barrier(&self) -> Arc<Barrier>;
}

/// A producer which is unique to a queue
#[derive(Debug)]
pub struct SingleProducer<T> {
    /// The identifier of the next message to be inserted in the queue
    next: usize,
    /// The owned barrier used to signal when an item is published
    publish: Arc<Barrier>,
    /// The ring itself
    pub ring: Arc<RingBuffer<T>>,
}

impl<T> QueueUser for SingleProducer<T> {
    #[inline]
    fn publication_barrier(&self) -> Arc<Barrier> {
        self.publish.clone()
    }
}

impl<T> SingleProducer<T> {
    /// Gets the number of items in the queue
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let consumers_min = self.ring.get_last_seen_by_all_blocking_consumers(self.next);
        self.next - if consumers_min < 0 { 0 } else { consumers_min.unsigned_abs() }
    }

    /// Attempts to push a single item onto the queue
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    pub fn try_push(&mut self, item: T) -> Result<(), TrySendError<T>> {
        let consumers_min = self.ring.get_last_seen_by_all_blocking_consumers(self.next);
        let current_count = self.next - if consumers_min < 0 { 0 } else { consumers_min.unsigned_abs() };
        if current_count >= self.ring.buffer.len() {
            // buffer is full
            if self.ring.get_connected_consumers() == 0 {
                return Err(TrySendError::Disconnected(item));
            }
            return Err(TrySendError::Full(item));
        }
        // write
        self.ring.write_slot(self.next, item);
        self.publish.write(self.next);
        self.next += 1;
        Ok(())
    }

    /// Attempts to push multiple items coming from an iterator into the queue
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer when no item at all could be pushed
    pub fn try_push_iterator<I>(&mut self, provider: &mut I) -> Result<usize, TrySendError<()>>
    where
        I: Iterator<Item = T>,
    {
        let consumers_min = self.ring.get_last_seen_by_all_blocking_consumers(self.next);
        let current_count = self.next - if consumers_min < 0 { 0 } else { consumers_min.unsigned_abs() };
        let buffer_len = self.ring.buffer.len();
        if current_count >= buffer_len {
            // buffer is full
            if self.ring.get_connected_consumers() == 0 {
                return Err(TrySendError::Disconnected(()));
            }
            return Err(TrySendError::Full(()));
        }
        let free = buffer_len - current_count;
        let mut pushed = 0;
        for _ in 0..free {
            if let Some(item) = provider.next() {
                self.ring.write_slot(self.next, item);
                self.next += 1;
                pushed += 1;
            } else {
                break;
            }
        }
        if pushed == 0 {
            println!("nodata");
            Err(TrySendError::NoData)
        } else {
            self.publish.write(self.next);
            Ok(pushed)
        }
    }
}

/// A producer for a queue that can be concurrent with other (concurrent) producers
///
/// # Safety
///
/// Cloning this producer to get a new one is safe only after all consumers have been added.
/// Otherwise, some producer will not wait on all required consumers.
#[derive(Debug, Clone)]
pub struct ConcurrentProducer<T> {
    /// The identifier of the next message to be inserted in the queue
    next: Arc<CachePadded<AtomicUsize>>,
    /// The owned barrier used to signal when an item is published
    publish: Arc<Barrier>,
    /// The ring itself
    pub ring: Arc<RingBuffer<T>>,
}

impl<T> QueueUser for ConcurrentProducer<T> {
    #[inline]
    fn publication_barrier(&self) -> Arc<Barrier> {
        self.publish.clone()
    }
}

impl<T> ConcurrentProducer<T> {
    /// Gets the number of items in the queue
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let next = self.next.load(Ordering::Acquire);
        let consumers_min = self.ring.get_last_seen_by_all_blocking_consumers(next);
        next - if consumers_min < 0 { 0 } else { consumers_min.unsigned_abs() }
    }

    /// Attempts to push a single item onto the queue
    ///
    /// # Errors
    ///
    /// This returns a `TrySendError` when the queue is full or there no longer are any consumer
    pub fn try_push(&mut self, item: T) -> Result<(), TrySendError<T>> {
        let backoff = Backoff::new();
        let mut next = self.next.load(Ordering::Acquire);
        let consumers_min = self.ring.get_last_seen_by_all_blocking_consumers(next);

        loop {
            let current_count = next - if consumers_min < 0 { 0 } else { consumers_min.unsigned_abs() };
            if current_count >= self.ring.buffer.len() {
                // buffer is full
                if self.ring.get_connected_consumers() == 0 {
                    return Err(TrySendError::Disconnected(item));
                }
                return Err(TrySendError::Full(item));
            }

            // try to acquire
            if let Err(real_next) = self
                .next
                .compare_exchange_weak(next, next + 1, Ordering::AcqRel, Ordering::Relaxed)
            {
                next = real_next;
                backoff.spin(); // wait a bit
                continue;
            }

            // write
            self.ring.write_slot(next, item);

            // publish
            #[allow(clippy::cast_possible_wrap)]
            let next_signed = next as isize;
            while self
                .publish
                .cursor
                .compare_exchange_weak(next_signed - 1, next_signed, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                // wait for other producers to write and publish
                backoff.spin();
            }
            return Ok(());
        }
    }
}

/// The blocking mode for a consumer
/// Blocking consumers prevent producers from writing new items in the queue that would replace items not already seen by the consumer.
/// On the contrary, non-blocking consumers do not block producers, enabling producers to still write onto the queue.
/// Non-blocking consumers then run the risk of lagging behind. In that case trying to receive messages will produce `TryRecvError::Lagging`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerMode {
    /// In `Blocking` mode, a consumer is guaranteed to see all items from producers
    #[default]
    Blocking,
    /// In `NonBlocking` mode, a consumer may lag behind producers and may not see all items from producers
    NonBlocking,
}

/// A consumer of items from the queue
#[derive(Debug)]
pub struct Consumer<T> {
    /// Whether the consumer blocks producers
    mode: ConsumerMode,
    /// The identifier of the expected next message
    next: usize,
    /// The barrier the consumer is waiting on
    waiting_on: Arc<Barrier>,
    /// The owned barrier used to signal when an item has been handled by the consumer
    publish: Arc<Barrier>,
    /// The ring itself
    pub ring: Arc<RingBuffer<T>>,
    /// The value shared by all consumers, used to keep track of connected consumers
    _shared: Arc<usize>,
}

impl<T> QueueUser for Consumer<T> {
    #[inline]
    fn publication_barrier(&self) -> Arc<Barrier> {
        self.publish.clone()
    }
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        self.ring.add_consumer(self.waiting_on.clone(), Some(self.mode))
    }
}

/// An access to items from the queue through a consumer
pub struct ConsumerAccess<'a, T> {
    /// The parent consumer
    parent: &'a Consumer<T>,
    /// The identifier if the last item in this batch
    last_id: usize,
    /// The reference to the item itself
    items: &'a [T],
}

impl<'a, T> Deref for ConsumerAccess<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.items
    }
}

impl<'a, T> Drop for ConsumerAccess<'a, T> {
    fn drop(&mut self) {
        self.parent.publish.write(self.last_id);
    }
}

impl<T> Consumer<T> {
    /// Whether this consumer blocks producers
    /// By default, consumers block producers writing new items when they have not yet be seen.
    /// Setting a consumer as non-blocking enable producers to write event though the consumer may be lagging.
    #[must_use]
    pub fn blocking_mode(&self) -> ConsumerMode {
        self.mode
    }

    /// Gets the number of items in the queue accessible to this consumer
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let published = self.waiting_on.published();
        if published < 0 {
            // no item was pushed onto the queue
            return 0;
        }
        let published = published.unsigned_abs();
        if self.next > published {
            // no item for this consumer
            return 0;
        }
        published - self.next + 1
    }

    /// Attempts to receive a single item from the queue
    ///
    /// # Errors
    ///
    /// This returns a `TryRecvError` when the queue is empty, or when there is no longer any producer
    pub fn try_recv(&mut self) -> Result<ConsumerAccess<'_, T>, TryRecvError> {
        let published = self.waiting_on.published();
        if published < 0 {
            // no item was pushed onto the queue
            return Err(TryRecvError::Empty);
        }
        let published = published.unsigned_abs();
        if published < self.next {
            // still waiting
            return Err(TryRecvError::Empty);
        }
        if published >= self.next + self.ring.buffer.len() {
            // lagging
            return Err(TryRecvError::Lagging(published - self.next + 1));
        }
        // some item are ready
        let end_of_ring = self.next | self.ring.mask;
        let last_id = end_of_ring.min(published);
        #[allow(clippy::range_plus_one)]
        let items = self
            .ring
            .get_slots((self.next & self.ring.mask)..((last_id & self.ring.mask) + 1));
        self.next = last_id + 1;
        Ok(ConsumerAccess {
            parent: self,
            last_id,
            items,
        })
    }

    /// Attempts to receive a single item from the queue
    ///
    /// # Errors
    ///
    /// This returns a `TryRecvError` when the queue is empty, or when there is no longer any producer
    pub fn try_recv_copies(&mut self, buffer: &mut [T]) -> Result<usize, TryRecvError>
    where
        T: Copy,
    {
        let published = self.waiting_on.published();
        if published < 0 {
            // no item was pushed onto the queue
            return Err(TryRecvError::Empty);
        }
        let published = published.unsigned_abs();
        if published < self.next {
            // still waiting
            return Err(TryRecvError::Empty);
        }
        if published >= self.next + self.ring.buffer.len() {
            // lagging
            return Err(TryRecvError::Lagging(published - self.next + 1));
        }
        // some item are ready
        let end_of_buffer = self.next + buffer.len() - 1;
        let end_of_ring = self.next | self.ring.mask;
        let last_id = published.min(end_of_buffer).min(end_of_ring);
        let count = last_id - self.next + 1;
        #[allow(clippy::range_plus_one)]
        let items = self
            .ring
            .get_slots((self.next & self.ring.mask)..((last_id & self.ring.mask) + 1));
        buffer[..count].copy_from_slice(items);
        self.next = last_id + 1;
        self.publish.write(last_id);
        Ok(count)
    }
}

/// A circular queue to be accessed by producer(s) and consumers
#[derive(Debug)]
pub struct RingBuffer<T> {
    /// The buffer containing the items themselves
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// The mask to use for getting an index with the buffer
    pub(crate) mask: usize,
    /// The barrier enabling awaiting on the producer(s)
    producers_barrier: Arc<Barrier>,
    /// The value shared by all consumers, used to keep track of connected consumers
    consumers_shared: Arc<usize>,
    /// The barriers associated to consumers so that the queue can know when an item has been used by all consumers
    consumer_barriers: UnsafeCell<Vec<Arc<Barrier>>>,
}

/// SAFETY: The implementation guards the access to elements, this is fine for as long as `T` is itself `Sync`
unsafe impl<T> Sync for RingBuffer<T> where T: Sync {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        if core::mem::needs_drop::<T>() {
            // we need to drop all the items in the buffer
            let published = self.producers_barrier.published();
            if published < 0 {
                return;
            }
            let count = self.buffer.len().min(published.unsigned_abs() + 1);
            for slot in &mut self.buffer[..count] {
                unsafe {
                    slot.get_mut().assume_init_drop();
                }
            }
        }
    }
}

impl<T> RingBuffer<T> {
    /// Builds a single producer queue
    #[must_use]
    pub fn new_single_producer(queue_size: usize) -> SingleProducer<T> {
        let producer_barrier = Arc::new(Barrier::default());
        let ring = Arc::new(Self::new(producer_barrier.clone(), queue_size));
        SingleProducer {
            next: 0,
            publish: producer_barrier,
            ring,
        }
    }

    /// Builds a queue that support multiple producers
    #[must_use]
    pub fn new_multi_producers(queue_size: usize) -> ConcurrentProducer<T> {
        let producer_barrier = Arc::new(Barrier::default());
        let ring = Arc::new(Self::new(producer_barrier.clone(), queue_size));
        ConcurrentProducer {
            next: Arc::new(CachePadded::new(AtomicUsize::new(0))),
            publish: producer_barrier,
            ring,
        }
    }

    /// Creates a new buffer
    fn new(producers_barrier: Arc<Barrier>, queue_size: usize) -> Self {
        assert!(queue_size.is_power_of_two(), "size must be power of two");
        let buffer = (0..queue_size)
            .map(|_i| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Box<[_]>>();
        Self {
            buffer,
            mask: queue_size - 1,
            producers_barrier,
            consumers_shared: Arc::new(0),
            consumer_barriers: UnsafeCell::new(Vec::new()),
        }
    }

    /// Gets an access to the consumer publishing barriers
    fn get_consumer_barriers(&self) -> &Vec<Arc<Barrier>> {
        unsafe { &*self.consumer_barriers.get() }
    }

    /// Registers the publishing barrier of a consumer
    fn add_consumer_barrier(&self, barrier: Arc<Barrier>) {
        let consumer_barriers = unsafe { &mut *self.consumer_barriers.get() };
        consumer_barriers.push(barrier);
    }

    /// Gets an access to a slice of slots from the backing buffer
    #[inline]
    fn get_slots(&self, range: Range<usize>) -> &[T] {
        unsafe {
            (core::ptr::from_ref(self.buffer.get_unchecked(range)) as *const [T]) // assume init
                .as_ref()
                .unwrap()
        }
    }

    /// Overwrites an item to a slot
    #[inline]
    fn write_slot(&self, index: usize, item: T) {
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

    /// Adds a new consumer that directly wait on producers
    /// By default, consumers block producers writing new items when they have not yet be seen.
    /// Setting a consumer as non-blocking enable producers to write event though the consumer may be lagging.
    ///
    /// # Safety
    ///
    /// Adding a consumer is only safe BEFORE the producers start queueing items.
    pub fn add_consumer(self: &Arc<Self>, waiting_on: Arc<Barrier>, consumer_mode: Option<ConsumerMode>) -> Consumer<T> {
        let consumer_mode = consumer_mode.unwrap_or_default();
        let consumer_barrier = Arc::new(Barrier::default());
        self.add_consumer_barrier(consumer_barrier.clone());
        Consumer {
            mode: consumer_mode,
            next: 0,
            waiting_on,
            publish: consumer_barrier,
            ring: self.clone(),
            _shared: self.consumers_shared.clone(),
        }
    }

    /// Gets the number of connected producers
    #[must_use]
    #[inline]
    pub fn get_connected_producers(&self) -> usize {
        Arc::strong_count(&self.producers_barrier) - 1
    }

    /// Gets the number of connected consumers
    #[must_use]
    #[inline]
    pub fn get_connected_consumers(&self) -> usize {
        Arc::strong_count(&self.consumers_shared) - 1
    }

    /// Gets the last item that was seen by all blocking consumers
    ///
    /// # Safety
    ///
    /// This is safe for as long as no other thread is adding a consumer at the same time.
    #[must_use]
    #[inline]
    #[allow(clippy::cast_possible_wrap)]
    pub fn get_last_seen_by_all_blocking_consumers(&self, observer: usize) -> isize {
        self.get_consumer_barriers()
            .iter()
            .map(|b| b.published())
            .min()
            .unwrap_or(observer as isize)
    }
}

#[cfg(test)]
mod tests_init {
    use alloc::sync::Arc;

    use super::{Barrier, RingBuffer};

    #[test]
    fn size_power_of_two() {
        let _ring = RingBuffer::<usize>::new(Arc::new(Barrier::default()), 16);
    }

    #[test]
    #[should_panic(expected = "size must be power of two")]
    fn panic_on_non_power_of_two() {
        let _ring = RingBuffer::<usize>::new(Arc::new(Barrier::default()), 3);
    }
}

#[cfg(test)]
mod tests_drop {
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::{Barrier, RingBuffer};

    struct DropCallback(Box<dyn Fn()>);

    impl Drop for DropCallback {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    #[test]
    fn on_empty() {
        let _ring = RingBuffer::<DropCallback>::new(Arc::new(Barrier::default()), 16);
    }

    fn test_with_count(queue_size: usize, to_fill: usize, published: Option<usize>) {
        assert!(to_fill <= queue_size);
        if let Some(published) = published {
            assert!(published + 1 >= to_fill); // the cursor must be at least the number of filled slots
            if published >= queue_size {
                assert!(to_fill == queue_size);
            }
        } else {
            assert_eq!(to_fill, 0);
        }

        // create and fill the ring
        let mut ring = RingBuffer::<DropCallback>::new(Arc::new(Barrier::default()), queue_size);
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
            ring.producers_barrier.write(published);
        }

        // drop the ring
        drop(ring);
        assert_eq!(drop_count.load(Ordering::SeqCst), to_fill);
    }

    #[test]
    fn on_first_lap() {
        test_with_count(4, 0, None);
        test_with_count(4, 1, Some(0));
        test_with_count(4, 2, Some(1));
        test_with_count(4, 3, Some(2));
        test_with_count(4, 4, Some(3));
    }

    #[test]
    fn on_second_lap() {
        test_with_count(4, 4, Some(4));
        test_with_count(4, 4, Some(5));
        test_with_count(4, 4, Some(6));
        test_with_count(4, 4, Some(7));
        test_with_count(4, 4, Some(8));
    }
}
