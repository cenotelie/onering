/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! The consumers for the queue

use alloc::sync::Arc;
use core::ops::Deref;

use super::barriers::{Barrier, Output, OwnedOutput};
use super::ring::RingBuffer;
use super::{MultiBarrier, QueueUser, Sequence, SingleBarrier};
use crate::errors::TryRecvError;

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
pub struct Consumer<T, PO: Output + 'static, B> {
    /// The value shared by all consumers, used to keep track of connected consumers
    shared: Arc<usize>,
    /// Whether the consumer blocks producers
    mode: ConsumerMode,
    /// The expected next sequence
    next: usize,
    /// The barrier the consumer is waiting on
    waiting_on: B,
    /// The owned output used to signal when an item is published
    publish: Arc<OwnedOutput>,
    /// The ring itself
    pub(crate) ring: Arc<RingBuffer<T, PO>>,
}

impl<T, PO: Output + 'static, B> QueueUser for Consumer<T, PO, B> {
    type Item = T;
    type UserOutput = OwnedOutput;
    type ProducerOutput = PO;

    #[inline]
    fn queue(&self) -> &Arc<RingBuffer<Self::Item, Self::ProducerOutput>> {
        &self.ring
    }

    #[inline]
    fn output(&self) -> &Arc<Self::UserOutput> {
        &self.publish
    }
}

impl<T, PO: Output + 'static, B: Clone> Clone for Consumer<T, PO, B> {
    fn clone(&self) -> Self {
        let publish = Arc::new(OwnedOutput::new(self.publish.published().0));
        if self.mode == ConsumerMode::Blocking {
            self.ring.register_consumer_output(publish.clone());
        }
        Self {
            shared: self.shared.clone(),
            mode: self.mode,
            next: self.next,
            waiting_on: self.waiting_on.clone(),
            publish,
            ring: self.ring.clone(),
        }
    }
}

/// An access to items from the queue through a consumer
#[derive(Debug)]
pub struct ConsumerAccess<'a, T, PO: Output + 'static, B> {
    /// The parent consumer
    parent: &'a Consumer<T, PO, B>,
    /// The identifier if the last item in this batch
    last_id: Sequence,
    /// The reference to the item itself
    items: &'a [T],
    /// The next value to yield for the iterator
    next: usize,
}

impl<'a, T, PO: Output + 'static, B> Deref for ConsumerAccess<'a, T, PO, B> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.items
    }
}

impl<'a, T, PO: Output + 'static, B> Drop for ConsumerAccess<'a, T, PO, B> {
    fn drop(&mut self) {
        self.parent.publish.commit(self.last_id);
    }
}

impl<'a, T, PO: Output + 'static, B> Iterator for ConsumerAccess<'a, T, PO, B> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.items.len() {
            None
        } else {
            let index = self.next;
            self.next += 1;
            Some(&self.items[index])
        }
    }
}

impl<'a, T, PO: Output + 'static, B> ExactSizeIterator for ConsumerAccess<'a, T, PO, B> {
    fn len(&self) -> usize {
        self.items.len()
    }
}

impl<T, PO: Output + 'static> Consumer<T, PO, SingleBarrier<PO>> {
    /// Creates a new consumer that await for messages from all producers on a ring
    #[must_use]
    pub fn new(ring: Arc<RingBuffer<T, PO>>, mode: ConsumerMode) -> Self {
        let publish = Arc::new(OwnedOutput::default());
        if mode == ConsumerMode::Blocking {
            ring.register_consumer_output(publish.clone());
        }
        Self {
            shared: ring.consumers_shared.clone(),
            mode,
            next: 0,
            waiting_on: ring.producers_barrier.clone(),
            publish,
            ring,
        }
    }
}

impl<T, PO: Output + 'static> Consumer<T, PO, SingleBarrier<OwnedOutput>> {
    /// Creates a new consumer that awaits on a single other user, usually a consumer
    #[must_use]
    pub fn new_awaiting_on<U: QueueUser<Item = T, UserOutput = OwnedOutput, ProducerOutput = PO>>(
        other: &U,
        mode: ConsumerMode,
    ) -> Self {
        let ring = other.queue().clone();
        let publish = Arc::new(OwnedOutput::default());
        if mode == ConsumerMode::Blocking {
            ring.register_consumer_output(publish.clone());
        }
        Self {
            shared: ring.consumers_shared.clone(),
            mode,
            next: 0,
            waiting_on: SingleBarrier::await_on(other.output()),
            publish,
            ring,
        }
    }
}

impl<T, PO: Output + 'static> Consumer<T, PO, MultiBarrier<OwnedOutput>> {
    /// Creates a new consumer that awaits on multiple other users, usually consumers
    #[must_use]
    pub fn new_awaiting_multiple<'u, I>(others: I, mode: ConsumerMode) -> Self
    where
        I: IntoIterator<Item = &'u dyn QueueUser<Item = T, UserOutput = OwnedOutput, ProducerOutput = PO>>,
        T: 'u,
    {
        let mut ring = None;
        let outputs = others
            .into_iter()
            .map(|other| {
                ring.get_or_insert_with(|| other.queue().clone());
                other.output().clone()
            })
            .collect::<Vec<_>>();
        let ring = ring.unwrap();
        let publish = Arc::new(OwnedOutput::default());
        if mode == ConsumerMode::Blocking {
            ring.register_consumer_output(publish.clone());
        }
        Self {
            shared: ring.consumers_shared.clone(),
            mode,
            next: 0,
            waiting_on: MultiBarrier::await_on(outputs),
            publish,
            ring,
        }
    }
}

impl<T, PO: Output + 'static, B> Consumer<T, PO, B> {
    /// Whether this consumer blocks producers
    /// By default, consumers block producers writing new items when they have not yet be seen.
    /// Setting a consumer as non-blocking enable producers to write event though the consumer may be lagging.
    #[must_use]
    pub fn blocking_mode(&self) -> ConsumerMode {
        self.mode
    }
}

impl<T, PO: Output + 'static, B: Barrier> Consumer<T, PO, B> {
    /// Gets the number of items in the queue accessible to this consumer
    #[must_use]
    #[inline]
    pub fn get_number_of_items(&self) -> usize {
        let published = self.waiting_on.next(Sequence::from(self.next));
        if !published.is_valid_item() {
            // no item was pushed onto the queue
            return 0;
        }
        let published = published.as_index();
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
    pub fn try_recv(&mut self) -> Result<ConsumerAccess<'_, T, PO, B>, TryRecvError> {
        let published = self.waiting_on.next(Sequence::from(self.next));
        if !published.is_valid_item() {
            // no item was pushed onto the queue
            if self.ring.get_connected_producers() == 0 {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
        let published = published.as_index();
        if published < self.next {
            // still waiting
            if self.ring.get_connected_producers() == 0 {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
        if published >= self.next + self.ring.capacity() {
            // lagging
            self.next = published; // skip
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
            last_id: Sequence::from(last_id),
            items,
            next: 0,
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
        let published = self.waiting_on.next(Sequence::from(self.next));
        if !published.is_valid_item() {
            // no item was pushed onto the queue
            if self.ring.get_connected_producers() == 0 {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
        let published = published.as_index();
        if published < self.next {
            // still waiting
            if self.ring.get_connected_producers() == 0 {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
        if published >= self.next + self.ring.capacity() {
            // lagging
            self.next = published; // skip
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
        self.publish.commit(Sequence::from(last_id));
        Ok(count)
    }
}

#[cfg(test)]
mod test_recv {
    use alloc::sync::Arc;

    use super::{Consumer, ConsumerMode};
    use crate::errors::TryRecvError;
    use crate::queue::{Output, RingBuffer, Sequence};

    #[test]
    fn error_empty_on_ring_empty() {
        let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(4));
        let _fake_producer = ring.producers_shared.clone();
        let mut consumer = Consumer::new(ring, ConsumerMode::default());
        let error = consumer.try_recv().expect_err("expected error");
        assert_eq!(error, TryRecvError::Empty);
    }

    fn error_empty_on_expecting_value(published: isize, next: usize) {
        let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(4));
        let _fake_producer = ring.producers_shared.clone();
        let mut consumer = Consumer::new(ring, ConsumerMode::default());
        consumer.waiting_on.get_dependency().commit(Sequence::from(published));
        consumer.next = next;

        if published < 0 || published.unsigned_abs() < next {
            // empty
            let error = consumer.try_recv().expect_err("expected error");
            assert_eq!(error, TryRecvError::Empty);
            return;
        }
        let available = published.unsigned_abs() - next + 1;
        let is_lagging = available > 4 /* ring size */;
        if is_lagging {
            let error = consumer.try_recv().expect_err("expected error");
            assert_eq!(error, TryRecvError::Lagging(available));
        } else {
            assert!(
                consumer.try_recv().is_ok(),
                "unexpected error in published={published}, next={next}"
            );
        }
    }

    #[test]
    fn error_empty_on_expecting() {
        for published in -1..10_isize {
            for next in 0..10_usize {
                error_empty_on_expecting_value(published, next);
            }
        }
    }

    fn try_recv_slice_with(values: &[usize], published: isize, next: usize, expected: &[usize]) {
        let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(4));
        for (i, &v) in values.iter().enumerate() {
            ring.write_slot(i, v);
        }
        let _fake_producer = ring.producers_shared.clone();
        let mut consumer = Consumer::new(ring, ConsumerMode::default());
        consumer.waiting_on.get_dependency().commit(Sequence::from(published));
        consumer.next = next;

        let data = consumer.try_recv().unwrap();
        assert_eq!(expected.len(), data.len());
        for i in 0..expected.len() {
            assert_eq!(expected[i], data[i]);
        }
    }

    #[test]
    fn try_recv_slice() {
        // single value slice
        try_recv_slice_with(&[1, 2, 3, 4], 0, 0, &[1]);
        try_recv_slice_with(&[1, 2, 3, 4], 1, 1, &[2]);
        try_recv_slice_with(&[1, 2, 3, 4], 2, 2, &[3]);
        try_recv_slice_with(&[1, 2, 3, 4], 3, 3, &[4]);
        try_recv_slice_with(&[1, 2, 3, 4], 4, 4, &[1]);
        try_recv_slice_with(&[1, 2, 3, 4], 5, 5, &[2]);

        // longer slices
        try_recv_slice_with(&[1, 2, 3, 4], 1, 0, &[1, 2]);
        try_recv_slice_with(&[1, 2, 3, 4], 2, 0, &[1, 2, 3]);
        try_recv_slice_with(&[1, 2, 3, 4], 3, 0, &[1, 2, 3, 4]);
        try_recv_slice_with(&[1, 2, 3, 4], 3, 2, &[3, 4]);
        try_recv_slice_with(&[1, 2, 3, 4], 6, 4, &[1, 2, 3]);

        // up to the end of the ring
        try_recv_slice_with(&[1, 2, 3, 4], 5, 2, &[3, 4]);
        try_recv_slice_with(&[1, 2, 3, 4], 9, 6, &[3, 4]);
    }

    fn try_recv_copies_with(values: &[usize], published: isize, next: usize, expected: &[usize]) {
        let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(values.len()));
        for (i, &v) in values.iter().enumerate() {
            ring.write_slot(i, v);
        }
        let _fake_producer = ring.producers_shared.clone();
        let mut consumer = Consumer::new(ring, ConsumerMode::default());
        consumer.waiting_on.get_dependency().commit(Sequence::from(published));
        consumer.next = next;

        let mut buffer = vec![0; 4];
        let len = consumer.try_recv_copies(&mut buffer).unwrap();
        assert_eq!(expected.len(), len);
        for i in 0..expected.len() {
            assert_eq!(expected[i], buffer[i]);
        }
    }

    #[test]
    fn try_recv_copies() {
        // single value slice
        try_recv_copies_with(&[1, 2, 3, 4], 0, 0, &[1]);
        try_recv_copies_with(&[1, 2, 3, 4], 1, 1, &[2]);
        try_recv_copies_with(&[1, 2, 3, 4], 2, 2, &[3]);
        try_recv_copies_with(&[1, 2, 3, 4], 3, 3, &[4]);
        try_recv_copies_with(&[1, 2, 3, 4], 4, 4, &[1]);
        try_recv_copies_with(&[1, 2, 3, 4], 5, 5, &[2]);

        // longer slices
        try_recv_copies_with(&[1, 2, 3, 4], 1, 0, &[1, 2]);
        try_recv_copies_with(&[1, 2, 3, 4], 2, 0, &[1, 2, 3]);
        try_recv_copies_with(&[1, 2, 3, 4], 3, 0, &[1, 2, 3, 4]);
        try_recv_copies_with(&[1, 2, 3, 4], 3, 2, &[3, 4]);
        try_recv_copies_with(&[1, 2, 3, 4], 6, 4, &[1, 2, 3]);

        // up to the end of the ring
        try_recv_copies_with(&[1, 2, 3, 4], 5, 2, &[3, 4]);
        try_recv_copies_with(&[1, 2, 3, 4], 9, 6, &[3, 4]);
    }
}
