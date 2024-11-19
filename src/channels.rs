/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Synchronous channels backed by a ring buffer

use alloc::sync::Arc;

use crossbeam_utils::Backoff;

use crate::errors::{RecvError, SendError, TryRecvError, TrySendError};
use crate::queue::{
    ConcurrentProducer, Consumer, ConsumerAccess, ConsumerMode, Output, OwnedOutput, RingBuffer, SharedOutput, SingleBarrier,
    SingleProducer,
};

/// A receiver for a channel
#[derive(Debug)]
pub struct Receiver<T, PO: Output + 'static> {
    consumer: Consumer<T, PO, SingleBarrier<PO>>,
}

impl<T, PO: Output + 'static> Clone for Receiver<T, PO> {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer.clone(),
        }
    }
}

impl<T, PO: Output + 'static> Receiver<T, PO> {
    /// Gets whether the other side is disconnected
    #[must_use]
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.consumer.ring.get_connected_producers() == 0
    }

    /// Gets the capacity of the channel
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.consumer.ring.capacity()
    }

    /// Gets the number of items in the channel
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.consumer.get_number_of_items()
    }

    /// Gets whether the channel is empty
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.consumer.get_number_of_items() == 0
    }

    /// Gets whether the channel is full
    #[must_use]
    #[inline]
    pub fn is_full(&self) -> bool {
        self.consumer.get_number_of_items() == (self.consumer.ring.mask + 1)
    }

    /// Attempts to receive a single item from to the channel
    /// An item will be returned for as long as there are items in the channel, even if senders are not connected.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel is empty, or no sender is connected
    #[inline]
    pub fn try_recv(&mut self) -> Result<ConsumerAccess<'_, T, PO, SingleBarrier<PO>>, TryRecvError> {
        self.consumer.try_recv()
    }

    /// Blocks while waiting for the next item
    /// An item will be returned for as long as there are items in the channel, even if senders are not connected.
    ///
    /// # Errors
    ///
    /// Returns an error when no sender is connected
    pub fn recv(&mut self) -> Result<ConsumerAccess<'_, T, PO, SingleBarrier<PO>>, RecvError> {
        let backoff = Backoff::new();
        loop {
            match self.consumer.try_recv() {
                Ok(items) => {
                    return unsafe {
                        // SAFETY: transmute the lifetime due to compiler issues
                        Ok(core::mem::transmute::<
                            ConsumerAccess<'_, T, PO, SingleBarrier<PO>>,
                            ConsumerAccess<'_, T, PO, SingleBarrier<PO>>,
                        >(items))
                    };
                }
                Err(TryRecvError::Empty) => {
                    backoff.snooze();
                }
                Err(TryRecvError::Lagging(count)) => return Err(RecvError::Lagging(count)),
                Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
            }
        }
    }

    /// Disconnects this receiver by dropping it
    pub fn disconnect(self) {}
}

/// Common traits for all kind of sender
pub trait Sender: Sized {
    type Item;

    /// Gets whether the other side is disconnected
    #[must_use]
    fn is_disconnected(&self) -> bool;

    /// Gets the capacity of the channel
    #[must_use]
    fn capacity(&self) -> usize;

    /// Gets the number of items in the channel
    #[must_use]
    fn len(&self) -> usize;

    /// Gets whether the channel is empty
    #[must_use]
    fn is_empty(&self) -> bool;

    /// Gets whether the channel is full
    #[must_use]
    fn is_full(&self) -> bool;

    /// Attempts to send a single item on to the channel
    ///
    /// # Errors
    ///
    /// Returns an error when the channel is full, or no receiver is connected
    fn try_send(&mut self, item: Self::Item) -> Result<(), TrySendError<Self::Item>>;

    /// Blocks while sending the next item
    ///
    /// # Errors
    ///
    /// Returns an error when no receiver is connected
    fn send(&mut self, item: Self::Item) -> Result<(), SendError<Self::Item>>;

    /// Disconnects this sender by dropping it
    fn disconnect(self) {}
}

/// A sender for a single producer, multiple consumers queue
#[derive(Debug)]
pub struct SpSender<T> {
    producer: SingleProducer<T>,
}

impl<T> Sender for SpSender<T> {
    type Item = T;

    #[inline]
    fn is_disconnected(&self) -> bool {
        self.producer.ring.get_connected_consumers() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.producer.ring.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.producer.get_number_of_items()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.producer.get_number_of_items() == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.producer.get_number_of_items() == (self.producer.ring.mask + 1)
    }

    #[inline]
    fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        self.producer.try_push(item)
    }

    fn send(&mut self, mut item: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();
        loop {
            match self.producer.try_push(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(item_back)) => {
                    item = item_back;
                    backoff.snooze();
                }
                Err(TrySendError::NoData) => return Err(SendError(None)),
                Err(TrySendError::Disconnected(item)) => return Err(SendError(Some(item))),
            }
        }
    }
}

/// Creates a single producer, multiple consumers queue.
/// All consumers receive all items, meaning this is a dispatch queue.
#[must_use]
pub fn spmc<T>(queue_size: usize) -> (SpSender<T>, Receiver<T, OwnedOutput>) {
    let producer = SingleProducer::new(Arc::new(RingBuffer::new_single_producer(queue_size)));
    let consumer = Consumer::new(producer.ring.clone(), ConsumerMode::Blocking);
    (SpSender { producer }, Receiver { consumer })
}

/// A sender for a single producer, multiple consumers queue
#[derive(Debug, Clone)]
pub struct MpSender<T> {
    producer: ConcurrentProducer<T>,
}

impl<T> Sender for MpSender<T> {
    type Item = T;

    #[inline]
    fn is_disconnected(&self) -> bool {
        self.producer.ring.get_connected_consumers() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.producer.ring.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.producer.get_number_of_items()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.producer.get_number_of_items() == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.producer.get_number_of_items() == (self.producer.ring.mask + 1)
    }

    #[inline]
    fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        self.producer.try_push(item)
    }

    fn send(&mut self, mut item: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();
        loop {
            match self.producer.try_push(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(item_back)) => {
                    item = item_back;
                    backoff.snooze();
                }
                Err(TrySendError::NoData) => return Err(SendError(None)),
                Err(TrySendError::Disconnected(item)) => return Err(SendError(Some(item))),
            }
        }
    }
}

/// Creates a multiple producers, multiple consumers queue.
/// All consumers receive all items, meaning this is a dispatch queue.
#[must_use]
pub fn mpmc<T>(queue_size: usize) -> (MpSender<T>, Receiver<T, SharedOutput>) {
    let producer = ConcurrentProducer::new(Arc::new(RingBuffer::new_multi_producer(queue_size)));
    let consumer = Consumer::new(producer.ring.clone(), ConsumerMode::Blocking);
    (MpSender { producer }, Receiver { consumer })
}

#[cfg(test)]
mod tests_send_sync {
    pub fn assert_send<T: Send>(_thing: &T) {}
    pub fn assert_sync<T: Sync>(_thing: &T) {}
    pub fn assert_clone<T: Clone>(_thing: &T) {}

    #[test]
    fn spmc_are_send_sync_clone() {
        let (sender, receiver) = super::spmc::<usize>(4);
        assert_send(&sender);
        assert_sync(&sender);

        assert_send(&receiver);
        assert_sync(&receiver);
        assert_clone(&receiver);
    }

    #[test]
    fn mpmc_are_send_sync_clone() {
        let (sender, receiver) = super::mpmc::<usize>(4);
        assert_send(&sender);
        assert_sync(&sender);
        assert_clone(&sender);

        assert_send(&receiver);
        assert_sync(&receiver);
        assert_clone(&receiver);
    }
}
