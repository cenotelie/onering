/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Synchronous channels backed by a ring buffer

use crossbeam_utils::Backoff;

use crate::errors::{RecvError, SendError, TryRecvError, TrySendError};
use crate::queue::{Consumer, ConsumerAccess, ConsumerMode, QueueUser, RingBuffer, SingleProducer};

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

/// Common trait for all kind of receiver
pub trait Receiver: Sized {
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

    /// Attempts to receive a single item from to the channel
    /// An item will be returned for as long as there are items in the channel, even if senders are not connected.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel is empty, or no sender is connected
    fn try_recv(&mut self) -> Result<ConsumerAccess<'_, Self::Item>, TryRecvError>;

    /// Blocks while waiting for the next item
    /// An item will be returned for as long as there are items in the channel, even if senders are not connected.
    ///
    /// # Errors
    ///
    /// Returns an error when no sender is connected
    fn recv(&mut self) -> Result<ConsumerAccess<'_, Self::Item>, RecvError>;

    /// Disconnects this receiver by dropping it
    fn disconnect(self) {}
}

/// A sender for a single producer, multiple consumers queue
pub struct SpSender<T> {
    producer: SingleProducer<T>,
}

/// A receiver for a single producer, multiple consumers queue
pub struct SpReceiver<T> {
    consumer: Consumer<T>,
}

impl<T> Sender for SpSender<T> {
    type Item = T;

    #[inline]
    fn is_disconnected(&self) -> bool {
        self.producer.ring.get_connected_consumers() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.producer.ring.mask + 1
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

impl<T> Receiver for SpReceiver<T> {
    type Item = T;

    #[inline]
    fn is_disconnected(&self) -> bool {
        self.consumer.ring.get_connected_producers() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.consumer.ring.mask + 1
    }

    #[inline]
    fn len(&self) -> usize {
        self.consumer.get_number_of_items()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.consumer.get_number_of_items() == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.consumer.get_number_of_items() == (self.consumer.ring.mask + 1)
    }

    #[inline]
    fn try_recv(&mut self) -> Result<ConsumerAccess<'_, Self::Item>, TryRecvError> {
        self.consumer.try_recv()
    }

    fn recv(&mut self) -> Result<ConsumerAccess<'_, Self::Item>, RecvError> {
        let backoff = Backoff::new();
        loop {
            match self.consumer.try_recv() {
                Ok(items) => {
                    return unsafe {
                        // SAFETY: transmute the lifetime due to compiler issues
                        Ok(core::mem::transmute::<
                            ConsumerAccess<'_, Self::Item>,
                            ConsumerAccess<'_, Self::Item>,
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
}

/// Creates a single producer, multiple consumers queue.
/// All consumers receive all items, meaning this is a dispatch queue.
#[must_use]
pub fn channel_spmc<T>(queue_size: usize) -> SpSender<T> {
    let producer = RingBuffer::<T>::new_single_producer(queue_size);
    SpSender { producer }
}

impl<T> SpSender<T> {
    /// Adds a receiver to the channel
    /// By default, consumers block producers writing new items when they have not yet be seen.
    /// Setting a consumer as non-blocking enable producers to write event though the consumer may be lagging.
    pub fn add_receiver(&mut self, consumer_mode: Option<ConsumerMode>) -> SpReceiver<T> {
        SpReceiver {
            consumer: self
                .producer
                .ring
                .add_consumer(self.producer.publication_barrier(), consumer_mode),
        }
    }
}
