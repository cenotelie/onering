/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Implementation of different kind of channels

use alloc::sync::Arc;

use crate::errors::{RecvError, SendError, TryRecvError, TrySendError};
use crate::queue::{QueueCursor, QueueSlots, QueuesHub};
use crate::{Receiver, ReceiverIterator, ReceiverRefIterator, Sender};

/// The sole sender for a channel
#[derive(Debug)]
pub struct SingleSender<T> {
    cursor: QueueCursor<T>,
}

impl<T> Sender<T> for SingleSender<T> {
    #[inline]
    fn is_disconnected(&self) -> bool {
        self.cursor.is_unique()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.cursor.slots.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.cursor.slots.count_with_push_cursor(self.cursor.index)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cursor.slots.is_empty_with_push_cursor(self.cursor.index)
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.cursor.slots.is_full_with_push_cursor(self.cursor.index)
    }

    #[inline]
    fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        self.cursor.slots.try_push(&mut self.cursor.index, item)
    }

    #[inline]
    fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        self.cursor.slots.push(&mut self.cursor.index, item)
    }

    #[inline]
    fn into_unreceived(mut self) -> Result<Vec<T>, Self> {
        let mut buffer = Vec::with_capacity(self.cursor.slots.capacity());
        match self.cursor.slots.drain_with_push_cursor(self.cursor.index, &mut buffer) {
            Ok(_count) => Ok(buffer),
            Err(slots) => {
                self.cursor.slots = slots;
                Err(self)
            }
        }
    }
}

/// The sole receiver for a chennel
#[derive(Debug)]
pub struct SingleReceiver<T> {
    cursor: QueueCursor<T>,
}

impl<T> Receiver<T> for SingleReceiver<T> {
    #[inline]
    fn is_disconnected(&self) -> bool {
        self.cursor.is_unique()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.cursor.slots.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.cursor.slots.count_with_pop_cursor(self.cursor.index)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cursor.slots.is_empty_with_pop_cursor(self.cursor.index)
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.cursor.slots.is_full_with_pop_cursor(self.cursor.index)
    }

    #[inline]
    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.cursor.slots.try_pop(&mut self.cursor.index)
    }

    #[inline]
    fn recv(&mut self) -> Result<T, RecvError> {
        self.cursor.slots.pop(&mut self.cursor.index)
    }

    #[inline]
    fn iter_mut(&mut self) -> ReceiverRefIterator<'_, T, Self> {
        ReceiverRefIterator::new(self)
    }
}

impl<T> IntoIterator for SingleReceiver<T> {
    type Item = T;
    type IntoIter = ReceiverIterator<T, Self>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverIterator::new(self)
    }
}

#[allow(clippy::into_iter_without_iter)] // <- iter_mut is implement through a trait and not detected
impl<'r, T> IntoIterator for &'r mut SingleReceiver<T> {
    type Item = T;
    type IntoIter = ReceiverRefIterator<'r, T, SingleReceiver<T>>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverRefIterator::new(self)
    }
}

/// Creates a new channel with a single sender and a single receiver
/// The provided size MUST be a power of two.
///
/// # Panics
///
/// Raise a panic when the size is not a power of two.
#[must_use]
pub fn channel_spsc<T>(size: usize) -> (SingleSender<T>, SingleReceiver<T>) {
    let slots = Arc::new(QueueSlots::new(size));
    (
        SingleSender {
            cursor: QueueCursor {
                slots: slots.clone(),
                index: 0,
            },
        },
        SingleReceiver {
            cursor: QueueCursor { slots, index: 0 },
        },
    )
}

/// A receiver for multiple senders
#[derive(Debug)]
pub struct MsReceiver<T> {
    hub: QueuesHub<T>,
}

impl<T> MsReceiver<T> {
    /// Gets the number of connected senders
    #[inline]
    #[must_use]
    pub fn get_connected_senders(&self) -> usize {
        self.hub.connected_other_sides()
    }

    /// Adds a new sender for this receiver
    #[must_use]
    pub fn add_sender(&mut self) -> SingleSender<T> {
        let slots = Arc::new(QueueSlots::new(self.hub.queue_size));
        self.hub.cursors.push(QueueCursor {
            slots: slots.clone(),
            index: 0,
        });
        SingleSender {
            cursor: QueueCursor {
                slots: slots.clone(),
                index: 0,
            },
        }
    }

    /// Attempts to receive multiple available items from the channel
    ///
    /// # Errors
    ///
    /// Returns an error when the channel is empty, or no sender is connected
    #[inline]
    pub fn try_recv_multiple(&mut self, buffer: &mut Vec<T>) -> Result<usize, TryRecvError> {
        self.hub.try_pop_multiple(buffer)
    }
}

impl<T> Receiver<T> for MsReceiver<T> {
    #[inline]
    fn is_disconnected(&self) -> bool {
        self.hub.connected_other_sides() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.hub.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.hub.count_assuming_pop_use()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.hub.is_empty_assuming_pop_use()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.hub.is_full_assuming_pop_use()
    }

    #[inline]
    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.hub.try_pop()
    }

    #[inline]
    fn recv(&mut self) -> Result<T, RecvError> {
        self.hub.pop()
    }

    #[inline]
    fn iter_mut(&mut self) -> ReceiverRefIterator<'_, T, Self> {
        ReceiverRefIterator::new(self)
    }
}

impl<T> IntoIterator for MsReceiver<T> {
    type Item = T;
    type IntoIter = ReceiverIterator<T, Self>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverIterator::new(self)
    }
}

#[allow(clippy::into_iter_without_iter)] // <- iter_mut is implement through a trait and not detected
impl<'r, T> IntoIterator for &'r mut MsReceiver<T> {
    type Item = T;
    type IntoIter = ReceiverRefIterator<'r, T, MsReceiver<T>>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverRefIterator::new(self)
    }
}

/// Creates a new channel with a single receiver, but possibly multiple senders
/// At creation, the channel has no sender yet.
/// The provided size MUST be a power of two.
///
/// # Panics
///
/// Raise a panic when the size is not a power of two.
#[must_use]
pub fn channel_mpsc<T>(size: usize) -> MsReceiver<T> {
    assert!(size.is_power_of_two(), "size must be power of two, got {size}");
    MsReceiver {
        hub: QueuesHub {
            queue_size: size,
            cursors: Vec::new(),
            next: 0,
        },
    }
}

/// A sender for multiple receiver
#[derive(Debug)]
pub struct McSender<T> {
    hub: QueuesHub<T>,
}

impl<T> McSender<T> {
    /// Gets the number of connected receivers
    #[inline]
    #[must_use]
    pub fn get_connected_receivers(&self) -> usize {
        self.hub.connected_other_sides()
    }

    /// Adds a new receiver for this receiver
    #[must_use]
    pub fn add_receiver(&mut self) -> SingleReceiver<T> {
        let slots = Arc::new(QueueSlots::new(self.hub.queue_size));
        self.hub.cursors.push(QueueCursor {
            slots: slots.clone(),
            index: 0,
        });
        SingleReceiver {
            cursor: QueueCursor {
                slots: slots.clone(),
                index: 0,
            },
        }
    }

    /// Collects all the messages that can no longer be received because the corresponding receiver is disconnected
    #[must_use]
    pub fn get_unreceivables(&mut self) -> Vec<T> {
        let mut buffer = Vec::with_capacity(self.hub.queue_size);
        let _ = self.hub.get_unreceivables(&mut buffer);
        buffer
    }
}

impl<T: Clone> McSender<T> {
    /// Dispatch a copy of the same message to all connected receivers
    /// Returns the number of copies that were sent.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel is disconnected
    #[inline]
    pub fn dispatch(&mut self, item: T) -> Result<usize, SendError<T>> {
        self.hub.push_to_all(item)
    }
}

impl<T> Sender<T> for McSender<T> {
    #[inline]
    fn is_disconnected(&self) -> bool {
        self.hub.connected_other_sides() == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.hub.capacity()
    }

    #[inline]
    fn len(&self) -> usize {
        self.hub.count_assuming_push_use()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.hub.is_empty_assuming_push_use()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.hub.is_full_assuming_push_use()
    }

    #[inline]
    fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        self.hub.try_push(item)
    }

    #[inline]
    fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        self.hub.push(item)
    }

    #[inline]
    fn into_unreceived(mut self) -> Result<Vec<T>, Self> {
        if !self.is_disconnected() {
            // still has connected receivers
            return Err(self);
        }
        let mut buffer = Vec::with_capacity(self.hub.queue_size);
        self.hub.get_unreceivables(&mut buffer).unwrap();
        Ok(buffer)
    }
}

/// Creates a new channel with a single sender, but possibly multiple receivers
/// At creation, the channel has no receiver yet.
/// The provided size MUST be a power of two.
///
/// # Panics
///
/// Raise a panic when the size is not a power of two.
#[must_use]
pub fn channel_spmc<T>(size: usize) -> McSender<T> {
    assert!(size.is_power_of_two(), "size must be power of two, got {size}");
    McSender {
        hub: QueuesHub {
            queue_size: size,
            cursors: Vec::new(),
            next: 0,
        },
    }
}
