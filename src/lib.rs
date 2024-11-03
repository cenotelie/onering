/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! # Disruptor
//!
//! High throughput synchronous channels

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::module_name_repetitions)]

extern crate alloc;

pub mod channels;
#[cfg(feature = "std")]
pub mod disruptor;
pub mod errors;
pub mod pipeline;
pub mod prelude;
mod queue;

#[cfg(test)]
mod tests;

use core::marker::PhantomData;

use crate::errors::{RecvError, SendError, TryRecvError, TrySendError};

/// Common traits for all kind of sender
pub trait Sender<T>: Sized {
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
    fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>>;

    /// Blocks while sending the next item
    ///
    /// # Errors
    ///
    /// Returns an error when no receiver is connected
    fn send(&mut self, item: T) -> Result<(), SendError<T>>;

    /// Disconnects this sender by dropping it
    fn disconnect(self) {}

    /// Consume this sender and get back the non-received items
    /// This method expect the other receiver(s) to not be connected anymore.
    /// As long as there are still connected receivers, this will fail.
    ///
    /// # Errors
    ///
    /// Returns itself when there are at least another receiver still connected.
    fn into_unreceived(self) -> Result<Vec<T>, Self>;
}

/// Common trait for all kind of receiver
pub trait Receiver<T>: Sized + IntoIterator<Item = T, IntoIter = ReceiverIterator<T, Self>> {
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
    fn try_recv(&mut self) -> Result<T, TryRecvError>;

    /// Blocks while waiting for the next item
    /// An item will be returned for as long as there are items in the channel, even if senders are not connected.
    ///
    /// # Errors
    ///
    /// Returns an error when no sender is connected
    fn recv(&mut self) -> Result<T, RecvError>;

    /// Gets an iterator from a mutable reference.
    /// The iterator yields the next item from the channel on each call to `next`
    fn iter_mut(&mut self) -> ReceiverRefIterator<'_, T, Self>;

    /// Disconnects this receiver by dropping it
    fn disconnect(self) {}
}

/// An iterator that owns a receiver and yield the next item from the channel on each call to `next`
pub struct ReceiverIterator<T, R> {
    receiver: R,
    _item: PhantomData<T>,
}

impl<T, R> ReceiverIterator<T, R> {
    /// Build an iterator from the receiver
    pub fn new(receiver: R) -> ReceiverIterator<T, R> {
        Self {
            receiver,
            _item: PhantomData,
        }
    }
}

impl<T, R> Iterator for ReceiverIterator<T, R>
where
    R: Receiver<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

/// An iterator that has a reference to a receiver and yield the next item from the channel on each call to `next`
pub struct ReceiverRefIterator<'r, T, R> {
    receiver: &'r mut R,
    _item: PhantomData<T>,
}

impl<'r, T, R> ReceiverRefIterator<'r, T, R> {
    /// Build an iterator from the receiver
    pub fn new(receiver: &'r mut R) -> ReceiverRefIterator<'r, T, R> {
        Self {
            receiver,
            _item: PhantomData,
        }
    }
}

impl<'r, T, R> Iterator for ReceiverRefIterator<'r, T, R>
where
    R: Receiver<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}
