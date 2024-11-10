/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Definition of errors for this crate

use core::fmt::{Debug, Display};

/// Error when trying to send an item
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The message could not be sent because the channel is full.
    Full(T),
    /// There wasn't any data in to be pushed
    NoData,
    /// The message could not be sent because the channel is disconnected.
    Disconnected(T),
}

impl<T> Display for TrySendError<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Full(_) => write!(f, "failed to send: the channel is full"),
            Self::NoData => write!(f, "failed to send: no data to send"),
            Self::Disconnected(_) => write!(f, "failed to send: the channel is disconnected"),
        }
    }
}

impl<T: Debug> core::error::Error for TrySendError<T> {}

impl<T> TrySendError<T> {
    /// Gets back the wrapped message
    #[must_use]
    pub fn into_inner(self) -> Option<T> {
        match self {
            Self::Full(item) | Self::Disconnected(item) => Some(item),
            Self::NoData => None,
        }
    }

    /// Tests whether the cause of the error is the channel being full
    #[must_use]
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    /// Tests whether the cause of the error is the channel being disconnected
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }
}

/// The message could not be sent because the channel is disconnected
///
/// The error contains the message so it can be recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub Option<T>);

impl<T> SendError<T> {
    /// Gets back the wrapped message
    #[must_use]
    pub fn into_inner(self) -> Option<T> {
        self.0
    }
}

impl<T> From<TrySendError<T>> for SendError<T> {
    fn from(value: TrySendError<T>) -> Self {
        Self(value.into_inner())
    }
}

/// Error when trying to receive an item
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// A message could not be received because the channel is empty
    Empty,
    /// The receiver lags behind
    Lagging(usize),
    /// The message could not be received because the channel is empty and disconnected
    Disconnected,
}

impl Display for TryRecvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => write!(f, "failed to receive: the channel is empty"),
            Self::Lagging(count) => write!(f, "failed to receive: lagging behind {count} messages"),
            Self::Disconnected => write!(f, "failed to receive: the channel is disconnected"),
        }
    }
}

impl core::error::Error for TryRecvError {}

impl TryRecvError {
    /// Tests whether the cause of the error is the channel being empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Tests whether the cause of the error is the channel being disconnected
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

/// A message could not be received because the channel is empty and disconnected
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The receiver lags behind
    Lagging(usize),
    /// The message could not be received because the channel is empty and disconnected
    Disconnected,
}

impl Display for RecvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Lagging(count) => write!(f, "failed to receive: lagging behind {count} messages"),
            Self::Disconnected => write!(f, "failed to receive: the channel is disconnected"),
        }
    }
}

impl core::error::Error for RecvError {}
