/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! # Disruptor
//!
//! High throughput synchronous queue and channels.
//! The queue provided here do not allow sending the ownership of queued items onto other threads.
//! Instead, receivers (consumers) will only see immutable references to the items.

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::module_name_repetitions)]

extern crate alloc;

// pub mod affinity;
pub mod channels;
pub mod errors;
pub mod queue;

// #[cfg(test)]
// mod tests;
