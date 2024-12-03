/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! # Onering
//!
//! High throughput synchronous queue and channels.
//! The implementation of the queue is freely inspired by the [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor).
//! As in a typical disruptor fashion, consumers see all items pushed onto the queue.
//! The implementation is then better suited for dispatching all items to all consumers.
//!
//! Therefore, the queue provided here do not allow sending the ownership of queued items onto other threads.
//! Instead, receivers (consumers) will only see immutable references to the items.
//! When items can be copied (implements `Copy`), copies can be obtained instead.
//!
//!
//! ## Example
//!
//! Create a queue with a single producer and 5 event consumers.
//! ```
//! use std::sync::Arc;
//! use onering::errors::TryRecvError;
//! use onering::queue::{Consumer, ConsumerMode, RingBuffer, SingleProducer};
//!
//! let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(256));
//! let mut consumers = (0..5)
//!     .map(|_| Consumer::new(ring.clone(), ConsumerMode::Blocking))
//!     .collect::<Vec<_>>();
//! let mut producer = SingleProducer::new(ring);
//!
//! let consumer_threads = (0..5)
//!     .map(|_| {
//!         let mut consumer = consumers.pop().unwrap();
//!         std::thread::spawn({
//!             move || {
//!                 let mut count = 0;
//!                 loop {
//!                     match consumer.try_recv() {
//!                         Ok(items) => {
//!                             for _item in items {
//!                                 // handle item
//!                             }
//!                         },
//!                         Err(TryRecvError::Disconnected) => {
//!                             break;
//!                         }
//!                         Err(_) => {/* retry */}
//!                     }
//!                 }
//!             }
//!         })
//!     })
//!     .collect::<Vec<_>>();
//!
//! for item in 0..1000 {
//!     while producer.try_push(item).is_err() {}
//! }
//! drop(producer); // so that `TryRecvError::Disconnected` is raised
//!
//! for consumer in consumer_threads {
//!     consumer.join().unwrap();
//! }
//! ```
//!
//!
//! ## `no-std` support
//!
//! `onering` is compatible with `no-std` context, having a `std` feature which is activated by default.
//! To use `onering` without the `std`, deactivate the default features in your `Cargo.toml` file.
//!
//!
//! ## License
//!
//! Copyright 2024 Cénotélie Opérations SAS
//!
//! Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//!
//! The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//!
//! THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//!

#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::module_name_repetitions)]

extern crate alloc;

pub mod channels;
pub mod errors;
pub mod queue;
mod utils;
