/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

use core::cell::RefCell;

mod crossbeam;
mod disruptor;
mod perf;

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 32;

/// The number of messages
pub const SCALE_MSG_COUNT: usize = 500_000_000;

/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;

/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;

fn assert_send<T: Send>(_thing: &T) {}

#[test]
fn test_singe_sender_receiver_are_send() {
    // usize: Send + Sync
    let (sender, receiver) = crate::channels::channel_spsc::<usize>(4);
    assert_send(&sender);
    assert_send(&receiver);

    // RefCell: Send + !Sync
    let (sender, receiver) = crate::channels::channel_spsc::<RefCell<usize>>(4);
    assert_send(&sender);
    assert_send(&receiver);
}
