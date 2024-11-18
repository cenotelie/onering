/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

// mod crossbeam;
mod queue;
// mod perf;

/// The size of the queue to use
pub const SCALE_QUEUE_SIZE: usize = 256;

/// The number of messages
pub const SCALE_MSG_COUNT: usize = 500_000_000;

/// The number of producers in a multiple producers, singe consumer test
pub const SCALE_PRODUCERS: usize = 5;

/// The number of consumers in a multiple producers, singe consumer test
pub const SCALE_CONSUMERS: usize = 5;
