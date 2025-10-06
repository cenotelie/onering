/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Wait and retry strategies

use std::time::Duration;

use crossbeam_utils::Backoff;

/// A wait strategy
pub trait WaitStrategy: Default {
    /// Wait a little bit
    fn wait(&self);
}

/// Delegatates to crossbeam `Backoff` to busy-spin
#[derive(Debug, Default)]
pub struct SpinWaitStrategy {
    inner: Backoff,
}

impl WaitStrategy for SpinWaitStrategy {
    fn wait(&self) {
        self.inner.spin();
    }
}

/// Delegatates to crossbeam `Backoff` to snooze
#[derive(Debug, Default)]
pub struct SnoozeWaitStrategy {
    inner: Backoff,
}

impl WaitStrategy for SnoozeWaitStrategy {
    fn wait(&self) {
        self.inner.snooze();
    }
}

/// Yield the thread to the OS
#[derive(Debug, Default)]
pub struct YieldWaitStrategy;

impl WaitStrategy for YieldWaitStrategy {
    fn wait(&self) {
        std::thread::yield_now();
    }
}

/// Sleep a bit each time
#[derive(Debug)]
pub struct SleepWaitStrategy;

impl Default for SleepWaitStrategy {
    fn default() -> Self {
        std::thread::sleep(Duration::from_millis(1));
        Self
    }
}

impl WaitStrategy for SleepWaitStrategy {
    fn wait(&self) {
        std::thread::sleep(Duration::from_millis(1));
    }
}
