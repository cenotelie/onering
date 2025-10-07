/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Wait and retry strategies

use std::cell::Cell;

use crossbeam_utils::Backoff;

/// A wait strategy
pub trait WaitStrategy: Default {
    /// Wait a little bit
    fn wait(&self);
}

/// Delegatates to crossbeam `Backoff` to busy-spin
#[derive(Debug, Default)]
pub struct ImmediateWaitStrategy;

impl WaitStrategy for ImmediateWaitStrategy {
    #[inline]
    fn wait(&self) {}
}

/// Linear backoff with a spin
#[derive(Debug, Default)]
pub struct LinearBackoffSpinWaitStrategy {
    step: Cell<u32>,
}

impl WaitStrategy for LinearBackoffSpinWaitStrategy {
    #[inline]
    fn wait(&self) {
        let step = self.step.get();
        for _ in 0..(step * 10) {
            std::hint::spin_loop();
        }
        self.step.set(step + 1);
    }
}

/// Delegatates to crossbeam `Backoff` to busy-spin
#[derive(Debug, Default)]
pub struct ExponentialSpinWaitStrategy {
    inner: Backoff,
}

impl WaitStrategy for ExponentialSpinWaitStrategy {
    #[inline]
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
    #[inline]
    fn wait(&self) {
        self.inner.snooze();
    }
}

/// Yield the thread to the OS
#[derive(Debug, Default)]
pub struct YieldWaitStrategy;

impl WaitStrategy for YieldWaitStrategy {
    #[inline]
    fn wait(&self) {
        std::thread::yield_now();
    }
}

thread_local! {
    static BACKOFF_SPIN_COUNTER: Cell<u64> = const { Cell::new(0) };
}

/// Linear backoff with a spin
#[derive(Debug, Default)]
pub struct PersistentBackoffSpinWaitStrategy;

impl PersistentBackoffSpinWaitStrategy {
    #[inline]
    fn wait_impl() {
        let step = BACKOFF_SPIN_COUNTER.get();
        for _ in 0..(step * 10) {
            std::hint::spin_loop();
        }
        BACKOFF_SPIN_COUNTER.set(step + 1);
    }
}

impl WaitStrategy for PersistentBackoffSpinWaitStrategy {
    #[inline]
    fn wait(&self) {
        Self::wait_impl();
    }
}

thread_local! {
    static SLEEP_COUNTER: Cell<u64> = const { Cell::new(0) };
}

/// Sleep a bit
#[derive(Debug)]
pub struct SleepWaitStrategy;

impl SleepWaitStrategy {
    #[inline]
    fn wait_impl() {
        let step = SLEEP_COUNTER.get();
        for _ in 0..(step / 100) {
            std::hint::black_box(std::time::SystemTime::now()); // ~ 110ns
        }
        SLEEP_COUNTER.set(step + 1);
    }
}

impl Default for SleepWaitStrategy {
    fn default() -> Self {
        Self::wait_impl();
        Self
    }
}

impl WaitStrategy for SleepWaitStrategy {
    #[inline]
    fn wait(&self) {
        Self::wait_impl();
    }
}
