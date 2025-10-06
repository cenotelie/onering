/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Low-level utils for queues

use std::marker::PhantomData;

/// An ARM-specific memory barrier.
/// This is only required due to a weaker memory model on ARM.
/// See [DMB](https://developer.arm.com/documentation/dui0489/c/arm-and-thumb-instructions/miscellaneous-instructions/dmb--dsb--and-isb)
#[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
#[inline]
pub fn arm_memory_barrier() {
    use core::arch::asm;
    unsafe { asm!("dmb sy") }
}

/// An ARM-specific memory barrier.
/// This is only required due to a weaker memory model on ARM.
/// See [DMB](https://developer.arm.com/documentation/dui0489/c/arm-and-thumb-instructions/miscellaneous-instructions/dmb--dsb--and-isb)
#[cfg(not(any(target_arch = "arm", target_arch = "aarch64")))]
#[inline]
pub fn arm_memory_barrier() {}

/// Like `PhantomData` with `Send`, `Sync`, `Clone`
#[derive(Debug)]
pub struct Phantom<T> {
    _use_t: PhantomData<T>,
}

impl<T> Default for Phantom<T> {
    fn default() -> Self {
        Self { _use_t: PhantomData }
    }
}

impl<T> Clone for Phantom<T> {
    fn clone(&self) -> Self {
        Self { _use_t: PhantomData }
    }
}

unsafe impl<T> Send for Phantom<T> {}
unsafe impl<T> Sync for Phantom<T> {}
