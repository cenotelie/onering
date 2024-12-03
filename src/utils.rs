/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Low-level utils for queues

/// An ARM-specific memory barrier.
/// This is only required due to a weaker memory model on ARM.
/// See [DMB](https://developer.arm.com/documentation/dui0489/c/arm-and-thumb-instructions/miscellaneous-instructions/dmb--dsb--and-isb)
#[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
#[inline]
pub fn arm_memory_barriers() {
    use core::arch::asm;
    unsafe { asm!("dmb sy") }
}

/// An ARM-specific memory barrier.
/// This is only required due to a weaker memory model on ARM.
/// See [DMB](https://developer.arm.com/documentation/dui0489/c/arm-and-thumb-instructions/miscellaneous-instructions/dmb--dsb--and-isb)
#[cfg(not(any(target_arch = "arm", target_arch = "aarch64")))]
#[inline]
pub fn arm_memory_barrier() {}
