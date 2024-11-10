/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! API for thread affinity to cores

/// The identifier of a CPU core
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct CoreId(usize);

#[cfg(any(target_os = "android", target_os = "linux"))]
pub mod linux {
    use libc::{cpu_set_t, sched_getaffinity, sched_setaffinity, CPU_ISSET, CPU_SET, CPU_SETSIZE};

    use super::CoreId;

    #[must_use]
    pub fn get_core_ids() -> Vec<CoreId> {
        let Some(full_set) = get_affinity_mask() else {
            return Vec::new();
        };
        let mut core_ids: Vec<CoreId> = Vec::new();
        for i in 0..CPU_SETSIZE as usize {
            if unsafe { CPU_ISSET(i, &full_set) } {
                core_ids.push(CoreId(i));
            }
        }
        core_ids
    }

    #[allow(clippy::must_use_candidate)]
    pub fn set_for_current(core_id: CoreId) -> bool {
        // Turn `core_id` into a `libc::cpu_set_t` with only
        // one core active.
        let mut set = new_cpu_set();

        unsafe { CPU_SET(core_id.0, &mut set) };

        // Set the current thread's core affinity.
        let res = unsafe {
            sched_setaffinity(
                0, // Defaults to current thread
                core::mem::size_of::<cpu_set_t>(),
                &set,
            )
        };
        res == 0
    }

    fn get_affinity_mask() -> Option<cpu_set_t> {
        let mut set = new_cpu_set();

        // Try to get current core affinity mask.
        let result = unsafe {
            sched_getaffinity(
                0, // Defaults to current thread
                core::mem::size_of::<cpu_set_t>(),
                &mut set,
            )
        };

        if result == 0 {
            Some(set)
        } else {
            None
        }
    }

    fn new_cpu_set() -> cpu_set_t {
        unsafe { core::mem::zeroed::<cpu_set_t>() }
    }
}
