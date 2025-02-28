/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Barriers to synchronise agents working on a common queue

use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::fmt::Debug;
use core::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

use crossbeam_utils::CachePadded;

use super::Sequence;
use crate::utils::arm_memory_barrier;

/// The output of a user of a queue, be it a producer or a consumer.
/// For producers, this is the last sequence available to consumers.
/// For consumers, this is the last item they finished handling that could then be seen by other downchain consumers.
pub trait Output: Debug + Send + Sync {
    /// Get the published sequence using `Relaxed`
    #[must_use]
    fn published(&self) -> Sequence;

    /// Write and publish the specificed sequence
    fn commit(&self, sequence: Sequence);

    /// Tries to commit and publish a sequence.
    ///
    /// # Errors
    ///
    /// Returns the current sequence if it was not the expected one
    fn try_commit(&self, expected: Sequence, new: Sequence) -> Result<(), Sequence>;
}

/// The output of a single queue user
/// The construction of producers and consumers guarantee a single writer
#[derive(Debug)]
#[repr(transparent)]
pub struct OwnedOutput {
    inner: CachePadded<UnsafeCell<isize>>,
}

unsafe impl Send for OwnedOutput {}
unsafe impl Sync for OwnedOutput {}

impl Default for OwnedOutput {
    fn default() -> Self {
        Self {
            inner: CachePadded::new(UnsafeCell::new(-1)),
        }
    }
}

impl OwnedOutput {
    pub(crate) fn new(value: isize) -> Self {
        Self {
            inner: CachePadded::new(UnsafeCell::new(value)),
        }
    }
}

impl Output for OwnedOutput {
    #[inline]
    fn published(&self) -> Sequence {
        let r = Sequence::from(unsafe { self.inner.get().read_volatile() });
        arm_memory_barrier();
        r
    }

    #[inline]
    fn commit(&self, sequence: Sequence) {
        arm_memory_barrier();
        unsafe { self.inner.get().write_volatile(sequence.0) }
    }

    #[inline]
    fn try_commit(&self, _expected: Sequence, new: Sequence) -> Result<(), Sequence> {
        arm_memory_barrier();
        unsafe {
            self.inner.get().write_volatile(new.0);
        }
        Ok(())
    }
}

/// The common output for multiple queue users, usually concurrent producers
#[derive(Debug)]
#[repr(transparent)]
pub struct SharedOutput {
    inner: CachePadded<AtomicIsize>,
}

impl Default for SharedOutput {
    fn default() -> Self {
        Self {
            inner: CachePadded::new(AtomicIsize::new(-1)),
        }
    }
}

impl Output for SharedOutput {
    #[inline]
    fn published(&self) -> Sequence {
        Sequence::from(self.inner.load(Ordering::Acquire))
    }

    #[inline]
    fn commit(&self, sequence: Sequence) {
        self.inner.store(sequence.0, Ordering::Release);
    }

    #[inline]
    fn try_commit(&self, expected: Sequence, new: Sequence) -> Result<(), Sequence> {
        if let Err(e) = self
            .inner
            .compare_exchange_weak(expected.0, new.0, Ordering::AcqRel, Ordering::Relaxed)
        {
            Err(Sequence::from(e))
        } else {
            Ok(())
        }
    }
}

/// A barrier to be used to await for available sequences
pub trait Barrier: Debug + Clone + Send + Sync {
    /// Get the next sequence available through this barrier
    /// Use an observer's sequence to optimize in the case of a `MultiBarrier`
    #[must_use]
    fn next(&self, observer: Sequence) -> Sequence;
}

/// A barrier to be used to await for the output of a single other queue user, producer or consumer
#[derive(Debug)]
#[repr(transparent)]
pub struct SingleBarrier<O: ?Sized> {
    /// The single dependency
    dependency: Arc<O>,
}

impl<O> Clone for SingleBarrier<O> {
    fn clone(&self) -> Self {
        Self {
            dependency: self.dependency.clone(),
        }
    }
}

impl<O: Output + 'static> Barrier for SingleBarrier<O> {
    #[inline]
    fn next(&self, _observer: Sequence) -> Sequence {
        self.dependency.published()
    }
}

impl<O: Output + 'static> SingleBarrier<O> {
    /// Creates a barrier that awaits on a single output
    #[must_use]
    pub fn await_on(dependency: &Arc<O>) -> Self {
        Self {
            dependency: dependency.clone(),
        }
    }

    /// Gets the dependency, i.e. the output for the user the barrier is waiting on
    #[must_use]
    pub(crate) fn get_dependency(&self) -> &Arc<O> {
        &self.dependency
    }
}

/// A barrier to be used to await for the output of multiple other queue users, producer or consumers
#[derive(Debug)]
pub struct MultiBarrier<O: ?Sized> {
    /// The maximum number of dependencies
    pub(crate) max_dependencies: usize,
    /// All the dependencies
    dependencies: UnsafeCell<Vec<Arc<O>>>,
    /// Whether the barrier is locked when modifying the dependencies
    lock: AtomicBool,
}

unsafe impl<O> Sync for MultiBarrier<O> {}

impl<O> Clone for MultiBarrier<O> {
    fn clone(&self) -> Self {
        let mut dependencies = Vec::with_capacity(self.max_dependencies);
        for dep in self.get_dependencies() {
            dependencies.push(dep.clone());
        }
        Self {
            max_dependencies: self.max_dependencies,
            dependencies: UnsafeCell::new(dependencies),
            lock: AtomicBool::new(false),
        }
    }
}

impl<O: Output + 'static> Barrier for MultiBarrier<O> {
    #[inline]
    fn next(&self, _observer: Sequence) -> Sequence {
        self.get_dependencies()
            .iter()
            .map(|o| o.published())
            .min()
            .unwrap_or_default()
        // if self.dependencies.is_empty() {
        //     // short circuit to simplify return
        //     return Sequence::default();
        // }
        // let mut acc: Option<(usize, Sequence)> = None;
        // let mut index = 0;
        // while index < self.dependencies.len() {
        //     let published = unsafe { self.dependencies.get_unchecked(index) }.published();
        //     if !published.is_valid_item() || published <= observer {
        //         if index != 0 {
        //             // put on first because it is supposed to be the slowest
        //             self.dependencies.swap(0, index);
        //         }
        //         return published;
        //     }
        //     acc = match acc {
        //         None => Some((index, published)),
        //         Some((acc_index, acc)) => {
        //             if published < acc {
        //                 Some((index, published))
        //             } else {
        //                 Some((acc_index, acc))
        //             }
        //         }
        //     };
        //     index += 1;
        // }
        // let (index, min) = unsafe { acc.unwrap_unchecked() }; // safe because we checked dependencies is not empty
        // if index != 0 {
        //     // put on first because it is supposed to be the slowest
        //     self.dependencies.swap(0, index);
        // }
        // min
    }
}

impl<O> MultiBarrier<O> {
    /// Creates a new empty barrier
    #[must_use]
    pub fn new(max_dependencies: usize) -> Self {
        Self {
            max_dependencies,
            dependencies: UnsafeCell::new(Vec::with_capacity(max_dependencies)),
            lock: AtomicBool::new(false),
        }
    }

    /// Gets the inner dependencies
    #[inline]
    #[must_use]
    fn get_dependencies(&self) -> &[Arc<O>] {
        unsafe { self.dependencies.get().as_ref().unwrap() }.as_slice()
    }

    /// Creates a multi barrier that awaits on multiple outputs
    #[must_use]
    pub fn await_on(dependencies: Vec<Arc<O>>) -> Self {
        Self {
            max_dependencies: dependencies.len(),
            dependencies: UnsafeCell::new(dependencies),
            lock: AtomicBool::new(false),
        }
    }

    /// Adds a dependency to this barrier
    pub(crate) fn add_dependency(&self, output: Arc<O>) -> Result<(), Arc<O>> {
        loop {
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                let deps = unsafe { self.dependencies.get().as_mut().unwrap() };
                if deps.len() >= self.max_dependencies {
                    self.lock.store(false, Ordering::Relaxed);
                    return Err(output);
                }
                deps.push(output);
                self.lock.store(false, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    /// Removes a dependency from this barrier
    pub(crate) fn remove_dependency(&self, output: &Arc<O>) {
        loop {
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                let deps = unsafe { self.dependencies.get().as_mut().unwrap() };
                deps.retain(|candidate| !Arc::ptr_eq(candidate, output));
                self.lock.store(false, Ordering::Relaxed);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests_multi_barrier {
    use alloc::sync::Arc;

    use super::{MultiBarrier, OwnedOutput};
    use crate::queue::Sequence;
    use crate::queue::barriers::Barrier;

    #[test]
    fn test_next_no_dep() {
        let barrier = MultiBarrier::<OwnedOutput>::new(16);
        for i in -1..5_isize {
            assert_eq!(barrier.next(Sequence::from(i)), Sequence::default());
        }
    }

    fn test_next_single_dep_with_value(published: isize) {
        let barrier = MultiBarrier::new(16);
        barrier.add_dependency(Arc::new(OwnedOutput::new(published))).unwrap();
        for observer in -1..(published + 4) {
            assert_eq!(barrier.next(Sequence::from(observer)), Sequence::from(published));
        }
    }

    #[test]
    fn test_next_single_dep() {
        for i in -1..5 {
            test_next_single_dep_with_value(i);
        }
    }

    fn test_next_multi_deps_with_values(published: &[isize], observer: isize, expected: isize) {
        let barrier = MultiBarrier::new(16);
        for &published in published {
            barrier.add_dependency(Arc::new(OwnedOutput::new(published))).unwrap();
        }
        let r = barrier.next(Sequence::from(observer));
        assert_eq!(r, Sequence::from(expected));
    }

    #[test]
    fn test_next_multi_deps() {
        // return first less or equal to observer
        test_next_multi_deps_with_values(&[-1, 0, 1, 2], -1, -1);
        test_next_multi_deps_with_values(&[-1, 0, 1, 2], 0, -1);
        test_next_multi_deps_with_values(&[-1, 0, 1, 2], 1, -1);
        test_next_multi_deps_with_values(&[-1, 0, 1, 2], 2, -1);

        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 8, 6);
        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 7, 6);
        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 6, 6);
        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 5, 5);
        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 4, 4);
        // test_next_multi_deps_with_values(&[6, 5, 4, 3], 3, 3);

        // general case, observer is before, get the min
        test_next_multi_deps_with_values(&[5, 7, 4, 9], -1, 4);
        test_next_multi_deps_with_values(&[5, 7, 4, 9], 0, 4);
        test_next_multi_deps_with_values(&[5, 7, 4, 9], 1, 4);
        test_next_multi_deps_with_values(&[5, 7, 4, 9], 2, 4);
        test_next_multi_deps_with_values(&[5, 7, 4, 9], 3, 4);
        test_next_multi_deps_with_values(&[5, 7, 4, 9], 4, 4);
    }
}
