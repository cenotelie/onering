/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Barriers to synchronise agents working on a common queue

use alloc::sync::Arc;
use core::fmt::Debug;
use core::sync::atomic::{AtomicIsize, Ordering};

use crossbeam_utils::CachePadded;

use super::Sequence;

/// The output of a user of a queue, be it a producer or a consumer.
/// For producers, this is the last sequence available to consumers.
/// For consumers, this is the last item they finished handling that could then be consumed by other consumers.
#[derive(Debug)]
#[repr(transparent)]
pub struct UserOutput {
    /// The value for the published sequence
    pub(crate) sequence: CachePadded<AtomicIsize>,
}

impl Default for UserOutput {
    fn default() -> Self {
        Self {
            sequence: CachePadded::new(AtomicIsize::new(-1)),
        }
    }
}

impl UserOutput {
    /// Get the published sequence using `Relaxed`
    #[must_use]
    #[inline]
    pub fn published(&self) -> Sequence {
        Sequence::from(self.sequence.load(Ordering::Acquire))
    }

    /// Write and publish the specificed sequence
    #[inline]
    pub fn commit(&self, sequence: Sequence) {
        self.sequence.store(sequence.0, Ordering::Release);
    }

    /// Creates the output using an initial value
    #[must_use]
    pub fn new<I>(value: I) -> Self
    where
        I: Into<Sequence>,
    {
        let value: Sequence = value.into();
        Self {
            sequence: CachePadded::new(AtomicIsize::new(value.0)),
        }
    }
}

/// A barrier to be used to await for available sequences
pub trait Barrier: Debug + Send + Sync {
    /// Get the next sequence available through this barrier
    /// Use an observer's sequence to optimize in the case of a `MultiBarrier`
    #[must_use]
    fn next(&self, observer: Sequence) -> Sequence;

    /// Clone the barrier
    /// We cannot use `Clone` because it is not `dyn`-compatible
    #[must_use]
    fn duplicate(&self) -> Box<dyn Barrier>;
}

/// A barrier to be used to await for the output of a single other queue user, producer or consumer
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct SingleBarrier {
    /// The single dependency
    dependency: Arc<UserOutput>,
}

impl Barrier for SingleBarrier {
    #[inline]
    fn next(&self, _observer: Sequence) -> Sequence {
        self.dependency.published()
    }

    fn duplicate(&self) -> Box<dyn Barrier> {
        Box::new(SingleBarrier {
            dependency: self.dependency.clone(),
        })
    }
}

impl SingleBarrier {
    /// Creates a new barrier and the associated dependency
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            dependency: Arc::new(UserOutput::default()),
        }
    }

    /// Creates a barrier that awaits on a single output
    #[must_use]
    pub fn await_on(dependency: &Arc<UserOutput>) -> Self {
        Self {
            dependency: dependency.clone(),
        }
    }

    /// Gets the dependency, i.e. the output for the user the barrier is waiting on
    #[must_use]
    pub(crate) fn get_dependency(&self) -> &Arc<UserOutput> {
        &self.dependency
    }
}

/// A barrier to be used to await for the output of multiple other queue users, producer or consumers
#[derive(Debug, Clone)]
pub struct MultiBarrier {
    /// All the dependencies
    dependencies: Vec<Arc<UserOutput>>,
}

// SAFETY: Safe only when the `next` method is call by only one specific thread.
// this is supposed to be the case by construction.
unsafe impl Send for MultiBarrier {}
unsafe impl Sync for MultiBarrier {}

impl Barrier for MultiBarrier {
    #[inline]
    fn next(&self, _observer: Sequence) -> Sequence {
        self.dependencies.iter().map(|o| o.published()).min().unwrap_or_default()
        // if dependencies.is_empty() {
        //     // short circuit to simplify return
        //     return Sequence::default();
        // }
        // let mut acc: Option<(usize, Sequence)> = None;
        // let mut index = 0;
        // while index < dependencies.len() {
        //     let published = unsafe { dependencies.get_unchecked(index) }.published();
        //     if !published.is_valid_item() || published <= observer {
        //         if index != 0 {
        //             // put on first because it is supposed to be the slowest
        //             dependencies.swap(0, index);
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
        //     dependencies.swap(0, index);
        // }
        // min
    }

    fn duplicate(&self) -> Box<dyn Barrier> {
        Box::new(MultiBarrier {
            dependencies: self.dependencies.clone(),
        })
    }
}

impl Default for MultiBarrier {
    /// Creates a new empty barrier
    fn default() -> Self {
        Self {
            dependencies: Vec::with_capacity(4),
        }
    }
}

impl MultiBarrier {
    /// Creates a multi barrier that awaits on multiple outputs
    #[must_use]
    pub fn await_on(dependencies: Vec<Arc<UserOutput>>) -> Self {
        Self { dependencies }
    }

    /// Adds a dependency to this barrier
    ///
    /// # Safety
    ///
    /// This method is only safe when called during the setup phase of the queue.
    pub(crate) fn add_dependency(&mut self, output: &Arc<UserOutput>) {
        self.dependencies.push(output.clone());
    }
}

#[cfg(test)]
mod tests_multi_barrier {
    use alloc::sync::Arc;

    use super::{MultiBarrier, UserOutput};
    use crate::queue::barriers::Barrier;
    use crate::queue::Sequence;

    #[test]
    fn test_next_no_dep() {
        let barrier = MultiBarrier::default();
        for i in -1..5_isize {
            assert_eq!(barrier.next(Sequence::from(i)), Sequence::default());
        }
    }

    fn test_next_single_dep_with_value(published: isize) {
        let mut barrier = MultiBarrier::default();
        barrier.add_dependency(&Arc::new(UserOutput::new(published)));
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
        let mut barrier = MultiBarrier::default();
        for &published in published {
            barrier.add_dependency(&Arc::new(UserOutput::new(published)));
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
