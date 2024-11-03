/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Implementation of a circular queue for channels

use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::fmt::Debug;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::Backoff;

use crate::errors::{RecvError, SendError, TryRecvError, TrySendError};

/// A slot in a queue
#[derive(Debug)]
pub struct Slot<T> {
    content: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
}

/// SAFETY: This is safe because the `content` of the slot is only accessed when after checking the `state`
unsafe impl<T> Sync for Slot<T> {}

/// A circular queue, to be accessed using cursors
/// A queue is expected to be accessed by only two threads, one for pushing items, the other for popping them.
#[derive(Debug)]
pub struct QueueSlots<T> {
    /// The buffer containing the slots themselves
    buffer: Box<[Slot<T>]>,
    /// The mask to use for getting an index with the buffer
    mask: usize,
}

impl<T> Drop for QueueSlots<T> {
    fn drop(&mut self) {
        if core::mem::needs_drop::<T>() {
            let slots_count = self.buffer.len();
            for (index, slot) in self.buffer.iter_mut().enumerate() {
                // no need to use atomic primitives since we got exclusive access
                let state = *slot.state.get_mut();
                let flag = (state - index) % slots_count;
                if flag != 0 {
                    // not free, need to drop the item
                    unsafe {
                        slot.content.get_mut().assume_init_drop();
                    }
                }
            }
        }
    }
}

impl<T> QueueSlots<T> {
    /// Creates a circular queue with the specified size
    pub fn new(size: usize) -> QueueSlots<T> {
        assert!(size.is_power_of_two(), "size must be power of two, got {size}");
        let buffer = (0..size)
            .map(|i| Slot {
                state: AtomicUsize::new(i),
                content: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Box<[_]>>();
        Self { buffer, mask: size - 1 }
    }

    /// Gets the capacity of the queue
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Gets the nunmber of messages actually in the queue using the cursor to push new items
    pub fn count_with_push_cursor(&self, cursor: usize) -> usize {
        let mut index = cursor;
        let mut count = 0;
        while count < self.buffer.len() {
            let state = unsafe { self.buffer.get_unchecked(index & self.mask) }
                .state
                .load(Ordering::Acquire);
            if state != index + 1 {
                // not an occupied slot, stop
                return count;
            }
            // continue
            if index == 0 {
                break;
            }
            index -= 1;
            count += 1;
        }
        count
    }

    /// Gets the nunmber of messages actually in the queue using the cursor to pop items
    pub fn count_with_pop_cursor(&self, cursor: usize) -> usize {
        let mut index = cursor;
        let mut count = 0;
        while count < self.buffer.len() {
            let state = unsafe { self.buffer.get_unchecked(index & self.mask) }
                .state
                .load(Ordering::Acquire);
            if state != index + 1 {
                // not an occupied slot, stop
                return count;
            }
            // continue
            index += 1;
            count += 1;
        }
        count
    }

    /// Determines whether the queue is empty using the cursor to push new items
    pub fn is_empty_with_push_cursor(&self, cursor: usize) -> bool {
        let mut index = cursor;
        let mut count = 0;
        while count < self.buffer.len() {
            let state = unsafe { self.buffer.get_unchecked(index & self.mask) }
                .state
                .load(Ordering::Acquire);
            if state == index + 1 {
                // not free
                return false;
            }
            // continue
            if index == 0 {
                index = self.buffer.len() - 1;
            } else {
                index -= 1;
            }
            count += 1;
        }
        true
    }

    /// Determines whether the queue is empty using the cursor to pop items
    #[inline]
    pub fn is_empty_with_pop_cursor(&self, cursor: usize) -> bool {
        let state = unsafe { self.buffer.get_unchecked(cursor & self.mask) }
            .state
            .load(Ordering::Acquire);
        state == cursor // slot is free => queue is empty
    }

    /// Determines whether the queue is full using the cursor to push new items
    #[inline]
    pub fn is_full_with_push_cursor(&self, cursor: usize) -> bool {
        let state = unsafe { self.buffer.get_unchecked(cursor & self.mask) }
            .state
            .load(Ordering::Acquire);
        state != cursor // slot is not free => queue is full
    }

    /// Determines whether the queue is full using the cursor to pop items
    pub fn is_full_with_pop_cursor(&self, cursor: usize) -> bool {
        let mut index = cursor;
        let mut count = 0;
        while count < self.buffer.len() {
            let state = unsafe { self.buffer.get_unchecked(index & self.mask) }
                .state
                .load(Ordering::Acquire);
            if state != index + 1 {
                // free => queue is nor full
                return false;
            }
            // continue
            index += 1;
            count += 1;
        }
        true
    }

    /// Attempts to push an item using the specified cursor
    pub fn try_push(self: &Arc<Self>, cursor: &mut usize, item: T) -> Result<(), TrySendError<T>> {
        let slot = unsafe { self.buffer.get_unchecked(*cursor & self.mask) };
        let slot_state = slot.state.load(Ordering::Acquire);
        if slot_state == *cursor {
            // slot is available
            unsafe { slot.content.get().write(MaybeUninit::new(item)) };
            // make it available to read
            slot.state.store(*cursor + 1, Ordering::Release);
            *cursor += 1;
            Ok(())
        } else {
            // slot is full => queue is full
            Err(if Arc::strong_count(self) == 1 {
                TrySendError::Disconnected(item)
            } else {
                TrySendError::Full(item)
            })
        }
    }

    /// Blocks while pushing an item using the specified cursor
    pub fn push(self: &Arc<Self>, cursor: &mut usize, mut item: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();
        loop {
            match self.try_push(cursor, item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(item)) => return Err(SendError(item)),
                Err(TrySendError::Full(item_back)) => {
                    item = item_back;
                    // queue is full, we need to wait for another thread to make progress
                    backoff.snooze();
                }
            }
        }
    }

    /// Attempts to pop an item using the specified cursor
    pub fn try_pop(self: &Arc<Self>, cursor: &mut usize) -> Result<T, TryRecvError> {
        let slot = unsafe { self.buffer.get_unchecked(*cursor & self.mask) };
        let slot_state = slot.state.load(Ordering::Acquire);
        if slot_state == *cursor + 1 {
            // slot has content
            let item = unsafe { slot.content.get().read().assume_init() };
            // make it available to write again on next cycle
            slot.state.store(*cursor + self.buffer.len(), Ordering::Release);
            *cursor += 1;
            Ok(item)
        } else {
            // slot is empty => queue is empty
            Err(if Arc::strong_count(self) == 1 {
                TryRecvError::Disconnected
            } else {
                TryRecvError::Empty
            })
        }
    }

    /// Blocks while getting the next item using the specified cursor
    pub fn pop(self: &Arc<Self>, cursor: &mut usize) -> Result<T, RecvError> {
        let backoff = Backoff::new();
        loop {
            match self.try_pop(cursor) {
                Ok(item) => return Ok(item),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {
                    // queue is empty, we need to wait for another thread to make progress
                    backoff.snooze();
                }
            }
        }
    }

    /// Drains the content of the queue using the cursor for pushing items
    pub fn drain_with_push_cursor(self: Arc<Self>, cursor: usize, buffer: &mut Vec<T>) -> Result<usize, Arc<Self>> {
        if Arc::strong_count(&self) != 1 {
            return Err(self);
        }
        let mut index = cursor;
        let mut count = 0;
        while count < self.buffer.len() {
            let slot = unsafe { self.buffer.get_unchecked(index & self.mask) };
            let state = slot.state.load(Ordering::Acquire);
            if state != index + 1 {
                // no data in slot => stop here
                return Ok(count);
            }
            // data is in slot
            let item = unsafe { slot.content.get().read().assume_init() };
            buffer.push(item);
            // continue
            if index == 0 {
                break;
            }
            index -= 1;
            count += 1;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests_slots {
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::QueueSlots;

    struct DropCallback(Box<dyn Fn()>);

    impl Drop for DropCallback {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    #[test]
    fn test_slots_drop_empty() {
        let _queue = QueueSlots::<DropCallback>::new(4);
    }

    fn test_slots_drop_full_with(count: usize, lap_count: usize) {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let mut queue = QueueSlots::<DropCallback>::new(count);
        for (index, slot) in queue.buffer.iter_mut().enumerate() {
            let content = DropCallback(Box::new({
                let drop_count = drop_count.clone();
                move || {
                    drop_count.fetch_add(1, Ordering::SeqCst);
                }
            }));
            slot.content.get_mut().write(content);
            *slot.state.get_mut() = lap_count * count + index + 1;
        }
        drop(queue);
        assert_eq!(count, drop_count.load(Ordering::SeqCst));
    }

    #[test]
    fn test_slots_drop_full() {
        test_slots_drop_full_with(4, 0);
        test_slots_drop_full_with(4, 1);
        test_slots_drop_full_with(4, 2);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_slots_drop_full_mid_lap() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let mut queue = QueueSlots::<DropCallback>::new(4);

        for slot in &mut queue.buffer {
            slot.content.get_mut().write(DropCallback(Box::new({
                let drop_count = drop_count.clone();
                move || {
                    drop_count.fetch_add(1, Ordering::SeqCst);
                }
            })));
        }

        *queue.buffer[0].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 0 + 1;
        *queue.buffer[1].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 1 + 1;
        *queue.buffer[2].state.get_mut() = /*lap*/ 1 * /*count*/ 4 + /*index*/ 2 + 1;
        *queue.buffer[3].state.get_mut() = /*lap*/ 1 * /*count*/ 4 + /*index*/ 3 + 1;

        drop(queue);
        assert_eq!(4, drop_count.load(Ordering::SeqCst));
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_slots_drop_half_full_middle() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let mut queue = QueueSlots::<DropCallback>::new(4);

        queue.buffer[1].content.get_mut().write(DropCallback(Box::new({
            let drop_count = drop_count.clone();
            move || {
                drop_count.fetch_add(1, Ordering::SeqCst);
            }
        })));
        queue.buffer[2].content.get_mut().write(DropCallback(Box::new({
            let drop_count = drop_count.clone();
            move || {
                drop_count.fetch_add(1, Ordering::SeqCst);
            }
        })));

        *queue.buffer[0].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 0 + 0;
        *queue.buffer[1].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 1 + 1;
        *queue.buffer[2].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 2 + 1;
        *queue.buffer[3].state.get_mut() = /*lap*/ 1 * /*count*/ 4 + /*index*/ 3 + 0;

        drop(queue);
        assert_eq!(2, drop_count.load(Ordering::SeqCst));
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_slots_drop_half_full_split() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let mut queue = QueueSlots::<DropCallback>::new(4);

        queue.buffer[0].content.get_mut().write(DropCallback(Box::new({
            let drop_count = drop_count.clone();
            move || {
                drop_count.fetch_add(1, Ordering::SeqCst);
            }
        })));
        queue.buffer[3].content.get_mut().write(DropCallback(Box::new({
            let drop_count = drop_count.clone();
            move || {
                drop_count.fetch_add(1, Ordering::SeqCst);
            }
        })));

        *queue.buffer[0].state.get_mut() = /*lap*/ 3 * /*count*/ 4 + /*index*/ 0 + 1;
        *queue.buffer[1].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 1 + 0;
        *queue.buffer[2].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 2 + 0;
        *queue.buffer[3].state.get_mut() = /*lap*/ 2 * /*count*/ 4 + /*index*/ 3 + 1;

        drop(queue);
        assert_eq!(2, drop_count.load(Ordering::SeqCst));
    }
}

/// A cursor to access a queue
pub struct QueueCursor<T> {
    /// The backing circular queue
    pub slots: Arc<QueueSlots<T>>,
    /// The current index within the queue
    pub index: usize,
}

impl<T> Debug for QueueCursor<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_map().key(&"index").value(&self.index).finish()
    }
}

impl<T> QueueCursor<T> {
    /// Gets whether this is the only cursor for the associated queue
    #[must_use]
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.slots) == 1
    }
}

/// Hub to access multiple queues, either for aggregating multiple sender, or for dispatching to multiple receivers
#[derive(Debug)]
pub struct QueuesHub<T> {
    /// The size of the queues
    pub queue_size: usize,
    /// The aggregated cursors
    pub cursors: Vec<QueueCursor<T>>,
    /// Index of the next cursor to use
    pub next: usize,
}

impl<T> QueuesHub<T> {
    /// Gets the number of cursors on the other side of this hub
    pub fn connected_other_sides(&self) -> usize {
        self.cursors.iter().map(|cursor| Arc::strong_count(&cursor.slots) - 1).sum()
    }

    /// Gets the capacity of the queues
    pub fn capacity(&self) -> usize {
        self.queue_size
    }

    /// Gets the nunmber of messages actually in the queue assuming this hub is used to push items
    pub fn count_assuming_push_use(&self) -> usize {
        self.cursors
            .iter()
            .map(|cursor| cursor.slots.count_with_push_cursor(cursor.index))
            .sum()
    }

    /// Gets the nunmber of messages actually in the queue assuming this hub is used to pop items
    pub fn count_assuming_pop_use(&self) -> usize {
        self.cursors
            .iter()
            .map(|cursor| cursor.slots.count_with_pop_cursor(cursor.index))
            .sum()
    }

    /// Determines whether the queue is empty assuming this hub is used to push items
    pub fn is_empty_assuming_push_use(&self) -> bool {
        self.cursors
            .iter()
            .all(|cursor| cursor.slots.is_empty_with_push_cursor(cursor.index))
    }

    /// Determines whether the queue is empty assuming this hub is used to pop items
    pub fn is_empty_assuming_pop_use(&self) -> bool {
        self.cursors
            .iter()
            .all(|cursor| cursor.slots.is_empty_with_pop_cursor(cursor.index))
    }

    /// Determines whether the queue is full assuming this hub is used to push items
    pub fn is_full_assuming_push_use(&self) -> bool {
        self.cursors
            .iter()
            .all(|cursor| cursor.slots.is_full_with_push_cursor(cursor.index))
    }

    /// Determines whether the queue is full assuming this hub is used to pop items
    pub fn is_full_assuming_pop_use(&self) -> bool {
        self.cursors
            .iter()
            .all(|cursor| cursor.slots.is_full_with_pop_cursor(cursor.index))
    }

    /// Attempts to push an item in the case of a single sender, multiple receivers
    pub fn try_push(&mut self, mut item: T) -> Result<(), TrySendError<T>> {
        let mut count = 0;
        while count < self.cursors.len() {
            debug_assert!(self.next < self.cursors.len());
            let cursor = unsafe { self.cursors.get_unchecked_mut(self.next) };
            match cursor.slots.try_push(&mut cursor.index, item) {
                Ok(()) => {
                    return Ok(());
                }
                Err(error) => {
                    count += 1;
                    let to_remove = error.is_disconnected();
                    item = error.into_inner();
                    if to_remove {
                        self.cursors.swap_remove(self.next);
                    } else {
                        self.next += 1;
                    }
                    if self.next >= self.cursors.len() {
                        self.next = 0;
                    }
                }
            }
        }
        if self.cursors.is_empty() {
            Err(TrySendError::Disconnected(item))
        } else {
            Err(TrySendError::Full(item))
        }
    }

    /// Tries to push for a specific queue
    fn try_push_single_queue(&mut self, index: usize, item: T) -> Result<(), TrySendError<T>> {
        let cursor = unsafe { self.cursors.get_unchecked_mut(index) };
        match cursor.slots.try_push(&mut cursor.index, item) {
            Ok(()) => Ok(()),
            Err(error) => {
                if error.is_disconnected() {
                    // remove from the cursor
                    self.cursors.remove(index);
                }
                Err(error)
            }
        }
    }

    /// Blocks while pushing an item
    pub fn push(&mut self, mut item: T) -> Result<(), SendError<T>> {
        let backoff = Backoff::new();
        loop {
            match self.try_push(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(item)) => return Err(SendError(item)),
                Err(TrySendError::Full(item_back)) => {
                    item = item_back;
                    // queue is full, we need to wait for another thread to make progress
                    backoff.snooze();
                }
            }
        }
    }

    /// Attempts to pop an item in the case of a single receiver, mutiple senders
    pub fn try_pop(&mut self) -> Result<T, TryRecvError> {
        let mut count = 0;
        while count < self.cursors.len() {
            debug_assert!(self.next < self.cursors.len());
            let cursor = unsafe { self.cursors.get_unchecked_mut(self.next) };
            let result = cursor.slots.try_pop(&mut cursor.index);
            match result {
                Ok(item) => {
                    return Ok(item);
                }
                Err(error) => {
                    count += 1;
                    if error == TryRecvError::Disconnected {
                        self.cursors.swap_remove(self.next);
                    } else {
                        self.next += 1;
                    }
                    if self.next >= self.cursors.len() {
                        self.next = 0;
                    }
                }
            }
        }
        if self.cursors.is_empty() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Blocks while getting the next item
    pub fn pop(&mut self) -> Result<T, RecvError> {
        let backoff = Backoff::new();
        loop {
            match self.try_pop() {
                Ok(item) => return Ok(item),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {
                    // queue is empty, we need to wait for another thread to make progress
                    backoff.snooze();
                }
            }
        }
    }

    /// Attempts to pop multiple items from all the connected senders
    pub fn try_pop_multiple(&mut self, buffer: &mut Vec<T>) -> Result<usize, TryRecvError> {
        let mut count = 0;
        let mut popped = 0;
        while count < self.cursors.len() {
            debug_assert!(self.next < self.cursors.len());
            let cursor = unsafe { self.cursors.get_unchecked_mut(self.next) };
            let result = cursor.slots.try_pop(&mut cursor.index);
            match result {
                Ok(item) => {
                    buffer.push(item);
                    popped += 1;
                }
                Err(error) => {
                    count += 1;
                    if error == TryRecvError::Disconnected {
                        self.cursors.swap_remove(self.next);
                    } else {
                        self.next += 1;
                    }
                    if self.next >= self.cursors.len() {
                        self.next = 0;
                    }
                }
            }
        }
        if popped > 0 {
            Ok(popped)
        } else if self.cursors.is_empty() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Attempts to get the unreceived items assuming this is a hub to push items
    pub fn get_unreceivables(&mut self, buffer: &mut Vec<T>) -> Result<usize, ()> {
        let cursors_count = self.cursors.len();
        if cursors_count == 0 {
            return Ok(0);
        }

        let mut total = 0;
        self.cursors = self
            .cursors
            .drain(..)
            .filter_map(|mut cursor| {
                match cursor.slots.drain_with_push_cursor(cursor.index, buffer) {
                    Ok(count) => {
                        // this one was disconnected
                        total += count;
                        None
                    }
                    Err(slots) => {
                        // still connected on the other side
                        cursor.slots = slots;
                        Some(cursor)
                    }
                }
            })
            .collect();

        if cursors_count == self.cursors.len() {
            // did not remove a single cursor => no disconnected
            Err(())
        } else {
            Ok(total)
        }
    }
}

impl<T: Clone> QueuesHub<T> {
    /// Pushes the same item onto all associated queues
    pub fn push_to_all(&mut self, item: T) -> Result<usize, SendError<T>> {
        let mut success = self.cursors.iter().map(|_| false).collect::<Vec<_>>();
        let mut sucess_total = 0;
        while sucess_total != self.cursors.len() {
            let mut index = 0;
            while index < self.cursors.len() {
                if success[index] {
                    index += 1;
                    continue;
                }
                match self.try_push_single_queue(index, item.clone()) {
                    Ok(()) => {
                        success[index] = true;
                        sucess_total += 1;
                        index += 1;
                    }
                    Err(error) => {
                        if error.is_disconnected() {
                            success.remove(index);
                        } else {
                            index += 1;
                        }
                    }
                }
            }
        }
        if sucess_total == 0 {
            Err(SendError(item))
        } else {
            Ok(sucess_total)
        }
    }
}
