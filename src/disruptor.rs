/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! The disruptor, high throughput event handlers

use std::sync::Arc;
use std::thread::JoinHandle;

use crate::channels::{McSender, SingleSender};
use crate::errors::TryRecvError;
use crate::pipeline::PipelineElement;
use crate::{Receiver, Sender};

/// The different mode for a disruptor instance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// A single thread disruptor
    Single,
    /// A concurrent (multi-threaded) diruptor with a a specific number of threads
    Concurrent(usize),
}

pub type ThreadStartHandler<E> = dyn Fn(usize, &mut E) + Send + Sync + 'static;
pub type ThreadEndHandler<E, T> = dyn Fn(usize, &mut E) -> T + Send + Sync + 'static;

/// A builder to build a disruptor
pub struct Builder<ELEMENT, T> {
    mode: Option<Mode>,
    queue_size: Option<usize>,
    pipeline: Option<ELEMENT>,
    thread_on_start: Option<Arc<ThreadStartHandler<ELEMENT>>>,
    thread_on_end: Arc<ThreadEndHandler<ELEMENT, T>>,
}

impl<ELEMENT> Default for Builder<ELEMENT, ()> {
    fn default() -> Self {
        Self {
            mode: None,
            queue_size: None,
            pipeline: None,
            thread_on_start: None,
            thread_on_end: Arc::new(|_, _| {}),
        }
    }
}

impl<ELEMENT> Builder<ELEMENT, ()> {
    /// Gets a new builder
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<ELEMENT, T> Builder<ELEMENT, T> {
    /// Sets the mode to single-threaded
    #[must_use]
    pub fn mode_single(mut self) -> Self {
        self.mode = Some(Mode::Single);
        self
    }

    /// Sets the mode to multi-threaded
    #[must_use]
    pub fn mode_concurrent(mut self, thread_number: usize) -> Self {
        self.mode = Some(Mode::Concurrent(thread_number));
        self
    }

    /// Sets the size of queues for message passing
    #[must_use]
    pub fn queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = Some(queue_size);
        self
    }

    /// Sets the pipeline to use to handle events
    #[must_use]
    pub fn pipeline(mut self, pipeline: ELEMENT) -> Self {
        self.pipeline = Some(pipeline);
        self
    }

    /// Sets the handler called when starting a thread
    #[must_use]
    pub fn on_thread_start<F>(mut self, handler: F) -> Self
    where
        F: Fn(usize, &mut ELEMENT) + Send + Sync + 'static,
    {
        self.thread_on_start = Some(Arc::new(handler));
        self
    }

    /// Sets the handler called when a thread ends
    #[must_use]
    pub fn on_thread_end<F, T2>(self, handler: F) -> Builder<ELEMENT, T2>
    where
        F: Fn(usize, &mut ELEMENT) -> T2 + Send + Sync + 'static,
    {
        Builder {
            mode: self.mode,
            queue_size: self.queue_size,
            pipeline: self.pipeline,
            thread_on_start: self.thread_on_start,
            thread_on_end: Arc::new(handler),
        }
    }

    /// Starts the disruptor according to this configuration
    #[must_use]
    pub fn start<EVENT>(mut self) -> Disruptor<EVENT, T>
    where
        T: Send + 'static,
        EVENT: Send + 'static,
        ELEMENT: PipelineElement<Event = EVENT> + Send + 'static,
    {
        self.mode = Some(self.mode.unwrap_or(Mode::Concurrent(4)));
        match self.mode.unwrap() {
            Mode::Single => Disruptor::new_single(self),
            Mode::Concurrent(_) => Disruptor::new_concurrent(&self),
        }
    }
}

enum DisruptorSender<E> {
    Single(SingleSender<E>),
    Multi(McSender<E>),
}

impl<E> Sender<E> for DisruptorSender<E> {
    #[inline]
    fn is_disconnected(&self) -> bool {
        match self {
            Self::Single(sender) => sender.is_disconnected(),
            Self::Multi(sender) => sender.is_disconnected(),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        match self {
            Self::Single(sender) => sender.capacity(),
            Self::Multi(sender) => sender.capacity(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Single(sender) => sender.len(),
            Self::Multi(sender) => sender.len(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            Self::Single(sender) => sender.is_empty(),
            Self::Multi(sender) => sender.is_empty(),
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        match self {
            Self::Single(sender) => sender.is_full(),
            Self::Multi(sender) => sender.is_full(),
        }
    }

    #[inline]
    fn try_send(&mut self, item: E) -> Result<(), crate::prelude::TrySendError<E>> {
        match self {
            Self::Single(sender) => sender.try_send(item),
            Self::Multi(sender) => sender.try_send(item),
        }
    }

    #[inline]
    fn send(&mut self, item: E) -> Result<(), crate::prelude::SendError<E>> {
        match self {
            Self::Single(sender) => sender.send(item),
            Self::Multi(sender) => sender.send(item),
        }
    }

    #[inline]
    fn into_unreceived(self) -> Result<Vec<E>, Self> {
        match self {
            Self::Single(sender) => sender.into_unreceived().map_err(|sender| Self::Single(sender)),
            Self::Multi(sender) => sender.into_unreceived().map_err(|sender| Self::Multi(sender)),
        }
    }
}

/// The disruptor, an parallel event handler
pub struct Disruptor<EVENT, T> {
    sender: DisruptorSender<EVENT>,
    thread_handles: Vec<JoinHandle<T>>,
}

impl<EVENT, T> Disruptor<EVENT, T>
where
    T: Send + 'static,
    EVENT: Send + 'static,
{
    //// Builds a new single-threaded disruptor
    fn new_single<ELEMENT>(config: Builder<ELEMENT, T>) -> Disruptor<EVENT, T>
    where
        ELEMENT: PipelineElement<Event = EVENT> + Send + 'static,
    {
        let (sender, mut consumer) = crate::channels::channel_spsc(config.queue_size.unwrap_or(32));
        let handles = vec![std::thread::Builder::new()
            .name(String::from("disruptor-0000"))
            .spawn({
                let thread_on_start = config.thread_on_start.clone();
                let thread_on_end = config.thread_on_end.clone();
                let mut pipeline = config.pipeline.unwrap();
                let index = 0;
                move || {
                    loop {
                        if let Some(handler) = thread_on_start.as_ref() {
                            handler(index, &mut pipeline);
                        }
                        match consumer.try_recv() {
                            Ok(event) => {
                                pipeline.on_event(event);
                            }
                            Err(TryRecvError::Disconnected) => {
                                break;
                            }
                            Err(TryRecvError::Empty) => {}
                        }
                    }
                    (thread_on_end.as_ref())(index, &mut pipeline)
                }
            })
            .unwrap()];
        Self {
            sender: DisruptorSender::Single(sender),
            thread_handles: handles,
        }
    }

    //// Builds a new multi-threaded disruptor
    fn new_concurrent<ELEMENT>(config: &Builder<ELEMENT, T>) -> Disruptor<EVENT, T>
    where
        ELEMENT: PipelineElement<Event = EVENT> + Send + 'static,
    {
        let mut sender = crate::channels::channel_spmc(config.queue_size.unwrap_or(32));
        let Some(Mode::Concurrent(concurrency)) = config.mode else {
            panic!("expected concurrency mode")
        };
        let handles = (0..concurrency)
            .map(|index| {
                std::thread::Builder::new()
                    .name(format!("disruptor-{index:04}"))
                    .spawn({
                        let mut consumer = sender.add_receiver();
                        let thread_on_start = config.thread_on_start.clone();
                        let thread_on_end = config.thread_on_end.clone();
                        let mut pipeline = config.pipeline.clone().unwrap();
                        move || {
                            loop {
                                if let Some(handler) = thread_on_start.as_ref() {
                                    handler(index, &mut pipeline);
                                }
                                match consumer.try_recv() {
                                    Ok(event) => {
                                        pipeline.on_event(event);
                                    }
                                    Err(TryRecvError::Disconnected) => {
                                        break;
                                    }
                                    Err(TryRecvError::Empty) => {}
                                }
                            }
                            (thread_on_end.as_ref())(index, &mut pipeline)
                        }
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Self {
            sender: DisruptorSender::Multi(sender),
            thread_handles: handles,
        }
    }

    /// Gets the associated sender
    #[inline]
    pub fn sender(&mut self) -> &mut impl Sender<EVENT> {
        &mut self.sender
    }

    /// Shutdowns the instance
    #[must_use]
    pub fn shutdown(self) -> Vec<T> {
        drop(self.sender);
        self.thread_handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    }
}
