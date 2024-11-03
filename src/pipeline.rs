/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

//! Data model for parallel pipelines

use alloc::sync::Arc;
use core::marker::PhantomData;

/// An element in a pipeline that handles events
pub trait PipelineElement: Sized + Clone {
    /// The type of the original event
    type Event;
    /// The output type for events handled at this step
    type Output;

    /// Inserts the event in the pipeline and make it through each step in turn.
    ///
    /// Return `None` when the event did not reach this step
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output>;

    /// Builds a pipeline by adding an additional sequential step after this point
    fn then<ON, H>(self, handler: H) -> impl PipelineElement<Event = Self::Event, Output = ON>
    where
        H: Fn(Self::Output) -> ON + Send + Sync + 'static,
    {
        PipelineMap {
            handler: Arc::new(handler),
            before: self,
        }
    }

    /// Adds a filtering step
    fn filter<H>(self, handler: H) -> impl PipelineElement<Event = Self::Event, Output = Self::Output>
    where
        H: Fn(Self::Output) -> Option<Self::Output> + Send + Sync + 'static,
    {
        PipelineFilter {
            handler: Arc::new(handler),
            before: self,
        }
    }

    /// Adds a filtering step that also maps to another type
    fn filter_map<ON, H>(self, handler: H) -> impl PipelineElement<Event = Self::Event, Output = ON>
    where
        H: Fn(Self::Output) -> Option<ON> + Send + Sync + 'static,
    {
        PipelineFilter {
            handler: Arc::new(handler),
            before: self,
        }
    }

    /// Adds an enumeration step that counts the received events
    fn enumerate(self) -> impl PipelineElement<Event = Self::Event, Output = (usize, Self::Output)> {
        PipelineEnumerate { before: self, count: 0 }
    }

    /// Adds a copying step
    fn copied<'a, O>(self) -> impl PipelineElement<Event = Self::Event, Output = O>
    where
        O: 'a + Copy,
        Self: PipelineElement<Output = &'a O>,
    {
        PipelineMap {
            handler: Arc::new(|input_ref: &O| *input_ref),
            before: self,
        }
    }

    /// Adds a clone step
    fn cloned<'a, O>(self) -> impl PipelineElement<Event = Self::Event, Output = O>
    where
        O: 'a + Clone,
        Self: PipelineElement<Output = &'a O>,
    {
        #[allow(clippy::redundant_closure_for_method_calls)]
        PipelineMap {
            handler: Arc::new(|input: &O| input.clone()),
            before: self,
        }
    }

    /// Adds a accumulation step that wait for a number of events
    fn accumulate(self, count: usize) -> impl PipelineElement<Event = Self::Event, Output = Box<[Self::Output]>> {
        PipelineBatcher {
            before: self,
            count,
            batch: Vec::with_capacity(count),
        }
    }

    /// Adds a folding step every element into an accumulator by applying an operation, returning the final result.
    fn fold<ON, H>(self, init: ON, fold: H) -> impl PipelineElement<Event = Self::Event, Output = ON>
    where
        ON: Clone,
        H: Fn(ON, Self::Output) -> FoldResult<ON> + Send + Sync + 'static,
    {
        PipelineFold {
            before: self,
            init,
            accumulator: None,
            fold: Arc::new(fold),
        }
    }

    /// Reduces the elements to a single one, by repeatedly applying a reducing operation.
    fn reduce<H>(self, reduce: H) -> impl PipelineElement<Event = Self::Event, Output = Self::Output>
    where
        Self::Output: Clone,
        H: Fn(Self::Output, Self::Output) -> FoldResult<Self::Output> + Send + Sync + 'static,
    {
        PipelineReduce {
            before: self,
            accumulator: None,
            reduce: Arc::new(reduce),
        }
    }
}

pub trait PipelineFaillibleElement: PipelineElement {
    type Error;

    fn catch_error<H>(self, handler: H) -> PipelineOnError<Self::Output, Self::Error, Self>
    where
        H: Fn(Self::Error) + Send + Sync + 'static,
    {
        PipelineOnError {
            handler: Arc::new(handler),
            before: self,
            _data: PhantomData,
        }
    }
}

impl<O, E, T> PipelineFaillibleElement for T
where
    T: PipelineElement<Output = Result<O, E>>,
{
    type Error = E;
}

#[derive(Debug)]
pub struct PipelineEntry<I> {
    _data: PhantomData<I>,
}

impl<I> Default for PipelineEntry<I> {
    fn default() -> Self {
        Self { _data: PhantomData }
    }
}

impl<I> Clone for PipelineEntry<I> {
    fn clone(&self) -> Self {
        PipelineEntry::<I>::default()
    }
}

impl<I> PipelineElement for PipelineEntry<I> {
    type Event = I;
    type Output = I;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        Some(event)
    }
}

pub struct PipelineMap<I, O, BEFORE> {
    handler: Arc<dyn Fn(I) -> O + Send + Sync>,
    before: BEFORE,
}

impl<I, O, BEFORE: Clone> Clone for PipelineMap<I, O, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            before: self.before.clone(),
        }
    }
}

impl<EVENT, I, O, BEFORE> PipelineElement for PipelineMap<I, O, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = O;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        Some((self.handler)(my_event))
    }
}

pub struct PipelineFilter<I, O, BEFORE> {
    handler: Arc<dyn Fn(I) -> Option<O> + Send + Sync>,
    before: BEFORE,
}

impl<I, O, BEFORE: Clone> Clone for PipelineFilter<I, O, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            before: self.before.clone(),
        }
    }
}

impl<EVENT, I, O, BEFORE> PipelineElement for PipelineFilter<I, O, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = O;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        (self.handler)(my_event)
    }
}

pub struct PipelineEnumerate<BEFORE> {
    before: BEFORE,
    count: usize,
}

impl<BEFORE: Clone> Clone for PipelineEnumerate<BEFORE> {
    fn clone(&self) -> Self {
        Self {
            before: self.before.clone(),
            count: 0,
        }
    }
}

impl<EVENT, I, BEFORE> PipelineElement for PipelineEnumerate<BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = (usize, I);

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        let c = self.count;
        self.count += 1;
        Some((c, my_event))
    }
}

pub struct PipelineBatcher<I, BEFORE> {
    before: BEFORE,
    count: usize,
    batch: Vec<I>,
}

impl<I, BEFORE: Clone> Clone for PipelineBatcher<I, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            before: self.before.clone(),
            count: self.count,
            batch: Vec::with_capacity(self.count),
        }
    }
}

impl<EVENT, I, BEFORE> PipelineElement for PipelineBatcher<I, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = Box<[I]>;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        self.batch.push(my_event);
        if self.batch.len() >= self.count {
            let mut to_be_sent = Vec::with_capacity(self.count);
            core::mem::swap(&mut self.batch, &mut to_be_sent);
            Some(to_be_sent.into_boxed_slice())
        } else {
            None
        }
    }
}

pub enum FoldResult<O> {
    Accumulate(O),
    Yield(O),
}

pub struct PipelineFold<I, O, BEFORE> {
    before: BEFORE,
    init: O,
    accumulator: Option<O>,
    fold: Arc<dyn Fn(O, I) -> FoldResult<O>>,
}

impl<I, O: Clone, BEFORE: Clone> Clone for PipelineFold<I, O, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            before: self.before.clone(),
            init: self.init.clone(),
            accumulator: None,
            fold: self.fold.clone(),
        }
    }
}

impl<EVENT, I, O: Clone, BEFORE> PipelineElement for PipelineFold<I, O, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = O;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        match (self.fold)(self.accumulator.take().unwrap_or_else(|| self.init.clone()), my_event) {
            FoldResult::Accumulate(accumulator) => {
                self.accumulator = Some(accumulator);
                None
            }
            FoldResult::Yield(value) => {
                self.accumulator = None;
                Some(value)
            }
        }
    }
}

pub struct PipelineReduce<I, BEFORE> {
    before: BEFORE,
    accumulator: Option<I>,
    reduce: Arc<dyn Fn(I, I) -> FoldResult<I>>,
}

impl<I, BEFORE: Clone> Clone for PipelineReduce<I, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            before: self.before.clone(),
            accumulator: None,
            reduce: self.reduce.clone(),
        }
    }
}

impl<EVENT, I, BEFORE> PipelineElement for PipelineReduce<I, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = I>,
{
    type Event = EVENT;
    type Output = I;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        match self.accumulator.take() {
            None => {
                self.accumulator = Some(my_event);
                None
            }
            Some(accumulator) => match (self.reduce)(accumulator, my_event) {
                FoldResult::Accumulate(accumulator) => {
                    self.accumulator = Some(accumulator);
                    None
                }
                FoldResult::Yield(value) => {
                    self.accumulator = None;
                    Some(value)
                }
            },
        }
    }
}

pub struct PipelineOnError<I, E, BEFORE> {
    handler: Arc<dyn Fn(E) + Send + Sync>,
    before: BEFORE,
    _data: PhantomData<I>,
}

impl<I, E, BEFORE: Clone> Clone for PipelineOnError<I, E, BEFORE> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            before: self.before.clone(),
            _data: PhantomData,
        }
    }
}

impl<EVENT, I, E, BEFORE> PipelineElement for PipelineOnError<Result<I, E>, E, BEFORE>
where
    BEFORE: PipelineElement<Event = EVENT, Output = Result<I, E>>,
{
    type Event = EVENT;
    type Output = I;

    #[inline]
    fn on_event(&mut self, event: Self::Event) -> Option<Self::Output> {
        let my_event = self.before.on_event(event)?;
        match my_event {
            Ok(my_event) => Some(my_event),
            Err(error) => {
                (self.handler)(error);
                None
            }
        }
    }
}

/// Begins the creation of a new pipeline
#[must_use]
pub fn new_pipeline<I>() -> PipelineEntry<I> {
    PipelineEntry::default()
}
