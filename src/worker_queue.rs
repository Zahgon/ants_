/*
 * Copyright (c) 2019. Ants Authors. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
//! The worker abstraction and the queues that park idle workers.

use std::sync::Arc;

use crate::deps::gotime::Duration;
use crate::worker_loop_queue::LoopQueue;
use crate::worker_stack::WorkerStack;
use crate::Error;

/// A recyclable executor owned by a pool.
///
/// The pool only ever needs to stop a worker and to know when it was last used;
/// everything else about it is the concrete worker's business.
pub trait Worker: Send + Sync + 'static {
    /// Tells the worker to stop, i.e. Go's `finish()`.
    fn finish(&self);
    /// When the worker was last returned to the queue, in Unix nanoseconds.
    fn last_used_time(&self) -> i64;
    /// Records when the worker was returned to the queue.
    fn set_last_used_time(&self, time: i64);
}

/// A container of idle workers.
///
/// The two implementations differ in more than their data structure: the stack
/// grows on demand, the ring buffer is pre-allocated and can refuse an insert.
pub trait WorkerQueue<W: Worker>: Send {
    /// How many workers are parked.
    fn len(&self) -> usize;
    /// Whether no worker is parked.
    fn is_empty(&self) -> bool;
    /// Parks `worker`, or fails with [`Error::QueueIsFull`].
    fn insert(&mut self, worker: Arc<W>) -> Result<(), Error>;
    /// Takes a worker out of the queue, if there is one.
    fn detach(&mut self) -> Option<Arc<W>>;
    /// Removes and returns every worker unused for longer than `duration`.
    fn refresh(&mut self, duration: Duration) -> Vec<Arc<W>>;
    /// Finishes and drops every parked worker.
    fn reset(&mut self);
}

/// Which queue implementation a pool parks its idle workers in.
///
/// A plain integer rather than an enum, because the original is a plain `int`
/// and its factory deliberately accepts — and defaults — unknown values.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct QueueType(pub i32);

impl QueueType {
    /// A LIFO stack that grows on demand.
    pub const STACK: QueueType = QueueType(1);
    /// A fixed-capacity ring buffer, used under `PreAlloc`.
    pub const LOOP_QUEUE: QueueType = QueueType(1 << 1);
}

/// Builds the queue `queue_type` names, falling back to a stack for any
/// unrecognised value.
///
/// # Panics
///
/// Panics when a loop queue is asked for with a non-positive `size`. The pool
/// only selects a loop queue under `PreAlloc`, which rejects a non-positive
/// capacity with [`Error::InvalidPreAllocSize`] before ever getting here.
pub fn new_worker_queue<W: Worker>(queue_type: QueueType, size: i32) -> Box<dyn WorkerQueue<W>> {
    match queue_type {
        QueueType::LOOP_QUEUE => {
            Box::new(LoopQueue::new(size).expect("ants: a loop queue needs a positive size"))
        }
        _ => Box::new(WorkerStack::new(size)),
    }
}
