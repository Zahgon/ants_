// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! A pool that runs whatever closure you hand it.

use std::sync::Arc;

use crate::deps::context::Context;
use crate::deps::gotime::Duration;
use crate::options::Opt;
use crate::pool_common::PoolCore;
use crate::Error;

/// A unit of work for a [`Pool`].
///
/// `None` is the original's nil `func()`: submitting it to an open pool stops
/// the worker that receives it, and submitting it to a closed pool is how the
/// tests observe [`Error::PoolClosed`].
pub type Task = Option<Box<dyn FnOnce() + Send + 'static>>;

/// Wraps `body` as a [`Task`].
#[must_use]
pub fn task<F>(body: F) -> Task
where
    F: FnOnce() + Send + 'static,
{
    Some(Box::new(body))
}

/// A thread pool that limits and recycles a mass of workers.
/// The pool capacity can be fixed or unlimited.
pub struct Pool {
    core: Arc<PoolCore<Task>>,
}

impl Pool {
    /// Instantiates a `Pool` with customized options.
    pub fn new(size: i32, options: &[Opt]) -> Result<Pool, Error> {
        let core = PoolCore::new(
            size,
            options,
            Arc::new(|task: Task| match task {
                None => false,
                Some(body) => {
                    body();
                    true
                }
            }),
        )?;
        Ok(Pool { core })
    }

    /// Submits a task to the pool.
    ///
    /// Note that you are allowed to call `Pool::submit` from within a running
    /// task, but what calls for special attention is that you will get blocked
    /// with the last `Pool::submit` call once the current pool runs out of its
    /// capacity, and to avoid this you should instantiate a `Pool` with
    /// [`crate::with_nonblocking`]`(true)`.
    pub fn submit(&self, task: Task) -> Result<(), Error> {
        self.try_submit(task).map_err(|(err, _)| err)
    }

    /// Like [`Self::submit`], but hands the task back when it could not be
    /// delivered, so a multi-pool can retry it on another pool.
    pub(crate) fn try_submit(&self, task: Task) -> Result<(), (Error, Task)> {
        if self.is_closed() {
            return Err((Error::PoolClosed, task));
        }
        match self.core.retrieve_worker() {
            Ok(worker) => {
                worker.input(task);
                Ok(())
            }
            Err(err) => Err((err, task)),
        }
    }

    /// Returns the number of workers currently running.
    #[must_use]
    pub fn running(&self) -> i32 {
        self.core.running()
    }

    /// Returns the number of available workers; `-1` indicates this pool is
    /// unlimited.
    #[must_use]
    pub fn free(&self) -> i32 {
        self.core.free()
    }

    /// Returns the number of callers waiting to be served.
    #[must_use]
    pub fn waiting(&self) -> i32 {
        self.core.waiting()
    }

    /// Returns the capacity of this pool.
    #[must_use]
    pub fn cap(&self) -> i32 {
        self.core.cap()
    }

    /// Changes the capacity of this pool; note that it has no effect on an
    /// infinite or a pre-allocated pool.
    pub fn tune(&self, size: i32) {
        self.core.tune(size);
    }

    /// Indicates whether the pool is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    /// Closes this pool and releases the worker queue.
    pub fn release(&self) {
        self.core.release();
    }

    /// Like [`Self::release`] but with a timeout; it waits for all workers to
    /// exit before timing out.
    pub fn release_timeout(&self, timeout: Duration) -> Result<(), Error> {
        self.core.release_timeout(timeout)
    }

    /// Like [`Self::release`] but with a context; it waits for all workers to
    /// exit before the context is done.
    ///
    /// Note that if the context is `None`, it is the same as
    /// [`Self::release`]: it returns immediately without waiting for all
    /// workers to exit.
    pub fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        self.core.release_context(ctx)
    }

    /// Reboots a closed pool; it does nothing if the pool is not closed.
    pub fn reboot(&self) {
        self.core.reboot();
    }
}
