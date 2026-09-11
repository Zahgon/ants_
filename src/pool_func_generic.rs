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

//! A pool that runs one unified function over a statically-typed argument.

use std::sync::Arc;

use crate::deps::context::Context;
use crate::deps::gotime::Duration;
use crate::options::Opt;
use crate::pool_common::PoolCore;
use crate::Error;

/// The unified function a [`PoolWithFuncGeneric`] applies to every argument.
///
/// `None` is the original's nil `func(T)`, which every constructor rejects with
/// [`Error::LackPoolFunc`].
pub type PoolFunc<T> = Option<Arc<dyn Fn(T) + Send + Sync + 'static>>;

/// Wraps `body` as a [`PoolFunc`].
#[must_use]
pub fn pool_func<T, F>(body: F) -> PoolFunc<T>
where
    F: Fn(T) + Send + Sync + 'static,
{
    Some(Arc::new(body))
}

/// The generic version of [`crate::PoolWithFunc`].
pub struct PoolWithFuncGeneric<T: Send + 'static> {
    core: Arc<PoolCore<T>>,
}

impl<T: Send + 'static> PoolWithFuncGeneric<T> {
    /// Instantiates a `PoolWithFuncGeneric<T>` with customized options.
    pub fn new(
        size: i32,
        pf: PoolFunc<T>,
        options: &[Opt],
    ) -> Result<PoolWithFuncGeneric<T>, Error> {
        let Some(pf) = pf else {
            return Err(Error::LackPoolFunc);
        };
        let core = PoolCore::new(
            size,
            options,
            Arc::new(move |argument: T| {
                pf(argument);
                true
            }),
        )?;
        Ok(PoolWithFuncGeneric { core })
    }

    /// Passes the argument to the pool to start a new task.
    pub fn invoke(&self, argument: T) -> Result<(), Error> {
        self.try_invoke(argument).map_err(|(err, _)| err)
    }

    /// Like [`Self::invoke`], but hands the argument back when it could not be
    /// delivered, so a multi-pool can retry it on another pool.
    pub(crate) fn try_invoke(&self, argument: T) -> Result<(), (Error, T)> {
        if self.is_closed() {
            return Err((Error::PoolClosed, argument));
        }
        match self.core.retrieve_worker() {
            Ok(worker) => {
                worker.input(argument);
                Ok(())
            }
            Err(err) => Err((err, argument)),
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
    pub fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        self.core.release_context(ctx)
    }

    /// Reboots a closed pool; it does nothing if the pool is not closed.
    pub fn reboot(&self) {
        self.core.reboot();
    }
}
