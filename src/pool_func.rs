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

//! A pool that runs one unified function over dynamically-typed arguments.

use std::any::Any;
use std::sync::Arc;

use crate::deps::context::Context;
use crate::deps::gotime::Duration;
use crate::options::Opt;
use crate::pool_common::PoolCore;
use crate::pool_func_generic::PoolFunc;
use crate::Error;

/// A dynamically-typed argument, i.e. Go's `any`.
///
/// `None` is that type's nil: delivering it to an open pool stops the worker
/// that receives it, exactly as a nil `any` does in the original.
pub type AnyArg = Option<Box<dyn Any + Send + 'static>>;

/// The unified function a [`PoolWithFunc`] applies to every argument.
pub type AnyFunc = PoolFunc<AnyArg>;

/// Wraps `value` as an [`AnyArg`].
#[must_use]
pub fn arg<A>(value: A) -> AnyArg
where
    A: Any + Send + 'static,
{
    Some(Box::new(value))
}

/// Like [`crate::Pool`] but accepts a unified function for all workers to
/// execute.
pub struct PoolWithFunc {
    core: Arc<PoolCore<AnyArg>>,
}

impl PoolWithFunc {
    /// Instantiates a `PoolWithFunc` with customized options.
    pub fn new(size: i32, pf: AnyFunc, options: &[Opt]) -> Result<PoolWithFunc, Error> {
        let Some(pf) = pf else {
            return Err(Error::LackPoolFunc);
        };
        let core = PoolCore::new(
            size,
            options,
            Arc::new(move |argument: AnyArg| {
                if argument.is_none() {
                    return false;
                }
                pf(argument);
                true
            }),
        )?;
        Ok(PoolWithFunc { core })
    }

    /// Passes an argument to the pool.
    ///
    /// Note that you are allowed to call `PoolWithFunc::invoke` from within a
    /// running task, but what calls for special attention is that you will get
    /// blocked with the last `PoolWithFunc::invoke` call once the current pool
    /// runs out of its capacity, and to avoid this you should instantiate a
    /// `PoolWithFunc` with [`crate::with_nonblocking`]`(true)`.
    pub fn invoke(&self, argument: AnyArg) -> Result<(), Error> {
        self.try_invoke(argument).map_err(|(err, _)| err)
    }

    /// Like [`Self::invoke`], but hands the argument back when it could not be
    /// delivered, so a multi-pool can retry it on another pool.
    pub(crate) fn try_invoke(&self, argument: AnyArg) -> Result<(), (Error, AnyArg)> {
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
