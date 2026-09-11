// MIT License

// Copyright (c) 2023 Andy Pan

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

//! Several `PoolWithFunc`s behind one façade.

use crate::deps::context::Context;
use crate::deps::gotime::Duration;
use crate::multipool::{LoadBalancingStrategy, MultiPoolCore, Poolish};
use crate::options::Opt;
use crate::pool_func::{AnyArg, AnyFunc, PoolWithFunc};
use crate::Error;

impl Poolish for PoolWithFunc {
    type Arg = AnyArg;

    fn deliver(&self, argument: AnyArg) -> Result<(), (Error, AnyArg)> {
        self.try_invoke(argument)
    }
    fn running(&self) -> i32 {
        PoolWithFunc::running(self)
    }
    fn free(&self) -> i32 {
        PoolWithFunc::free(self)
    }
    fn waiting(&self) -> i32 {
        PoolWithFunc::waiting(self)
    }
    fn cap(&self) -> i32 {
        PoolWithFunc::cap(self)
    }
    fn tune(&self, size: i32) {
        PoolWithFunc::tune(self, size);
    }
    fn release(&self) {
        PoolWithFunc::release(self);
    }
    fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        PoolWithFunc::release_context(self, ctx)
    }
    fn reboot(&self) {
        PoolWithFunc::reboot(self);
    }
}

/// Consists of multiple `PoolWithFunc`s, from which you will benefit the performance
/// improvement on basis of the fine-grained locking that reduces the lock
/// contention.
///
/// `MultiPoolWithFunc` is a good fit for the scenario where you have a large number of
/// tasks to submit, and you don't want the single pool to be the bottleneck.
pub struct MultiPoolWithFunc {
    core: MultiPoolCore<PoolWithFunc>,
}

impl MultiPoolWithFunc {
    /// Instantiates a `MultiPoolWithFunc` with a size of the pool list and a size per
    /// pool, and the load-balancing strategy.
    pub fn new(
        size: i32,
        size_per_pool: i32,
        pf: AnyFunc,
        lbs: LoadBalancingStrategy,
        options: &[Opt],
    ) -> Result<MultiPoolWithFunc, Error> {
        let core = MultiPoolCore::new(size, lbs, || {
            PoolWithFunc::new(size_per_pool, pf.clone(), options)
        })?;
        Ok(MultiPoolWithFunc { core })
    }

    /// Submits an argument to a pool selected by the load-balancing strategy.
    pub fn invoke(&self, argument: AnyArg) -> Result<(), Error> {
        self.core.dispatch(argument)
    }

    /// Returns the number of the currently running workers across all pools.
    #[must_use]
    pub fn running(&self) -> i32 {
        self.core.running()
    }

    /// Returns the number of the currently running workers in the specific
    /// pool.
    pub fn running_by_index(&self, index: i32) -> Result<i32, Error> {
        self.core.running_by_index(index)
    }

    /// Returns the number of available workers across all pools.
    #[must_use]
    pub fn free(&self) -> i32 {
        self.core.free()
    }

    /// Returns the number of available workers in the specific pool.
    pub fn free_by_index(&self, index: i32) -> Result<i32, Error> {
        self.core.free_by_index(index)
    }

    /// Returns the number of the currently waiting callers across all pools.
    #[must_use]
    pub fn waiting(&self) -> i32 {
        self.core.waiting()
    }

    /// Returns the number of the currently waiting callers in the specific
    /// pool.
    pub fn waiting_by_index(&self, index: i32) -> Result<i32, Error> {
        self.core.waiting_by_index(index)
    }

    /// Returns the capacity of this multi-pool.
    #[must_use]
    pub fn cap(&self) -> i32 {
        self.core.cap()
    }

    /// Resizes each pool in the multi-pool.
    ///
    /// Note that this method doesn't resize the overall capacity of the
    /// multi-pool.
    pub fn tune(&self, size: i32) {
        self.core.tune(size);
    }

    /// Indicates whether the multi-pool is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    /// Closes the multi-pool with a timeout; it waits for all pools to be
    /// closed before timing out.
    pub fn release_timeout(&self, timeout: Duration) -> Result<(), Error> {
        self.core.release_timeout(timeout)
    }

    /// Closes the multi-pool with a context; it waits for all pools to be
    /// closed before the context is done.
    pub fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        self.core.release_context(ctx)
    }

    /// Reboots a released multi-pool.
    pub fn reboot(&self) {
        self.core.reboot();
    }
}
