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

//! Several pools behind one façade, plus the machinery all three share.

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::thread;

use crate::deps::context::Context;
use crate::deps::gotime::Duration;
use crate::options::Opt;
use crate::pool::{Pool, Task};
use crate::{Error, CLOSED, OPENED};

/// Represents the type of load-balancing algorithm.
///
/// A transparent integer rather than an enum, because the original is a plain
/// `int` and its constructors are expected to reject values that name no
/// strategy at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LoadBalancingStrategy(pub i32);

/// Distributes tasks to a list of pools in rotation.
pub const ROUND_ROBIN: LoadBalancingStrategy = LoadBalancingStrategy(1 << 1);

/// Always selects the pool with the least number of pending tasks.
pub const LEAST_TASKS: LoadBalancingStrategy = LoadBalancingStrategy(1 << 2);

/// What a multi-pool needs from the pools it fronts.
pub(crate) trait Poolish: Send + Sync {
    /// What one unit of work looks like for this pool flavour.
    type Arg: Send;

    /// Hands `argument` to this pool, giving it back when delivery failed so
    /// that the multi-pool can retry it elsewhere.
    fn deliver(&self, argument: Self::Arg) -> Result<(), (Error, Self::Arg)>;
    fn running(&self) -> i32;
    fn free(&self) -> i32;
    fn waiting(&self) -> i32;
    fn cap(&self) -> i32;
    fn tune(&self, size: i32);
    fn release(&self);
    fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error>;
    fn reboot(&self);
}

impl Poolish for Pool {
    type Arg = Task;

    fn deliver(&self, argument: Task) -> Result<(), (Error, Task)> {
        self.try_submit(argument)
    }
    fn running(&self) -> i32 {
        Pool::running(self)
    }
    fn free(&self) -> i32 {
        Pool::free(self)
    }
    fn waiting(&self) -> i32 {
        Pool::waiting(self)
    }
    fn cap(&self) -> i32 {
        Pool::cap(self)
    }
    fn tune(&self, size: i32) {
        Pool::tune(self, size);
    }
    fn release(&self) {
        Pool::release(self);
    }
    fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        Pool::release_context(self, ctx)
    }
    fn reboot(&self) {
        Pool::reboot(self);
    }
}

/// Releases every pool concurrently and aggregates whatever went wrong.
///
/// The original spreads the work over an `errgroup.Group` and then drains a
/// buffered channel once per pool, so its aggregate is assembled in *completion*
/// order and the segments come out in a different order from run to run.
/// Joining a scoped thread per pool in index order gives the same concurrency,
/// the same segments and the same separator, but a reproducible ordering.
pub(crate) fn release_pools<P: Poolish>(ctx: Option<&Context>, pools: &[P]) -> Result<(), Error> {
    let outcomes: Vec<Result<(), Error>> = thread::scope(|scope| {
        let handles: Vec<_> = pools
            .iter()
            .enumerate()
            .map(|(index, pool)| {
                scope.spawn(move || {
                    pool.release_context(ctx)
                        .map_err(|err| Error::Pools(format!("pool {index}: {err}")))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap_or(Ok(())))
            .collect()
    });

    let mut detail = String::new();
    for outcome in outcomes {
        if let Err(err) = outcome {
            detail.push_str(&err.to_string());
            detail.push_str(" | ");
        }
    }
    if detail.is_empty() {
        return Ok(());
    }
    Err(Error::Pools(detail.trim_end_matches(" | ").to_owned()))
}

/// The list of pools, the rotation cursor and the shared state that every
/// multi-pool flavour keeps.
pub(crate) struct MultiPoolCore<P: Poolish> {
    pools: Vec<P>,
    index: AtomicU32,
    state: AtomicI32,
    lbs: LoadBalancingStrategy,
}

impl<P: Poolish> MultiPoolCore<P> {
    pub(crate) fn new<F>(
        size: i32,
        lbs: LoadBalancingStrategy,
        make_pool: F,
    ) -> Result<MultiPoolCore<P>, Error>
    where
        F: Fn() -> Result<P, Error>,
    {
        if size <= 0 {
            return Err(Error::InvalidMultiPoolSize);
        }
        if lbs != ROUND_ROBIN && lbs != LEAST_TASKS {
            return Err(Error::InvalidLoadBalancingStrategy);
        }
        let mut pools = Vec::with_capacity(size as usize);
        for _ in 0..size {
            match make_pool() {
                Ok(pool) => pools.push(pool),
                Err(err) => {
                    // Release all previously created pools to avoid a resource
                    // leak.
                    for pool in &pools {
                        pool.release();
                    }
                    return Err(err);
                }
            }
        }
        Ok(MultiPoolCore {
            pools,
            index: AtomicU32::new(u32::MAX),
            state: AtomicI32::new(OPENED),
            lbs,
        })
    }

    fn next(&self, lbs: LoadBalancingStrategy) -> i32 {
        if lbs == ROUND_ROBIN {
            let ticket = self.index.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
            return (ticket % self.pools.len() as u32) as i32;
        }
        if lbs == LEAST_TASKS {
            let mut least_tasks = i32::MAX;
            let mut index = 0;
            for (candidate, pool) in self.pools.iter().enumerate() {
                let running = pool.running();
                if running < least_tasks {
                    least_tasks = running;
                    index = candidate as i32;
                }
            }
            return index;
        }
        -1
    }

    /// Submits `argument` to a pool selected by the load-balancing strategy.
    pub(crate) fn dispatch(&self, argument: P::Arg) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }
        let argument = match self.pools[self.next(self.lbs) as usize].deliver(argument) {
            Ok(()) => return Ok(()),
            Err((err, argument)) => {
                if err != Error::PoolOverload || self.lbs != ROUND_ROBIN {
                    return Err(err);
                }
                argument
            }
        };
        self.pools[self.next(LEAST_TASKS) as usize]
            .deliver(argument)
            .map_err(|(err, _)| err)
    }

    /// Returns the number of the currently running workers across all pools.
    pub(crate) fn running(&self) -> i32 {
        self.pools.iter().map(Poolish::running).sum()
    }

    /// Returns the number of the currently running workers in the specific
    /// pool.
    pub(crate) fn running_by_index(&self, index: i32) -> Result<i32, Error> {
        Ok(self.pool_at(index)?.running())
    }

    /// Returns the number of available workers across all pools.
    pub(crate) fn free(&self) -> i32 {
        self.pools.iter().map(Poolish::free).sum()
    }

    /// Returns the number of available workers in the specific pool.
    pub(crate) fn free_by_index(&self, index: i32) -> Result<i32, Error> {
        Ok(self.pool_at(index)?.free())
    }

    /// Returns the number of the currently waiting callers across all pools.
    pub(crate) fn waiting(&self) -> i32 {
        self.pools.iter().map(Poolish::waiting).sum()
    }

    /// Returns the number of the currently waiting callers in the specific
    /// pool.
    pub(crate) fn waiting_by_index(&self, index: i32) -> Result<i32, Error> {
        Ok(self.pool_at(index)?.waiting())
    }

    /// Returns the capacity of this multi-pool.
    pub(crate) fn cap(&self) -> i32 {
        self.pools.iter().map(Poolish::cap).sum()
    }

    /// Resizes each pool in the multi-pool.
    ///
    /// Note that this method doesn't resize the overall capacity of the
    /// multi-pool.
    pub(crate) fn tune(&self, size: i32) {
        for pool in &self.pools {
            pool.tune(size);
        }
    }

    /// Indicates whether the multi-pool is closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == CLOSED
    }

    /// Closes the multi-pool with a timeout; it waits for all pools to be
    /// closed before timing out.
    pub(crate) fn release_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let (ctx, cancel) = Context::with_timeout(timeout);
        let result = self.release_context(Some(&ctx));
        cancel.cancel();
        result
    }

    /// Closes the multi-pool with a context; it waits for all pools to be
    /// closed before the context is done.
    pub(crate) fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        if self
            .state
            .compare_exchange(OPENED, CLOSED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::PoolClosed);
        }
        release_pools(ctx, &self.pools)
    }

    /// Reboots a released multi-pool.
    pub(crate) fn reboot(&self) {
        if self
            .state
            .compare_exchange(CLOSED, OPENED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.index.store(0, Ordering::SeqCst);
            for pool in &self.pools {
                pool.reboot();
            }
        }
    }

    fn pool_at(&self, index: i32) -> Result<&P, Error> {
        if index < 0 || index as usize >= self.pools.len() {
            return Err(Error::InvalidPoolIndex);
        }
        Ok(&self.pools[index as usize])
    }
}

/// Consists of multiple pools, from which you will benefit the performance
/// improvement on basis of the fine-grained locking that reduces the lock
/// contention.
///
/// `MultiPool` is a good fit for the scenario where you have a large number of
/// tasks to submit, and you don't want the single pool to be the bottleneck.
pub struct MultiPool {
    core: MultiPoolCore<Pool>,
}

impl MultiPool {
    /// Instantiates a `MultiPool` with a size of the pool list and a size per
    /// pool, and the load-balancing strategy.
    pub fn new(
        size: i32,
        size_per_pool: i32,
        lbs: LoadBalancingStrategy,
        options: &[Opt],
    ) -> Result<MultiPool, Error> {
        let core = MultiPoolCore::new(size, lbs, || Pool::new(size_per_pool, options))?;
        Ok(MultiPool { core })
    }

    /// Submits a task to a pool selected by the load-balancing strategy.
    pub fn submit(&self, task: Task) -> Result<(), Error> {
        self.core.dispatch(task)
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
