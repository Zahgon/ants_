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

//! Crate `ants` implements an efficient and reliable thread pool for Rust.
//!
//! With ants, Rust applications are able to limit the number of active worker
//! threads, recycle workers efficiently, and reduce the memory footprint
//! significantly. Crate `ants` is extremely useful in the scenarios where a
//! massive number of workers are created and destroyed frequently, such as
//! highly-concurrent batch processing systems, HTTP servers, services of
//! asynchronous tasks, etc.
//!
//! ```
//! use std::sync::atomic::{AtomicI32, Ordering};
//! use std::sync::Arc;
//!
//! let counter = Arc::new(AtomicI32::new(0));
//! let pool = ants::Pool::new(10, &[]).unwrap();
//! for _ in 0..100 {
//!     let counter = Arc::clone(&counter);
//!     pool.submit(ants::task(move || {
//!         counter.fetch_add(1, Ordering::SeqCst);
//!     }))
//!     .unwrap();
//! }
//! pool.release_timeout(ants::Duration::SECOND).unwrap();
//! assert_eq!(100, counter.load(Ordering::SeqCst));
//! ```

#![warn(missing_docs)]

pub mod deps;
mod multipool;
mod multipool_func;
mod multipool_func_generic;
mod options;
pub mod pkg;
mod pool;
mod pool_common;
mod pool_func;
mod pool_func_generic;
mod worker;
mod worker_loop_queue;
mod worker_queue;
mod worker_stack;

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

pub use crate::deps::golog::{StdLogger, Target};
pub use crate::deps::gotime::{sleep, Duration};
pub use crate::multipool::{LoadBalancingStrategy, MultiPool, LEAST_TASKS, ROUND_ROBIN};
pub use crate::multipool_func::MultiPoolWithFunc;
pub use crate::multipool_func_generic::MultiPoolWithFuncGeneric;
pub use crate::options::{
    panic_handler, with_disable_purge, with_expiry_duration, with_logger, with_max_blocking_tasks,
    with_nonblocking, with_options, with_panic_handler, with_pre_alloc, Opt, Options, PanicHandler,
};
pub use crate::pool::{task, Pool, Task};
pub use crate::pool_func::{arg, AnyArg, AnyFunc, PoolWithFunc};
pub use crate::pool_func_generic::{pool_func, PoolFunc, PoolWithFuncGeneric};

/// The default capacity for a default pool.
pub const DEFAULT_ANTS_POOL_SIZE: i32 = i32::MAX;

/// The interval time to clean up workers.
pub const DEFAULT_CLEAN_INTERVAL_TIME: Duration = Duration::SECOND;

/// Represents that the pool is opened.
pub const OPENED: i32 = 0;

/// Represents that the pool is closed.
pub const CLOSED: i32 = 1;

/// Everything that can go wrong in `ants`.
///
/// The original exposes these as package-level `error` values compared by
/// identity; a Rust enum gives the same identity comparison with the same
/// message text.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Returned when invokers don't provide a function for the pool.
    LackPoolFunc,
    /// Returned when setting a negative number as the periodic duration to
    /// purge workers.
    InvalidPoolExpiry,
    /// Returned when submitting a task to a closed pool.
    PoolClosed,
    /// Returned when the pool is full and no workers are available.
    PoolOverload,
    /// Returned when trying to set up a negative capacity under `PreAlloc`
    /// mode.
    InvalidPreAllocSize,
    /// Returned after the operations timed out.
    Timeout,
    /// Returned when trying to retrieve a pool with an invalid index.
    InvalidPoolIndex,
    /// Returned when trying to create a multi-pool with an invalid
    /// load-balancing strategy.
    InvalidLoadBalancingStrategy,
    /// Returned when trying to create a multi-pool with an invalid size.
    InvalidMultiPoolSize,
    /// Returned when a pre-allocated worker queue cannot take another worker.
    #[doc(hidden)]
    QueueIsFull,
    /// The context governing the operation was cancelled.
    Canceled,
    /// The context governing the operation reached its deadline.
    DeadlineExceeded,
    /// One or more pools of a multi-pool failed to release, each rendered as
    /// `pool <index>: <error>` and joined by `" | "`.
    Pools(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Error::LackPoolFunc => "must provide function for pool",
            Error::InvalidPoolExpiry => "invalid expiry for pool",
            Error::PoolClosed => "this pool has been closed",
            Error::PoolOverload => "too many goroutines blocked on submit or Nonblocking is set",
            Error::InvalidPreAllocSize => "can not set up a negative capacity under PreAlloc mode",
            Error::Timeout => "operation timed out",
            Error::InvalidPoolIndex => "invalid pool index",
            Error::InvalidLoadBalancingStrategy => "invalid load-balancing strategy",
            Error::InvalidMultiPoolSize => "invalid size for multiple pool",
            Error::QueueIsFull => "the queue is full",
            Error::Canceled => "context canceled",
            Error::DeadlineExceeded => "context deadline exceeded",
            Error::Pools(detail) => return f.write_str(detail),
        };
        f.write_str(message)
    }
}

impl std::error::Error for Error {}

/// Used for logging formatted messages.
///
/// The original's `Printf(format string, args ...any)` splits into two steps in
/// Rust, which has no variadics: the caller formats, and the logger renders the
/// header and writes the line.
pub trait Logger: Send + Sync {
    /// Writes one already-formatted message.
    fn printf(&self, message: &str);
}

/// The logger a pool uses when no other one was configured.
///
/// Writes to stderr with the prefix `[ants]: `, a date, and a microsecond-
/// resolution local time.
#[must_use]
pub fn default_logger() -> Arc<dyn Logger> {
    static DEFAULT_LOGGER: OnceLock<Arc<StdLogger>> = OnceLock::new();
    let logger = DEFAULT_LOGGER.get_or_init(|| {
        Arc::new(StdLogger::new(
            Target::Stderr,
            "[ants]: ",
            deps::golog::LSTD_FLAGS | deps::golog::LMSGPREFIX | deps::golog::LMICROSECONDS,
        ))
    });
    Arc::clone(logger) as Arc<dyn Logger>
}

/// Determines whether the channel of a worker should be a buffered channel to
/// get the best performance. Inspired by fasthttp at
/// <https://github.com/valyala/fasthttp/blob/master/workerpool.go#L139>
pub(crate) fn worker_chan_cap() -> usize {
    static WORKER_CHAN_CAP: OnceLock<usize> = OnceLock::new();
    *WORKER_CHAN_CAP.get_or_init(|| {
        let parallelism = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        // Use a blocking channel when there is a single processor. This
        // switches context from sender to receiver immediately, which results
        // in higher performance.
        if parallelism == 1 {
            return 0;
        }
        // Use a non-blocking worker channel when there is more than one
        // processor, since otherwise the sender might be dragged down if the
        // receiver is CPU-bound.
        1
    })
}

/// Init an instance pool when first touching ants.
fn default_ants_pool() -> &'static Pool {
    static DEFAULT_ANTS_POOL: OnceLock<Pool> = OnceLock::new();
    DEFAULT_ANTS_POOL.get_or_init(|| {
        Pool::new(DEFAULT_ANTS_POOL_SIZE, &[]).expect("ants: the default pool is always valid")
    })
}

/// Submits a task to the default pool.
pub fn submit(task: Task) -> Result<(), Error> {
    default_ants_pool().submit(task)
}

/// Returns the number of the currently running workers of the default pool.
#[must_use]
pub fn running() -> i32 {
    default_ants_pool().running()
}

/// Returns the capacity of the default pool.
#[must_use]
pub fn cap() -> i32 {
    default_ants_pool().cap()
}

/// Returns the available workers of the default pool.
#[must_use]
pub fn free() -> i32 {
    default_ants_pool().free()
}

/// Closes the default pool.
pub fn release() {
    default_ants_pool().release();
}

/// Like [`release`] but with a timeout; it waits for all workers to exit before
/// timing out.
pub fn release_timeout(timeout: Duration) -> Result<(), Error> {
    default_ants_pool().release_timeout(timeout)
}

/// Like [`release`] but with a context; it waits for all workers to exit before
/// the context is done.
///
/// Note that if the context is `None`, it is the same as [`release`]: it
/// returns immediately without waiting for all workers to exit.
pub fn release_context(ctx: Option<&deps::context::Context>) -> Result<(), Error> {
    default_ants_pool().release_context(ctx)
}

/// Reboots the default pool.
pub fn reboot() {
    default_ants_pool().reboot();
}

/// Internals the original's in-package tests reach directly.
///
/// Go compiles a package's `_test.go` files into the package itself, so its
/// worker-queue tests can name unexported items. Rust integration tests are
/// external crates and cannot, so the white-box surface is published here,
/// hidden from the documentation, rather than the tests being weakened to
/// black-box ones.
#[doc(hidden)]
pub mod internal {
    pub use crate::worker::GoWorker;
    pub use crate::worker_loop_queue::LoopQueue;
    pub use crate::worker_queue::{new_worker_queue, QueueType, Worker, WorkerQueue};
    pub use crate::worker_stack::WorkerStack;
}
