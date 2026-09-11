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

//! Everything that can be tuned when instantiating a pool.

use std::any::Any;
use std::sync::Arc;

use crate::deps::gotime::Duration;
use crate::Logger;

/// A handler invoked with the value a panicking worker was carrying.
pub type PanicHandler = Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync + 'static>;

/// A single change to [`Options`].
///
/// Named `Opt` rather than `Option` so it cannot shadow [`std::option::Option`].
/// Because Rust has no variadic arguments, the constructors take a slice of
/// these where the original takes `options ...Option`.
pub struct Opt(Box<dyn Fn(&mut Options) + Send + Sync + 'static>);

impl Opt {
    fn new(apply: impl Fn(&mut Options) + Send + Sync + 'static) -> Opt {
        Opt(Box::new(apply))
    }
}

pub(crate) fn load_options(options: &[Opt]) -> Options {
    let mut opts = Options::default();
    for option in options {
        (option.0)(&mut opts);
    }
    opts
}

/// Contains all options which will be applied when instantiating an ants pool.
#[derive(Clone, Default)]
pub struct Options {
    /// A period for the scavenger thread to clean up those expired workers,
    /// the scavenger scans all workers every `expiry_duration` and cleans up
    /// those workers that haven't been used for more than `expiry_duration`.
    pub expiry_duration: Duration,

    /// Indicates whether to make memory pre-allocation when initializing Pool.
    pub pre_alloc: bool,

    /// Max number of callers blocking on `submit`.
    /// 0 (default value) means no such limit.
    pub max_blocking_tasks: i32,

    /// When `nonblocking` is true, `Pool::submit` will never be blocked.
    /// [`crate::Error::PoolOverload`] will be returned when `Pool::submit`
    /// cannot be done at once.
    /// When `nonblocking` is true, `max_blocking_tasks` is inoperative.
    pub nonblocking: bool,

    /// Used to handle panics from each worker thread.
    /// If `None`, the default behavior is to capture the value given to the
    /// panic and resume normal execution and print that value along with the
    /// stack trace of the thread.
    pub panic_handler: Option<PanicHandler>,

    /// The customized logger for logging info, if it is not set,
    /// the default standard logger is used.
    pub logger: Option<Arc<dyn Logger>>,

    /// When `disable_purge` is true, workers are not purged and are resident.
    pub disable_purge: bool,
}

impl Options {
    /// The logger this configuration writes through: the customized one when
    /// there is one, and the package default otherwise.
    #[must_use]
    pub fn logger(&self) -> Arc<dyn Logger> {
        match self.logger.as_ref() {
            Some(logger) => Arc::clone(logger),
            None => crate::default_logger(),
        }
    }
}

/// Accepts the whole [`Options`] config.
#[must_use]
pub fn with_options(options: Options) -> Opt {
    Opt::new(move |opts| *opts = options.clone())
}

/// Sets up the interval time of cleaning up workers.
#[must_use]
pub fn with_expiry_duration(expiry_duration: Duration) -> Opt {
    Opt::new(move |opts| opts.expiry_duration = expiry_duration)
}

/// Indicates whether it should allocate for workers up front.
#[must_use]
pub fn with_pre_alloc(pre_alloc: bool) -> Opt {
    Opt::new(move |opts| opts.pre_alloc = pre_alloc)
}

/// Sets up the maximum number of callers that are blocked when the pool reaches
/// its capacity.
#[must_use]
pub fn with_max_blocking_tasks(max_blocking_tasks: i32) -> Opt {
    Opt::new(move |opts| opts.max_blocking_tasks = max_blocking_tasks)
}

/// Indicates that the pool will return [`crate::Error::PoolOverload`] when
/// there are no available workers.
#[must_use]
pub fn with_nonblocking(nonblocking: bool) -> Opt {
    Opt::new(move |opts| opts.nonblocking = nonblocking)
}

/// Sets up the panic handler.
#[must_use]
pub fn with_panic_handler(panic_handler: PanicHandler) -> Opt {
    Opt::new(move |opts| opts.panic_handler = Some(Arc::clone(&panic_handler)))
}

/// Sets up a customized logger.
#[must_use]
pub fn with_logger(logger: Arc<dyn Logger>) -> Opt {
    Opt::new(move |opts| opts.logger = Some(Arc::clone(&logger)))
}

/// Indicates whether we turn off automatic purging.
#[must_use]
pub fn with_disable_purge(disable: bool) -> Opt {
    Opt::new(move |opts| opts.disable_purge = disable)
}

/// Wraps `handler` as a [`PanicHandler`], the way `WithPanicHandler` accepts a
/// bare `func(any)`.
#[must_use]
pub fn panic_handler<F>(handler: F) -> PanicHandler
where
    F: Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
{
    Arc::new(handler)
}
