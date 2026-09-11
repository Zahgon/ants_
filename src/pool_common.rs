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

//! The machinery every pool flavour shares.

use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::Duration as StdDuration;

use crate::deps::cond::Cond;
use crate::deps::context::{CancelFunc, Context};
use crate::deps::event::Event;
use crate::deps::gotime::{now_unix_nano, Duration};
use crate::options::{load_options, Opt, Options};
use crate::pkg::sync::SpinLock;
use crate::worker::GoWorker;
use crate::worker_queue::{new_worker_queue, QueueType, Worker, WorkerQueue};
use crate::{Error, CLOSED, DEFAULT_CLEAN_INTERVAL_TIME, OPENED};

/// How a pool turns one payload into work.
///
/// Returns `false` when the payload was the stop sentinel — a nil `func()` or a
/// nil `any` — which tells the worker to exit rather than run anything.
type RunPayload<T> = Arc<dyn Fn(T) -> bool + Send + Sync + 'static>;

/// How often the `ticktock` thread refreshes the pool's cached clock.
const NOW_TIME_UPDATE_INTERVAL: Duration = Duration::from_millis(500);

/// Background threads park in short slices so that a cancelled context and a
/// closed pool are both noticed promptly.
const SHUTDOWN_POLL_INTERVAL: StdDuration = StdDuration::from_millis(1);

/// Contains all common fields for other sophisticated pools.
pub struct PoolCore<T: Send + 'static> {
    /// Capacity of the pool. A negative value means that the capacity of the
    /// pool is limitless; an infinite pool avoids the potential issue of
    /// endless blocking caused by nested usage of a pool: submitting a task to
    /// a pool which submits a new task to the same pool.
    capacity: AtomicI32,

    /// The number of the currently running workers.
    running: AtomicI32,

    /// Lock protecting the worker queue, and the queue it protects.
    lock: SpinLock<Box<dyn WorkerQueue<GoWorker<T>>>>,

    /// Used to notice the pool to close itself.
    state: AtomicI32,

    /// Condition variable for waiting to get an idle worker.
    cond: Cond,

    /// Indicates that all workers are done. Replaced on every reboot, which is
    /// what the original's `sync.Once` is reset for.
    all_done: Mutex<Arc<Event>>,

    /// Speeds up the obtainment of a usable worker in `retrieve_worker`.
    worker_cache: Mutex<Vec<Arc<GoWorker<T>>>>,

    /// The number of callers already blocked on `submit`, protected by `lock`.
    waiting: AtomicI32,

    purge_done: AtomicI32,
    purge_ctx: Mutex<Option<Context>>,
    stop_purge: Mutex<Option<CancelFunc>>,

    ticktock_done: AtomicI32,
    ticktock_ctx: Mutex<Option<Context>>,
    stop_ticktock: Mutex<Option<CancelFunc>>,

    now: AtomicI64,

    options: Options,

    run_payload: RunPayload<T>,
}

impl<T: Send + 'static> PoolCore<T> {
    pub(crate) fn new(
        size: i32,
        options: &[Opt],
        run_payload: RunPayload<T>,
    ) -> Result<Arc<PoolCore<T>>, Error> {
        let size = if size <= 0 { -1 } else { size };

        let mut opts = load_options(options);

        if !opts.disable_purge {
            let expiry = opts.expiry_duration;
            if expiry < Duration::ZERO {
                return Err(Error::InvalidPoolExpiry);
            } else if expiry == Duration::ZERO {
                opts.expiry_duration = DEFAULT_CLEAN_INTERVAL_TIME;
            }
        }

        if opts.logger.is_none() {
            opts.logger = Some(crate::default_logger());
        }

        let workers: Box<dyn WorkerQueue<GoWorker<T>>> = if opts.pre_alloc {
            if size == -1 {
                return Err(Error::InvalidPreAllocSize);
            }
            new_worker_queue(QueueType::LOOP_QUEUE, size)
        } else {
            new_worker_queue(QueueType::STACK, 0)
        };

        crate::worker::install_panic_hook();

        let pool = Arc::new(PoolCore {
            capacity: AtomicI32::new(size),
            running: AtomicI32::new(0),
            lock: SpinLock::new(workers),
            state: AtomicI32::new(OPENED),
            cond: Cond::new(),
            all_done: Mutex::new(Arc::new(Event::new())),
            worker_cache: Mutex::new(Vec::new()),
            waiting: AtomicI32::new(0),
            purge_done: AtomicI32::new(0),
            purge_ctx: Mutex::new(None),
            stop_purge: Mutex::new(None),
            ticktock_done: AtomicI32::new(0),
            ticktock_ctx: Mutex::new(None),
            stop_ticktock: Mutex::new(None),
            now: AtomicI64::new(0),
            options: opts,
            run_payload,
        });

        pool.go_purge();
        pool.go_ticktock();

        Ok(pool)
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }

    pub(crate) fn cond(&self) -> &Cond {
        &self.cond
    }

    pub(crate) fn run_payload(&self) -> &RunPayload<T> {
        &self.run_payload
    }

    pub(crate) fn all_done(&self) -> Arc<Event> {
        Arc::clone(&lock(&self.all_done))
    }

    /// Returns a retired worker to the cache so a later `retrieve_worker` can
    /// reuse its channel, i.e. `workerCache.Put`.
    pub(crate) fn recycle(&self, worker: Arc<GoWorker<T>>) {
        lock(&self.worker_cache).push(worker);
    }

    fn fetch_cached_worker(&self) -> Arc<GoWorker<T>> {
        match lock(&self.worker_cache).pop() {
            Some(worker) => worker,
            None => Arc::new(GoWorker::new(crate::worker_chan_cap())),
        }
    }

    /// Clears stale workers periodically; it runs in an individual thread, as a
    /// scavenger.
    fn purge_stale_workers(self: Arc<Self>) {
        // Copy to a local variable to avoid a race with reboot().
        let Some(purge_ctx) = lock(&self.purge_ctx).clone() else {
            self.purge_done.store(1, Ordering::SeqCst);
            return;
        };
        let expiry = self.options.expiry_duration;

        loop {
            if wait_for_tick(&purge_ctx, expiry) {
                break;
            }

            if self.is_closed() {
                break;
            }

            let (stale_workers, is_dormant) = {
                let mut workers = self.lock.lock();
                let stale_workers = workers.refresh(expiry);
                let running = self.running();
                let is_dormant = running == 0 || running as usize == stale_workers.len();
                (stale_workers, is_dormant)
            };

            // Clean up the stale workers.
            for worker in &stale_workers {
                worker.finish();
            }
            drop(stale_workers);

            // There might be a situation where all workers have been cleaned up
            // (no worker is running), while some invokers are still stuck in
            // cond.wait(), then we need to awake those invokers.
            if is_dormant && self.waiting() > 0 {
                self.cond.broadcast();
            }
        }

        self.purge_done.store(1, Ordering::SeqCst);
    }

    /// Updates the current time in the pool regularly.
    fn ticktock(self: Arc<Self>) {
        // Copy to a local variable to avoid a race with reboot().
        let Some(ticktock_ctx) = lock(&self.ticktock_ctx).clone() else {
            self.ticktock_done.store(1, Ordering::SeqCst);
            return;
        };

        loop {
            if wait_for_tick(&ticktock_ctx, NOW_TIME_UPDATE_INTERVAL) {
                break;
            }

            if self.is_closed() {
                break;
            }

            self.now.store(now_unix_nano(), Ordering::Release);
        }

        self.ticktock_done.store(1, Ordering::SeqCst);
    }

    fn go_purge(self: &Arc<Self>) {
        if self.options.disable_purge {
            return;
        }

        // Start a thread to clean up expired workers periodically.
        let (ctx, cancel) = Context::with_cancel();
        *lock(&self.purge_ctx) = Some(ctx);
        *lock(&self.stop_purge) = Some(cancel);
        let pool = Arc::clone(self);
        thread::Builder::new()
            .name("ants-purge".to_owned())
            .spawn(move || pool.purge_stale_workers())
            .expect("ants: failed to spawn the purge thread");
    }

    fn go_ticktock(self: &Arc<Self>) {
        self.now.store(now_unix_nano(), Ordering::Release);
        let (ctx, cancel) = Context::with_cancel();
        *lock(&self.ticktock_ctx) = Some(ctx);
        *lock(&self.stop_ticktock) = Some(cancel);
        let pool = Arc::clone(self);
        thread::Builder::new()
            .name("ants-ticktock".to_owned())
            .spawn(move || pool.ticktock())
            .expect("ants: failed to spawn the ticktock thread");
    }

    fn now_time(&self) -> i64 {
        self.now.load(Ordering::Acquire)
    }

    /// Returns the number of workers currently running.
    pub(crate) fn running(&self) -> i32 {
        self.running.load(Ordering::SeqCst)
    }

    /// Returns the number of available workers; `-1` indicates this pool is
    /// unlimited.
    pub(crate) fn free(&self) -> i32 {
        let capacity = self.cap();
        if capacity < 0 {
            return -1;
        }
        capacity - self.running()
    }

    /// Returns the number of callers waiting to be served.
    pub(crate) fn waiting(&self) -> i32 {
        self.waiting.load(Ordering::SeqCst)
    }

    /// Returns the capacity of this pool.
    pub(crate) fn cap(&self) -> i32 {
        self.capacity.load(Ordering::SeqCst)
    }

    /// Changes the capacity of this pool; note that it has no effect on an
    /// infinite or a pre-allocated pool.
    pub(crate) fn tune(&self, size: i32) {
        let capacity = self.cap();
        if capacity == -1 || size <= 0 || size == capacity || self.options.pre_alloc {
            return;
        }
        self.capacity.store(size, Ordering::SeqCst);
        if size > capacity {
            if size - capacity == 1 {
                self.cond.signal();
                return;
            }
            self.cond.broadcast();
        }
    }

    /// Indicates whether the pool is closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == CLOSED
    }

    /// Closes this pool and releases the worker queue.
    pub(crate) fn release(&self) {
        if self
            .state
            .compare_exchange(OPENED, CLOSED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if let Some(stop_purge) = lock(&self.stop_purge).take() {
            stop_purge.cancel();
        }
        if let Some(stop_ticktock) = lock(&self.stop_ticktock).take() {
            stop_ticktock.cancel();
        }

        {
            let mut workers = self.lock.lock();
            workers.reset();
        }

        // There might be some callers waiting in retrieve_worker(), so we need
        // to wake them up to prevent those callers blocking infinitely.
        self.cond.broadcast();

        // If there are no running workers at the time of release, close
        // all_done immediately so that reboot() or release_context() won't
        // block on it indefinitely. If workers are still running, the last one
        // to exit will close all_done in its epilogue.
        if self.running() == 0 {
            self.all_done().close();
        }
    }

    /// Like [`Self::release`] but with a timeout; it waits for all workers to
    /// exit before timing out.
    pub(crate) fn release_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let (ctx, cancel) = Context::with_timeout(timeout);
        let result = self.release_context(Some(&ctx));
        cancel.cancel();
        match result {
            Err(Error::DeadlineExceeded) => Err(Error::Timeout),
            other => other,
        }
    }

    /// Like [`Self::release`] but with a context; it waits for all workers to
    /// exit before the context is done.
    ///
    /// Note that if the context is `None`, it is the same as
    /// [`Self::release`]: it returns immediately without waiting for all
    /// workers to exit.
    pub(crate) fn release_context(&self, ctx: Option<&Context>) -> Result<(), Error> {
        if self.is_closed()
            || (!self.options.disable_purge && lock(&self.stop_purge).is_none())
            || lock(&self.stop_ticktock).is_none()
        {
            return Err(Error::PoolClosed);
        }

        let purge_ctx = lock(&self.purge_ctx).clone();
        let ticktock_ctx = lock(&self.ticktock_ctx).clone();

        self.release();

        // Don't wait for all workers to exit, just return immediately if the
        // context is nil.
        let Some(ctx) = ctx else {
            return Ok(());
        };

        let all_done = self.all_done();
        loop {
            if !all_done.is_closed() {
                if let Some(err) = ctx.err() {
                    return Err(err);
                }
                if !all_done.wait_timeout(SHUTDOWN_POLL_INTERVAL) {
                    continue;
                }
            }

            if !self.options.disable_purge {
                if let Some(purge_ctx) = purge_ctx.as_ref() {
                    purge_ctx.wait();
                }
            }
            if let Some(ticktock_ctx) = ticktock_ctx.as_ref() {
                ticktock_ctx.wait();
            }

            if self.running() == 0
                && (self.options.disable_purge || self.purge_done.load(Ordering::SeqCst) == 1)
                && self.ticktock_done.load(Ordering::SeqCst) == 1
            {
                return Ok(());
            }
            if let Some(err) = ctx.err() {
                return Err(err);
            }
            thread::yield_now();
        }
    }

    /// Reboots a closed pool; it does nothing if the pool is not closed.
    ///
    /// If you intend to reboot a closed pool, use [`Self::release_timeout`]
    /// instead of [`Self::release`] to ensure that all workers are stopped and
    /// resources are released before rebooting, otherwise you may run into a
    /// data race.
    pub(crate) fn reboot(self: &Arc<Self>) {
        if self.state.load(Ordering::SeqCst) != CLOSED {
            return;
        }

        // Wait for all workers to exit. The all_done latch is closed either by
        // release() (if no workers were running) or by the last exiting worker.
        self.all_done().wait();

        // Wait for the purge and ticktock threads to exit completely, so that
        // their purge_done/ticktock_done stores don't race with the resets
        // below.
        if !self.options.disable_purge {
            while self.purge_done.load(Ordering::SeqCst) != 1 {
                thread::yield_now();
            }
        }
        while self.ticktock_done.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }

        if self
            .state
            .compare_exchange(CLOSED, OPENED, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        self.purge_done.store(0, Ordering::SeqCst);
        self.go_purge();
        self.ticktock_done.store(0, Ordering::SeqCst);
        self.go_ticktock();
        *lock(&self.all_done) = Arc::new(Event::new());
    }

    pub(crate) fn add_running(&self, delta: i32) -> i32 {
        self.running.fetch_add(delta, Ordering::SeqCst) + delta
    }

    fn add_waiting(&self, delta: i32) {
        self.waiting.fetch_add(delta, Ordering::SeqCst);
    }

    /// Returns an available worker to run the tasks.
    pub(crate) fn retrieve_worker(self: &Arc<Self>) -> Result<Arc<GoWorker<T>>, Error> {
        let mut workers = self.lock.lock();

        loop {
            // First try to fetch the worker from the queue.
            if let Some(worker) = workers.detach() {
                drop(workers);
                return Ok(worker);
            }

            // If the worker queue is empty, and we don't run out of the pool
            // capacity, then just spawn a new worker thread.
            let capacity = self.cap();
            if capacity == -1 || capacity > self.running() {
                let worker = self.fetch_cached_worker();
                crate::worker::run(self, &worker);
                drop(workers);
                return Ok(worker);
            }

            // Bail out early if it's in nonblocking mode or the number of
            // pending callers reaches the maximum limit value.
            if self.options.nonblocking
                || (self.options.max_blocking_tasks != 0
                    && self.waiting() >= self.options.max_blocking_tasks)
            {
                drop(workers);
                return Err(Error::PoolOverload);
            }

            // Otherwise, we'll have to keep them blocked and wait for at least
            // one worker to be put back into the pool.
            self.add_waiting(1);
            workers = self.cond.wait(workers);
            self.add_waiting(-1);

            if self.is_closed() {
                drop(workers);
                return Err(Error::PoolClosed);
            }
        }
    }

    /// Puts a worker back into the free pool, recycling the threads.
    pub(crate) fn revert_worker(&self, worker: &Arc<GoWorker<T>>) -> bool {
        let capacity = self.cap();
        if (capacity > 0 && self.running() > capacity) || self.is_closed() {
            self.cond.broadcast();
            return false;
        }

        worker.set_last_used_time(self.now_time());

        let mut workers = self.lock.lock();
        // To avoid leaking workers, add a double check in the lock scope.
        // Issue: https://github.com/panjf2000/ants/issues/113
        if self.is_closed() {
            return false;
        }
        if workers.insert(Arc::clone(worker)).is_err() {
            return false;
        }
        // Notify the invoker stuck in 'retrieve_worker()' that there is an
        // available worker in the worker queue.
        self.cond.signal();
        drop(workers);

        true
    }
}

/// Waits out one tick, reporting `true` when the context was cancelled instead.
///
/// This is the original's `select` over `ctx.Done()` and `ticker.C`: the thread
/// parks for the whole interval and is woken the moment the context is
/// cancelled.
fn wait_for_tick(ctx: &Context, interval: Duration) -> bool {
    ctx.wait_done_for(interval)
}

/// A poisoned bookkeeping lock still holds valid state; a panicking worker must
/// not wedge the pool it panicked in.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
