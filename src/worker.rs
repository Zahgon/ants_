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

//! The actual executor: a thread that receives payloads and runs them.

use std::any::Any;
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, Once, PoisonError};
use std::thread;

use crate::pool_common::PoolCore;
use crate::worker_queue::Worker;

/// Worker threads get a smaller stack than the 2 MiB Rust hands out by default.
///
/// A goroutine starts at a few kilobytes and grows; an OS thread cannot. The
/// pool sizes this library is built for — thousands of concurrent workers —
/// only fit if each stack reservation is modest.
const WORKER_STACK_SIZE: usize = 512 * 1024;

/// What a pool sends down a worker's channel.
enum Message<T> {
    /// A payload to process.
    Payload(T),
    /// Stop, i.e. Go's `finish()`.
    Finish,
}

/// The actual executor who runs the tasks.
///
/// One `GoWorker` owns one channel and, while running, one thread. The struct
/// deliberately holds no reference back to its pool: the thread captures the
/// pool instead, so a parked worker sitting in the pool's queue does not form a
/// reference cycle with it.
#[derive(Debug)]
pub struct GoWorker<T: Send + 'static> {
    /// The payload channel.
    sender: SyncSender<Message<T>>,
    /// The receiving end, claimed by the worker thread for its lifetime and
    /// handed back when the thread exits so the worker can be recycled.
    receiver: Mutex<Receiver<Message<T>>>,
    /// Updated when putting a worker back into the queue.
    last_used: AtomicI64,
}

impl<T: Send + 'static> GoWorker<T> {
    /// A worker with a `capacity`-slot channel and no thread behind it yet.
    #[must_use]
    pub fn new(capacity: usize) -> GoWorker<T> {
        let (sender, receiver) = sync_channel(capacity);
        GoWorker {
            sender,
            receiver: Mutex::new(receiver),
            last_used: AtomicI64::new(0),
        }
    }

    /// A worker that has never run, stamped as last used at `last_used`.
    #[must_use]
    pub fn with_last_used(last_used: i64) -> GoWorker<T> {
        let worker = GoWorker::new(crate::worker_chan_cap());
        worker.set_last_used_time(last_used);
        worker
    }

    /// Hands `payload` to the worker, i.e. Go's `inputFunc`/`inputArg`.
    pub(crate) fn input(&self, payload: T) {
        let _ = self.sender.send(Message::Payload(payload));
    }
}

impl<T: Send + 'static> Worker for GoWorker<T> {
    fn finish(&self) {
        let _ = self.sender.send(Message::Finish);
    }

    fn last_used_time(&self) -> i64 {
        self.last_used.load(Ordering::Acquire)
    }

    fn set_last_used_time(&self, time: i64) {
        self.last_used.store(time, Ordering::Release);
    }
}

/// Starts a thread to repeat the process that performs the function calls.
pub(crate) fn run<T: Send + 'static>(pool: &Arc<PoolCore<T>>, worker: &Arc<GoWorker<T>>) {
    pool.add_running(1);
    let pool = Arc::clone(pool);
    let worker = Arc::clone(worker);
    thread::Builder::new()
        .name("ants-worker".to_owned())
        .stack_size(WORKER_STACK_SIZE)
        .spawn(move || work(pool, worker))
        .expect("ants: failed to spawn a worker thread");
}

fn work<T: Send + 'static>(pool: Arc<PoolCore<T>>, worker: Arc<GoWorker<T>>) {
    let mut panicked: Option<Box<dyn Any + Send>> = None;

    loop {
        let received = {
            let receiver = worker
                .receiver
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            receiver.recv()
        };
        let Ok(Message::Payload(payload)) = received else {
            // `Message::Finish`, or a channel that can no longer deliver.
            break;
        };

        match call(&pool, payload) {
            // A nil payload tells the worker to stop, the way a nil `func()`
            // or a nil `any` does in the original.
            Ok(false) => break,
            Ok(true) => {
                if !pool.revert_worker(&worker) {
                    break;
                }
            }
            Err(payload) => {
                panicked = Some(payload);
                break;
            }
        }
    }

    // The epilogue below is the deferred block of the original's `run()`, in
    // the same order: account for the exit, recycle the worker, report the
    // panic, then wake one caller that may be waiting for a free worker.
    if pool.add_running(-1) == 0 && pool.is_closed() {
        pool.all_done().close();
    }
    pool.recycle(worker);
    if let Some(payload) = panicked {
        match pool.options().panic_handler.as_ref() {
            Some(handler) => handler(payload),
            None => pool.options().logger().printf(&format!(
                "worker exits from panic: {}\n{}\n",
                describe(payload.as_ref()),
                Backtrace::force_capture()
            )),
        }
    }
    pool.cond().signal();
}

type CallResult = Result<bool, Box<dyn Any + Send>>;

/// Runs one payload, reporting `Ok(false)` when it was the stop sentinel and
/// `Err` when it panicked.
fn call<T: Send + 'static>(pool: &Arc<PoolCore<T>>, payload: T) -> CallResult {
    IN_TASK.with(|in_task| in_task.set(true));
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| (pool.run_payload())(payload)));
    IN_TASK.with(|in_task| in_task.set(false));
    outcome
}

thread_local! {
    /// Set while a worker thread is inside a user payload, so that the panic
    /// hook stays quiet for a panic the pool is about to recover from.
    static IN_TASK: Cell<bool> = const { Cell::new(false) };
}

static PANIC_HOOK: Once = Once::new();

/// Silences the default panic message for panics the pool recovers from.
///
/// Go's `recover()` swallows the panic outright; Rust's default hook would
/// print a `thread '...' panicked at ...` block to stderr before unwinding
/// reaches [`panic::catch_unwind`], which the original never emits. Panics from
/// anywhere else still reach the previous hook untouched.
pub(crate) fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if IN_TASK.try_with(|in_task| in_task.get()).unwrap_or(false) {
                return;
            }
            previous(info);
        }));
    });
}

/// Renders a recovered panic payload the way Go's `%v` renders a recovered
/// value.
fn describe(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_owned()
}
