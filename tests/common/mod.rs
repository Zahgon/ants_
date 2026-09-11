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

//! Fixtures shared by the whole suite.
//!
//! Go compiles every `_test.go` file of a package into one test binary, so
//! `ants_test.go` reaches the helpers declared in `ants_benchmark_test.go` and
//! the `sum`/`wg` globals declared in `example_test.go`. Rust integration tests
//! are separate crates, so those shared declarations live here and each test
//! file pulls them in with `mod common;`.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::any::Any;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use ants::internal::Worker;
use ants::{AnyArg, Duration};

// ---------------------------------------------------------------------------
// Constants, from ants_benchmark_test.go and ants_test.go
// ---------------------------------------------------------------------------

/// The sleep, in milliseconds, that `demo_func` stands in for real work with.
pub const BENCH_PARAM: i64 = 10;

/// The expiry the benchmarks configure their pools with.
pub const DEFAULT_EXPIRED_TIME: Duration = Duration::from_secs(10);

/// 1 << 10.
pub const KIB: u64 = 1024;
/// 1 << 20.
pub const MIB: u64 = 1_048_576;

/// The argument, in milliseconds, handed to the pooled functions.
pub const PARAM: i64 = 100;

/// The capacity of the pools that stand in for a busy application pool.
pub const ANTS_SIZE: i32 = 1000;

/// The capacity of the pools the coverage-driving tests build.
///
/// The original uses 10000. A goroutine is a few kilobytes of growable stack
/// multiplexed onto `GOMAXPROCS` threads; a Rust worker is a real OS thread,
/// and `TestRestCodeCoverage` keeps six pools of this size alive at once.
/// Scaled down so the suite stays inside the operating system's thread budget;
/// every assertion in the suite is about counts and errors, none about this
/// number.
pub const TEST_SIZE: i32 = 500;

/// How many payloads the throughput-shaped tests push through a pool.
///
/// The original uses 100000, which `TestNoPool` turns into 100000 *concurrent*
/// goroutines. Scaled down for the same reason as [`TEST_SIZE`].
pub const N: i32 = 2000;

// ---------------------------------------------------------------------------
// Allocation accounting, standing in for runtime.ReadMemStats
// ---------------------------------------------------------------------------

/// Cumulative bytes handed out by the allocator, i.e. `MemStats.TotalAlloc`.
static TOTAL_ALLOC: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// SAFETY: every method forwards to the system allocator unchanged; the only
// addition is a relaxed counter update.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TOTAL_ALLOC.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        TOTAL_ALLOC.fetch_add(
            new_size.saturating_sub(layout.size()) as u64,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static CUR_MEM: AtomicU64 = AtomicU64::new(0);

/// Reports the megabytes allocated since the previous call, the way each test
/// logs `curMem = mem.TotalAlloc/MiB - curMem`.
pub fn report_memory_usage() {
    let total = TOTAL_ALLOC.load(Ordering::Relaxed) / MIB;
    let previous = CUR_MEM.load(Ordering::Relaxed);
    let current = total.wrapping_sub(previous);
    CUR_MEM.store(current, Ordering::Relaxed);
    println!("memory usage:{current} MB");
}

// ---------------------------------------------------------------------------
// A Go channel of struct{}
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ChanState {
    closed: bool,
    available: usize,
}

/// An unbuffered `chan struct{}`.
///
/// The suite uses a channel in both of Go's modes: a rendezvous send that
/// releases exactly one blocked receiver, and a close that releases every
/// receiver forever. Neither is expressible with `std::sync::mpsc`, whose
/// receiving end cannot be shared between threads.
pub struct Chan {
    state: Mutex<ChanState>,
    changed: Condvar,
    is_nil: bool,
}

impl Chan {
    /// `make(chan struct{})`.
    #[must_use]
    pub fn new() -> Arc<Chan> {
        Arc::new(Chan {
            state: Mutex::new(ChanState::default()),
            changed: Condvar::new(),
            is_nil: false,
        })
    }

    /// A nil channel: every send and every receive on it blocks forever.
    #[must_use]
    pub fn nil() -> Arc<Chan> {
        Arc::new(Chan {
            state: Mutex::new(ChanState::default()),
            changed: Condvar::new(),
            is_nil: true,
        })
    }

    /// `<-ch`: blocks until a value arrives or the channel is closed.
    pub fn recv(&self) {
        let mut state = self.lock();
        loop {
            if self.is_nil {
                state = self.wait(state);
                continue;
            }
            if state.available > 0 {
                state.available -= 1;
                self.changed.notify_all();
                return;
            }
            if state.closed {
                return;
            }
            state = self.wait(state);
        }
    }

    /// `ch <- struct{}{}`: blocks until a receiver takes the value.
    pub fn send(&self) {
        let mut state = self.lock();
        if self.is_nil {
            loop {
                state = self.wait(state);
            }
        }
        state.available += 1;
        self.changed.notify_all();
        while state.available > 0 && !state.closed {
            state = self.wait(state);
        }
    }

    /// `close(ch)`: releases every current and future receiver.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        self.changed.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, ChanState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'a>(&self, state: MutexGuard<'a, ChanState>) -> MutexGuard<'a, ChanState> {
        self.changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner)
    }
}

// ---------------------------------------------------------------------------
// sync.WaitGroup
// ---------------------------------------------------------------------------

/// A `sync.WaitGroup`: a counter that a waiter blocks on until it reaches zero.
pub struct WaitGroup {
    count: Mutex<i64>,
    changed: Condvar,
}

impl WaitGroup {
    /// A wait group with a zero counter.
    #[must_use]
    pub const fn new() -> WaitGroup {
        WaitGroup {
            count: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    /// Adds `delta` to the counter.
    pub fn add(&self, delta: i64) {
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        *count += delta;
        assert!(*count >= 0, "sync: negative WaitGroup counter");
        if *count == 0 {
            self.changed.notify_all();
        }
    }

    /// Decrements the counter by one.
    pub fn done(&self) {
        self.add(-1);
    }

    /// Blocks until the counter reaches zero.
    pub fn wait(&self) {
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        while *count != 0 {
            count = self
                .changed
                .wait(count)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Default for WaitGroup {
    fn default() -> WaitGroup {
        WaitGroup::new()
    }
}

// ---------------------------------------------------------------------------
// Package-level test state, from example_test.go
// ---------------------------------------------------------------------------

/// The accumulator the `*Calc` tests and the examples add into.
pub static SUM: AtomicI32 = AtomicI32::new(0);

/// The wait group those same tests count down.
pub static WG: WaitGroup = WaitGroup::new();

/// Adds a dynamically-typed `i32` into [`SUM`] and counts down [`WG`].
pub fn inc_sum(i: AnyArg) {
    inc_sum_int(*i.unwrap().downcast::<i32>().unwrap());
}

/// Adds `i` into [`SUM`] and counts down [`WG`].
pub fn inc_sum_int(i: i32) {
    SUM.fetch_add(i, Ordering::SeqCst);
    WG.done();
}

// ---------------------------------------------------------------------------
// The pooled functions, from ants_benchmark_test.go
// ---------------------------------------------------------------------------

/// Sleeps [`BENCH_PARAM`] milliseconds.
pub fn demo_func() {
    ants::sleep(Duration::MILLISECOND * BENCH_PARAM);
}

/// Sleeps for the dynamically-typed number of milliseconds it is handed.
pub fn demo_pool_func(args: AnyArg) {
    let n = *args.unwrap().downcast::<i64>().unwrap();
    ants::sleep(Duration::MILLISECOND * n);
}

/// Sleeps for `n` milliseconds.
pub fn demo_pool_func_int(n: i64) {
    ants::sleep(Duration::MILLISECOND * n);
}

/// Set to 1 to let every [`long_running_func`] return.
pub static STOP_LONG_RUNNING_FUNC: AtomicI32 = AtomicI32::new(0);

/// Spins until [`STOP_LONG_RUNNING_FUNC`] is set.
///
/// The original yields with `runtime.Gosched()`, which hands the processor to
/// another goroutine on the same OS thread, so any number of these occupy at
/// most `GOMAXPROCS` threads. A Rust worker is an OS thread of its own, and
/// several tests leave these spinning for the rest of the run, so the port
/// parks for a millisecond instead of yielding — the same "run until told to
/// stop" semantics without starving every other test of a core.
pub fn long_running_func() {
    while STOP_LONG_RUNNING_FUNC.load(Ordering::SeqCst) == 0 {
        ants::sleep(Duration::MILLISECOND);
    }
}

/// Blocks on the dynamically-typed channel it is handed.
pub fn long_running_pool_func(arg: AnyArg) {
    arg.unwrap().downcast::<Arc<Chan>>().unwrap().recv();
}

/// Blocks on `ch`.
pub fn long_running_pool_func_ch(ch: Arc<Chan>) {
    ch.recv();
}

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Serialises the suite.
///
/// Go runs the tests of one package one at a time unless they opt into
/// `t.Parallel()`, and none of these do: they release and reboot the
/// process-global default pool and share [`SUM`] and [`WG`]. Rust's harness
/// runs tests in parallel by default, so every test takes this lock first.
pub fn serial() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `require.Eventually`: polls `condition` every `tick` until it holds or
/// `wait_for` elapses.
pub fn eventually<F>(mut condition: F, wait_for: Duration, tick: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + wait_for.to_std();
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        ants::sleep(tick);
    }
}

/// Asserts that two worker slices hold the very same workers, in order.
///
/// The original compares `[]worker` with `require.EqualValues`, which on a
/// slice of interface values holding distinct pointers is pointer identity.
pub fn assert_same_workers<W: Worker>(actual: &[Arc<W>], expected: &[Arc<W>], message: &str) {
    assert_eq!(actual.len(), expected.len(), "{message}");
    for (index, (left, right)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            Arc::ptr_eq(left, right),
            "{message} (differ at index {index})"
        );
    }
}

/// Collects what an example prints, so its `// Output:` block becomes an
/// explicit assertion rather than a comment the harness matches.
#[derive(Default)]
pub struct ExampleOutput {
    lines: Vec<String>,
}

impl ExampleOutput {
    /// An empty transcript.
    #[must_use]
    pub fn new() -> ExampleOutput {
        ExampleOutput::default()
    }

    /// Records — and prints — one line of the example's output.
    pub fn printf(&mut self, line: String) {
        println!("{line}");
        self.lines.push(line);
    }

    /// What the example printed, in order.
    #[must_use]
    pub fn lines(&self) -> Vec<&str> {
        self.lines.iter().map(String::as_str).collect()
    }
}

/// Renders a recovered panic payload the way Go's `%v` renders a recovered
/// value.
#[must_use]
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_owned()
}
