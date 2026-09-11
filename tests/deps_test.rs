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

//! Tests for the behaviour `ants` used to get from the Go standard library and
//! now implements itself.
//!
//! The original delegated error text to `errors.New`, the log-line layout to
//! `log.Logger`, signed durations to `time.Duration`, cancellation to
//! `context`, and the pool's lock to `sync.Locker`. Go's own test suites
//! covered those; in Rust they are this crate's code, so they need tests of
//! their own.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread;

use ants::deps::context::Context;
use ants::deps::golog::{LDATE, LMICROSECONDS, LMSGPREFIX, LSTD_FLAGS, LTIME, LUTC};
use ants::pkg::sync::new_spin_lock;
use ants::{Duration, Error, Logger, StdLogger, Target};

/// Every error's message is protocol surface; these are the exact strings the
/// original's package-level `error` values carry.
#[test]
fn test_error_messages() {
    assert_eq!(
        "must provide function for pool",
        Error::LackPoolFunc.to_string()
    );
    assert_eq!(
        "invalid expiry for pool",
        Error::InvalidPoolExpiry.to_string()
    );
    assert_eq!("this pool has been closed", Error::PoolClosed.to_string());
    assert_eq!(
        "too many goroutines blocked on submit or Nonblocking is set",
        Error::PoolOverload.to_string()
    );
    assert_eq!(
        "can not set up a negative capacity under PreAlloc mode",
        Error::InvalidPreAllocSize.to_string()
    );
    assert_eq!("operation timed out", Error::Timeout.to_string());
    assert_eq!("invalid pool index", Error::InvalidPoolIndex.to_string());
    assert_eq!(
        "invalid load-balancing strategy",
        Error::InvalidLoadBalancingStrategy.to_string()
    );
    assert_eq!(
        "invalid size for multiple pool",
        Error::InvalidMultiPoolSize.to_string()
    );
    assert_eq!("the queue is full", Error::QueueIsFull.to_string());
    assert_eq!("context canceled", Error::Canceled.to_string());
    assert_eq!(
        "context deadline exceeded",
        Error::DeadlineExceeded.to_string()
    );
    assert_eq!(
        "pool 0: context canceled | pool 3: operation timed out",
        Error::Pools("pool 0: context canceled | pool 3: operation timed out".to_owned())
            .to_string()
    );
    let boxed: Box<dyn std::error::Error> = Box::new(Error::PoolClosed);
    assert_eq!("this pool has been closed", boxed.to_string());
}

/// The default logger renders `2026/09/09 14:03:11.482913 [ants]: <message>`,
/// i.e. `log.LstdFlags|log.Lmsgprefix|log.Lmicroseconds` with the `[ants]: `
/// prefix.
#[test]
fn test_default_logger_line_format() {
    let logger = StdLogger::new(
        Target::Stderr,
        "[ants]: ",
        LSTD_FLAGS | LMSGPREFIX | LMICROSECONDS,
    );
    let line = logger.format("worker exits from panic: Oops!");

    let tail = "[ants]: worker exits from panic: Oops!\n";
    assert!(line.ends_with(tail), "unexpected line: {line:?}");

    // "2026/09/09 14:03:11.482913 " — date, time with microseconds, one space.
    let head = &line[..line.len() - tail.len()];
    assert_eq!(27, head.len(), "unexpected header: {head:?}");
    assert!(
        head[..4].bytes().all(|b| b.is_ascii_digit()),
        "year: {head:?}"
    );
    assert_eq!("/", &head[4..5]);
    assert_eq!("/", &head[7..8]);
    assert_eq!(" ", &head[10..11]);
    assert_eq!(":", &head[13..14]);
    assert_eq!(":", &head[16..17]);
    assert_eq!(".", &head[19..20]);
    assert!(
        head[20..26].bytes().all(|b| b.is_ascii_digit()),
        "microseconds: {head:?}"
    );
    assert_eq!(" ", &head[26..27]);
}

/// Each header flag contributes exactly what the original's `formatHeader`
/// contributes, and no more.
#[test]
fn test_logger_header_flags() {
    // No flags at all: the prefix leads, and nothing else is added.
    assert_eq!(
        "P: hello\n",
        StdLogger::new(Target::Stderr, "P: ", 0).format("hello")
    );

    // Lmsgprefix moves the prefix in front of the message, which with no
    // date or time is the same place.
    assert_eq!(
        "P: hello\n",
        StdLogger::new(Target::Stderr, "P: ", LMSGPREFIX).format("hello")
    );

    // A message that already ends in a newline does not get a second one.
    assert_eq!(
        "P: hello\n",
        StdLogger::new(Target::Stderr, "P: ", 0).format("hello\n")
    );

    // LstdFlags is date plus second-resolution time: "2026/09/09 14:03:11 ".
    let line = StdLogger::new(Target::Stderr, "", LSTD_FLAGS).format("hello");
    assert!(line.ends_with("hello\n"), "unexpected line: {line:?}");
    assert_eq!(20, line.len() - "hello\n".len());

    // Ldate alone is "2026/09/09 ".
    let line = StdLogger::new(Target::Stderr, "", LDATE).format("hello");
    assert_eq!(11, line.len() - "hello\n".len());

    // Ltime alone is "14:03:11 ".
    let line = StdLogger::new(Target::Stderr, "", LTIME).format("hello");
    assert_eq!(9, line.len() - "hello\n".len());

    // LUTC changes the clock, not the layout.
    let line = StdLogger::new(Target::Stderr, "", LSTD_FLAGS | LUTC).format("hello");
    assert_eq!(20, line.len() - "hello\n".len());

    // Without Lmsgprefix the prefix leads the whole line, ahead of the date.
    let line = StdLogger::new(Target::Stderr, "P: ", LDATE).format("hello");
    assert!(line.starts_with("P: "), "unexpected line: {line:?}");
    let line = StdLogger::new(Target::Stderr, "P: ", LDATE | LMSGPREFIX).format("hello");
    assert!(!line.starts_with("P: "), "unexpected line: {line:?}");
    assert!(line.ends_with("P: hello\n"), "unexpected line: {line:?}");
}

/// Both targets write the rendered line; the harness captures them, so this
/// asserts only that neither panics and that the rendering is shared.
#[test]
fn test_logger_writes_to_both_targets() {
    let out = StdLogger::new(Target::Stdout, "out: ", 0);
    let err = StdLogger::new(Target::Stderr, "err: ", 0);
    assert_eq!("out: hello\n", out.format("hello"));
    assert_eq!("err: hello\n", err.format("hello"));
    out.printf("hello");
    err.printf("hello");
}

/// `time.Duration` is a signed nanosecond count, and `ants` depends on that
/// sign to reject a negative expiry.
#[test]
fn test_duration_is_signed_nanoseconds() {
    assert_eq!(1, Duration::NANOSECOND.as_nanos());
    assert_eq!(1_000, Duration::MICROSECOND.as_nanos());
    assert_eq!(1_000_000, Duration::MILLISECOND.as_nanos());
    assert_eq!(1_000_000_000, Duration::SECOND.as_nanos());
    assert_eq!(0, Duration::ZERO.as_nanos());
    assert_eq!(Duration::ZERO, Duration::default());

    assert_eq!(-1, Duration::from_nanos(-1).as_nanos());
    assert!(Duration::from_nanos(-1) < Duration::ZERO);
    assert_eq!(Duration::MILLISECOND * 100, Duration::from_millis(100));
    assert_eq!(Duration::SECOND * 3, Duration::from_secs(3));

    let wait = Duration::MILLISECOND * 100;
    assert_eq!(Duration::from_millis(150), wait + wait / 2);

    assert_eq!(std::time::Duration::from_secs(1), Duration::SECOND.to_std());
    // Every Go API that takes a duration treats a negative one as "no wait".
    assert_eq!(std::time::Duration::ZERO, Duration::from_nanos(-1).to_std());
    assert_eq!(std::time::Duration::ZERO, Duration::ZERO.to_std());
}

/// `time.Now().UnixNano()` moves forward and lands in this century.
#[test]
fn test_now_unix_nano_advances() {
    let before = ants::deps::gotime::now_unix_nano();
    ants::sleep(Duration::MILLISECOND * 5);
    let after = ants::deps::gotime::now_unix_nano();
    assert!(after > before, "{after} should be after {before}");
    assert!(before > 1_600_000_000_000_000_000, "{before} looks unset");
}

/// A background context is never done; a cancelled one reports
/// `context canceled`; an expired one reports `context deadline exceeded`.
#[test]
fn test_context_cancellation_and_deadline() {
    let background = Context::background();
    assert!(!background.is_done());
    assert_eq!(None, background.err());
    assert!(!background.wait_done_for(Duration::MILLISECOND * 5));

    let (ctx, cancel) = Context::with_cancel();
    assert!(!ctx.is_done());
    assert_eq!(None, ctx.err());
    cancel.cancel();
    assert!(ctx.is_done());
    assert_eq!(Some(Error::Canceled), ctx.err());
    // Cancelling twice is a no-op.
    cancel.cancel();
    assert_eq!(Some(Error::Canceled), ctx.err());

    let (ctx, _cancel) = Context::with_timeout(Duration::MILLISECOND * 20);
    assert!(!ctx.is_done());
    assert!(ctx.wait_done_for(Duration::SECOND));
    assert_eq!(Some(Error::DeadlineExceeded), ctx.err());

    // Cancellation wins over expiry.
    let (ctx, cancel) = Context::with_timeout(Duration::SECOND * 30);
    cancel.cancel();
    assert_eq!(Some(Error::Canceled), ctx.err());

    // A clone shares the same signal.
    let (ctx, cancel) = Context::with_cancel();
    let clone = ctx.clone();
    cancel.cancel();
    assert!(clone.is_done());
}

/// A cancelled context wakes every thread parked on it, which is what stops the
/// purge and ticktock threads promptly.
#[test]
fn test_context_wakes_waiters() {
    let (ctx, cancel) = Context::with_cancel();
    let waiter = {
        let ctx = ctx.clone();
        thread::spawn(move || ctx.wait_done_for(Duration::SECOND * 30))
    };
    ants::sleep(Duration::MILLISECOND * 20);
    cancel.cancel();
    assert!(waiter.join().unwrap(), "the waiter should have been woken");
}

/// The spin lock excludes: concurrent increments through it never lose one.
#[test]
fn test_spin_lock_is_mutually_exclusive() {
    let lock = Arc::new(new_spin_lock());
    let counter = Arc::new(AtomicI32::new(0));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..1000 {
                    let guard = lock.lock();
                    // Read-modify-write under the lock; a lock that let two
                    // threads in at once would lose increments here.
                    let seen = counter.load(Ordering::Relaxed);
                    counter.store(seen + 1, Ordering::Relaxed);
                    drop(guard);
                }
            })
        })
        .collect();
    for handle in threads {
        handle.join().expect("a locking thread panicked");
    }
    assert_eq!(8000, counter.load(Ordering::SeqCst));
}

/// The lock guards the value it owns, and hands it back through `Deref`.
#[test]
fn test_spin_lock_guards_its_data() {
    let lock = ants::pkg::sync::SpinLock::new(vec![1i32, 2, 3]);
    {
        let guard = lock.lock();
        assert_eq!(3, guard.len());
        assert_eq!(&[1, 2, 3], guard.as_slice());
    }
    {
        let mut guard = lock.lock();
        guard.push(4);
        assert_eq!(4, guard.len());
    }
    assert_eq!(4, lock.lock().len());
}
