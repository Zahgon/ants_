//! A `chan struct{}` that is only ever closed: a one-shot broadcast latch.

use std::sync::{Condvar, Mutex};
use std::time::{Duration as StdDuration, Instant};

/// A latch that starts open and is closed exactly once.
///
/// Go signals "this has happened, and it has happened for everybody" by closing
/// a channel: every current and future receive on a closed channel completes
/// immediately. `Event` is that, and nothing more.
#[derive(Debug)]
pub(crate) struct Event {
    closed: Mutex<bool>,
    changed: Condvar,
}

impl Event {
    pub(crate) fn new() -> Event {
        Event {
            closed: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    /// Closes the latch and wakes every waiter. Closing twice is a no-op, which
    /// is why the Go code's `sync.Once` around `close(p.allDone)` has no
    /// counterpart here.
    pub(crate) fn close(&self) {
        let mut closed = lock(&self.closed);
        if !*closed {
            *closed = true;
            self.changed.notify_all();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        *lock(&self.closed)
    }

    /// Blocks until the latch is closed, i.e. `<-ch`.
    pub(crate) fn wait(&self) {
        let mut closed = lock(&self.closed);
        while !*closed {
            closed = self
                .changed
                .wait(closed)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Blocks until the latch is closed or `timeout` elapses, reporting whether
    /// the latch closed.
    pub(crate) fn wait_timeout(&self, timeout: StdDuration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut closed = lock(&self.closed);
        while !*closed {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return *closed;
            };
            if remaining.is_zero() {
                return *closed;
            }
            let (guard, _) = self
                .changed
                .wait_timeout(closed, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            closed = guard;
        }
        true
    }
}

/// A poisoned lock still holds a valid `bool`; a panicking worker must not
/// wedge the pool it panicked in.
fn lock(mutex: &Mutex<bool>) -> std::sync::MutexGuard<'_, bool> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
