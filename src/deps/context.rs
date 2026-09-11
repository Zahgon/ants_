//! The slice of Go's `context` package that the pool's shutdown path uses.

use std::sync::Arc;
use std::time::Instant;

use crate::deps::event::Event;
use crate::deps::gotime::Duration;
use crate::Error;

#[derive(Debug)]
struct Inner {
    cancelled: Event,
    deadline: Option<Instant>,
}

/// A cancellation signal, optionally carrying a deadline.
///
/// Cloning a `Context` shares the same signal, as passing a `context.Context`
/// by value does in Go.
#[derive(Clone, Debug)]
pub struct Context {
    inner: Arc<Inner>,
}

/// The handle returned alongside a cancellable [`Context`], i.e. Go's
/// `context.CancelFunc`.
///
/// Unlike Go's, it is not `Drop`-triggered: nothing here is cancelled
/// implicitly.
#[derive(Clone, Debug)]
pub struct CancelFunc {
    inner: Arc<Inner>,
}

impl CancelFunc {
    /// Cancels the associated context. Calling it more than once is a no-op.
    pub fn cancel(&self) {
        self.inner.cancelled.close();
    }
}

impl Context {
    /// A context that is never cancelled and has no deadline.
    #[must_use]
    pub fn background() -> Context {
        Context {
            inner: Arc::new(Inner {
                cancelled: Event::new(),
                deadline: None,
            }),
        }
    }

    /// A context cancellable through the returned handle.
    #[must_use]
    pub fn with_cancel() -> (Context, CancelFunc) {
        let inner = Arc::new(Inner {
            cancelled: Event::new(),
            deadline: None,
        });
        (
            Context {
                inner: Arc::clone(&inner),
            },
            CancelFunc { inner },
        )
    }

    /// A context that expires after `timeout` and can be cancelled earlier
    /// through the returned handle.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> (Context, CancelFunc) {
        let inner = Arc::new(Inner {
            cancelled: Event::new(),
            deadline: Some(Instant::now() + timeout.to_std()),
        });
        (
            Context {
                inner: Arc::clone(&inner),
            },
            CancelFunc { inner },
        )
    }

    /// Whether the context has been cancelled or its deadline has passed, i.e.
    /// whether `<-ctx.Done()` would proceed.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.inner.cancelled.is_closed() || self.past_deadline()
    }

    /// Why the context is done, or `None` while it is still live.
    ///
    /// Cancellation wins over expiry, matching Go: a context cancelled before
    /// its deadline reports `context canceled`.
    #[must_use]
    pub fn err(&self) -> Option<Error> {
        if self.inner.cancelled.is_closed() {
            Some(Error::Canceled)
        } else if self.past_deadline() {
            Some(Error::DeadlineExceeded)
        } else {
            None
        }
    }

    /// Blocks for at most `timeout` waiting for the context to become done,
    /// reporting whether it did.
    ///
    /// This is the original's `select` over `ctx.Done()` and a timer: a
    /// cancellation wakes the caller at once rather than at the next poll.
    pub fn wait_done_for(&self, timeout: Duration) -> bool {
        let timeout = timeout.to_std();
        let bounded = match self.remaining() {
            Some(remaining) => remaining.min(timeout),
            None => timeout,
        };
        if self.inner.cancelled.wait_timeout(bounded) {
            return true;
        }
        self.is_done()
    }

    /// Blocks until the context becomes done, i.e. `<-ctx.Done()`.
    pub(crate) fn wait(&self) {
        loop {
            if self.is_done() {
                return;
            }
            match self.remaining() {
                Some(remaining) => {
                    if self.inner.cancelled.wait_timeout(remaining) {
                        return;
                    }
                }
                None => {
                    self.inner.cancelled.wait();
                    return;
                }
            }
        }
    }

    fn past_deadline(&self) -> bool {
        self.inner
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    fn remaining(&self) -> Option<std::time::Duration> {
        self.inner
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }
}
