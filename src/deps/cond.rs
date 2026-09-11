//! `sync.Cond`: a condition variable over a lock the caller already owns.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use crate::pkg::sync::SpinLockGuard;

#[derive(Debug, Default)]
struct Waiters {
    /// Threads currently parked in `wait`, or about to be.
    parked: u64,
    /// Wake-up tokens issued by `signal` and not yet consumed.
    tokens: u64,
    /// Bumped by `broadcast`; a waiter whose generation has moved on leaves.
    generation: u64,
}

/// A condition variable that pairs with an arbitrary lock.
///
/// [`std::sync::Condvar`] can only pair with a [`std::sync::Mutex`], but the
/// pool's lock is the spin-lock from [`crate::pkg::sync`], exactly as
/// `sync.NewCond(p.lock)` pairs Go's condition variable with an arbitrary
/// `sync.Locker`. The internal mutex below is bookkeeping only: it is never
/// held while the caller's lock is being acquired, so the two cannot deadlock
/// against each other.
#[derive(Debug)]
pub(crate) struct Cond {
    waiters: Mutex<Waiters>,
    changed: Condvar,
}

impl Cond {
    pub(crate) fn new() -> Cond {
        Cond {
            waiters: Mutex::new(Waiters::default()),
            changed: Condvar::new(),
        }
    }

    /// Atomically releases `guard`, blocks until woken, then reacquires the
    /// lock and hands the guard back.
    ///
    /// Registration happens while `guard` is still held, so a `signal` racing
    /// with this call cannot slip past an unregistered waiter.
    pub(crate) fn wait<'a, T: ?Sized>(&self, guard: SpinLockGuard<'a, T>) -> SpinLockGuard<'a, T> {
        let lock = guard.lock_ref();
        let mut waiters = self.lock();
        waiters.parked += 1;
        let generation = waiters.generation;
        drop(guard);

        loop {
            if waiters.tokens > 0 {
                waiters.tokens -= 1;
                waiters.parked -= 1;
                break;
            }
            if waiters.generation != generation {
                waiters.parked -= 1;
                break;
            }
            waiters = self
                .changed
                .wait(waiters)
                .unwrap_or_else(PoisonError::into_inner);
        }
        drop(waiters);

        lock.lock()
    }

    /// Wakes one waiting thread, if there is one. Like Go's `Cond.Signal` it
    /// does not require the caller to hold the associated lock.
    pub(crate) fn signal(&self) {
        let mut waiters = self.lock();
        if waiters.parked > waiters.tokens {
            waiters.tokens += 1;
            self.changed.notify_one();
        }
    }

    /// Wakes every waiting thread.
    pub(crate) fn broadcast(&self) {
        let mut waiters = self.lock();
        if waiters.parked > 0 {
            waiters.generation = waiters.generation.wrapping_add(1);
            waiters.tokens = 0;
            self.changed.notify_all();
        }
    }

    fn lock(&self) -> MutexGuard<'_, Waiters> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
