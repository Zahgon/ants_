// Copyright 2019 Andy Pan & Dietoad. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

const MAX_BACKOFF: u32 = 16;

/// A spin-lock with exponential backoff.
///
/// Unlike [`std::sync::Mutex`] it never parks the calling thread: a contending
/// thread yields the processor `backoff` times and doubles `backoff`, capped at
/// 16, exactly as the original does. It guards the data it owns, so a lock that
/// protects nothing is a `SpinLock<()>`.
pub struct SpinLock<T: ?Sized> {
    state: AtomicU32,
    data: UnsafeCell<T>,
}

// SAFETY: `state` serialises every access to `data`, so sharing a `&SpinLock<T>`
// across threads is sound whenever `T` may itself be sent across them.
unsafe impl<T: ?Sized + Send> Send for SpinLock<T> {}
// SAFETY: as above — mutual exclusion is what makes `&SpinLock<T>` shareable.
unsafe impl<T: ?Sized + Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    /// Wraps `data` in a new, unlocked spin-lock.
    #[must_use]
    pub const fn new(data: T) -> SpinLock<T> {
        SpinLock {
            state: AtomicU32::new(0),
            data: UnsafeCell::new(data),
        }
    }
}

impl<T: ?Sized> SpinLock<T> {
    /// Acquires the lock, spinning with exponential backoff until it is free.
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        let mut backoff = 1u32;
        while self
            .state
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // Leverage the exponential backoff algorithm,
            // see https://en.wikipedia.org/wiki/Exponential_backoff.
            for _ in 0..backoff {
                thread::yield_now();
            }
            if backoff < MAX_BACKOFF {
                backoff <<= 1;
            }
        }
        SpinLockGuard {
            lock: self,
            _not_send: PhantomData,
        }
    }

    pub(crate) fn unlock(&self) {
        self.state.store(0, Ordering::Release);
    }
}

/// Proof that the [`SpinLock`] is held, and the only way to reach its data.
pub struct SpinLockGuard<'a, T: ?Sized> {
    lock: &'a SpinLock<T>,
    _not_send: PhantomData<*const ()>,
}

impl<'a, T: ?Sized> SpinLockGuard<'a, T> {
    /// The lock this guard holds, so that a caller which must release and later
    /// reacquire it — a condition-variable wait — can find its way back.
    pub(crate) fn lock_ref(&self) -> &'a SpinLock<T> {
        self.lock
    }
}

impl<T: ?Sized> Deref for SpinLockGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding the guard means holding the lock.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: holding the guard means holding the lock exclusively.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

/// Instantiates a spin-lock that guards nothing but itself.
#[must_use]
pub fn new_spin_lock() -> SpinLock<()> {
    SpinLock::new(())
}
