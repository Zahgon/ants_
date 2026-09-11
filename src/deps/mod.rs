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

//! Narrow reimplementations of the Go runtime and standard-library behaviour
//! that `ants` relies on and that the Rust standard library does not supply.
//!
//! Each module here reproduces the *observable* behaviour of one Go facility —
//! it is not a general-purpose port of that package:
//!
//! * [`gotime`] — `time.Duration`, which is a **signed** 64-bit nanosecond
//!   count. `std::time::Duration` is unsigned and therefore cannot express the
//!   negative expiry that [`crate::Error::InvalidPoolExpiry`] exists to reject.
//! * `event` — a `chan struct{}` that is closed exactly once, i.e. a
//!   broadcast latch. Used for `allDone` and for context cancellation.
//! * `cond` — `sync.Cond`, a condition variable over an *arbitrary* lock.
//!   `std::sync::Condvar` only pairs with `std::sync::Mutex`, and the pool's
//!   lock is the spin lock from [`crate::pkg::sync`].
//! * [`context`] — the slice of `context.Context` that `ReleaseContext` uses:
//!   `Background`, `WithCancel`, `WithTimeout`, `Done` and `Err`.
//! * [`golog`] — the line format of `log.Logger`, which is what the default
//!   `ants` logger writes to stderr when a worker panics without a handler.

pub(crate) mod cond;
pub mod context;
pub(crate) mod event;
pub mod golog;
pub mod gotime;
