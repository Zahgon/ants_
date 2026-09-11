/*
 * Copyright (c) 2019. Ants Authors. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
//! The default worker queue: a LIFO stack that grows on demand.

use std::sync::Arc;

use crate::deps::gotime::{now_unix_nano, Duration};
use crate::worker_queue::{Worker, WorkerQueue};
use crate::Error;

/// A last-in-first-out stack of idle workers.
///
/// Recycling the most recently used worker keeps the oldest ones idle, which is
/// what makes the sorted-by-last-use invariant hold and lets [`Self::refresh`]
/// find the expired prefix with a binary search.
#[derive(Debug)]
pub struct WorkerStack<W: Worker> {
    items: Vec<Arc<W>>,
}

impl<W: Worker> WorkerStack<W> {
    /// An empty stack with room reserved for `size` workers.
    #[must_use]
    pub fn new(size: i32) -> WorkerStack<W> {
        WorkerStack {
            items: Vec::with_capacity(size.max(0) as usize),
        }
    }

    /// The parked workers, oldest first.
    #[must_use]
    pub fn items(&self) -> &[Arc<W>] {
        &self.items
    }

    /// The index of the last worker whose last-use time is at or before
    /// `expiry_time`, searching the inclusive range `[l, r]`, or `-1` when no
    /// worker in that range has expired.
    #[must_use]
    pub fn binary_search(&self, l: i32, r: i32, expiry_time: i64) -> i32 {
        let (mut l, mut r) = (l, r);
        while l <= r {
            // Avoid overflow when computing mid.
            let mid = l + ((r - l) >> 1);
            if expiry_time < self.items[mid as usize].last_used_time() {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        r
    }
}

impl<W: Worker> WorkerQueue<W> for WorkerStack<W> {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn insert(&mut self, worker: Arc<W>) -> Result<(), Error> {
        self.items.push(worker);
        Ok(())
    }

    fn detach(&mut self) -> Option<Arc<W>> {
        self.items.pop()
    }

    fn refresh(&mut self, duration: Duration) -> Vec<Arc<W>> {
        let n = self.len();
        if n == 0 {
            return Vec::new();
        }

        let expiry_time = now_unix_nano() - duration.as_nanos();
        let index = self.binary_search(0, n as i32 - 1, expiry_time);
        if index == -1 {
            return Vec::new();
        }
        self.items.drain(..(index + 1) as usize).collect()
    }

    fn reset(&mut self) {
        for worker in &self.items {
            worker.finish();
        }
        self.items.clear();
    }
}
