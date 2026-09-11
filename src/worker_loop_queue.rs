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
//! The pre-allocated worker queue: a fixed-capacity ring buffer.

use std::sync::Arc;

use crate::deps::gotime::{now_unix_nano, Duration};
use crate::worker_queue::{Worker, WorkerQueue};
use crate::Error;

/// A fixed-capacity circular queue of idle workers, used under `PreAlloc`.
///
/// It is first-in-first-out rather than LIFO, and it refuses an insert once
/// full instead of growing — that refusal is how a pool whose capacity was
/// tuned downwards sheds its surplus workers.
#[derive(Debug)]
pub struct LoopQueue<W: Worker> {
    items: Vec<Option<Arc<W>>>,
    head: usize,
    tail: usize,
    size: usize,
    is_full: bool,
}

impl<W: Worker> LoopQueue<W> {
    /// A queue holding at most `size` workers, or `None` when `size` is not
    /// positive.
    #[must_use]
    pub fn new(size: i32) -> Option<LoopQueue<W>> {
        if size <= 0 {
            return None;
        }
        let size = size as usize;
        Some(LoopQueue {
            items: (0..size).map(|_| None).collect(),
            head: 0,
            tail: 0,
            size,
            is_full: false,
        })
    }

    /// The ring's backing storage, including the empty slots.
    #[must_use]
    pub fn items(&self) -> &[Option<Arc<W>>] {
        &self.items
    }

    /// The true index of the last worker whose last-use time is at or before
    /// `expiry_time`, or `-1` when nothing has expired.
    ///
    /// The ring is sorted by last-use time only in its rotated order, so the
    /// search runs over mapped positions and converts the answer back.
    #[must_use]
    pub fn binary_search(&self, expiry_time: i64) -> i32 {
        let n = self.items.len();
        // If no need to remove work, return -1.
        if self.is_empty() || expiry_time < self.last_used(self.head) {
            return -1;
        }

        // example
        // size = 8, head = 7, tail = 4
        // [ 2, 3, 4, 5, nil, nil, nil,  1]  true position
        //   0  1  2  3    4   5     6   7
        //              tail          head
        //
        //   1  2  3  4  nil nil   nil   0   mapped position
        //            r                  l
        //
        // base algorithm is a copy from worker_stack
        // map head and tail to effective left and right
        let base = self.head;
        let mut r = ((self.tail + n - 1 - self.head) % n) as i32;
        let mut l = 0i32;
        while l <= r {
            // Avoid overflow when computing mid.
            let mid = l + ((r - l) >> 1);
            // Calculate true mid position from mapped mid position.
            let tmid = (mid as usize + base) % n;
            if expiry_time < self.last_used(tmid) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        // Return true position from mapped position.
        (r + base as i32 + n as i32) % n as i32
    }

    fn last_used(&self, index: usize) -> i64 {
        self.items[index]
            .as_ref()
            .map_or(0, |worker| worker.last_used_time())
    }
}

impl<W: Worker> WorkerQueue<W> for LoopQueue<W> {
    fn len(&self) -> usize {
        if self.size == 0 || self.is_empty() {
            return 0;
        }
        if self.head == self.tail && self.is_full {
            return self.size;
        }
        if self.tail > self.head {
            return self.tail - self.head;
        }
        self.size - self.head + self.tail
    }

    fn is_empty(&self) -> bool {
        self.head == self.tail && !self.is_full
    }

    fn insert(&mut self, worker: Arc<W>) -> Result<(), Error> {
        if self.is_full {
            return Err(Error::QueueIsFull);
        }
        self.items[self.tail] = Some(worker);
        self.tail = (self.tail + 1) % self.size;
        if self.tail == self.head {
            self.is_full = true;
        }
        Ok(())
    }

    fn detach(&mut self) -> Option<Arc<W>> {
        if self.is_empty() {
            return None;
        }
        let worker = self.items[self.head].take();
        self.head = (self.head + 1) % self.size;
        self.is_full = false;
        worker
    }

    fn refresh(&mut self, duration: Duration) -> Vec<Arc<W>> {
        let expiry_time = now_unix_nano() - duration.as_nanos();
        let index = self.binary_search(expiry_time);
        if index == -1 {
            return Vec::new();
        }
        let index = index as usize;

        let mut expiry = Vec::new();
        if self.head <= index {
            for slot in &mut self.items[self.head..=index] {
                expiry.extend(slot.take());
            }
        } else {
            // The expired range wraps: the segment that starts at zero comes
            // first, exactly as the original appends it.
            for slot in &mut self.items[..=index] {
                expiry.extend(slot.take());
            }
            for slot in &mut self.items[self.head..] {
                expiry.extend(slot.take());
            }
        }

        self.head = (index + 1) % self.size;
        if !expiry.is_empty() {
            self.is_full = false;
        }
        expiry
    }

    fn reset(&mut self) {
        if self.is_empty() {
            return;
        }
        while let Some(worker) = self.detach() {
            worker.finish();
        }
        self.head = 0;
        self.tail = 0;
    }
}
