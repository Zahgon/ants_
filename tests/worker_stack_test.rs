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
//! Tests for the LIFO worker stack.

mod common;

use std::sync::Arc;

use ants::deps::gotime::now_unix_nano;
use ants::internal::{new_worker_queue, GoWorker, QueueType, WorkerQueue, WorkerStack};
use ants::{Duration, Task};

/// The worker queues are generic over their worker; the original's tests build
/// `goWorker`s, which is what a `Pool` parks.
type TestWorker = GoWorker<Task>;

fn worker(last_used: i64) -> Arc<TestWorker> {
    Arc::new(GoWorker::with_last_used(last_used))
}

#[test]
fn test_new_worker_stack() {
    let _serial = common::serial();

    let size = 100;
    let mut q = WorkerStack::<TestWorker>::new(size);
    assert_eq!(0, q.len(), "Len error");
    assert!(q.is_empty(), "IsEmpty error");
    assert!(q.detach().is_none(), "Dequeue error");
}

#[test]
fn test_worker_stack() {
    let _serial = common::serial();

    let mut q = new_worker_queue::<TestWorker>(QueueType(-1), 0);

    for _ in 0..5 {
        if q.insert(worker(now_unix_nano())).is_err() {
            break;
        }
    }
    assert_eq!(5, q.len(), "Len error");

    let expired = now_unix_nano();

    assert!(q.insert(worker(expired)).is_ok(), "Enqueue error");

    ants::sleep(Duration::SECOND);

    for _ in 0..6 {
        assert!(q.insert(worker(now_unix_nano())).is_ok(), "Enqueue error");
    }
    assert_eq!(12, q.len(), "Len error");
    q.refresh(Duration::SECOND);
    assert_eq!(6, q.len(), "Len error");
}

// It seems that something wrong with time.Now() on Windows, not sure whether it
// is a bug on Windows, so exclude this test from Windows platform temporarily.
#[test]
fn test_search() {
    let _serial = common::serial();

    let mut q = WorkerStack::<TestWorker>::new(0);

    let mut curr_time = now_unix_nano();

    // 1
    let expiry1 = curr_time;
    curr_time += 1;
    let _ = q.insert(worker(curr_time));

    let last = |q: &WorkerStack<TestWorker>| q.len() as i32 - 1;

    assert_eq!(
        0,
        q.binary_search(0, last(&q), curr_time),
        "index should be 0"
    );
    assert_eq!(
        -1,
        q.binary_search(0, last(&q), expiry1),
        "index should be -1"
    );

    // 2
    curr_time += 1;
    let expiry2 = curr_time;
    curr_time += 1;
    let _ = q.insert(worker(curr_time));

    assert_eq!(
        -1,
        q.binary_search(0, last(&q), expiry1),
        "index should be -1"
    );

    assert_eq!(
        0,
        q.binary_search(0, last(&q), expiry2),
        "index should be 0"
    );

    assert_eq!(
        1,
        q.binary_search(0, last(&q), curr_time),
        "index should be 1"
    );

    // more
    for _ in 0..5 {
        curr_time += 1;
        let _ = q.insert(worker(curr_time));
    }

    curr_time += 1;
    let expiry3 = curr_time;

    let _ = q.insert(worker(expiry3));

    for _ in 0..10 {
        curr_time += 1;
        let _ = q.insert(worker(curr_time));
    }

    assert_eq!(
        7,
        q.binary_search(0, last(&q), expiry3),
        "index should be 7"
    );
}
