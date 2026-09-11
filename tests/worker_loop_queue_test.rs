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
//! Tests for the pre-allocated ring-buffer worker queue.

mod common;

use std::sync::Arc;

use ants::deps::gotime::now_unix_nano;
use ants::internal::{GoWorker, LoopQueue, Worker, WorkerQueue};
use ants::{Duration, Error, Task};

use common::assert_same_workers;

type TestWorker = GoWorker<Task>;

fn worker(last_used: i64) -> Arc<TestWorker> {
    Arc::new(GoWorker::with_last_used(last_used))
}

/// The occupied slots of `q` over `range`, as the original's
/// `q.items[a:b]` slice of workers.
fn slots(q: &LoopQueue<TestWorker>, range: std::ops::Range<usize>) -> Vec<Arc<TestWorker>> {
    q.items()[range].iter().flatten().cloned().collect()
}

#[test]
fn test_new_loop_queue() {
    let _serial = common::serial();

    let size = 100;
    let mut q = LoopQueue::<TestWorker>::new(size).expect("a positive size yields a queue");
    assert_eq!(0, q.len(), "Len error");
    assert!(q.is_empty(), "IsEmpty error");
    assert!(q.detach().is_none(), "Dequeue error");

    assert!(LoopQueue::<TestWorker>::new(0).is_none());
}

#[test]
fn test_loop_queue() {
    let _serial = common::serial();

    let size = 10;
    let mut q = LoopQueue::<TestWorker>::new(size).expect("a positive size yields a queue");

    for _ in 0..5 {
        if q.insert(worker(now_unix_nano())).is_err() {
            break;
        }
    }
    assert_eq!(5, q.len(), "Len error");
    let _ = q.detach();
    assert_eq!(4, q.len(), "Len error");

    ants::sleep(Duration::SECOND);

    for _ in 0..6 {
        if q.insert(worker(now_unix_nano())).is_err() {
            break;
        }
    }
    assert_eq!(10, q.len(), "Len error");

    let err = q.insert(worker(now_unix_nano()));
    assert_eq!(Err(Error::QueueIsFull), err, "Enqueue, error");

    q.refresh(Duration::SECOND);
    assert_eq!(6, q.len(), "Len error: {}", q.len());
}

#[test]
fn test_rotated_queue_search() {
    let _serial = common::serial();

    let size = 10;
    let mut q = LoopQueue::<TestWorker>::new(size).expect("a positive size yields a queue");

    let mut curr_time = now_unix_nano();

    // 1
    let expiry1 = curr_time;
    curr_time += 1;
    let _ = q.insert(worker(curr_time));

    assert_eq!(0, q.binary_search(curr_time), "index should be 0");
    assert_eq!(-1, q.binary_search(expiry1), "index should be -1");

    // 2
    curr_time += 1;
    let expiry2 = curr_time;
    curr_time += 1;
    let _ = q.insert(worker(curr_time));

    assert_eq!(-1, q.binary_search(expiry1), "index should be -1");

    assert_eq!(0, q.binary_search(expiry2), "index should be 0");

    assert_eq!(1, q.binary_search(curr_time), "index should be 1");

    // more
    for _ in 0..5 {
        curr_time += 1;
        let _ = q.insert(worker(curr_time));
    }

    curr_time += 1;
    let expiry3 = curr_time;
    let _ = q.insert(worker(expiry3));

    let mut err = Ok(());
    while err != Err(Error::QueueIsFull) {
        curr_time += 1;
        err = q.insert(worker(curr_time));
    }

    assert_eq!(7, q.binary_search(expiry3), "index should be 7");

    // rotate
    for _ in 0..6 {
        let _ = q.detach();
    }

    curr_time += 1;
    let expiry4 = curr_time;
    let _ = q.insert(worker(expiry4));

    for _ in 0..4 {
        curr_time += 1;
        let _ = q.insert(worker(curr_time));
    }
    //	head = 6, tail = 5, insert direction ->
    // [expiry4, time, time, time,  time, nil/tail,  time/head, time, time, time]
    assert_eq!(0, q.binary_search(expiry4), "index should be 0");

    for _ in 0..3 {
        let _ = q.detach();
    }
    curr_time += 1;
    let expiry5 = curr_time;
    let _ = q.insert(worker(expiry5));

    //	head = 6, tail = 5, insert direction ->
    // [expiry4, time, time, time,  time, expiry5,  nil/tail, nil, nil, time/head]
    assert_eq!(5, q.binary_search(expiry5), "index should be 5");

    for _ in 0..3 {
        curr_time += 1;
        let _ = q.insert(worker(curr_time));
    }
    //	head = 9, tail = 9, insert direction ->
    // [expiry4, time, time, time,  time, expiry5,  time, time, time, time/head/tail]
    assert_eq!(-1, q.binary_search(expiry2), "index should be -1");

    assert_eq!(
        9,
        q.binary_search(q.items()[9].as_ref().unwrap().last_used_time()),
        "index should be 9"
    );
    assert_eq!(8, q.binary_search(curr_time), "index should be 8");
}

#[test]
fn test_retrieve_expiry() {
    let _serial = common::serial();

    let size = 10usize;
    let mut q = LoopQueue::<TestWorker>::new(size as i32).expect("a positive size yields a queue");
    let mut expirew: Vec<Arc<TestWorker>> = Vec::new();
    let u = Duration::SECOND;

    // test [ time+1s, time+1s, time+1s, time+1s, time+1s, time, time, time, time, time]
    for _ in 0..size / 2 {
        let _ = q.insert(worker(now_unix_nano()));
    }
    expirew.extend(slots(&q, 0..size / 2));
    ants::sleep(u);

    for _ in 0..size / 2 {
        let _ = q.insert(worker(now_unix_nano()));
    }
    let workers = q.refresh(u);

    assert_same_workers(&workers, &expirew, "expired workers aren't right");

    // test [ time, time, time, time, time, time+1s, time+1s, time+1s, time+1s, time+1s]
    ants::sleep(u);

    for _ in 0..size / 2 {
        let _ = q.insert(worker(now_unix_nano()));
    }
    expirew.clear();
    expirew.extend(slots(&q, size / 2..size));

    let workers2 = q.refresh(u);

    assert_same_workers(&workers2, &expirew, "expired workers aren't right");

    // test [ time+1s, time+1s, time+1s, nil, nil, time+1s, time+1s, time+1s, time+1s, time+1s]
    for _ in 0..size / 2 {
        let _ = q.insert(worker(now_unix_nano()));
    }
    for _ in 0..size / 2 {
        let _ = q.detach();
    }
    for _ in 0..3 {
        let _ = q.insert(worker(now_unix_nano()));
    }
    ants::sleep(u);

    expirew.clear();
    expirew.extend(slots(&q, 0..3));
    expirew.extend(slots(&q, size / 2..size));

    let workers3 = q.refresh(u);

    assert_same_workers(&workers3, &expirew, "expired workers aren't right");
}
