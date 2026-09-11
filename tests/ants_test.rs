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

//! The pool test suite.

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use ants::deps::context::Context;
use ants::deps::golog::LSTD_FLAGS;
use ants::{
    arg, panic_handler, pool_func, task, with_disable_purge, with_expiry_duration, with_logger,
    with_max_blocking_tasks, with_nonblocking, with_options, with_panic_handler, with_pre_alloc,
    AnyArg, Duration, Error, LoadBalancingStrategy, MultiPool, MultiPoolWithFunc,
    MultiPoolWithFuncGeneric, Options, Pool, PoolWithFunc, PoolWithFuncGeneric, StdLogger, Target,
    DEFAULT_CLEAN_INTERVAL_TIME, LEAST_TASKS, ROUND_ROBIN,
};

use common::{
    demo_func, demo_pool_func, demo_pool_func_int, eventually, long_running_func,
    long_running_pool_func, long_running_pool_func_ch, panic_message, report_memory_usage, Chan,
    WaitGroup, ANTS_SIZE, N, PARAM, STOP_LONG_RUNNING_FUNC, SUM, TEST_SIZE, WG,
};

/// The stack a bare `go func()` stand-in gets; these threads only sleep.
const PLAIN_THREAD_STACK: usize = 64 * 1024;

/// `TestAntsPoolWaitToGetWorker` is used to test waiting to get a worker.
#[test]
fn test_ants_pool_wait_to_get_worker() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = Pool::new(ANTS_SIZE, &[]).unwrap();

    for _ in 0..N {
        wg.add(1);
        let wg = Arc::clone(&wg);
        let _ = p.submit(task(move || {
            demo_pool_func(arg(PARAM));
            wg.done();
        }));
    }
    wg.wait();
    println!("pool, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

#[test]
fn test_ants_pool_wait_to_get_worker_pre_malloc() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = Pool::new(ANTS_SIZE, &[with_pre_alloc(true)]).unwrap();

    for _ in 0..N {
        wg.add(1);
        let wg = Arc::clone(&wg);
        let _ = p.submit(task(move || {
            demo_pool_func(arg(PARAM));
            wg.done();
        }));
    }
    wg.wait();
    println!("pool, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

/// `TestAntsPoolWithFuncWaitToGetWorker` is used to test waiting to get a
/// worker.
#[test]
fn test_ants_pool_with_func_wait_to_get_worker() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let inner = Arc::clone(&wg);
    let p = PoolWithFunc::new(
        ANTS_SIZE,
        pool_func(move |i: AnyArg| {
            demo_pool_func(i);
            inner.done();
        }),
        &[],
    )
    .unwrap();

    for _ in 0..N {
        wg.add(1);
        let _ = p.invoke(arg(PARAM));
    }
    wg.wait();
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

/// `TestAntsPoolWithFuncGenericWaitToGetWorker` is used to test waiting to get
/// a worker.
#[test]
fn test_ants_pool_with_func_generic_wait_to_get_worker() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let inner = Arc::clone(&wg);
    let p = PoolWithFuncGeneric::new(
        ANTS_SIZE,
        pool_func(move |i: i64| {
            demo_pool_func_int(i);
            inner.done();
        }),
        &[],
    )
    .unwrap();

    for _ in 0..N {
        wg.add(1);
        let _ = p.invoke(PARAM);
    }
    wg.wait();
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

#[test]
fn test_ants_pool_with_func_wait_to_get_worker_pre_malloc() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let inner = Arc::clone(&wg);
    let p = PoolWithFunc::new(
        ANTS_SIZE,
        pool_func(move |i: AnyArg| {
            demo_pool_func(i);
            inner.done();
        }),
        &[with_pre_alloc(true)],
    )
    .unwrap();

    for _ in 0..N {
        wg.add(1);
        let _ = p.invoke(arg(PARAM));
    }
    wg.wait();
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

#[test]
fn test_ants_pool_with_func_generic_wait_to_get_worker_pre_malloc() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let inner = Arc::clone(&wg);
    let p = PoolWithFuncGeneric::new(
        ANTS_SIZE,
        pool_func(move |i: i64| {
            demo_pool_func_int(i);
            inner.done();
        }),
        &[with_pre_alloc(true)],
    )
    .unwrap();

    for _ in 0..N {
        wg.add(1);
        let _ = p.invoke(PARAM);
    }
    wg.wait();
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

/// `TestAntsPoolGetWorkerFromCache` is used to test getting a worker from the
/// worker cache.
#[test]
fn test_ants_pool_get_worker_from_cache() {
    let _serial = common::serial();

    let p = Pool::new(TEST_SIZE, &[]).unwrap();

    for _ in 0..ANTS_SIZE {
        let _ = p.submit(task(demo_func));
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 2);
    let _ = p.submit(task(demo_func));
    println!("pool, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

/// `TestAntsPoolWithFuncGetWorkerFromCache` is used to test getting a worker
/// from the worker cache.
#[test]
fn test_ants_pool_with_func_get_worker_from_cache() {
    let _serial = common::serial();

    let dur = 10i64;
    let p = PoolWithFunc::new(TEST_SIZE, pool_func(demo_pool_func), &[]).unwrap();

    for _ in 0..ANTS_SIZE {
        let _ = p.invoke(arg(dur));
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 2);
    let _ = p.invoke(arg(dur));
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

/// `TestAntsPoolWithFuncGenericGetWorkerFromCache` is used to test getting a
/// worker from the worker cache.
#[test]
fn test_ants_pool_with_func_generic_get_worker_from_cache() {
    let _serial = common::serial();

    let dur = 10i64;
    let p = PoolWithFuncGeneric::new(TEST_SIZE, pool_func(demo_pool_func_int), &[]).unwrap();

    for _ in 0..ANTS_SIZE {
        let _ = p.invoke(dur);
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 2);
    let _ = p.invoke(dur);
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

#[test]
fn test_ants_pool_with_func_get_worker_from_cache_pre_malloc() {
    let _serial = common::serial();

    let dur = 10i64;
    let p = PoolWithFunc::new(
        TEST_SIZE,
        pool_func(demo_pool_func),
        &[with_pre_alloc(true)],
    )
    .unwrap();

    for _ in 0..ANTS_SIZE {
        let _ = p.invoke(arg(dur));
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 2);
    let _ = p.invoke(arg(dur));
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

#[test]
fn test_ants_pool_with_func_generic_get_worker_from_cache_pre_malloc() {
    let _serial = common::serial();

    let dur = 10i64;
    let p = PoolWithFuncGeneric::new(
        TEST_SIZE,
        pool_func(demo_pool_func_int),
        &[with_pre_alloc(true)],
    )
    .unwrap();

    for _ in 0..ANTS_SIZE {
        let _ = p.invoke(dur);
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 2);
    let _ = p.invoke(dur);
    println!("pool with func, running workers number:{}", p.running());
    report_memory_usage();
    p.release();
}

// Contrast between workers without a pool and workers with an ants pool.

#[test]
fn test_no_pool() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let mut threads = Vec::with_capacity(N as usize);
    for _ in 0..N {
        wg.add(1);
        let wg = Arc::clone(&wg);
        threads.push(
            thread::Builder::new()
                .stack_size(PLAIN_THREAD_STACK)
                .spawn(move || {
                    demo_func();
                    wg.done();
                })
                .expect("spawning a plain thread"),
        );
    }

    wg.wait();
    for handle in threads {
        let _ = handle.join();
    }
    report_memory_usage();
}

#[test]
fn test_ants_pool() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    for _ in 0..N {
        wg.add(1);
        let wg = Arc::clone(&wg);
        let _ = ants::submit(task(move || {
            demo_func();
            wg.done();
        }));
    }
    wg.wait();

    println!("pool, capacity:{}", ants::cap());
    println!("pool, running workers number:{}", ants::running());
    println!("pool, free workers number:{}", ants::free());

    report_memory_usage();
    ants::release();
}

#[test]
fn test_panic_handler() {
    let _serial = common::serial();

    let panic_counter = Arc::new(AtomicI64::new(0));
    let wg = Arc::new(WaitGroup::new());

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p0 = Pool::new(
        10,
        &[with_panic_handler(panic_handler(move |p| {
            counter.fetch_add(1, Ordering::SeqCst);
            println!("catch panic with PanicHandler: {}", panic_message(&*p));
            waiter.done();
        }))],
    );
    assert!(
        p0.is_ok(),
        "create new pool failed: {:?}",
        p0.as_ref().err()
    );
    let p0 = p0.unwrap();
    wg.add(1);
    let _ = p0.submit(task(|| {
        panic!("Oops!");
    }));
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(1, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p0.running(), "pool should be empty after panic");

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p1 = PoolWithFunc::new(
        10,
        pool_func(|p: AnyArg| std::panic::resume_unwind(p.unwrap())),
        &[with_panic_handler(panic_handler(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            waiter.done();
        }))],
    );
    assert!(
        p1.is_ok(),
        "create new pool with func failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    wg.add(1);
    let _ = p1.invoke(arg("Oops!".to_owned()));
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(2, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p1.running(), "pool should be empty after panic");

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p2 = PoolWithFuncGeneric::new(
        10,
        pool_func(|s: String| std::panic::panic_any(s)),
        &[with_panic_handler(panic_handler(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            waiter.done();
        }))],
    );
    assert!(
        p2.is_ok(),
        "create new pool with func failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    wg.add(1);
    let _ = p2.invoke("Oops!".to_owned());
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(3, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p2.running(), "pool should be empty after panic");

    p0.release();
    p1.release();
    p2.release();
}

#[test]
fn test_panic_handler_pre_malloc() {
    let _serial = common::serial();

    let panic_counter = Arc::new(AtomicI64::new(0));
    let wg = Arc::new(WaitGroup::new());

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p0 = Pool::new(
        10,
        &[
            with_pre_alloc(true),
            with_panic_handler(panic_handler(move |p| {
                counter.fetch_add(1, Ordering::SeqCst);
                println!("catch panic with PanicHandler: {}", panic_message(&*p));
                waiter.done();
            })),
        ],
    );
    assert!(
        p0.is_ok(),
        "create new pool failed: {:?}",
        p0.as_ref().err()
    );
    let p0 = p0.unwrap();
    wg.add(1);
    let _ = p0.submit(task(|| {
        panic!("Oops!");
    }));
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(1, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p0.running(), "pool should be empty after panic");

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p1 = PoolWithFunc::new(
        10,
        pool_func(|p: AnyArg| std::panic::resume_unwind(p.unwrap())),
        &[
            with_pre_alloc(true),
            with_panic_handler(panic_handler(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                waiter.done();
            })),
        ],
    );
    assert!(
        p1.is_ok(),
        "create new pool with func failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    wg.add(1);
    let _ = p1.invoke(arg("Oops!".to_owned()));
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(2, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p1.running(), "pool should be empty after panic");

    let counter = Arc::clone(&panic_counter);
    let waiter = Arc::clone(&wg);
    let p2 = PoolWithFuncGeneric::new(
        10,
        pool_func(|p: String| std::panic::panic_any(p)),
        &[
            with_pre_alloc(true),
            with_panic_handler(panic_handler(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                waiter.done();
            })),
        ],
    );
    assert!(
        p2.is_ok(),
        "create new pool with func failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    wg.add(1);
    let _ = p2.invoke("Oops!".to_owned());
    wg.wait();
    let c = panic_counter.load(Ordering::SeqCst);
    assert_eq!(3, c, "panic handler didn't work, panicCounter: {c}");
    assert_eq!(0, p1.running(), "pool should be empty after panic");

    p0.release();
    p1.release();
    p2.release();
}

#[test]
fn test_pool_panic_without_handler() {
    let _serial = common::serial();

    let p0 = Pool::new(10, &[]);
    assert!(
        p0.is_ok(),
        "create new pool failed: {:?}",
        p0.as_ref().err()
    );
    let p0 = p0.unwrap();
    let _ = p0.submit(task(|| {
        panic!("Oops!");
    }));

    let p1 = PoolWithFunc::new(
        10,
        pool_func(|p: AnyArg| std::panic::resume_unwind(p.unwrap())),
        &[],
    );
    assert!(
        p1.is_ok(),
        "create new pool with func failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    let _ = p1.invoke(arg("Oops!".to_owned()));

    let p2 = PoolWithFuncGeneric::new(10, pool_func(|p: String| std::panic::panic_any(p)), &[]);
    assert!(
        p2.is_ok(),
        "create new pool with func failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    let _ = p2.invoke("Oops!".to_owned());

    p0.release();
    p1.release();
    p2.release();
}

#[test]
fn test_pool_panic_without_handler_pre_malloc() {
    let _serial = common::serial();

    let p0 = Pool::new(10, &[with_pre_alloc(true)]);
    assert!(
        p0.is_ok(),
        "create new pool failed: {:?}",
        p0.as_ref().err()
    );
    let p0 = p0.unwrap();
    let _ = p0.submit(task(|| {
        panic!("Oops!");
    }));

    let p1 = PoolWithFunc::new(
        10,
        pool_func(|p: AnyArg| {
            std::panic::resume_unwind(p.unwrap());
        }),
        &[],
    );
    assert!(
        p1.is_ok(),
        "create new pool with func failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    let _ = p1.invoke(arg("Oops!".to_owned()));

    let p2 = PoolWithFuncGeneric::new(
        10,
        pool_func(|p: AnyArg| {
            std::panic::resume_unwind(p.unwrap());
        }),
        &[],
    );
    assert!(
        p2.is_ok(),
        "create new pool with func failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    let _ = p2.invoke(arg("Oops!".to_owned()));

    p0.release();
    p1.release();
    p2.release();
}

#[test]
fn test_purge_pool() {
    let _serial = common::serial();

    let size = 500;
    let ch = Chan::new();

    let p = Pool::new(size, &[]);
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();

    for i in 0..size {
        let j = i + 1;
        let ch = Arc::clone(&ch);
        let _ = p.submit(task(move || {
            ch.recv();
            let d = i64::from(j % 100);
            ants::sleep(Duration::MILLISECOND * d);
        }));
    }
    assert_eq!(
        size,
        p.running(),
        "pool should be full, expected: {}, but got: {}",
        size,
        p.running()
    );

    ch.close();
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 5);
    assert_eq!(
        0,
        p.running(),
        "pool should be empty after purge, but got {}",
        p.running()
    );

    let ch = Chan::new();
    let signal = Arc::clone(&ch);
    let f = pool_func(move |i: AnyArg| {
        signal.recv();
        let d = *i.unwrap().downcast::<i64>().unwrap() % 100;
        ants::sleep(Duration::MILLISECOND * d);
    });

    let p1 = PoolWithFunc::new(size, f, &[]);
    assert!(
        p1.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();

    for i in 0..size {
        let _ = p1.invoke(arg(i64::from(i)));
    }
    assert_eq!(
        size,
        p1.running(),
        "pool should be full, expected: {}, but got: {}",
        size,
        p1.running()
    );

    ch.close();
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 5);
    assert_eq!(
        0,
        p1.running(),
        "pool should be empty after purge, but got {}",
        p1.running()
    );

    let ch = Chan::new();
    let signal = Arc::clone(&ch);
    let f1 = pool_func(move |i: i64| {
        signal.recv();
        let d = i % 100;
        ants::sleep(Duration::MILLISECOND * d);
    });

    let p2 = PoolWithFuncGeneric::new(size, f1, &[]);
    assert!(
        p2.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();

    for i in 0..size {
        let _ = p2.invoke(i64::from(i));
    }
    assert_eq!(
        size,
        p2.running(),
        "pool should be full, expected: {}, but got: {}",
        size,
        p2.running()
    );

    ch.close();
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 5);
    assert_eq!(
        0,
        p2.running(),
        "pool should be empty after purge, but got {}",
        p2.running()
    );

    p.release();
    p1.release();
    p2.release();
}

#[test]
fn test_purge_pre_malloc_pool() {
    let _serial = common::serial();

    let p = Pool::new(10, &[with_pre_alloc(true)]);
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    let _ = p.submit(task(demo_func));
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 3);
    assert_eq!(0, p.running(), "all p should be purged");

    let p1 = PoolWithFunc::new(10, pool_func(demo_pool_func), &[]);
    assert!(
        p1.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    let _ = p1.invoke(arg(1i64));
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 3);
    assert_eq!(0, p1.running(), "all p should be purged");

    let p2 = PoolWithFuncGeneric::new(10, pool_func(demo_pool_func_int), &[]);
    assert!(
        p2.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    let _ = p2.invoke(1i64);
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME * 3);
    assert_eq!(0, p2.running(), "all p should be purged");

    p.release();
    p1.release();
    p2.release();
}

#[test]
fn test_nonblocking_submit() {
    let _serial = common::serial();

    let pool_size = 10;
    let p = Pool::new(pool_size, &[with_nonblocking(true)]);
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.submit(task(long_running_func)),
            "nonblocking submit when pool is not full shouldn't return error"
        );
    }
    let ch = Chan::new();
    let ch1 = Chan::new();
    let f = {
        let ch = Arc::clone(&ch);
        let ch1 = Arc::clone(&ch1);
        move || {
            ch.recv();
            ch1.close();
        }
    };
    // p is full now.
    assert_eq!(
        Ok(()),
        p.submit(task(f)),
        "nonblocking submit when pool is not full shouldn't return error"
    );
    assert_eq!(
        Err(Error::PoolOverload),
        p.submit(task(demo_func)),
        "nonblocking submit when pool is full should get an Error::PoolOverload"
    );
    // interrupt f to get an available worker
    ch.close();
    ch1.recv();
    assert_eq!(
        Ok(()),
        p.submit(task(demo_func)),
        "nonblocking submit when pool is not full shouldn't return error"
    );

    p.release();
}

#[test]
fn test_max_blocking_submit() {
    let _serial = common::serial();

    let pool_size = 10;
    let p = Pool::new(pool_size, &[with_max_blocking_tasks(1)]);
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = Arc::new(p.unwrap());
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.submit(task(long_running_func)),
            "submit when pool is not full shouldn't return error"
        );
    }
    let ch = Chan::new();
    let f = {
        let ch = Arc::clone(&ch);
        move || ch.recv()
    };
    // p is full now.
    assert_eq!(
        Ok(()),
        p.submit(task(f)),
        "submit when pool is not full shouldn't return error"
    );
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let err_ch: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let blocker = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        let err_ch = Arc::clone(&err_ch);
        thread::spawn(move || {
            // should be blocked. blocking num == 1
            if let Err(err) = p.submit(task(demo_func)) {
                *err_ch.lock().unwrap() = Some(err);
            }
            wg.done();
        })
    };
    ants::sleep(Duration::SECOND);
    // already reached max blocking limit
    assert_eq!(
        Err(Error::PoolOverload),
        p.submit(task(demo_func)),
        "blocking submit when pool reach max blocking submit should return Error::PoolOverload"
    );
    // interrupt f to make blocking submit successful.
    ch.close();
    wg.wait();
    let _ = blocker.join();
    assert!(
        err_ch.lock().unwrap().is_none(),
        "blocking submit when pool is full should not return error"
    );

    p.release();
}

#[test]
fn test_nonblocking_submit_with_func() {
    let _serial = common::serial();

    let pool_size = 10;
    let ch = Chan::new();
    let wg = Arc::new(WaitGroup::new());
    let waiter = Arc::clone(&wg);
    let p = PoolWithFunc::new(
        pool_size,
        pool_func(move |i: AnyArg| {
            long_running_pool_func(i);
            waiter.done();
        }),
        &[with_nonblocking(true)],
    );
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    wg.add(i64::from(pool_size));
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.invoke(arg(Arc::clone(&ch))),
            "nonblocking submit when pool is not full shouldn't return error"
        );
    }
    // p is full now.
    assert_eq!(
        Ok(()),
        p.invoke(arg(Arc::clone(&ch))),
        "nonblocking submit when pool is not full shouldn't return error"
    );
    assert_eq!(
        Err(Error::PoolOverload),
        p.invoke(None),
        "nonblocking submit when pool is full should get an Error::PoolOverload"
    );
    // interrupt f to get an available worker
    ch.close();
    wg.wait();
    wg.add(1);
    assert_eq!(
        Ok(()),
        p.invoke(arg(Arc::clone(&ch))),
        "nonblocking submit when pool is not full shouldn't return error"
    );
    wg.wait();

    p.release();
}

#[test]
fn test_nonblocking_submit_with_func_generic() {
    let _serial = common::serial();

    let pool_size = 10;
    let wg = Arc::new(WaitGroup::new());
    let waiter = Arc::clone(&wg);
    let p = PoolWithFuncGeneric::new(
        pool_size,
        pool_func(move |ch: Arc<Chan>| {
            long_running_pool_func_ch(ch);
            waiter.done();
        }),
        &[with_nonblocking(true)],
    );
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    let ch = Chan::new();
    wg.add(i64::from(pool_size));
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.invoke(Arc::clone(&ch)),
            "nonblocking submit when pool is not full shouldn't return error"
        );
    }
    // p is full now.
    assert_eq!(
        Ok(()),
        p.invoke(Arc::clone(&ch)),
        "nonblocking submit when pool is not full shouldn't return error"
    );
    assert_eq!(
        Err(Error::PoolOverload),
        p.invoke(Chan::nil()),
        "nonblocking submit when pool is full should get an Error::PoolOverload"
    );
    // interrupt f to get an available worker
    ch.close();
    wg.wait();
    wg.add(1);
    assert_eq!(
        Ok(()),
        p.invoke(Arc::clone(&ch)),
        "nonblocking submit when pool is not full shouldn't return error"
    );
    wg.wait();

    p.release();
}

#[test]
fn test_max_blocking_submit_with_func() {
    let _serial = common::serial();

    let ch = Chan::new();
    let pool_size = 10;
    let p = PoolWithFunc::new(
        pool_size,
        pool_func(long_running_pool_func),
        &[with_max_blocking_tasks(1)],
    );
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = Arc::new(p.unwrap());
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.invoke(arg(Arc::clone(&ch))),
            "submit when pool is not full shouldn't return error"
        );
    }
    // p is full now.
    assert_eq!(
        Ok(()),
        p.invoke(arg(Arc::clone(&ch))),
        "submit when pool is not full shouldn't return error"
    );
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let err_ch: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let blocker = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        let err_ch = Arc::clone(&err_ch);
        let ch = Arc::clone(&ch);
        thread::spawn(move || {
            // should be blocked. blocking num == 1
            if let Err(err) = p.invoke(arg(ch)) {
                *err_ch.lock().unwrap() = Some(err);
            }
            wg.done();
        })
    };
    ants::sleep(Duration::SECOND);
    // already reached max blocking limit
    assert_eq!(
        Err(Error::PoolOverload),
        p.invoke(arg(Arc::clone(&ch))),
        "blocking submit when pool reach max blocking submit should return Error::PoolOverload"
    );
    // interrupt one func to make blocking submit successful.
    ch.close();
    wg.wait();
    let _ = blocker.join();
    assert!(
        err_ch.lock().unwrap().is_none(),
        "blocking submit when pool is full should not return error"
    );

    p.release();
}

#[test]
fn test_max_blocking_submit_with_func_generic() {
    let _serial = common::serial();

    let pool_size = 10;
    let p = PoolWithFuncGeneric::new(
        pool_size,
        pool_func(long_running_pool_func_ch),
        &[with_max_blocking_tasks(1)],
    );
    assert!(
        p.is_ok(),
        "create TimingPool failed: {:?}",
        p.as_ref().err()
    );
    let p = Arc::new(p.unwrap());
    let ch = Chan::new();
    for _ in 0..pool_size - 1 {
        assert_eq!(
            Ok(()),
            p.invoke(Arc::clone(&ch)),
            "submit when pool is not full shouldn't return error"
        );
    }
    // p is full now.
    assert_eq!(
        Ok(()),
        p.invoke(Arc::clone(&ch)),
        "submit when pool is not full shouldn't return error"
    );
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let err_ch: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let blocker = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        let err_ch = Arc::clone(&err_ch);
        let ch = Arc::clone(&ch);
        thread::spawn(move || {
            // should be blocked. blocking num == 1
            if let Err(err) = p.invoke(ch) {
                *err_ch.lock().unwrap() = Some(err);
            }
            wg.done();
        })
    };
    ants::sleep(Duration::SECOND);
    // already reached max blocking limit
    assert_eq!(
        Err(Error::PoolOverload),
        p.invoke(Arc::clone(&ch)),
        "blocking submit when pool reach max blocking submit should return Error::PoolOverload"
    );
    // interrupt one func to make blocking submit successful.
    ch.close();
    wg.wait();
    let _ = blocker.join();
    assert!(
        err_ch.lock().unwrap().is_none(),
        "blocking submit when pool is full should not return error"
    );

    p.release();
}

#[test]
fn test_reboot_default_pool() {
    let _serial = common::serial();

    ants::reboot(); // should do nothing inside
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let waiter = Arc::clone(&wg);
    let _ = ants::submit(task(move || {
        demo_func();
        waiter.done();
    }));
    wg.wait();
    assert_eq!(Ok(()), ants::release_timeout(Duration::SECOND));
    assert_eq!(
        Err(Error::PoolClosed),
        ants::submit(None),
        "pool should be closed"
    );
    ants::reboot();
    wg.add(1);
    let waiter = Arc::clone(&wg);
    assert_eq!(
        Ok(()),
        ants::submit(task(move || waiter.done())),
        "pool should be rebooted"
    );
    wg.wait();

    ants::release();
}

#[test]
fn test_reboot_new_pool() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = Pool::new(10, &[]);
    assert!(p.is_ok(), "create Pool failed: {:?}", p.as_ref().err());
    let p = p.unwrap();
    wg.add(1);
    let waiter = Arc::clone(&wg);
    let _ = p.submit(task(move || {
        demo_func();
        waiter.done();
    }));
    wg.wait();
    assert_eq!(Ok(()), p.release_timeout(Duration::SECOND));
    assert_eq!(
        Err(Error::PoolClosed),
        p.submit(None),
        "pool should be closed"
    );
    p.reboot();
    wg.add(1);
    let waiter = Arc::clone(&wg);
    assert_eq!(
        Ok(()),
        p.submit(task(move || waiter.done())),
        "pool should be rebooted"
    );
    wg.wait();

    let waiter = Arc::clone(&wg);
    let p1 = PoolWithFunc::new(
        10,
        pool_func(move |i: AnyArg| {
            demo_pool_func(i);
            waiter.done();
        }),
        &[],
    );
    assert!(
        p1.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p1.as_ref().err()
    );
    let p1 = p1.unwrap();
    wg.add(1);
    let _ = p1.invoke(arg(1i64));
    wg.wait();
    assert_eq!(Ok(()), p1.release_timeout(Duration::SECOND));
    assert_eq!(
        Err(Error::PoolClosed),
        p1.invoke(None),
        "pool should be closed"
    );
    p1.reboot();
    wg.add(1);
    assert_eq!(Ok(()), p1.invoke(arg(1i64)), "pool should be rebooted");
    wg.wait();

    let waiter = Arc::clone(&wg);
    let p2 = PoolWithFuncGeneric::new(
        10,
        pool_func(move |i: i64| {
            demo_pool_func_int(i);
            waiter.done();
        }),
        &[],
    );
    assert!(
        p2.is_ok(),
        "create TimingPoolWithFunc failed: {:?}",
        p2.as_ref().err()
    );
    let p2 = p2.unwrap();
    wg.add(1);
    let _ = p2.invoke(1i64);
    wg.wait();
    assert_eq!(Ok(()), p2.release_timeout(Duration::SECOND));
    assert_eq!(
        Err(Error::PoolClosed),
        p2.invoke(1i64),
        "pool should be closed"
    );
    p2.reboot();
    wg.add(1);
    assert_eq!(Ok(()), p2.invoke(1i64), "pool should be rebooted");
    wg.wait();

    p.release();
    p1.release();
    p2.release();
}

#[test]
fn test_infinite_pool() {
    let _serial = common::serial();

    let c = Chan::new();
    let p = Arc::new(Pool::new(-1, &[]).unwrap());
    let inner_pool = Arc::clone(&p);
    let inner_chan = Arc::clone(&c);
    let _ = p.submit(task(move || {
        let _ = inner_pool.submit(task(move || inner_chan.recv()));
    }));
    c.send();
    let n = p.running();
    assert_eq!(2, n, "expect 2 workers running, but got {n}");
    let n = p.free();
    assert_eq!(
        -1, n,
        "expect -1 of free workers by unlimited pool, but got {n}"
    );
    p.tune(10);
    let capacity = p.cap();
    assert_eq!(-1, capacity, "expect capacity: -1 but got {capacity}");

    let err = Pool::new(-1, &[with_pre_alloc(true)]).err();
    assert_eq!(Some(Error::InvalidPreAllocSize), err);
}

fn test_pool_with_disable_purge(p: &Pool, num_worker: i32, wait_for_purge: Duration) {
    let sig = Chan::new();
    let wg1 = Arc::new(WaitGroup::new());
    let wg2 = Arc::new(WaitGroup::new());
    wg1.add(i64::from(num_worker));
    wg2.add(i64::from(num_worker));
    for _ in 0..num_worker {
        let sig = Arc::clone(&sig);
        let wg1 = Arc::clone(&wg1);
        let wg2 = Arc::clone(&wg2);
        let _ = p.submit(task(move || {
            wg1.done();
            sig.recv();
            wg2.done();
        }));
    }
    wg1.wait();

    let running_cnt = p.running();
    assert_eq!(
        num_worker, running_cnt,
        "expect {num_worker} workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(0, free_cnt, "expect 0 free workers, but got {free_cnt}");

    // Finish all tasks and sleep for a while to wait for purging, since we've
    // disabled the purge mechanism, we should see that all workers are still
    // running after the sleep.
    sig.close();
    wg2.wait();
    ants::sleep(wait_for_purge + wait_for_purge / 2);

    let running_cnt = p.running();
    assert_eq!(
        num_worker, running_cnt,
        "expect {num_worker} workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(0, free_cnt, "expect 0 free workers, but got {free_cnt}");

    let err = p.release_timeout(wait_for_purge + wait_for_purge / 2);
    assert_eq!(Ok(()), err, "release pool failed: {err:?}");

    let running_cnt = p.running();
    assert_eq!(
        0, running_cnt,
        "expect 0 workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(
        num_worker, free_cnt,
        "expect {num_worker} free workers, but got {free_cnt}"
    );
}

#[test]
fn test_with_disable_purge_pool() {
    let _serial = common::serial();

    let num_worker = 10;
    let p = Pool::new(num_worker, &[with_disable_purge(true)]).unwrap();
    test_pool_with_disable_purge(&p, num_worker, DEFAULT_CLEAN_INTERVAL_TIME);
}

#[test]
fn test_with_disable_purge_and_with_expiration_pool() {
    let _serial = common::serial();

    let num_worker = 10;
    let expired_duration = Duration::MILLISECOND * 100;
    let p = Pool::new(
        num_worker,
        &[
            with_disable_purge(true),
            with_expiry_duration(expired_duration),
        ],
    )
    .unwrap();
    test_pool_with_disable_purge(&p, num_worker, expired_duration);
}

fn test_pool_func_with_disable_purge(
    p: &PoolWithFunc,
    num_worker: i32,
    wg1: &WaitGroup,
    wg2: &WaitGroup,
    sig: &Chan,
    wait_for_purge: Duration,
) {
    for i in 0..num_worker {
        let _ = p.invoke(arg(i64::from(i)));
    }
    wg1.wait();

    let running_cnt = p.running();
    assert_eq!(
        num_worker, running_cnt,
        "expect {num_worker} workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(0, free_cnt, "expect 0 free workers, but got {free_cnt}");

    // Finish all tasks and sleep for a while to wait for purging, since we've
    // disabled the purge mechanism, we should see that all workers are still
    // running after the sleep.
    sig.close();
    wg2.wait();
    ants::sleep(wait_for_purge + wait_for_purge / 2);

    let running_cnt = p.running();
    assert_eq!(
        num_worker, running_cnt,
        "expect {num_worker} workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(0, free_cnt, "expect 0 free workers, but got {free_cnt}");

    let err = p.release_timeout(wait_for_purge + wait_for_purge / 2);
    assert_eq!(Ok(()), err, "release pool failed: {err:?}");

    let running_cnt = p.running();
    assert_eq!(
        0, running_cnt,
        "expect 0 workers running, but got {running_cnt}"
    );
    let free_cnt = p.free();
    assert_eq!(
        num_worker, free_cnt,
        "expect {num_worker} free workers, but got {free_cnt}"
    );
}

#[test]
fn test_with_disable_purge_pool_func() {
    let _serial = common::serial();

    let num_worker = 10;
    let sig = Chan::new();
    let wg1 = Arc::new(WaitGroup::new());
    let wg2 = Arc::new(WaitGroup::new());
    wg1.add(i64::from(num_worker));
    wg2.add(i64::from(num_worker));
    let (task_sig, task_wg1, task_wg2) = (Arc::clone(&sig), Arc::clone(&wg1), Arc::clone(&wg2));
    let p = PoolWithFunc::new(
        num_worker,
        pool_func(move |_: AnyArg| {
            task_wg1.done();
            task_sig.recv();
            task_wg2.done();
        }),
        &[with_disable_purge(true)],
    )
    .unwrap();
    test_pool_func_with_disable_purge(
        &p,
        num_worker,
        &wg1,
        &wg2,
        &sig,
        DEFAULT_CLEAN_INTERVAL_TIME,
    );
}

#[test]
fn test_with_disable_purge_and_with_expiration_pool_func() {
    let _serial = common::serial();

    let num_worker = 2;
    let sig = Chan::new();
    let wg1 = Arc::new(WaitGroup::new());
    let wg2 = Arc::new(WaitGroup::new());
    wg1.add(i64::from(num_worker));
    wg2.add(i64::from(num_worker));
    let expired_duration = Duration::MILLISECOND * 100;
    let (task_sig, task_wg1, task_wg2) = (Arc::clone(&sig), Arc::clone(&wg1), Arc::clone(&wg2));
    let p = PoolWithFunc::new(
        num_worker,
        pool_func(move |_: AnyArg| {
            task_wg1.done();
            task_sig.recv();
            task_wg2.done();
        }),
        &[
            with_disable_purge(true),
            with_expiry_duration(expired_duration),
        ],
    )
    .unwrap();
    test_pool_func_with_disable_purge(&p, num_worker, &wg1, &wg2, &sig, expired_duration);
}

#[test]
fn test_infinite_pool_with_func() {
    let _serial = common::serial();

    let c = Chan::new();
    let signal = Arc::clone(&c);
    let p = PoolWithFunc::new(
        -1,
        pool_func(move |i: AnyArg| {
            demo_pool_func(i);
            signal.recv();
        }),
        &[],
    );
    assert!(
        p.is_ok(),
        "create pool with func failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    let _ = p.invoke(arg(10i64));
    let _ = p.invoke(arg(10i64));
    c.send();
    c.send();
    let n = p.running();
    assert_eq!(2, n, "expect 2 workers running, but got {n}");
    let n = p.free();
    assert_eq!(
        -1, n,
        "expect -1 of free workers by unlimited pool, but got {n}"
    );
    p.tune(10);
    let capacity = p.cap();
    assert_eq!(-1, capacity, "expect capacity: -1 but got {capacity}");

    let err = PoolWithFunc::new(-1, pool_func(demo_pool_func), &[with_pre_alloc(true)]).err();
    assert_eq!(
        Some(Error::InvalidPreAllocSize),
        err,
        "expect Error::InvalidPreAllocSize but got {err:?}"
    );

    p.release();
}

#[test]
fn test_infinite_pool_with_func_generic() {
    let _serial = common::serial();

    let c = Chan::new();
    let signal = Arc::clone(&c);
    let p = PoolWithFuncGeneric::new(
        -1,
        pool_func(move |i: i64| {
            demo_pool_func_int(i);
            signal.recv();
        }),
        &[],
    );
    assert!(
        p.is_ok(),
        "create pool with func failed: {:?}",
        p.as_ref().err()
    );
    let p = p.unwrap();
    let _ = p.invoke(10i64);
    let _ = p.invoke(10i64);
    c.send();
    c.send();
    let n = p.running();
    assert_eq!(2, n, "expect 2 workers running, but got {n}");
    let n = p.free();
    assert_eq!(
        -1, n,
        "expect -1 of free workers by unlimited pool, but got {n}"
    );
    p.tune(10);
    let capacity = p.cap();
    assert_eq!(-1, capacity, "expect capacity: -1 but got {capacity}");

    let err =
        PoolWithFuncGeneric::new(-1, pool_func(demo_pool_func_int), &[with_pre_alloc(true)]).err();
    assert_eq!(
        Some(Error::InvalidPreAllocSize),
        err,
        "expect Error::InvalidPreAllocSize but got {err:?}"
    );

    p.release();
}

#[test]
fn test_release_when_running_pool() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = Pool::new(1, &[]);
    assert!(p.is_ok(), "create pool failed: {:?}", p.as_ref().err());
    let p = Arc::new(p.unwrap());
    wg.add(2);

    let aaa = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start aaa");
            for i in 0..30 {
                let j = i;
                let _ = p.submit(task(move || {
                    println!("do task {j}");
                    ants::sleep(Duration::SECOND);
                }));
            }
            wg.done();
            println!("stop aaa");
        })
    };

    let bbb = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start bbb");
            for i in 100..130 {
                let j = i;
                let _ = p.submit(task(move || {
                    println!("do task {j}");
                    ants::sleep(Duration::SECOND);
                }));
            }
            wg.done();
            println!("stop bbb");
        })
    };

    ants::sleep(Duration::SECOND * 3);
    p.release();
    println!("wait for all threads to exit...");
    wg.wait();
    let _ = aaa.join();
    let _ = bbb.join();
}

#[test]
fn test_release_when_running_pool_with_func() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = PoolWithFunc::new(
        1,
        pool_func(|i: AnyArg| {
            println!("do task {}", i.unwrap().downcast::<i64>().unwrap());
            ants::sleep(Duration::SECOND);
        }),
        &[],
    );
    assert!(
        p.is_ok(),
        "create pool with func failed: {:?}",
        p.as_ref().err()
    );
    let p = Arc::new(p.unwrap());

    wg.add(2);
    let aaa = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start aaa");
            for i in 0..30i64 {
                let _ = p.invoke(arg(i));
            }
            wg.done();
            println!("stop aaa");
        })
    };

    let bbb = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start bbb");
            for i in 100..130i64 {
                let _ = p.invoke(arg(i));
            }
            wg.done();
            println!("stop bbb");
        })
    };

    ants::sleep(Duration::SECOND * 3);
    p.release();
    println!("wait for all threads to exit...");
    wg.wait();
    let _ = aaa.join();
    let _ = bbb.join();
}

#[test]
fn test_release_when_running_pool_with_func_generic() {
    let _serial = common::serial();

    let wg = Arc::new(WaitGroup::new());
    let p = PoolWithFuncGeneric::new(
        1,
        pool_func(|i: i64| {
            println!("do task {i}");
            ants::sleep(Duration::SECOND);
        }),
        &[],
    );
    assert!(
        p.is_ok(),
        "create pool with func failed: {:?}",
        p.as_ref().err()
    );
    let p = Arc::new(p.unwrap());
    wg.add(2);

    let aaa = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start aaa");
            for i in 0..30i64 {
                let _ = p.invoke(i);
            }
            wg.done();
            println!("stop aaa");
        })
    };

    let bbb = {
        let p = Arc::clone(&p);
        let wg = Arc::clone(&wg);
        thread::spawn(move || {
            println!("start bbb");
            for i in 100..130i64 {
                let _ = p.invoke(i);
            }
            wg.done();
            println!("stop bbb");
        })
    };

    ants::sleep(Duration::SECOND * 3);
    p.release();
    println!("wait for all threads to exit...");
    wg.wait();
    let _ = aaa.join();
    let _ = bbb.join();
}

#[test]
fn test_rest_code_coverage() {
    let _serial = common::serial();

    let negative = Duration::from_nanos(-1);

    let err = Pool::new(-1, &[with_expiry_duration(negative)]).err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let err = Pool::new(1, &[with_expiry_duration(negative)]).err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let err = PoolWithFunc::new(
        -1,
        pool_func(demo_pool_func),
        &[with_expiry_duration(negative)],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let err = PoolWithFunc::new(
        1,
        pool_func(demo_pool_func),
        &[with_expiry_duration(negative)],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let err = PoolWithFunc::new(1, None, &[with_expiry_duration(negative)]).err();
    assert_eq!(Some(Error::LackPoolFunc), err);
    let err = PoolWithFuncGeneric::new(
        -1,
        pool_func(demo_pool_func_int),
        &[with_expiry_duration(negative)],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let err = PoolWithFuncGeneric::new(
        1,
        pool_func(demo_pool_func_int),
        &[with_expiry_duration(negative)],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);
    let f: ants::PoolFunc<i64> = None;
    let err = PoolWithFuncGeneric::new(1, f, &[with_expiry_duration(negative)]).err();
    assert_eq!(Some(Error::LackPoolFunc), err);

    let options = Options {
        expiry_duration: Duration::SECOND * 10,
        nonblocking: true,
        pre_alloc: true,
        ..Options::default()
    };
    let pool_opts = Pool::new(1, &[with_options(options)]).unwrap();
    println!("Pool with options, capacity: {}", pool_opts.cap());

    let p0 = Pool::new(
        TEST_SIZE,
        &[with_logger(Arc::new(StdLogger::new(
            Target::Stderr,
            "",
            LSTD_FLAGS,
        )))],
    )
    .unwrap();
    for _ in 0..N {
        let _ = p0.submit(task(demo_func));
    }
    println!("pool, capacity:{}", p0.cap());
    println!("pool, running workers number:{}", p0.running());
    println!("pool, free workers number:{}", p0.free());
    p0.tune(TEST_SIZE);
    p0.tune(TEST_SIZE / 10);
    println!(
        "pool, after tuning capacity, capacity:{}, running:{}",
        p0.cap(),
        p0.running()
    );

    let p1 = Pool::new(TEST_SIZE, &[with_pre_alloc(true)]).unwrap();
    for _ in 0..N {
        let _ = p1.submit(task(demo_func));
    }
    println!("pre-malloc pool, capacity:{}", p1.cap());
    println!("pre-malloc pool, running workers number:{}", p1.running());
    println!("pre-malloc pool, free workers number:{}", p1.free());
    p1.tune(TEST_SIZE);
    p1.tune(TEST_SIZE / 10);
    println!(
        "pre-malloc pool, after tuning capacity, capacity:{}, running:{}",
        p1.cap(),
        p1.running()
    );

    let p2 = PoolWithFunc::new(TEST_SIZE, pool_func(demo_pool_func), &[]).unwrap();
    for _ in 0..N {
        let _ = p2.invoke(arg(PARAM));
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME);
    println!("pool with func, capacity:{}", p2.cap());
    println!("pool with func, running workers number:{}", p2.running());
    println!("pool with func, free workers number:{}", p2.free());
    p2.tune(TEST_SIZE);
    p2.tune(TEST_SIZE / 10);
    println!(
        "pool with func, after tuning capacity, capacity:{}, running:{}",
        p2.cap(),
        p2.running()
    );

    let p3 = PoolWithFuncGeneric::new(TEST_SIZE, pool_func(demo_pool_func_int), &[]).unwrap();
    for _ in 0..N {
        let _ = p3.invoke(PARAM);
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME);
    println!("pool with func, capacity:{}", p3.cap());
    println!("pool with func, running workers number:{}", p3.running());
    println!("pool with func, free workers number:{}", p3.free());
    p3.tune(TEST_SIZE);
    p3.tune(TEST_SIZE / 10);
    println!(
        "pool with func, after tuning capacity, capacity:{}, running:{}",
        p3.cap(),
        p3.running()
    );

    let p4 = PoolWithFunc::new(
        TEST_SIZE,
        pool_func(demo_pool_func),
        &[with_pre_alloc(true)],
    )
    .unwrap();
    for _ in 0..N {
        let _ = p4.invoke(arg(PARAM));
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME);
    println!("pre-malloc pool with func, capacity:{}", p4.cap());
    println!(
        "pre-malloc pool with func, running workers number:{}",
        p4.running()
    );
    println!(
        "pre-malloc pool with func, free workers number:{}",
        p4.free()
    );
    p4.tune(TEST_SIZE);
    p4.tune(TEST_SIZE / 10);
    println!(
        "pre-malloc pool with func, after tuning capacity, capacity:{}, running:{}",
        p4.cap(),
        p4.running()
    );

    let p5 = PoolWithFuncGeneric::new(
        TEST_SIZE,
        pool_func(demo_pool_func_int),
        &[with_pre_alloc(true)],
    )
    .unwrap();
    for _ in 0..N {
        let _ = p5.invoke(PARAM);
    }
    ants::sleep(DEFAULT_CLEAN_INTERVAL_TIME);
    println!("pre-malloc pool with func, capacity:{}", p5.cap());
    println!(
        "pre-malloc pool with func, running workers number:{}",
        p5.running()
    );
    println!(
        "pre-malloc pool with func, free workers number:{}",
        p5.free()
    );
    p5.tune(TEST_SIZE);
    p5.tune(TEST_SIZE / 10);
    println!(
        "pre-malloc pool with func, after tuning capacity, capacity:{}, running:{}",
        p5.cap(),
        p5.running()
    );

    // The original registers a deferred release and a deferred submit per pool,
    // and Go runs deferred calls last-in-first-out: each pool is released and
    // then submitted to, from p5 back down to p0.
    p5.release();
    let _ = p5.invoke(PARAM);
    p4.release();
    let _ = p4.invoke(arg(PARAM));
    p3.release();
    let _ = p3.invoke(PARAM);
    p2.release();
    let _ = p2.invoke(arg(PARAM));
    p1.release();
    let _ = p1.submit(task(demo_func));
    p0.release();
    let _ = p0.submit(task(demo_func));
}

#[test]
fn test_pool_tune_scale_up() {
    let _serial = common::serial();

    let c = Chan::new();
    // Test Pool
    let p = Arc::new(Pool::new(2, &[]).unwrap());
    for _ in 0..2 {
        let c = Arc::clone(&c);
        let _ = p.submit(task(move || c.recv()));
    }
    let n = p.running();
    assert_eq!(2, n, "expect 2 workers running, but got {}", p.running());
    // test pool tune scale up one
    p.tune(3);
    {
        let c = Arc::clone(&c);
        let _ = p.submit(task(move || c.recv()));
    }
    let n = p.running();
    assert_eq!(3, n, "expect 3 workers running, but got {n}");
    // test pool tune scale up multiple
    let wg = Arc::new(WaitGroup::new());
    let mut submitters = Vec::new();
    for _ in 0..5 {
        wg.add(1);
        let p = Arc::clone(&p);
        let c = Arc::clone(&c);
        let wg = Arc::clone(&wg);
        submitters.push(thread::spawn(move || {
            let _ = p.submit(task(move || c.recv()));
            wg.done();
        }));
    }
    p.tune(8);
    wg.wait();
    let n = p.running();
    assert_eq!(8, n, "expect 8 workers running, but got {n}");
    for _ in 0..8 {
        c.send();
    }
    for handle in submitters.drain(..) {
        let _ = handle.join();
    }
    p.release();

    // Test PoolWithFunc
    let signal = Arc::clone(&c);
    let pf =
        Arc::new(PoolWithFunc::new(2, pool_func(move |_: AnyArg| signal.recv()), &[]).unwrap());
    for _ in 0..2 {
        let _ = pf.invoke(arg(1i64));
    }
    let n = pf.running();
    assert_eq!(2, n, "expect 2 workers running, but got {n}");
    // test pool tune scale up one
    pf.tune(3);
    let _ = pf.invoke(arg(1i64));
    let n = pf.running();
    assert_eq!(3, n, "expect 3 workers running, but got {n}");
    // test pool tune scale up multiple
    for _ in 0..5 {
        wg.add(1);
        let pf = Arc::clone(&pf);
        let wg = Arc::clone(&wg);
        submitters.push(thread::spawn(move || {
            let _ = pf.invoke(arg(1i64));
            wg.done();
        }));
    }
    pf.tune(8);
    wg.wait();
    let n = pf.running();
    assert_eq!(8, n, "expect 8 workers running, but got {n}");
    for _ in 0..8 {
        c.send();
    }
    for handle in submitters.drain(..) {
        let _ = handle.join();
    }
    pf.release();

    // Test PoolWithFuncGeneric
    let signal = Arc::clone(&c);
    let pfg =
        Arc::new(PoolWithFuncGeneric::new(2, pool_func(move |_: i64| signal.recv()), &[]).unwrap());
    for _ in 0..2 {
        let _ = pfg.invoke(1i64);
    }
    let n = pfg.running();
    assert_eq!(2, n, "expect 2 workers running, but got {n}");
    // test pool tune scale up one
    pfg.tune(3);
    let _ = pfg.invoke(1i64);
    let n = pfg.running();
    assert_eq!(3, n, "expect 3 workers running, but got {n}");
    // test pool tune scale up multiple
    for _ in 0..5 {
        wg.add(1);
        let pfg = Arc::clone(&pfg);
        let wg = Arc::clone(&wg);
        submitters.push(thread::spawn(move || {
            let _ = pfg.invoke(1i64);
            wg.done();
        }));
    }
    pfg.tune(8);
    wg.wait();
    let n = pfg.running();
    assert_eq!(8, n, "expect 8 workers running, but got {n}");
    for _ in 0..8 {
        c.send();
    }
    for handle in submitters.drain(..) {
        let _ = handle.join();
    }
    c.close();
    pfg.release();
}

#[test]
fn test_release_timeout() {
    let _serial = common::serial();

    let p = Pool::new(10, &[]);
    assert_eq!(Ok(()), p.as_ref().map(|_| ()));
    let p = p.unwrap();
    for _ in 0..5 {
        let _ = p.submit(task(|| ants::sleep(Duration::SECOND)));
    }
    assert_ne!(0, p.running());
    let err = p.release_timeout(Duration::SECOND * 2);
    assert_eq!(Ok(()), err);

    let pf = PoolWithFunc::new(
        10,
        pool_func(|i: AnyArg| {
            let dur = *i.unwrap().downcast::<Duration>().unwrap();
            ants::sleep(dur);
        }),
        &[],
    );
    assert_eq!(Ok(()), pf.as_ref().map(|_| ()));
    let pf = pf.unwrap();
    for _ in 0..5 {
        let _ = pf.invoke(arg(Duration::SECOND));
    }
    assert_ne!(0, pf.running());
    let err = pf.release_timeout(Duration::SECOND * 2);
    assert_eq!(Ok(()), err);

    let pfg = PoolWithFuncGeneric::new(10, pool_func(|d: Duration| ants::sleep(d)), &[]);
    assert_eq!(Ok(()), pfg.as_ref().map(|_| ()));
    let pfg = pfg.unwrap();
    for _ in 0..5 {
        let _ = pfg.invoke(Duration::SECOND);
    }
    assert_ne!(0, pfg.running());
    let err = pfg.release_timeout(Duration::SECOND * 2);
    assert_eq!(Ok(()), err);
}

#[test]
fn test_default_pool_release_timeout() {
    let _serial = common::serial();

    ants::reboot(); // should do nothing inside
    for _ in 0..5 {
        let _ = ants::submit(task(|| ants::sleep(Duration::SECOND)));
    }
    assert_ne!(0, ants::running());
    let err = ants::release_timeout(Duration::SECOND * 2);
    assert_eq!(Ok(()), err);
}

#[test]
fn test_default_pool_release_context() {
    let _serial = common::serial();

    ants::reboot();
    for _ in 0..5 {
        let _ = ants::submit(task(|| ants::sleep(Duration::SECOND)));
    }
    assert_ne!(0, ants::running());
    let err = ants::release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
}

#[test]
fn test_release_context_with_nil() {
    let _serial = common::serial();

    let p = Pool::new(10, &[]);
    assert_eq!(Ok(()), p.as_ref().map(|_| ()));
    let p = p.unwrap();
    for _ in 0..5 {
        let _ = p.submit(task(|| ants::sleep(Duration::SECOND)));
    }
    assert_ne!(0, p.running());

    // Passing a nil context should release immediately without waiting for
    // workers to exit.
    let err = p.release_context(None);
    assert_eq!(Ok(()), err);
    assert!(p.is_closed());
}

#[test]
fn test_multi_pool() {
    let _serial = common::serial();

    let err = MultiPool::new(-1, 10, LoadBalancingStrategy(8), &[]).err();
    assert_eq!(Some(Error::InvalidMultiPoolSize), err);
    let err = MultiPool::new(10, -1, LoadBalancingStrategy(8), &[]).err();
    assert_eq!(Some(Error::InvalidLoadBalancingStrategy), err);
    let err = MultiPool::new(
        10,
        10,
        ROUND_ROBIN,
        &[with_expiry_duration(Duration::from_nanos(-1))],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);

    let test_fn = |mp: &MultiPool| {
        for _ in 0..50 {
            let err = mp.submit(task(long_running_func));
            assert_eq!(Ok(()), err);
        }
        assert_eq!(mp.waiting(), 0);
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(11));
        assert_eq!(50, mp.running());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(11));
        assert_eq!(0, mp.free());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(11));
        assert_eq!(50, mp.cap());
        assert!(!mp.is_closed());
        for i in 0..10 {
            let n = mp.waiting_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
            let n = mp.running_by_index(i).unwrap_or(-1);
            assert_eq!(5, n);
            let n = mp.free_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
        }
        STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
        assert_eq!(Ok(()), mp.release_timeout(Duration::SECOND * 3));
        assert_eq!(
            Err(Error::PoolClosed),
            mp.release_timeout(Duration::SECOND * 3)
        );
        assert_eq!(Err(Error::PoolClosed), mp.submit(None));
        assert_eq!(0, mp.running());
        assert!(mp.is_closed());
        STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);
    };

    let mp = MultiPool::new(10, 5, ROUND_ROBIN, &[]).unwrap();
    test_fn(&mp);

    mp.reboot();
    test_fn(&mp);

    let mp = MultiPool::new(10, 5, LEAST_TASKS, &[]).unwrap();
    test_fn(&mp);

    mp.reboot();
    test_fn(&mp);

    mp.tune(10);
}

#[test]
fn test_multi_pool_with_func() {
    let _serial = common::serial();

    let err = MultiPoolWithFunc::new(
        -1,
        10,
        pool_func(long_running_pool_func),
        LoadBalancingStrategy(8),
        &[],
    )
    .err();
    assert_eq!(Some(Error::InvalidMultiPoolSize), err);
    let err = MultiPoolWithFunc::new(
        10,
        -1,
        pool_func(long_running_pool_func),
        LoadBalancingStrategy(8),
        &[],
    )
    .err();
    assert_eq!(Some(Error::InvalidLoadBalancingStrategy), err);
    let err = MultiPoolWithFunc::new(
        10,
        10,
        pool_func(long_running_pool_func),
        ROUND_ROBIN,
        &[with_expiry_duration(Duration::from_nanos(-1))],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);

    let mut ch = Chan::new();
    let test_fn = |mp: &MultiPoolWithFunc, ch: &mut Arc<Chan>| {
        for _ in 0..50 {
            let err = mp.invoke(arg(Arc::clone(ch)));
            assert_eq!(Ok(()), err);
        }
        assert_eq!(mp.waiting(), 0);
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(11));
        assert_eq!(50, mp.running());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(11));
        assert_eq!(0, mp.free());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(11));
        assert_eq!(50, mp.cap());
        assert!(!mp.is_closed());
        for i in 0..10 {
            let n = mp.waiting_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
            let n = mp.running_by_index(i).unwrap_or(-1);
            assert_eq!(5, n);
            let n = mp.free_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
        }
        ch.close();
        assert_eq!(Ok(()), mp.release_timeout(Duration::SECOND * 3));
        assert_eq!(
            Err(Error::PoolClosed),
            mp.release_timeout(Duration::SECOND * 3)
        );
        assert_eq!(Err(Error::PoolClosed), mp.invoke(None));
        assert_eq!(0, mp.running());
        assert!(mp.is_closed());
        *ch = Chan::new();
    };

    let mp =
        MultiPoolWithFunc::new(10, 5, pool_func(long_running_pool_func), ROUND_ROBIN, &[]).unwrap();
    test_fn(&mp, &mut ch);

    mp.reboot();
    test_fn(&mp, &mut ch);

    let mp =
        MultiPoolWithFunc::new(10, 5, pool_func(long_running_pool_func), LEAST_TASKS, &[]).unwrap();
    test_fn(&mp, &mut ch);

    mp.reboot();
    test_fn(&mp, &mut ch);

    mp.tune(10);
}

#[test]
fn test_multi_pool_with_func_generic() {
    let _serial = common::serial();

    let err = MultiPoolWithFuncGeneric::new(
        -1,
        10,
        pool_func(long_running_pool_func_ch),
        LoadBalancingStrategy(8),
        &[],
    )
    .err();
    assert_eq!(Some(Error::InvalidMultiPoolSize), err);
    let err = MultiPoolWithFuncGeneric::new(
        10,
        -1,
        pool_func(long_running_pool_func_ch),
        LoadBalancingStrategy(8),
        &[],
    )
    .err();
    assert_eq!(Some(Error::InvalidLoadBalancingStrategy), err);
    let err = MultiPoolWithFuncGeneric::new(
        10,
        10,
        pool_func(long_running_pool_func_ch),
        ROUND_ROBIN,
        &[with_expiry_duration(Duration::from_nanos(-1))],
    )
    .err();
    assert_eq!(Some(Error::InvalidPoolExpiry), err);

    let mut ch = Chan::new();
    let test_fn = |mp: &MultiPoolWithFuncGeneric<Arc<Chan>>, ch: &mut Arc<Chan>| {
        for _ in 0..50 {
            let err = mp.invoke(Arc::clone(ch));
            assert_eq!(Ok(()), err);
        }
        assert_eq!(mp.waiting(), 0);
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.waiting_by_index(11));
        assert_eq!(50, mp.running());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.running_by_index(11));
        assert_eq!(0, mp.free());
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(-1));
        assert_eq!(Err(Error::InvalidPoolIndex), mp.free_by_index(11));
        assert_eq!(50, mp.cap());
        assert!(!mp.is_closed());
        for i in 0..10 {
            let n = mp.waiting_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
            let n = mp.running_by_index(i).unwrap_or(-1);
            assert_eq!(5, n);
            let n = mp.free_by_index(i).unwrap_or(-1);
            assert_eq!(0, n);
        }
        ch.close();
        assert_eq!(Ok(()), mp.release_timeout(Duration::SECOND * 3));
        assert_eq!(
            Err(Error::PoolClosed),
            mp.release_timeout(Duration::SECOND * 3)
        );
        assert_eq!(Err(Error::PoolClosed), mp.invoke(Chan::nil()));
        assert_eq!(0, mp.running());
        assert!(mp.is_closed());
        *ch = Chan::new();
    };

    let mp = MultiPoolWithFuncGeneric::new(
        10,
        5,
        pool_func(long_running_pool_func_ch),
        ROUND_ROBIN,
        &[],
    )
    .unwrap();
    test_fn(&mp, &mut ch);

    mp.reboot();
    test_fn(&mp, &mut ch);

    let mp = MultiPoolWithFuncGeneric::new(
        10,
        5,
        pool_func(long_running_pool_func_ch),
        LEAST_TASKS,
        &[],
    )
    .unwrap();
    test_fn(&mp, &mut ch);

    mp.reboot();
    test_fn(&mp, &mut ch);

    mp.tune(10);
}

#[test]
fn test_multi_pool_release_context() {
    let _serial = common::serial();

    let mp = MultiPool::new(10, 5, ROUND_ROBIN, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();

    for _ in 0..50 {
        let err = mp.submit(task(long_running_func));
        assert_eq!(Ok(()), err);
    }
    assert_eq!(50, mp.running());

    // Signal workers to stop, then release with a background context.
    STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());
    STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);

    // Calling release_context on a closed pool should return PoolClosed.
    assert_eq!(
        Err(Error::PoolClosed),
        mp.release_context(Some(&Context::background()))
    );

    // Test with the LeastTasks strategy.
    let mp = MultiPool::new(10, 5, LEAST_TASKS, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.submit(task(long_running_func));
        assert_eq!(Ok(()), err);
    }
    assert_eq!(50, mp.running());

    STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());
    STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);

    // Test that a cancelled context returns an error.
    let mp = MultiPool::new(10, 5, ROUND_ROBIN, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.submit(task(long_running_func));
        assert_eq!(Ok(()), err);
    }
    let (ctx, cancel) = Context::with_cancel();
    cancel.cancel(); // cancel immediately
    let err = mp.release_context(Some(&ctx));
    assert!(err.is_err());
    STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
    assert!(eventually(
        || mp.running() == 0,
        Duration::SECOND * 3,
        Duration::MILLISECOND * 100
    ));
    STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);

    // Test reboot after release_context.
    let mp = MultiPool::new(10, 5, ROUND_ROBIN, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.submit(task(long_running_func));
        assert_eq!(Ok(()), err);
    }
    STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);

    mp.reboot();
    assert!(!mp.is_closed());
    for _ in 0..50 {
        let err = mp.submit(task(long_running_func));
        assert_eq!(Ok(()), err);
    }
    assert_eq!(50, mp.running());
    STOP_LONG_RUNNING_FUNC.store(1, Ordering::SeqCst);
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    STOP_LONG_RUNNING_FUNC.store(0, Ordering::SeqCst);
}

#[test]
fn test_multi_pool_with_func_release_context() {
    let _serial = common::serial();

    let ch = Chan::new();
    let mp = MultiPoolWithFunc::new(10, 5, pool_func(long_running_pool_func), ROUND_ROBIN, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();

    for _ in 0..50 {
        let err = mp.invoke(arg(Arc::clone(&ch)));
        assert_eq!(Ok(()), err);
    }
    assert_eq!(50, mp.running());

    ch.close();
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());

    // Calling release_context on a closed pool should return PoolClosed.
    assert_eq!(
        Err(Error::PoolClosed),
        mp.release_context(Some(&Context::background()))
    );

    // Test with the LeastTasks strategy.
    let ch = Chan::new();
    let mp = MultiPoolWithFunc::new(10, 5, pool_func(long_running_pool_func), LEAST_TASKS, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.invoke(arg(Arc::clone(&ch)));
        assert_eq!(Ok(()), err);
    }
    ch.close();
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());

    // Test that a cancelled context returns an error.
    let ch = Chan::new();
    let mp = MultiPoolWithFunc::new(10, 5, pool_func(long_running_pool_func), ROUND_ROBIN, &[]);
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.invoke(arg(Arc::clone(&ch)));
        assert_eq!(Ok(()), err);
    }
    let (ctx, cancel) = Context::with_cancel();
    cancel.cancel();
    let err = mp.release_context(Some(&ctx));
    assert!(err.is_err());
    ch.close();
    assert!(eventually(
        || mp.running() == 0,
        Duration::SECOND * 3,
        Duration::MILLISECOND * 100
    ));
}

#[test]
fn test_multi_pool_with_func_generic_release_context() {
    let _serial = common::serial();

    let ch = Chan::new();
    let mp = MultiPoolWithFuncGeneric::new(
        10,
        5,
        pool_func(long_running_pool_func_ch),
        ROUND_ROBIN,
        &[],
    );
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();

    for _ in 0..50 {
        let err = mp.invoke(Arc::clone(&ch));
        assert_eq!(Ok(()), err);
    }
    assert_eq!(50, mp.running());

    ch.close();
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());

    // Calling release_context on a closed pool should return PoolClosed.
    assert_eq!(
        Err(Error::PoolClosed),
        mp.release_context(Some(&Context::background()))
    );

    // Test with the LeastTasks strategy.
    let ch = Chan::new();
    let mp = MultiPoolWithFuncGeneric::new(
        10,
        5,
        pool_func(long_running_pool_func_ch),
        LEAST_TASKS,
        &[],
    );
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.invoke(Arc::clone(&ch));
        assert_eq!(Ok(()), err);
    }
    ch.close();
    let err = mp.release_context(Some(&Context::background()));
    assert_eq!(Ok(()), err);
    assert_eq!(0, mp.running());
    assert!(mp.is_closed());

    // Test that a cancelled context returns an error.
    let ch = Chan::new();
    let mp = MultiPoolWithFuncGeneric::new(
        10,
        5,
        pool_func(long_running_pool_func_ch),
        ROUND_ROBIN,
        &[],
    );
    assert_eq!(Ok(()), mp.as_ref().map(|_| ()));
    let mp = mp.unwrap();
    for _ in 0..50 {
        let err = mp.invoke(Arc::clone(&ch));
        assert_eq!(Ok(()), err);
    }
    let (ctx, cancel) = Context::with_cancel();
    cancel.cancel();
    let err = mp.release_context(Some(&ctx));
    assert!(err.is_err());
    ch.close();
    assert!(eventually(
        || mp.running() == 0,
        Duration::SECOND * 3,
        Duration::MILLISECOND * 100
    ));
}

#[test]
fn test_reboot_new_pool_calc() {
    let _serial = common::serial();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let pool = Pool::new(10, &[]);
    assert_eq!(Ok(()), pool.as_ref().map(|_| ()));
    let pool = pool.unwrap();
    // Use the default pool.
    for i in 0..run_times {
        let j = i;
        let _ = pool.submit(task(move || common::inc_sum_int(j)));
    }
    WG.wait();
    assert_eq!(
        499_500,
        SUM.load(Ordering::SeqCst),
        "The result should be 499500"
    );

    SUM.store(0, Ordering::SeqCst);
    WG.add(i64::from(run_times));
    // use both release and release_timeout and you will run into a panic
    let err = pool.release_timeout(Duration::SECOND);
    assert_eq!(Ok(()), err);
    pool.reboot();

    for i in 0..run_times {
        let j = i;
        let _ = pool.submit(task(move || common::inc_sum_int(j)));
    }
    WG.wait();
    assert_eq!(
        499_500,
        SUM.load(Ordering::SeqCst),
        "The result should be 499500"
    );

    pool.release();
}

#[test]
fn test_reboot_new_pool_with_pre_alloc_calc() {
    let _serial = common::serial();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let pool = Pool::new(10, &[with_pre_alloc(true)]);
    assert_eq!(Ok(()), pool.as_ref().map(|_| ()));
    let pool = pool.unwrap();
    // Use the default pool.
    for i in 0..run_times {
        let j = i;
        let _ = pool.submit(task(move || common::inc_sum_int(j)));
    }
    WG.wait();
    assert_eq!(
        499_500,
        SUM.load(Ordering::SeqCst),
        "The result should be 499500"
    );

    SUM.store(0, Ordering::SeqCst);
    let err = pool.release_timeout(Duration::SECOND);
    assert_eq!(Ok(()), err);
    pool.reboot();

    WG.add(i64::from(run_times));
    for i in 0..run_times {
        let j = i;
        let _ = pool.submit(task(move || common::inc_sum_int(j)));
    }
    WG.wait();
    assert_eq!(
        499_500,
        SUM.load(Ordering::SeqCst),
        "The result should be 499500"
    );

    pool.release();
}
