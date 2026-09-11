/*
 * Copyright (c) 2025. Andy Pan. All rights reserved.
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
//! The crate's runnable examples, each asserting the output it declares.

mod common;

use std::sync::atomic::Ordering;

use ants::{
    pool_func, task, MultiPool, MultiPoolWithFunc, MultiPoolWithFuncGeneric, Pool, PoolWithFunc,
    PoolWithFuncGeneric, ROUND_ROBIN,
};
use ants::{AnyArg, Duration};

use common::{inc_sum, inc_sum_int, ExampleOutput, SUM, WG};

#[test]
fn example_pool() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    ants::reboot(); // ensure the default pool is available

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));
    // Use the default pool.
    for i in 0..run_times {
        let j = i;
        let _ = ants::submit(task(move || inc_sum_int(j)));
    }
    WG.wait();
    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));

    SUM.store(0, Ordering::SeqCst);
    WG.add(i64::from(run_times));
    // Use the new pool.
    let pool = Pool::new(10, &[]).unwrap();
    for i in 0..run_times {
        let j = i;
        let _ = pool.submit(task(move || inc_sum_int(j)));
    }
    WG.wait();
    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    pool.release();

    // Output:
    // The result is 499500
    // The result is 499500
    assert_eq!(
        vec!["The result is 499500", "The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}

#[test]
fn example_pool_with_func() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let pool = PoolWithFunc::new(10, pool_func(inc_sum), &[]).unwrap();

    for i in 0..run_times {
        let _ = pool.invoke(ants::arg(i));
    }
    WG.wait();

    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    pool.release();

    // Output: The result is 499500
    assert_eq!(
        vec!["The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}

#[test]
fn example_pool_with_func_generic() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let pool = PoolWithFuncGeneric::new(10, pool_func(inc_sum_int), &[]).unwrap();

    for i in 0..run_times {
        let _ = pool.invoke(i);
    }
    WG.wait();

    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    pool.release();

    // Output: The result is 499500
    assert_eq!(
        vec!["The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}

#[test]
fn example_multi_pool() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let mp = MultiPool::new(10, run_times / 10, ROUND_ROBIN, &[]).unwrap();

    for i in 0..run_times {
        let j = i;
        let _ = mp.submit(task(move || inc_sum_int(j)));
    }
    WG.wait();

    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    let _ = mp.release_timeout(Duration::SECOND);

    // Output: The result is 499500
    assert_eq!(
        vec!["The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}

#[test]
fn example_multi_pool_with_func() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let mp =
        MultiPoolWithFunc::new(10, run_times / 10, pool_func(inc_sum), ROUND_ROBIN, &[]).unwrap();

    for i in 0..run_times {
        let _: Result<(), _> = mp.invoke(ants::arg(i) as AnyArg);
    }
    WG.wait();

    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    let _ = mp.release_timeout(Duration::SECOND);

    // Output: The result is 499500
    assert_eq!(
        vec!["The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}

#[test]
fn example_multi_pool_with_func_generic() {
    let _serial = common::serial();
    let mut output = ExampleOutput::new();

    SUM.store(0, Ordering::SeqCst);
    let run_times = 1000;
    WG.add(i64::from(run_times));

    let mp =
        MultiPoolWithFuncGeneric::new(10, run_times / 10, pool_func(inc_sum_int), ROUND_ROBIN, &[])
            .unwrap();

    for i in 0..run_times {
        let _ = mp.invoke(i);
    }
    WG.wait();

    output.printf(format!("The result is {}", SUM.load(Ordering::SeqCst)));
    let _ = mp.release_timeout(Duration::SECOND);

    // Output: The result is 499500
    assert_eq!(
        vec!["The result is 499500"],
        output.lines(),
        "example output mismatch"
    );
}
