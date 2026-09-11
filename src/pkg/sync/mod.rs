// Copyright 2019 Andy Pan & Dietoad. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//! Module sync provides some handy implementations for synchronization access.
//! At the moment, there is only an implementation of spin-lock.

mod spinlock;

pub use spinlock::{new_spin_lock, SpinLock, SpinLockGuard};
