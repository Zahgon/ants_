//! Go's `time.Duration`: a signed count of nanoseconds.

use std::ops::{Add, Div, Mul};
use std::time::{SystemTime, UNIX_EPOCH};

/// A duration measured in nanoseconds, signed exactly as Go's `time.Duration`
/// is.
///
/// The sign is not decoration: `ants` accepts a negative expiry and rejects it
/// with [`crate::Error::InvalidPoolExpiry`], which an unsigned
/// [`std::time::Duration`] could never represent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Duration(i64);

impl Duration {
    /// The zero duration.
    pub const ZERO: Duration = Duration(0);
    /// One nanosecond.
    pub const NANOSECOND: Duration = Duration(1);
    /// One microsecond, i.e. 1000 nanoseconds.
    pub const MICROSECOND: Duration = Duration(1_000);
    /// One millisecond, i.e. 1000 microseconds.
    pub const MILLISECOND: Duration = Duration(1_000_000);
    /// One second, i.e. 1000 milliseconds.
    pub const SECOND: Duration = Duration(1_000_000_000);

    /// Builds a duration from a raw, possibly negative, nanosecond count.
    #[must_use]
    pub const fn from_nanos(nanos: i64) -> Duration {
        Duration(nanos)
    }

    /// Builds a duration from a whole number of milliseconds.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Duration {
        Duration(millis * 1_000_000)
    }

    /// Builds a duration from a whole number of seconds.
    #[must_use]
    pub const fn from_secs(secs: i64) -> Duration {
        Duration(secs * 1_000_000_000)
    }

    /// The duration as a raw nanosecond count.
    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// The duration as a [`std::time::Duration`], clamping a negative value to
    /// zero the way every Go API that takes a `time.Duration` treats one.
    #[must_use]
    pub const fn to_std(self) -> std::time::Duration {
        if self.0 <= 0 {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_nanos(self.0 as u64)
        }
    }
}

impl Add for Duration {
    type Output = Duration;
    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0 + rhs.0)
    }
}

impl Mul<i64> for Duration {
    type Output = Duration;
    fn mul(self, rhs: i64) -> Duration {
        Duration(self.0 * rhs)
    }
}

impl Div<i64> for Duration {
    type Output = Duration;
    fn div(self, rhs: i64) -> Duration {
        Duration(self.0 / rhs)
    }
}

/// The current wall-clock time in nanoseconds since the Unix epoch, i.e. Go's
/// `time.Now().UnixNano()`.
#[must_use]
pub fn now_unix_nano() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(delta) => delta.as_nanos() as i64,
        Err(err) => -(err.duration().as_nanos() as i64),
    }
}

/// Blocks the calling thread for `duration`, i.e. Go's `time.Sleep`.
pub fn sleep(duration: Duration) {
    std::thread::sleep(duration.to_std());
}
