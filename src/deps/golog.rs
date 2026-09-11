//! The line format of Go's `log.Logger`.

use chrono::{Datelike, Local, Timelike, Utc};

/// Prefix each line with the date, `2009/01/23`.
pub const LDATE: u32 = 1;
/// Prefix each line with the time, `01:23:23`.
pub const LTIME: u32 = 1 << 1;
/// Give the time microsecond resolution, `01:23:23.123123`. Assumes [`LTIME`].
pub const LMICROSECONDS: u32 = 1 << 2;
/// Report the time in UTC rather than in the local time zone.
pub const LUTC: u32 = 1 << 5;
/// Move the prefix from the start of the line to just before the message.
pub const LMSGPREFIX: u32 = 1 << 6;
/// The initial values for the standard logger: [`LDATE`] and [`LTIME`].
pub const LSTD_FLAGS: u32 = LDATE | LTIME;

/// Where a [`StdLogger`] writes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Target {
    /// The process's standard output.
    Stdout,
    /// The process's standard error.
    Stderr,
}

/// A logger that renders each line the way Go's `log.Logger` does.
///
/// The header is assembled in Go's order — prefix (unless [`LMSGPREFIX`]),
/// date, time, then prefix again when [`LMSGPREFIX`] is set — and a trailing
/// newline is appended only when the message does not already end in one.
#[derive(Debug)]
pub struct StdLogger {
    target: Target,
    prefix: String,
    flags: u32,
}

impl StdLogger {
    /// Creates a logger writing to `target`, tagging each line with `prefix`
    /// and rendering the header selected by `flags`.
    #[must_use]
    pub fn new(target: Target, prefix: &str, flags: u32) -> StdLogger {
        StdLogger {
            target,
            prefix: prefix.to_owned(),
            flags,
        }
    }

    /// Renders `message` into a complete log line, header and newline included.
    #[must_use]
    pub fn format(&self, message: &str) -> String {
        let mut line = String::new();
        if self.flags & LMSGPREFIX == 0 {
            line.push_str(&self.prefix);
        }
        if self.flags & (LDATE | LTIME | LMICROSECONDS) != 0 {
            self.write_timestamp(&mut line);
        }
        if self.flags & LMSGPREFIX != 0 {
            line.push_str(&self.prefix);
        }
        line.push_str(message);
        if !line.ends_with('\n') {
            line.push('\n');
        }
        line
    }

    fn write_timestamp(&self, line: &mut String) {
        let (year, month, day, hour, minute, second, micros) = if self.flags & LUTC != 0 {
            let now = Utc::now();
            fields(now.year(), &now)
        } else {
            let now = Local::now();
            fields(now.year(), &now)
        };
        if self.flags & LDATE != 0 {
            line.push_str(&format!("{year:04}/{month:02}/{day:02} "));
        }
        if self.flags & (LTIME | LMICROSECONDS) != 0 {
            line.push_str(&format!("{hour:02}:{minute:02}:{second:02}"));
            if self.flags & LMICROSECONDS != 0 {
                line.push_str(&format!(".{micros:06}"));
            }
            line.push(' ');
        }
    }
}

impl crate::Logger for StdLogger {
    fn printf(&self, message: &str) {
        let line = self.format(message);
        // Written through the print macros rather than a `std::io::stderr()`
        // handle. Both reach the same file descriptor, but the macros honour
        // the process-wide output capture that a test harness installs and that
        // `thread::spawn` propagates into the worker threads. A raw handle
        // escapes it, and a worker reporting a panic would then interleave with
        // whatever the harness was in the middle of writing.
        match self.target {
            Target::Stdout => print!("{line}"),
            Target::Stderr => eprint!("{line}"),
        }
    }
}

fn fields<T: Timelike + Datelike>(year: i32, now: &T) -> (i32, u32, u32, u32, u32, u32, u32) {
    (
        year,
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.nanosecond() / 1_000,
    )
}
