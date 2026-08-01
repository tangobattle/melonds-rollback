//! Two long-lived threads for the two consoles.
//!
//! Everything a link does to both consoles at once — advance them a
//! frame, serialize them, load them back — has to happen concurrently:
//! the pair blocks on itself through the air, and at ~6 MB a console the
//! state work is memory-bound and overlaps well. The obvious way to say
//! that is [`std::thread::scope`], which is what this replaced.
//!
//! The problem with saying it that way is that a scope creates and joins
//! its threads every time it is entered, and a rollback tick enters one
//! three times — advance, capture, restore — hundreds of times a second.
//! An OS thread costs tens of microseconds to make and unmake, which is
//! a tenth of a capture. These threads are made once per link and parked
//! on a channel between jobs, so the same tick pays two wakeups instead.
//!
//! Nothing about the concurrency changes: the caller still blocks until
//! both halves are done, so a job may borrow from the caller's frame
//! exactly as a scoped one could, and the link's determinism argument
//! (frame delivery gates on emulated time alone, never on which thread
//! ran first) is untouched.

use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

type Job = Box<dyn FnOnce() + Send + 'static>;
type Outcome = std::thread::Result<()>;

/// A pair of parked worker threads, one per seat.
pub(crate) struct Pool {
    workers: [Worker; 2],
}

struct Worker {
    jobs: Sender<Job>,
    done: Receiver<Outcome>,
    handle: Option<JoinHandle<()>>,
}

impl Pool {
    pub(crate) fn new() -> Pool {
        Pool {
            workers: [Worker::new(), Worker::new()],
        }
    }

    /// Run both closures at once and return when both have finished.
    ///
    /// A panic in either is carried back and resumed on the caller,
    /// after the other half has been waited for — the same order
    /// `thread::scope` reports one in.
    pub(crate) fn run<'a>(&mut self, jobs: [Box<dyn FnOnce() + Send + 'a>; 2]) {
        let [a, b] = jobs;
        // SAFETY: both sends are matched by a receive below before this
        // function returns, so neither job outlives the borrows it
        // captured — the invariant `thread::scope` enforces statically.
        // A worker whose channel is closed has already died, and its
        // `done` receive then reports that rather than blocking.
        unsafe {
            self.workers[0].dispatch(erase(a));
            self.workers[1].dispatch(erase(b));
        }
        let first = self.workers[0].wait();
        let second = self.workers[1].wait();
        for outcome in [first, second] {
            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

/// Forget a job's lifetime so it can cross a channel. Sound only
/// because [`Pool::run`] joins before it returns.
unsafe fn erase<'a>(job: Box<dyn FnOnce() + Send + 'a>) -> Job {
    std::mem::transmute::<Box<dyn FnOnce() + Send + 'a>, Job>(job)
}

impl Worker {
    fn new() -> Worker {
        let (jobs, rx) = channel::<Job>();
        let (tx, done) = channel::<Outcome>();
        let handle = std::thread::Builder::new()
            // What `thread::scope` would have given these jobs, kept
            // explicit: a console frame runs in it comfortably, and the
            // deep-stack construction path never runs here.
            .stack_size(2 << 20)
            .name("melonds-link".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    let outcome = std::panic::catch_unwind(AssertUnwindSafe(job));
                    if tx.send(outcome).is_err() {
                        break;
                    }
                }
            })
            .expect("failed to spawn a link worker");
        Worker {
            jobs,
            done,
            handle: Some(handle),
        }
    }

    fn dispatch(&self, job: Job) {
        // A dead worker is a worker that panicked out of its loop; the
        // wait below turns that into the panic it was.
        let _ = self.jobs.send(job);
    }

    fn wait(&self) -> Outcome {
        self.done
            .recv()
            .unwrap_or_else(|_| Err(Box::new("a link worker died")))
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            // Closing the job channel is what ends the worker's loop.
            let (dead, _) = channel();
            drop(std::mem::replace(&mut worker.jobs, dead));
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}
