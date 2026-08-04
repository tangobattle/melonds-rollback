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
//! What a scope would have permitted that a channel cannot is borrowing:
//! a job handed to a long-lived thread must be `'static`. So jobs here
//! do not borrow — each one owns what it works on, a console and its
//! buffers, and hands all of it back through its result. The caller
//! still blocks until both halves are done, and the link's determinism
//! argument (frame delivery gates on emulated time alone, never on
//! which thread ran first) is untouched.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

/// What comes back over a worker's channel: the job's boxed result, or
/// the panic that took its place. [`Pool::run`] put the real type in,
/// so it is the one place that can take it back out.
type Payload = Box<dyn Any + Send>;
type Job = Box<dyn FnOnce() -> Payload + Send>;
type Outcome = std::thread::Result<Payload>;

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

    /// Run both jobs at once and return their results when both have
    /// finished.
    ///
    /// A panic in either is carried back and resumed on the caller,
    /// after the other half has been waited for — the same order
    /// `thread::scope` reports one in. Whatever the panicking job
    /// owned went down with it.
    pub(crate) fn run<R, F>(&mut self, jobs: [F; 2]) -> [R; 2]
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let [a, b] = jobs;
        self.workers[0].dispatch(Box::new(move || Box::new(a()) as Payload));
        self.workers[1].dispatch(Box::new(move || Box::new(b()) as Payload));
        let outcomes = [self.workers[0].wait(), self.workers[1].wait()];
        outcomes.map(|outcome| match outcome {
            Ok(result) => *result
                .downcast()
                .unwrap_or_else(|_| unreachable!("a result is what its job boxed")),
            Err(payload) => std::panic::resume_unwind(payload),
        })
    }
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
