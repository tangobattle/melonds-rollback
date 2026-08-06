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
#[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
    fn new() -> Worker {
        let (jobs, rx) = channel::<Job>();
        let (tx, done) = channel::<Outcome>();
        let handle = std::thread::Builder::new()
            // What `thread::scope` would have given these jobs, kept
            // explicit: a console frame runs in it comfortably, and the
            // deep-stack construction path never runs here.
            .stack_size(2 << 20)
            .name("melonds-link".into())
            .spawn(worker_loop(rx, tx))
            .expect("failed to spawn a link worker");
        Worker {
            jobs,
            done,
            handle: Some(handle),
        }
    }

    /// On wasm a worker comes out of the warm [`stock`] rather than
    /// being spawned here: a fresh Web Worker only finishes starting up
    /// once the browser's main thread has had a few event-loop turns,
    /// and the caller of [`Pool::run`] is about to spin that thread
    /// without yielding. Spawning at claim time would deadlock the
    /// first wait; the stock spawns while the app is still awaiting
    /// other things (see [`crate::warm_workers`]).
    #[cfg(target_arch = "wasm32")]
    fn new() -> Worker {
        let lease = stock::claim();
        Worker {
            jobs: lease.jobs,
            done: lease.done,
        }
    }

    fn dispatch(&self, job: Job) {
        // A dead worker is a worker that panicked out of its loop; the
        // wait below turns that into the panic it was.
        let _ = self.jobs.send(job);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn wait(&self) -> Outcome {
        self.done
            .recv()
            .unwrap_or_else(|_| Err(Box::new("a link worker died")))
    }

    /// The browser's main thread is forbidden from blocking — a futex
    /// wait traps the whole module — so waiting there is a spin over
    /// `try_recv`. Not as wasteful as it reads: the caller's next act
    /// is to use both results, the wait is the length of one console's
    /// half of a frame, and the main thread would have spent the same
    /// time computing exactly that inline on a console without a pair.
    #[cfg(target_arch = "wasm32")]
    fn wait(&self) -> Outcome {
        loop {
            match self.done.try_recv() {
                Ok(outcome) => return outcome,
                Err(std::sync::mpsc::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(Box::new("a link worker died"));
                }
            }
        }
    }
}

/// One worker's whole life: take a job, run it, hand the result back.
fn worker_loop(rx: Receiver<Job>, tx: Sender<Outcome>) -> impl FnOnce() + Send + 'static {
    move || {
        while let Ok(job) = rx.recv() {
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(job));
            if tx.send(outcome).is_err() {
                break;
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
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

/// On wasm the workers outlive the pool: they go back to the stock,
/// warm, for the next link — a Web Worker's startup is the expensive,
/// main-thread-entangled part (see [`Worker::new`]), and a parked one
/// costs a few MB of stack.
#[cfg(target_arch = "wasm32")]
impl Drop for Pool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            let (dead_jobs, _) = channel();
            let (_, dead_done) = channel();
            stock::put_back(stock::Lease::running(
                std::mem::replace(&mut worker.jobs, dead_jobs),
                std::mem::replace(&mut worker.done, dead_done),
            ));
        }
    }
}

/// The warm-worker stock behind the wasm [`Pool`]. See [`Worker::new`]
/// for why it exists at all.
#[cfg(target_arch = "wasm32")]
pub(crate) mod stock {
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Mutex;

    use super::{worker_loop, Job, Outcome};

    /// A parked worker: the sending half of its job queue and the
    /// receiving half of its results. Claiming one is moving these; the
    /// Web Worker itself never moves.
    pub(crate) struct Lease {
        pub(crate) jobs: Sender<Job>,
        pub(crate) done: Receiver<Outcome>,
        /// Set by the worker itself as its loop comes up — the line
        /// between "spawned" and "actually able to take a job", which
        /// is what [`ready`] reports to hosts.
        started: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl Lease {
        /// A lease for a worker known to be up — what a pool hands back
        /// when its link ends.
        pub(crate) fn running(jobs: Sender<Job>, done: Receiver<Outcome>) -> Lease {
            Lease {
                jobs,
                done,
                started: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }
    }

    /// Parked workers. Only ever touched from the thread that runs
    /// links (the browser main thread), so the lock is never contended
    /// — which matters, because contending on the main thread traps.
    static STOCK: Mutex<Vec<Lease>> = Mutex::new(Vec::new());

    /// Where the wasm-bindgen glue lives, for the worker bootstrap.
    /// `None` until resolved or told.
    static SHIM_URL: Mutex<Option<String>> = Mutex::new(None);

    /// Top the stock up to at least `count` parked (or still starting)
    /// workers. Call this from an async context that will yield to the
    /// browser afterwards — a spawned Web Worker only finishes starting
    /// once the main thread has had event-loop turns, which is the
    /// whole reason warming is separate from claiming.
    pub(crate) fn warm(count: usize) {
        let mut stock = STOCK.lock().unwrap();
        while stock.len() < count {
            stock.push(spawn_lease());
        }
    }

    pub(crate) fn claim() -> Lease {
        // An empty stock is a host that never warmed up; spawning here
        // still works if the caller yields before the first wait, and
        // deadlocks if it doesn't — which is at least the same failure
        // the missing warm-up already was.
        STOCK.lock().unwrap().pop().unwrap_or_else(spawn_lease)
    }

    pub(crate) fn put_back(lease: Lease) {
        STOCK.lock().unwrap().push(lease);
    }

    /// Whether `count` workers are parked here AND have actually come
    /// up. A host gates its (spinning, non-yielding) boot on this from
    /// a context that still yields — the readiness only ever flips
    /// while the browser's main thread is free to run worker startups.
    pub(crate) fn ready(count: usize) -> bool {
        STOCK
            .lock()
            .unwrap()
            .iter()
            .filter(|lease| lease.started.load(std::sync::atomic::Ordering::Acquire))
            .count()
            >= count
    }

    /// Point the worker bootstrap at the wasm-bindgen glue explicitly,
    /// for a host whose layout the stack-trace resolution below can't
    /// read.
    pub(crate) fn set_shim_url(url: String) {
        *SHIM_URL.lock().unwrap() = Some(url);
    }

    fn spawn_lease() -> Lease {
        let (jobs, rx) = channel::<Job>();
        let (tx, done) = channel::<Outcome>();
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = started.clone();
        let mut builder = wasm_thread::Builder::new()
            .stack_size(2 << 20)
            .name("melonds-link".into());
        if let Some(url) = shim_url() {
            builder = builder.wasm_bindgen_shim_url(url);
        }
        let body = worker_loop(rx, tx);
        builder
            .spawn(move || {
                flag.store(true, std::sync::atomic::Ordering::Release);
                body();
            })
            .expect("failed to spawn a link worker");
        Lease { jobs, done, started }
    }

    /// The wasm-bindgen glue's URL, off the current JS stack: the
    /// innermost frame that is an actual script (not wasm) is the glue
    /// shim that called into wasm. wasm_thread has the same idea
    /// built in, but its parser takes the first frame whatever it is —
    /// under Chrome that is a `wasm://` URL, and the worker dies on the
    /// import, silently.
    fn shim_url() -> Option<String> {
        if let Some(url) = SHIM_URL.lock().unwrap().clone() {
            return Some(url);
        }
        let url = js_sys::eval(
            r#"(() => {
                try { throw new Error(); } catch (e) {
                    for (const m of String(e.stack).matchAll(/(?:\(|@)(\S+?):\d+:\d+/g)) {
                        if (!m[1].startsWith('wasm://')) { return m[1]; }
                    }
                    return null;
                }
            })()"#,
        )
        .ok()?
        .as_string()?;
        *SHIM_URL.lock().unwrap() = Some(url.clone());
        Some(url)
    }
}
