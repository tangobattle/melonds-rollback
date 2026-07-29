//! The rollback session over a [`Link`]: the DS analogue of
//! `mgba_rollback::session`.
//!
//! Both peers run the *same* link — two consoles talking over emulated
//! wireless — and the only true inputs are the two players' pads. Every
//! frame the local peer feeds its own input in, predicts the remote's,
//! and simulates ahead; when a prediction turns out wrong the session
//! restores the last settled snapshot and re-simulates. Because a link
//! is a pure function of its inputs, both peers reach identical state
//! from identical confirmed input, which is what makes the prediction
//! safe to be wrong.

use std::sync::{Arc, Mutex};

use crate::{Input, Link, Snapshot};

/// What the caller must forward to the peer after an
/// [`advance`](Session::advance).
#[derive(Clone, Copy, Debug)]
pub struct Outgoing {
    /// The tick this input belongs to.
    pub tick: u32,
    /// The local player's input for that tick.
    pub input: Input,
    /// How far ahead of the peer this side is running, for the
    /// clock-sync governor on the other end.
    pub tick_advantage: i16,
}

/// What an [`advance`](Session::advance) did.
#[derive(Clone, Copy, Debug, Default)]
pub struct Report {
    /// Ticks simulated (settles plus speculation).
    pub ticks: u32,
    /// Depth of the rollback this advance performed, 0 if none.
    pub rollback_depth: u32,
}

/// Called once per simulated tick, for hosts collecting telemetry off
/// the running link. Fires on speculative ticks too — a tick can be
/// re-simulated after a rollback, so observers must tolerate repeats.
pub trait TickObserver: Send {
    fn on_tick(&mut self, link: &mut Link, tick: u32);
}

/// The link plus the bookkeeping the [`getgud::World`] callbacks write.
struct Shared {
    link: Link,
    /// Ticks simulated on the live link, so a snapshot can record where
    /// it was taken and `load` can skip a redundant restore.
    live_tick: u32,
    observer: Option<Box<dyn TickObserver>>,
    /// Which consoles anybody displays — the local seat. The other
    /// console never composits a framebuffer.
    visible: [bool; 2],
    /// First tick of the current advance whose output could reach the
    /// screen. Ticks below this are rollback re-simulation: nobody will
    /// ever see their frames, so nothing renders them.
    render_from: u32,
}

/// A snapshot tagged with the tick it was taken at.
struct SnapshotAt {
    snap: Snapshot,
    tick: u32,
    /// The link's audio mark at save time, so a rollback to this
    /// snapshot knows exactly how much its speculation appended — the
    /// amount `load` has to take back. Audio is not machine state and a
    /// savestate does not carry it, so it is tracked alongside.
    audio_produced: [u64; 2],
}

/// Cross-thread readout handle to a running session's link — for a
/// host pulling video or audio off the local console while the session
/// thread simulates.
#[derive(Clone)]
pub struct LinkHandle(Arc<Mutex<Shared>>);

impl LinkHandle {
    /// Run `f` against the live link. Blocks the simulation for its
    /// duration, so keep it short.
    pub fn with_link<R>(&self, f: impl FnOnce(&mut Link) -> R) -> R {
        f(&mut self.0.lock().unwrap().link)
    }
}

/// The [`getgud::World`] over a [`Link`]: `step` is one lockstep frame
/// of the pair, `save`/`load` are whole-link snapshots (both consoles
/// plus the frames in flight on the air), and prediction repeats the
/// peer's last input.
struct LinkWorld {
    shared: Arc<Mutex<Shared>>,
    local_player: usize,
    /// Snapshots the engine has finished with, kept for their
    /// allocations. A DS state is ~6 MB per console and the engine
    /// retires one nearly every tick, so recycling turns a steady
    /// stream of multi-megabyte allocations into buffer reuse.
    pool: Vec<Snapshot>,
}

impl getgud::World for LinkWorld {
    type Input = Input;
    type State = SnapshotAt;
    type Error = melonds::Error;

    fn step(&mut self, local: &Input, remotes: &[Input]) -> Result<(), melonds::Error> {
        let mut inputs = [Input::default(); 2];
        inputs[self.local_player] = *local;
        inputs[1 - self.local_player] = remotes[0];

        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        let render = shared.live_tick + 1 >= shared.render_from;
        shared
            .link
            .set_render([render && shared.visible[0], render && shared.visible[1]]);
        shared.link.tick(inputs);
        shared.live_tick += 1;
        if let Some(observer) = shared.observer.as_mut() {
            observer.on_tick(&mut shared.link, shared.live_tick);
        }
        Ok(())
    }

    fn save(&mut self) -> Result<SnapshotAt, melonds::Error> {
        let recycled = self.pool.pop();
        let mut shared = self.shared.lock().unwrap();
        let tick = shared.live_tick;
        let audio_produced = shared.link.audio_produced();
        Ok(SnapshotAt {
            snap: shared.link.snapshot_into(recycled)?,
            tick,
            audio_produced,
        })
    }

    fn recycle(&mut self, state: SnapshotAt) {
        // Two deep is enough to cover the settled state plus the one
        // being replaced; more would just hold memory.
        if self.pool.len() < 2 {
            self.pool.push(state.snap);
        }
    }

    fn load(&mut self, state: &SnapshotAt) -> Result<(), melonds::Error> {
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        // The engine loads the settled state before every re-simulation;
        // when nothing speculated past it, the link is already parked
        // there and — by determinism — holds exactly this state, so the
        // restore is pure cost. DS snapshots are ~37 MiB, which makes
        // skipping it worth the check.
        if shared.live_tick == state.tick {
            return Ok(());
        }
        shared.link.restore(&state.snap)?;
        // Audio is playback state, not machine state: the restore does
        // not touch it, so the speculation it voices has to be taken
        // back by hand.
        shared.link.revoke_audio_to(state.audio_produced);
        shared.live_tick = state.tick;
        Ok(())
    }

    /// Repeat-last: on this hardware a held direction or button is far
    /// more likely to persist than to change on any given frame.
    fn predict(&self, last_remote: &Input) -> Input {
        *last_remote
    }
}

/// A running two-player rollback session over a DS link.
pub struct Session {
    inner: getgud::Session<LinkWorld>,
    shared: Arc<Mutex<Shared>>,
    local_player: usize,
}

impl Session {
    /// Start a session over an already-booted (and, for a netbattle,
    /// already-connected) link. `present_delay` is how many ticks behind
    /// the simulation frontier the host presents — purely local.
    pub fn new(mut link: Link, local_player: usize, present_delay: u32) -> Result<Self, melonds::Error> {
        assert!(local_player < 2);
        let initial_state = SnapshotAt {
            snap: link.snapshot()?,
            tick: 0,
            audio_produced: link.audio_produced(),
        };
        let shared = Arc::new(Mutex::new(Shared {
            link,
            live_tick: 0,
            observer: None,
            visible: [local_player == 0, local_player == 1],
            render_from: 0,
        }));
        Ok(Session {
            inner: getgud::Session::new(getgud::SessionParams {
                present_delay,
                initial_remotes: vec![Input::default()],
                initial_state,
                world: LinkWorld {
                    shared: shared.clone(),
                    local_player,
                    pool: Vec::new(),
                },
            }),
            shared,
            local_player,
        })
    }

    pub fn local_player(&self) -> usize {
        self.local_player
    }

    /// Advance one frame: settle whatever the peer's newly-arrived input
    /// confirms (rolling back on a misprediction), then speculate up to
    /// the present target. Returns the input to forward to the peer.
    pub fn advance(&mut self, local_input: Input) -> Result<(Outgoing, Report), melonds::Error> {
        let before = self.inner.local_frontier();
        // The simulation only ever reaches the present target — the
        // frontier is the *input* frontier, `present_delay` ticks ahead
        // of it. Everything below the target is rollback replay whose
        // frames nobody sees; rendering resumes at the target so the
        // frame the host presents is composited. (Gating at the frontier
        // itself renders nothing at all: no simulated tick ever clears
        // that bar.)
        self.shared.lock().unwrap().render_from = before.saturating_sub(self.inner.present_delay());
        let frame = self.inner.advance(local_input)?;
        let tick = frame.tick;
        Ok((
            Outgoing {
                tick,
                input: local_input,
                tick_advantage: self.inner.local_tick_advantage(),
            },
            Report {
                ticks: self.inner.local_frontier().saturating_sub(before),
                rollback_depth: self.inner.last_misprediction_depth(),
            },
        ))
    }

    /// Feed one remote input packet, in tick order.
    pub fn add_remote_input(&mut self, input: Input, tick_advantage: i16) {
        self.inner.add_remote_input(0, input, tick_advantage);
    }

    /// Install a per-tick observer (telemetry). Replaces any previous.
    pub fn set_observer(&mut self, observer: Option<Box<dyn TickObserver>>) {
        self.shared.lock().unwrap().observer = observer;
    }

    /// Run `f` against the live link — for video/audio readout. Parked
    /// at the newest simulated tick.
    pub fn with_link<R>(&self, f: impl FnOnce(&mut Link) -> R) -> R {
        f(&mut self.shared.lock().unwrap().link)
    }

    /// A cloneable handle for readout from another thread.
    pub fn link_handle(&self) -> LinkHandle {
        LinkHandle(self.shared.clone())
    }

    /// Clock-sync skew for a throttler; read before [`advance`](Self::advance).
    pub fn skew(&self) -> i32 {
        self.inner.skew()
    }

    pub fn speculation_balance(&self) -> i32 {
        self.inner.speculation_balance()
    }

    pub fn local_queue_length(&self) -> usize {
        self.inner.local_queue_length()
    }

    /// Ticks the next advance could settle from buffered remote input
    /// alone — nonzero means advancing drains the local queue rather
    /// than only growing it.
    pub fn matchable(&self) -> usize {
        self.inner.matchable()
    }

    pub fn present_delay(&self) -> u32 {
        self.inner.present_delay()
    }

    pub fn set_present_delay(&mut self, present_delay: u32) {
        self.inner.set_present_delay(present_delay);
    }
}
