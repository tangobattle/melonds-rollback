//! Rollback netplay over a *link* of two DSes: both consoles run locally in one process and
//! talk to each other over an emulated local-wireless airwaves, and the
//! pair is the unit that snapshots and restores. This is the DS analogue
//! of the GBA side's `mgba_rollback::Link`.
//!
//! The games run their real wireless protocol — discovery, association,
//! the host's command/reply rounds — over the emulated air. Nothing is
//! spoofed or short-circuited, so a link battle behaves exactly as it
//! does on hardware.
//!
//! The two consoles run **concurrently**, one thread each, and what
//! keeps the pair deterministic is that frame delivery is gated on
//! emulated wifi time alone. Every MP frame is stamped with its
//! sender's wifi clock, and each console continually publishes a
//! `progress` bound with the meaning "every frame I will ever send from
//! now on is stamped strictly later than this". A receive polling at
//! wifi time `t` delivers exactly the queued frames stamped `<= t`, and
//! may conclude "nothing more can arrive" only once the peer's bound
//! passes `t` (or the peer finished its video frame, or left the air).
//! When both consoles block on each other, the one with the smaller
//! wait target proceeds empty-handed — a rule that reads emulated state
//! only. No decision consults the wall clock or thread timing, so a
//! link is a pure function of its inputs — the same ROMs, saves, clock
//! and key sequence always reach the same state, in this process or any
//! other, however the threads interleave.
//!
//! [`Link::snapshot`] captures both consoles *and* the frames still in
//! flight on the air; restoring it resumes the session exactly, which is
//! what a rollback netcode needs when a prediction turns out wrong.

pub mod session;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

use melonds::{InstanceId, Nds};

/// One console's input for one frame.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, Debug)]
pub struct Input {
    /// Active-high key bits (see [`melonds::keys`]).
    pub keys: u32,
    /// Stylus position, or `None` for a lifted stylus.
    pub touch: Option<(u16, u16)>,
}

impl Input {
    pub fn keys(keys: u32) -> Self {
        Input { keys, touch: None }
    }

    pub fn touch(x: u16, y: u16) -> Self {
        Input {
            keys: 0,
            touch: Some((x, y)),
        }
    }
}

/// A frame on the air. Which queue it lands in follows melonDS's
/// `Platform::MP_*` split: replies answer a host's command round, and
/// everything else (beacons, management frames, commands, acks) shares
/// one queue that both receive paths pop from.
#[derive(Clone, Debug)]
enum Frame {
    Reply { ts: u64, aid: u16, data: Vec<u8> },
    Other { ts: u64, data: Vec<u8> },
}

impl Frame {
    fn timestamp(&self) -> u64 {
        match self {
            Frame::Reply { ts, .. } | Frame::Other { ts, .. } => *ts,
        }
    }

    fn deliver(self, out: &mut [u8], ts_out: &mut u64) -> i32 {
        let (ts, data) = match self {
            Frame::Reply { ts, data, .. } | Frame::Other { ts, data } => (ts, data),
        };
        out[..data.len()].copy_from_slice(&data);
        *ts_out = ts;
        data.len() as i32
    }
}

#[derive(Default)]
struct Seat {
    incoming: VecDeque<Frame>,
    replies: VecDeque<Frame>,
    /// Everything this console will ever send from here on is stamped
    /// *strictly later* than this. Advanced by its own sends and by the
    /// wifi clock publish at the end of each emulated timer batch.
    progress: u64,
    /// Wait target and queue kind (`true` = replies) while blocked in a
    /// receive, for the both-frozen tiebreak. `None` while running.
    waiting: Option<(u64, bool)>,
    /// Finished its video frame; it will send nothing more until the next.
    frame_done: bool,
    /// Between `MP_Begin` and `MP_End` — i.e. on the air at all.
    attached: bool,
}

#[derive(Default)]
struct AirState {
    seats: [Seat; 2],
}

/// The emulated airwaves: two seats' frame queues plus the timestamp
/// bounds that gate delivery.
#[derive(Default)]
struct Air {
    state: Mutex<AirState>,
    cv: Condvar,
}

impl Air {
    fn send(&self, me: usize, frame: Frame) {
        let mut st = self.state.lock().unwrap();
        // Deliberately no progress bump here: with host-clock adoption
        // in play, a send proves nothing about *future* send stamps.
        // The batch-end publishes carry the bound.
        let peer = 1 - me;
        match frame {
            Frame::Reply { .. } => st.seats[peer].replies.push_back(frame),
            Frame::Other { .. } => st.seats[peer].incoming.push_back(frame),
        }
        self.cv.notify_all();
    }

    /// The console's wifi timer finished processing everything at or
    /// before `through`: any future send is strictly later. This SETS
    /// rather than maxes: a client adopting its host's clock at
    /// association can jump *backward*, and the bound must follow it
    /// down or a stale-high value would let the peer conclude "nothing
    /// more can arrive" about frames still to come.
    fn publish(&self, me: usize, through: u64) {
        let mut st = self.state.lock().unwrap();
        if through != st.seats[me].progress {
            st.seats[me].progress = through;
            self.cv.notify_all();
        }
    }

    /// The bound the peer may trust for giving up: the seat's published
    /// clock, capped by any frame still sitting unconsumed in its
    /// inbound queues — consuming a frame can reset the console's clock
    /// to that frame's timestamp (host-clock adoption at association),
    /// after which its sends are only bounded by that timestamp.
    fn bound(seat: &Seat) -> u64 {
        let queued = seat
            .incoming
            .iter()
            .chain(seat.replies.iter())
            .map(Frame::timestamp)
            .min();
        match queued {
            Some(ts) => seat.progress.min(ts),
            None => seat.progress,
        }
    }

    /// Whether a queued frame in the given queue is due at or before
    /// `target` — i.e. whether a receive gated there has something to
    /// take.
    fn due(seat: &Seat, want_reply: bool, target: u64) -> bool {
        let queue = if want_reply { &seat.replies } else { &seat.incoming };
        queue.front().is_some_and(|f| f.timestamp() <= target)
    }

    /// Block until either something is due for this receive, or it is
    /// *proven* nothing more can arrive at or before `target`: the
    /// peer's progress bound passed it, the peer finished its video
    /// frame (it sends nothing more this tick), or the peer left the
    /// air.
    ///
    /// The deadlock case — both consoles blocked on each other — only
    /// resolves once it is *terminal*: the peer is registered waiting
    /// AND nothing in the current state can ever wake it (nothing due
    /// at its target, and this console's own bound below it). That
    /// configuration is stable and made of emulated state alone, so
    /// however the threads interleave, both sides observe the same one
    /// — and the smaller wait target proceeds empty-handed, seat 0 on
    /// a tie. A merely *momentarily* blocked peer never triggers it:
    /// it will wake on its own and may yet send frames at or before
    /// our target, so concluding "nothing more" from its transient
    /// state is what would let wall timing leak into the simulation.
    fn gate<'a>(&'a self, me: usize, target: u64, want_reply: bool) -> MutexGuard<'a, AirState> {
        let peer = 1 - me;
        let mut st = self.state.lock().unwrap();
        let mut stalled = false;
        loop {
            let peer_frozen = match st.seats[peer].waiting {
                Some((peer_target, peer_reply)) => {
                    !Self::due(&st.seats[peer], peer_reply, peer_target)
                        && Self::bound(&st.seats[me]) <= peer_target
                        && st.seats[me].attached
                        && (target < peer_target || (target == peer_target && me == 0))
                }
                None => false,
            };
            let proceed = Self::due(&st.seats[me], want_reply, target)
                || Self::bound(&st.seats[peer]) > target
                || st.seats[peer].frame_done
                || !st.seats[peer].attached
                || peer_frozen;
            if proceed {
                st.seats[me].waiting = None;
                return st;
            }
            st.seats[me].waiting = Some((target, want_reply));
            self.cv.notify_all();
            let (guard, timeout) = self
                .cv
                .wait_timeout(st, std::time::Duration::from_secs(2))
                .unwrap();
            st = guard;
            if timeout.timed_out() && !stalled {
                // A gate should only ever wait for the peer's next
                // wifi batch or frame — wall-milliseconds. Multiple
                // seconds means the pair is wedged; dump everything a
                // liveness postmortem needs and keep waiting.
                stalled = true;
                let dump = |seat: &Seat| {
                    format!(
                        "progress={} bound={} waiting={:?} attached={} frame_done={} incoming={} replies={}",
                        seat.progress,
                        Self::bound(seat),
                        seat.waiting,
                        seat.attached,
                        seat.frame_done,
                        seat.incoming.len(),
                        seat.replies.len(),
                    )
                };
                log::warn!(
                    "air gate stalled >2s: seat {me} target={target} want_reply={want_reply}; me: {}; peer: {}",
                    dump(&st.seats[me]),
                    dump(&st.seats[peer]),
                );
            }
        }
    }
}

/// The process-global platform hook. melonDS resolves its callbacks at
/// link time, so there is exactly one; it routes to whichever [`Link`]
/// is currently live.
///
/// Links can overlap in time — a new match's link is created while an
/// old session's still winds down — so routing must be precise, not
/// "whatever exists": each link takes a serial, its consoles carry
/// `(serial << 1) | seat` as their callback token, and only tokens of
/// the current serial route. A stale link's consoles talk into the
/// void instead of into the new link's air, and its drop leaves the
/// new link's routing alone.
static LINK_SERIAL: AtomicUsize = AtomicUsize::new(0);
static CURRENT: OnceLock<Mutex<Option<(usize, Arc<Air>)>>> = OnceLock::new();

fn route(inst: InstanceId) -> Option<(Arc<Air>, usize)> {
    let guard = CURRENT.get()?.lock().unwrap();
    match &*guard {
        Some((serial, air)) if *serial == inst.0 >> 1 => Some((air.clone(), inst.0 & 1)),
        _ => None,
    }
}

struct Router;

impl melonds::Host for Router {
    fn mp_begin(&self, inst: InstanceId) {
        if let Some((air, me)) = route(inst) {
            let mut st = air.state.lock().unwrap();
            st.seats[me].attached = true;
            air.cv.notify_all();
        }
    }

    fn mp_end(&self, inst: InstanceId) {
        if let Some((air, me)) = route(inst) {
            let mut st = air.state.lock().unwrap();
            st.seats[me].attached = false;
            air.cv.notify_all();
        }
    }

    fn mp_send_packet(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some((air, me)) = route(inst) {
            air.send(me, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_cmd(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some((air, me)) = route(inst) {
            air.send(me, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_ack(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some((air, me)) = route(inst) {
            air.send(me, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_reply(&self, inst: InstanceId, data: &[u8], ts: u64, aid: u16) -> i32 {
        if let Some((air, me)) = route(inst) {
            air.send(
                me,
                Frame::Reply {
                    ts,
                    aid,
                    data: data.to_vec(),
                },
            );
        }
        data.len() as i32
    }

    fn mp_clock(&self, inst: InstanceId, now: u64) {
        if let Some((air, me)) = route(inst) {
            air.publish(me, now);
        }
    }

    fn mp_recv_packet(&self, inst: InstanceId, data: &mut [u8], now: u64, ts_out: &mut u64) -> Option<i32> {
        // Type-agnostic like LocalMP: both receive paths pop the same
        // queue, so filtering by frame type here head-blocks everything
        // behind the first command frame.
        let (air, me) = route(inst)?;
        air.publish(me, now.saturating_sub(1));
        let mut st = air.gate(me, now, false);
        Some(if Air::due(&st.seats[me], false, now) {
            let frame = st.seats[me].incoming.pop_front().unwrap();
            // Consuming this frame may reset the console's clock to its
            // timestamp (host sync adoption), so the bound follows.
            st.seats[me].progress = st.seats[me].progress.min(frame.timestamp());
            frame.deliver(data, ts_out)
        } else {
            0
        })
    }

    fn mp_recv_host_packet(&self, inst: InstanceId, data: &mut [u8], now: u64, ts_out: &mut u64) -> Option<i32> {
        let (air, me) = route(inst)?;
        air.publish(me, now.saturating_sub(1));
        let mut st = air.gate(me, now, false);
        Some(if Air::due(&st.seats[me], false, now) {
            let frame = st.seats[me].incoming.pop_front().unwrap();
            st.seats[me].progress = st.seats[me].progress.min(frame.timestamp());
            frame.deliver(data, ts_out)
        } else if st.seats[1 - me].attached || !st.seats[me].incoming.is_empty() {
            // Nothing due on the air right now is 0. `-1` means the
            // host is GONE and tears the session down with a
            // communication error, so it is reserved for a peer that
            // really left — and even then only once its parting frames
            // have been consumed.
            0
        } else {
            -1
        })
    }

    fn mp_recv_replies(&self, inst: InstanceId, data: &mut [u8], now: u64, ts: u64, aidmask: u16) -> u16 {
        let Some((air, me)) = route(inst) else { return 0 };
        air.publish(me, now.saturating_sub(1));
        let target = ts + REPLY_WINDOW_US;
        let mut mask = 0u16;
        loop {
            let mut st = air.gate(me, target, true);
            while Air::due(&st.seats[me], true, target) {
                if let Some(Frame::Reply { aid, data: payload, .. }) = st.seats[me].replies.pop_front() {
                    if aid == 0 {
                        continue;
                    }
                    // Payloads pack at (aid-1)*1024 while the returned
                    // mask uses the raw aid bit — mixing the two hands
                    // the host a zeroed slot for its first client.
                    let at = (aid as usize - 1) * 1024;
                    data[at..at + payload.len()].copy_from_slice(&payload);
                    mask |= 1 << aid;
                    if mask & aidmask == aidmask {
                        return mask;
                    }
                }
            }
            // The gate proved no further reply can arrive in the window.
            let me_seat = &st.seats[me];
            let peer = &st.seats[1 - me];
            let peer_frozen = match peer.waiting {
                Some((peer_target, peer_reply)) => {
                    !Air::due(peer, peer_reply, peer_target)
                        && Air::bound(me_seat) <= peer_target
                        && me_seat.attached
                        && (target < peer_target || (target == peer_target && me == 0))
                }
                None => false,
            };
            let gave_up = Air::bound(peer) > target || peer.frame_done || !peer.attached || peer_frozen;
            if gave_up {
                return mask;
            }
        }
    }
}

/// How far past a command round's timestamp a host waits for replies.
///
/// LocalMP also drops replies older than the command by 32 µs. That
/// horizon assumes both consoles run concurrently in wall-clock time;
/// a frame-locked pair's wifi clocks sit up to a video frame apart, so
/// applying it here would discard most of every round.
const REPLY_WINDOW_US: u64 = 2000;

/// A point-in-time capture of an entire link: both consoles and the
/// frames still in flight between them.
#[derive(Clone)]
pub struct Snapshot {
    consoles: [Vec<u8>; 2],
    incoming: [Vec<Frame>; 2],
    replies: [Vec<Frame>; 2],
    progress: [u64; 2],
    attached: [bool; 2],
}

impl Snapshot {
    /// Total bytes held, for callers budgeting a rollback buffer.
    pub fn size(&self) -> usize {
        self.consoles.iter().map(Vec::len).sum()
    }

    /// Serialize to bytes, so a primed link can be cached on disk
    /// instead of walked through the game's menus again.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.size() + 4096);
        let put_u64 = |out: &mut Vec<u8>, v: u64| out.extend_from_slice(&v.to_le_bytes());
        let put_frames = |out: &mut Vec<u8>, frames: &[Frame]| {
            put_u64(out, frames.len() as u64);
            for frame in frames {
                let (kind, ts, aid, data) = match frame {
                    Frame::Reply { ts, aid, data } => (1u8, *ts, *aid, data),
                    Frame::Other { ts, data } => (0u8, *ts, 0, data),
                };
                out.push(kind);
                put_u64(out, ts);
                out.extend_from_slice(&aid.to_le_bytes());
                put_u64(out, data.len() as u64);
                out.extend_from_slice(data);
            }
        };
        for i in 0..2 {
            put_u64(&mut out, self.consoles[i].len() as u64);
            out.extend_from_slice(&self.consoles[i]);
            put_frames(&mut out, &self.incoming[i]);
            put_frames(&mut out, &self.replies[i]);
            put_u64(&mut out, self.progress[i]);
            out.push(self.attached[i] as u8);
        }
        // The token-scheduler era serialized whose turn it was here;
        // the byte stays so cached links keep parsing.
        out.push(0);
        out
    }

    /// Inverse of [`to_bytes`](Self::to_bytes). `None` if the bytes are
    /// truncated or malformed.
    pub fn from_bytes(bytes: &[u8]) -> Option<Snapshot> {
        let mut at = 0usize;
        let u64_at = |at: &mut usize| -> Option<u64> {
            let v = u64::from_le_bytes(bytes.get(*at..*at + 8)?.try_into().ok()?);
            *at += 8;
            Some(v)
        };
        let mut consoles = [Vec::new(), Vec::new()];
        let mut incoming = [Vec::new(), Vec::new()];
        let mut replies = [Vec::new(), Vec::new()];
        let mut progress = [0u64; 2];
        let mut attached = [false; 2];
        for i in 0..2 {
            let len = u64_at(&mut at)? as usize;
            consoles[i] = bytes.get(at..at + len)?.to_vec();
            at += len;
            for queue in 0..2 {
                let count = u64_at(&mut at)? as usize;
                let mut frames = Vec::with_capacity(count);
                for _ in 0..count {
                    let kind = *bytes.get(at)?;
                    at += 1;
                    let ts = u64_at(&mut at)?;
                    let aid = u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?);
                    at += 2;
                    let len = u64_at(&mut at)? as usize;
                    let data = bytes.get(at..at + len)?.to_vec();
                    at += len;
                    frames.push(if kind == 1 {
                        Frame::Reply { ts, aid, data }
                    } else {
                        Frame::Other { ts, data }
                    });
                }
                if queue == 0 {
                    incoming[i] = frames;
                } else {
                    replies[i] = frames;
                }
            }
            progress[i] = u64_at(&mut at)?;
            attached[i] = *bytes.get(at)? != 0;
            at += 1;
        }
        // Skip the legacy turn byte.
        let _ = *bytes.get(at)?;
        Some(Snapshot {
            consoles,
            incoming,
            replies,
            progress,
            attached,
        })
    }
}

/// Two DSes wired together over emulated local wireless.
pub struct Link {
    consoles: [Nds; 2],
    air: Arc<Air>,
    serial: usize,
}

impl Link {
    /// Boot a pair on the same cart. `saves` are the two consoles' save
    /// memories, `rtc` the clock both are pinned to — pass identical
    /// values on every peer so the link stays a pure function of its
    /// inputs.
    ///
    /// Only one link may exist at a time: melonDS's platform callbacks
    /// are process-global.
    pub fn new(rom: &[u8], saves: [Option<&[u8]>; 2], rtc: (i32, i32, i32, i32, i32, i32)) -> Result<Self, melonds::Error> {
        // The router is installed once and forwards to whichever link is
        // live; a second install is the expected no-op on re-entry.
        let _ = melonds::install_host(Box::new(Router));

        // The MAC-forming instance ids stay 0 and 1 — they are part of
        // the simulation and must match on every peer — while the
        // routing tokens carry this link's serial.
        let serial = LINK_SERIAL.fetch_add(1, Ordering::Relaxed);
        let mut consoles = [
            Nds::new(rom, saves[0], 0, serial << 1)?,
            Nds::new(rom, saves[1], 1, (serial << 1) | 1)?,
        ];
        for nds in &mut consoles {
            nds.set_rtc(rtc.0, rtc.1, rtc.2, rtc.3, rtc.4, rtc.5);
            nds.boot();
        }

        let air = Arc::new(Air::default());
        *CURRENT.get_or_init(|| Mutex::new(None)).lock().unwrap() = Some((serial, air.clone()));
        Ok(Link { consoles, air, serial })
    }

    /// Advance both consoles one video frame. The consoles run
    /// concurrently; the air's timestamp gates keep every delivery a
    /// function of emulated time alone.
    pub fn tick(&mut self, inputs: [Input; 2]) {
        {
            let mut st = self.air.state.lock().unwrap();
            st.seats[0].frame_done = false;
            st.seats[1].frame_done = false;
        }
        let air = &self.air;
        std::thread::scope(|s| {
            for (i, nds) in self.consoles.iter_mut().enumerate() {
                s.spawn(move || {
                    match inputs[i].touch {
                        Some((x, y)) => nds.touch(x, y),
                        None => nds.release_screen(),
                    }
                    nds.set_keys(inputs[i].keys);
                    nds.run_frame();
                    let mut st = air.state.lock().unwrap();
                    st.seats[i].frame_done = true;
                    air.cv.notify_all();
                });
            }
        });
    }

    /// Toggle framebuffer production per console. A console nobody
    /// displays — the remote seat, or any seat during rollback
    /// re-simulation — skips its 2D compositing entirely; emulation
    /// (including display capture into VRAM) is bit-identical either
    /// way, only the framebuffer goes stale while off.
    pub fn set_render(&mut self, render: [bool; 2]) {
        for (nds, on) in self.consoles.iter_mut().zip(render) {
            nds.set_render(on);
        }
    }

    /// Capture the whole link — both consoles and the air between them.
    pub fn snapshot(&mut self) -> Result<Snapshot, melonds::Error> {
        self.snapshot_into(None)
    }

    /// Capture into a retired snapshot's buffers when one is offered.
    /// Rollback retires a snapshot nearly every tick, so reusing those
    /// allocations keeps a session off the allocator's hot path.
    pub fn snapshot_into(&mut self, recycled: Option<Snapshot>) -> Result<Snapshot, melonds::Error> {
        let mut consoles = match recycled {
            Some(snap) => snap.consoles,
            None => [Vec::new(), Vec::new()],
        };
        // The two consoles serialize independently, and at ~6 MB each
        // this is memory-bound — so do them at the same time rather than
        // one after the other. Nothing is shared: each call touches only
        // its own instance and its own buffer.
        let mut results = [None, None];
        std::thread::scope(|s| {
            for ((nds, buf), slot) in self
                .consoles
                .iter_mut()
                .zip(consoles.iter_mut())
                .zip(results.iter_mut())
            {
                s.spawn(move || *slot = Some(nds.save_state(buf)));
            }
        });
        for result in results {
            result.expect("snapshot thread did not run")?;
        }
        let st = self.air.state.lock().unwrap();
        Ok(Snapshot {
            consoles,
            incoming: [
                st.seats[0].incoming.iter().cloned().collect(),
                st.seats[1].incoming.iter().cloned().collect(),
            ],
            replies: [
                st.seats[0].replies.iter().cloned().collect(),
                st.seats[1].replies.iter().cloned().collect(),
            ],
            progress: [st.seats[0].progress, st.seats[1].progress],
            attached: [st.seats[0].attached, st.seats[1].attached],
        })
    }

    /// Resume from a capture. Simulation continues from the frame *after*
    /// the one that had completed when the snapshot was taken.
    pub fn restore(&mut self, snap: &Snapshot) -> Result<(), melonds::Error> {
        let mut results = [None, None];
        std::thread::scope(|s| {
            for ((nds, buf), slot) in self
                .consoles
                .iter_mut()
                .zip(snap.consoles.iter())
                .zip(results.iter_mut())
            {
                s.spawn(move || *slot = Some(nds.load_state(buf)));
            }
        });
        for result in results {
            result.expect("restore thread did not run")?;
        }
        let mut st = self.air.state.lock().unwrap();
        for i in 0..2 {
            st.seats[i].incoming = snap.incoming[i].iter().cloned().collect();
            st.seats[i].replies = snap.replies[i].iter().cloned().collect();
            st.seats[i].progress = snap.progress[i];
            st.seats[i].attached = snap.attached[i];
            st.seats[i].waiting = None;
            st.seats[i].frame_done = false;
        }
        self.air.cv.notify_all();
        Ok(())
    }

    /// Borrow one console, for video/audio/RAM readout.
    pub fn console(&mut self, player: usize) -> &mut Nds {
        &mut self.consoles[player]
    }

    /// Whether both consoles are currently on the air — true once a
    /// wireless session is up.
    pub fn connected(&self) -> bool {
        let st = self.air.state.lock().unwrap();
        st.seats[0].attached && st.seats[1].attached
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // Only unhook if the routing still points at THIS link: a
        // newer link may have taken over, and clearing its routing
        // would cut its consoles off the air mid-flight — which is
        // exactly what a stale session dropping after a new match
        // boots used to do.
        if let Some(slot) = CURRENT.get() {
            let mut guard = slot.lock().unwrap();
            if matches!(&*guard, Some((serial, _)) if *serial == self.serial) {
                *guard = None;
            }
        }
    }
}
