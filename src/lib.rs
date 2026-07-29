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
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use melonds::Nds;

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

    fn payload_len(&self) -> usize {
        match self {
            Frame::Reply { data, .. } | Frame::Other { data, .. } => data.len(),
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

/// Frames one seat's RX queue holds before the oldest starts falling off
/// the front.
///
/// A frame-locked pair has single digits in flight — a command round and
/// its reply, plus the host's beacons — so this is two orders of
/// magnitude of headroom above anything a healthy link produces. It is a
/// backstop against a queue that has stopped draining, not a working
/// depth: at this size the per-tick snapshot clone stays bounded instead
/// of growing with the match.
const AIR_QUEUE_DEPTH: usize = 256;

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
    /// Fired when a seat leaves the air — see [`Link::on_detach`].
    on_detach: Mutex<Option<Box<dyn FnMut(usize) + Send>>>,
}

impl Air {
    fn send(&self, me: usize, frame: Frame) {
        let mut st = self.state.lock().unwrap();
        // Deliberately no progress bump here: with host-clock adoption
        // in play, a send proves nothing about *future* send stamps.
        // The batch-end publishes carry the bound.
        let peer = 1 - me;
        // A radio that is not on the air hears nothing. Queueing for it
        // anyway would hold the frame until it attaches — and a seat
        // that never attaches back never drains, so a host's beacons
        // pile up in it for the rest of the match. Nothing pops them,
        // and every one is cloned into every per-tick snapshot.
        if !st.seats[peer].attached {
            return;
        }
        let queue = match frame {
            Frame::Reply { .. } => &mut st.seats[peer].replies,
            Frame::Other { .. } => &mut st.seats[peer].incoming,
        };
        queue.push_back(frame);
        // An RX queue is finite hardware: past its depth the oldest
        // frame is gone, not held. Without this a queue whose head has
        // stopped being due — a receiver whose clock adopted its host's
        // and jumped back behind a stamp it already holds — blocks at
        // the front while everything behind it accumulates unboundedly.
        // Dropping from the front is both what the hardware does and
        // what unblocks it.
        //
        // Deterministic, so it stays simulation: the depth is a
        // function of emulated state alone, both peers simulate both
        // consoles, and a snapshot carries the queues as they stand.
        while queue.len() > AIR_QUEUE_DEPTH {
            queue.pop_front();
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
            // Timed rather than plain: every state change that could
            // free this gate notifies under the same mutex, so a wake
            // can't be missed by construction — but a future path that
            // forgets to notify would hang the pair outright instead of
            // costing it a re-check.
            st = self
                .cv
                .wait_timeout(st, std::time::Duration::from_secs(2))
                .unwrap()
                .0;
        }
    }
}

/// One seat's radio: the per-console [`melonds::Host`] a link hands
/// each of its consoles at boot. It holds the link's air and its own
/// seat index, and it lives exactly as long as its console does — so
/// there is no routing table to keep in sync, links coexist freely
/// (each pair on its own air), and nothing a stale session does can
/// touch a newer link's wireless.
struct SeatHost {
    air: Arc<Air>,
    seat: usize,
}

impl melonds::Host for SeatHost {
    fn mp_begin(&self) {
        let mut st = self.air.state.lock().unwrap();
        st.seats[self.seat].attached = true;
        self.air.cv.notify_all();
    }

    fn mp_end(&self) {
        {
            let mut st = self.air.state.lock().unwrap();
            st.seats[self.seat].attached = false;
        }
        self.air.cv.notify_all();
        if let Some(hook) = self.air.on_detach.lock().unwrap().as_mut() {
            hook(self.seat);
        }
    }

    fn mp_send_packet(&self, data: &[u8], ts: u64) -> i32 {
        self.air.send(self.seat, Frame::Other { ts, data: data.to_vec() });
        data.len() as i32
    }

    fn mp_send_cmd(&self, data: &[u8], ts: u64) -> i32 {
        self.air.send(self.seat, Frame::Other { ts, data: data.to_vec() });
        data.len() as i32
    }

    fn mp_send_ack(&self, data: &[u8], ts: u64) -> i32 {
        self.air.send(self.seat, Frame::Other { ts, data: data.to_vec() });
        data.len() as i32
    }

    fn mp_send_reply(&self, data: &[u8], ts: u64, aid: u16) -> i32 {
        self.air.send(
            self.seat,
            Frame::Reply {
                ts,
                aid,
                data: data.to_vec(),
            },
        );
        data.len() as i32
    }

    fn mp_clock(&self, now: u64) {
        self.air.publish(self.seat, now);
    }

    fn mp_recv_packet(&self, data: &mut [u8], now: u64, ts_out: &mut u64) -> Option<i32> {
        // Type-agnostic like LocalMP: both receive paths pop the same
        // queue, so filtering by frame type here head-blocks everything
        // behind the first command frame.
        let me = self.seat;
        self.air.publish(me, now.saturating_sub(1));
        let mut st = self.air.gate(me, now, false);
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

    fn mp_recv_host_packet(&self, data: &mut [u8], now: u64, ts_out: &mut u64) -> Option<i32> {
        let me = self.seat;
        self.air.publish(me, now.saturating_sub(1));
        let mut st = self.air.gate(me, now, false);
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

    fn mp_recv_replies(&self, data: &mut [u8], now: u64, ts: u64, aidmask: u16) -> u16 {
        let me = self.seat;
        self.air.publish(me, now.saturating_sub(1));
        let target = ts + REPLY_WINDOW_US;
        let mut mask = 0u16;
        loop {
            let mut st = self.air.gate(me, target, true);
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
    ///
    /// The air counts. Its frames are cloned into every snapshot the
    /// same as console state is, so a queue that has stopped draining
    /// shows up here as a rollback buffer that grows with the match —
    /// which is exactly what a caller watching this number wants to see.
    pub fn size(&self) -> usize {
        let frames = |queues: &[Vec<Frame>; 2]| -> usize {
            queues
                .iter()
                .flatten()
                .map(|f| std::mem::size_of::<Frame>() + f.payload_len())
                .sum()
        };
        self.consoles.iter().map(Vec::len).sum::<usize>() + frames(&self.incoming) + frames(&self.replies)
    }

    /// Frames in flight per seat: `[incoming, replies]` for seat 0 then
    /// seat 1. A healthy link sits in single digits; a number that
    /// climbs with the match is a queue nothing is draining.
    pub fn air_depth(&self) -> [[usize; 2]; 2] {
        [
            [self.incoming[0].len(), self.replies[0].len()],
            [self.incoming[1].len(), self.replies[1].len()],
        ]
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
/// Frames of one console's audio the link will hold before it starts
/// dropping the oldest — a second at the SPU's output rate.
///
/// Only reached by a console nobody listens to (the remote seat), which
/// produces audio all the same and has nobody draining it. The listened
/// console never comes near: a host keeps it at tens of ms.
const AUDIO_CAP_FRAMES: usize = 48_000;

pub struct Link {
    consoles: [Nds; 2],
    air: Arc<Air>,
    /// Each console's audio, taken out of its SPU every tick and held
    /// here instead.
    ///
    /// Held here because it has to be revocable. melonDS's SPU ring is
    /// not: a savestate does not cover it (`SPU::DoSavestate` saves the
    /// channels, not the output buffer), so a restore leaves speculated
    /// audio sitting in it, and re-simulation then appends the same span
    /// again. Nothing in the SPU's API can take it back out — and the
    /// ring is ~43 ms, so the duplicates overflow it within a couple of
    /// frames, at which point it starts advancing its own read cursor
    /// and destroying the oldest audio. That is the crackle.
    ///
    /// Taking it out every tick fixes both halves: the SPU never
    /// accumulates enough to overflow, and what a rollback speculated is
    /// somewhere it can be removed from.
    audio: [Vec<i16>; 2],
    /// Per console, cumulative frames appended here and *kept* — net of
    /// revocation and of re-simulation swallowed on catch-up. The
    /// coordinate system the revocation math runs in.
    audio_produced: [u64; 2],
    /// Per console, frames of re-simulated audio still to swallow: the
    /// corrected regeneration of a span whose speculative version was
    /// already handed to a host. That cannot be unplayed, so queuing the
    /// regeneration would be an echo. Swallowed out of the catch-up's
    /// own fresh production instead, oldest first.
    audio_resim_drain: [u64; 2],
    /// Landing buffer for the per-tick take.
    audio_scratch: Vec<i16>,
}

impl Link {
    /// Boot a pair on the same cart. `saves` are the two consoles' save
    /// memories, `rtc` the clock both are pinned to — pass identical
    /// values on every peer so the link stays a pure function of its
    /// inputs.
    ///
    /// Links coexist freely: each console carries its own seat's radio,
    /// and each pair is on its own air, so a replay's display and stats
    /// pairs (or a lingering old session) never hear each other.
    pub fn new(rom: &[u8], saves: [Option<&[u8]>; 2], rtc: (i32, i32, i32, i32, i32, i32)) -> Result<Self, melonds::Error> {
        let air = Arc::new(Air::default());
        // The MAC-forming instance ids are 0 and 1 — they are part of
        // the simulation and must match on every peer.
        let mut consoles = [
            Nds::new(rom, saves[0], 0, Box::new(SeatHost { air: air.clone(), seat: 0 }))?,
            Nds::new(rom, saves[1], 1, Box::new(SeatHost { air: air.clone(), seat: 1 }))?,
        ];
        for nds in &mut consoles {
            nds.set_rtc(rtc.0, rtc.1, rtc.2, rtc.3, rtc.4, rtc.5);
            nds.boot();
        }

        Ok(Link {
            consoles,
            air,
            audio: [Vec::new(), Vec::new()],
            audio_produced: [0; 2],
            audio_resim_drain: [0; 2],
            audio_scratch: Vec::new(),
        })
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
        self.collect_audio();
    }

    /// Take this tick's audio out of both SPUs, swallowing whatever a
    /// rollback already owes.
    fn collect_audio(&mut self) {
        for i in 0..2 {
            let before = self.audio[i].len();
            pump_spu(&mut self.consoles[i], &mut self.audio[i], &mut self.audio_scratch);
            let mut delta = ((self.audio[i].len() - before) / 2) as u64;
            if self.audio_resim_drain[i] > 0 && delta > 0 {
                // Catch-up regeneration of audio a host already has:
                // drop it from the head of this tick's fresh span, so
                // the seam lands exactly where playback left off.
                let swallow = self.audio_resim_drain[i].min(delta) as usize;
                self.audio[i].drain(before..before + swallow * 2);
                self.audio_resim_drain[i] -= swallow as u64;
                delta -= swallow as u64;
            }
            self.audio_produced[i] += delta;
            // Nobody is draining the remote seat's, so bound it.
            let over = self.audio[i].len().saturating_sub(AUDIO_CAP_FRAMES * 2);
            if over > 0 {
                self.audio[i].drain(..over);
            }
        }
    }

    /// One console's per-side surface — the same view a [`Solo`] hands
    /// out, so a frontend reads a linked console and a lone one through
    /// one type.
    pub fn side(&mut self, player: usize) -> Side<'_> {
        Side {
            console: &mut self.consoles[player],
            audio: &mut self.audio[player],
        }
    }

    /// Frames appended and kept per console so far — what a snapshot
    /// records so [`revoke_audio_to`](Self::revoke_audio_to) can tell
    /// how much was speculated past it.
    pub fn audio_produced(&self) -> [u64; 2] {
        self.audio_produced
    }

    /// Take back everything appended since a snapshot recorded
    /// `produced`.
    ///
    /// The part still held here is dropped from the write end — the
    /// settled audio beneath it is final, and a blanket clear would skip
    /// over it. The part a host already took cannot be unplayed, so its
    /// corrected regeneration is swallowed during the catch-up instead
    /// of queuing as an echo. By determinism the catch-up regenerates
    /// the revoked span sample for sample, so playback resumes exactly
    /// where it left off.
    pub fn revoke_audio_to(&mut self, produced: [u64; 2]) {
        for i in 0..2 {
            let revoked = self.audio_produced[i].saturating_sub(produced[i]);
            let held = (self.audio[i].len() / 2) as u64;
            let dropped = revoked.min(held);
            let keep = self.audio[i].len() - dropped as usize * 2;
            self.audio[i].truncate(keep);
            self.audio_resim_drain[i] = revoked - dropped;
            self.audio_produced[i] = produced[i];
        }
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

    /// Install `hook`, fired with the seat index whenever a console
    /// leaves the air. This is the game's own link-session exit
    /// (melonDS's `MP_End`) reaching the shim — never a snapshot
    /// restore, which reconciles seat attachment directly — so it
    /// carries a trap's semantics without the interpreter a trap
    /// costs: it fires from game code acting, and a rollback
    /// re-simulation re-runs that act and fires it again at the same
    /// emulated moment. Called on a console's own tick thread, so keep
    /// it to a latch.
    pub fn on_detach(&mut self, hook: impl FnMut(usize) + Send + 'static) {
        *self.air.on_detach.lock().unwrap() = Some(Box::new(hook));
    }

    /// Frames in flight per seat: `[incoming, replies]` for seat 0 then
    /// seat 1. See [`Snapshot::air_depth`] — this is the live reading of
    /// the same thing, for a host that wants it without snapshotting.
    pub fn air_depth(&self) -> [[usize; 2]; 2] {
        let st = self.air.state.lock().unwrap();
        [
            [st.seats[0].incoming.len(), st.seats[0].replies.len()],
            [st.seats[1].incoming.len(), st.seats[1].replies.len()],
        ]
    }
}

/// One console of a boot, with the audio already taken out of its SPU:
/// the per-console surface a frontend reads. [`Link::side`] and
/// [`Solo::side`] hand out the same one, so nothing above here cares
/// whether the console it is reading has a pair.
pub struct Side<'a> {
    console: &'a mut Nds,
    audio: &'a mut Vec<i16>,
}

impl Side<'_> {
    /// The console itself: video, savedata, traps.
    pub fn console(&mut self) -> &mut Nds {
        self.console
    }

    /// Take up to `out`'s worth of this console's audio as interleaved
    /// stereo, and report how much is left behind. Taking it consumes
    /// it — in a link, what stays queued stays revocable.
    pub fn take_audio(&mut self, out: &mut [i16]) -> (usize, usize) {
        let frames = (out.len() / 2).min(self.audio.len() / 2);
        out[..frames * 2].copy_from_slice(&self.audio[..frames * 2]);
        self.audio.drain(..frames * 2);
        (frames, self.audio.len() / 2)
    }
}

/// Empty one console's SPU into `audio`. `scratch` is the landing
/// buffer for the read, kept by the caller so the resize amortizes.
fn pump_spu(console: &mut Nds, audio: &mut Vec<i16>, scratch: &mut Vec<i16>) {
    let queued = console.audio_queued();
    if queued > 0 {
        scratch.resize(queued * 2, 0);
        let got = console.read_audio(scratch);
        audio.extend_from_slice(&scratch[..got * 2]);
    }
}

/// One console running on its own: the boot a [`Link`] gives each of
/// its seats, with no pair, no air and no rollback.
///
/// What it keeps from the link is the SPU discipline: audio comes out
/// of the SPU every tick and pools here, because the SPU's own ring is
/// ~43 ms — a fast-forwarding host produces far more than that between
/// two reads of its sound callback, and the ring destroys its oldest
/// audio when it overflows.
pub struct Solo {
    console: Nds,
    /// This console's audio, pooled per tick. Bounded by
    /// [`AUDIO_CAP_FRAMES`]: nothing revokes here, so the cap alone
    /// keeps a host that stops draining from growing it without bound.
    audio: Vec<i16>,
    audio_scratch: Vec<i16>,
}

impl Solo {
    /// Boot one console. `rtc` pins the cart clock exactly as a link's
    /// boot does — a solo ride just has nobody to agree with.
    pub fn new(rom: &[u8], save: Option<&[u8]>, rtc: (i32, i32, i32, i32, i32, i32)) -> Result<Self, melonds::Error> {
        // A radio with no air: should the game ever touch its wireless,
        // sends vanish and receives report not-connected — the
        // [`melonds::Host`] defaults.
        struct Offline;
        impl melonds::Host for Offline {}
        let mut console = Nds::new(rom, save, 0, Box::new(Offline))?;
        console.set_rtc(rtc.0, rtc.1, rtc.2, rtc.3, rtc.4, rtc.5);
        console.boot();
        Ok(Solo {
            console,
            audio: Vec::new(),
            audio_scratch: Vec::new(),
        })
    }

    /// Advance one video frame.
    pub fn tick(&mut self, input: Input) {
        match input.touch {
            Some((x, y)) => self.console.touch(x, y),
            None => self.console.release_screen(),
        }
        self.console.set_keys(input.keys);
        self.console.run_frame();
        pump_spu(&mut self.console, &mut self.audio, &mut self.audio_scratch);
        let over = self.audio.len().saturating_sub(AUDIO_CAP_FRAMES * 2);
        if over > 0 {
            self.audio.drain(..over);
        }
    }

    /// The console's per-side surface, as a link hands out per seat.
    pub fn side(&mut self) -> Side<'_> {
        Side {
            console: &mut self.console,
            audio: &mut self.audio,
        }
    }
}

/// The air's retention rules, which nothing else can check: reaching
/// them through a live pair needs both consoles walked into a netbattle,
/// and what they guard against is a queue that quietly never empties.
#[cfg(test)]
mod tests {
    use super::*;

    fn frame(ts: u64) -> Frame {
        Frame::Other { ts, data: vec![0; 32] }
    }

    /// Wifi off is a radio that is not there to hear anything —
    /// `attached` tracks the console's wifi power exactly (melonDS calls
    /// `MP_Begin`/`MP_End` from `Wifi::UpdatePowerOn`).
    ///
    /// Holding frames for it instead is what let a console that left its
    /// comm screen keep collecting the other's beacons for the rest of
    /// the match. Nothing ever popped them, and every one was cloned
    /// into every per-tick snapshot, so the tick cost climbed until the
    /// pair stopped producing frames.
    #[test]
    fn a_seat_whose_wifi_is_off_receives_nothing() {
        let air = Air::default();

        air.send(0, frame(1));
        assert_eq!(air.state.lock().unwrap().seats[1].incoming.len(), 0);

        air.state.lock().unwrap().seats[1].attached = true;
        air.send(0, frame(2));
        assert_eq!(air.state.lock().unwrap().seats[1].incoming.len(), 1);
    }

    /// An RX queue is finite hardware. A receiver that stops draining —
    /// its clock adopted its host's and jumped back behind a stamp it
    /// already holds, so its head is never due — loses the oldest frames
    /// rather than accumulating behind the stuck one.
    #[test]
    fn an_undrained_queue_stops_at_the_rx_depth() {
        let air = Air::default();
        air.state.lock().unwrap().seats[1].attached = true;

        let sent = AIR_QUEUE_DEPTH as u64 * 3;
        for ts in 0..sent {
            air.send(0, frame(ts));
        }

        let st = air.state.lock().unwrap();
        let queue = &st.seats[1].incoming;
        assert_eq!(queue.len(), AIR_QUEUE_DEPTH);
        // The front is what falls off, so what survives is the newest.
        assert_eq!(queue.front().unwrap().timestamp(), sent - AIR_QUEUE_DEPTH as u64);
        assert_eq!(queue.back().unwrap().timestamp(), sent - 1);
    }
}
