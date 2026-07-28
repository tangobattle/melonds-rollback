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
//! What makes rollback possible is the scheduling discipline: exactly
//! one console executes at any moment, and every handoff between them is
//! a function of emulated state alone (the peer produced a frame we
//! were waiting for, the peer blocked too, or the peer finished its
//! video frame). No decision consults the wall clock or thread timing,
//! so a link is a pure function of its inputs — the same ROMs, saves,
//! clock and key sequence always reach the same state, in this process
//! or any other.
//!
//! [`Link::snapshot`] captures both consoles *and* the frames still in
//! flight on the air; restoring it resumes the session exactly, which is
//! what a rollback netcode needs when a prediction turns out wrong.

pub mod session;

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

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
    /// Newest wifi timestamp this console has been observed at.
    progress: u64,
    /// Blocked in a receive, waiting on the peer.
    parked: bool,
    /// Finished its video frame; it will send nothing more until the next.
    frame_done: bool,
    /// Between `MP_Begin` and `MP_End` — i.e. on the air at all.
    attached: bool,
}

#[derive(Default)]
struct AirState {
    seats: [Seat; 2],
    /// Which console holds the run token. Exactly one executes at a time.
    turn: usize,
}

/// The emulated airwaves plus the run token that serializes the pair.
#[derive(Default)]
struct Air {
    state: Mutex<AirState>,
    cv: Condvar,
}

impl Air {
    fn send(&self, me: usize, frame: Frame) {
        let mut st = self.state.lock().unwrap();
        let ts = frame.timestamp();
        if ts > st.seats[me].progress {
            st.seats[me].progress = ts;
        }
        let peer = 1 - me;
        match frame {
            Frame::Reply { .. } => st.seats[peer].replies.push_back(frame),
            Frame::Other { .. } => st.seats[peer].incoming.push_back(frame),
        }
        self.cv.notify_all();
    }

    /// Block until this console can be answered. Yields the run token to
    /// the peer while blocked, so the peer runs until it produces what we
    /// are waiting for, blocks itself, or finishes its frame.
    fn wait<'a>(&'a self, me: usize, ts: u64, want_reply: bool) -> std::sync::MutexGuard<'a, AirState> {
        let peer = 1 - me;
        let have = |st: &AirState| {
            if want_reply {
                !st.seats[me].replies.is_empty()
            } else {
                !st.seats[me].incoming.is_empty()
            }
        };

        let mut st = self.state.lock().unwrap();
        if ts > st.seats[me].progress {
            st.seats[me].progress = ts;
        }
        // Handing the token to a console that cannot run would wedge the
        // pair, so give up immediately instead.
        if have(&st) || st.seats[peer].frame_done || !st.seats[peer].attached {
            return st;
        }

        st.seats[me].parked = true;
        st.turn = peer;
        self.cv.notify_all();
        loop {
            let ready = have(&st)
                || st.seats[peer].frame_done
                || !st.seats[peer].attached
                // Both parked: nobody can produce anything, so the
                // token holder proceeds empty-handed.
                || st.seats[peer].parked;
            if ready && st.turn == me {
                st.seats[me].parked = false;
                return st;
            }
            st = self.cv.wait(st).unwrap();
        }
    }

    /// Block until it is this console's turn to execute.
    fn acquire(&self, me: usize) {
        let mut st = self.state.lock().unwrap();
        while st.turn != me {
            st = self.cv.wait(st).unwrap();
        }
    }
}

/// The process-global platform hook. melonDS resolves its callbacks at
/// link time, so there is exactly one; it routes to whichever [`Link`]
/// is currently live.
static CURRENT: OnceLock<Mutex<Option<Arc<Air>>>> = OnceLock::new();

fn current() -> Option<Arc<Air>> {
    CURRENT.get()?.lock().unwrap().clone()
}

struct Router;

impl melonds::Host for Router {
    fn mp_begin(&self, inst: InstanceId) {
        if let Some(air) = current() {
            let mut st = air.state.lock().unwrap();
            st.seats[inst.0 as usize].attached = true;
            air.cv.notify_all();
        }
    }

    fn mp_end(&self, inst: InstanceId) {
        if let Some(air) = current() {
            let mut st = air.state.lock().unwrap();
            st.seats[inst.0 as usize].attached = false;
            air.cv.notify_all();
        }
    }

    fn mp_send_packet(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some(air) = current() {
            air.send(inst.0 as usize, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_cmd(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some(air) = current() {
            air.send(inst.0 as usize, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_ack(&self, inst: InstanceId, data: &[u8], ts: u64) -> i32 {
        if let Some(air) = current() {
            air.send(inst.0 as usize, Frame::Other { ts, data: data.to_vec() });
        }
        data.len() as i32
    }

    fn mp_send_reply(&self, inst: InstanceId, data: &[u8], ts: u64, aid: u16) -> i32 {
        if let Some(air) = current() {
            air.send(
                inst.0 as usize,
                Frame::Reply {
                    ts,
                    aid,
                    data: data.to_vec(),
                },
            );
        }
        data.len() as i32
    }

    fn mp_recv_packet(&self, inst: InstanceId, data: &mut [u8], ts_out: &mut u64) -> Option<i32> {
        // Non-blocking, and type-agnostic like LocalMP: both receive
        // paths pop the same queue, so filtering by frame type here
        // head-blocks everything behind the first command frame.
        let air = current()?;
        let me = inst.0 as usize;
        let mut st = air.state.lock().unwrap();
        Some(match st.seats[me].incoming.pop_front() {
            Some(frame) => frame.deliver(data, ts_out),
            None => 0,
        })
    }

    fn mp_recv_host_packet(&self, inst: InstanceId, data: &mut [u8], ts_out: &mut u64) -> Option<i32> {
        let air = current()?;
        let me = inst.0 as usize;
        let ts = air.state.lock().unwrap().seats[me].progress;
        let mut st = air.wait(me, ts, false);
        Some(match st.seats[me].incoming.pop_front() {
            Some(frame) => frame.deliver(data, ts_out),
            // Nothing on the air right now is 0. `-1` means the host is
            // GONE and tears the session down with a communication
            // error, so it is reserved for a peer that really left.
            None => {
                if st.seats[1 - me].attached {
                    0
                } else {
                    -1
                }
            }
        })
    }

    fn mp_recv_replies(&self, inst: InstanceId, data: &mut [u8], ts: u64, aidmask: u16) -> u16 {
        let Some(air) = current() else { return 0 };
        let me = inst.0 as usize;
        let mut st = air.wait(me, ts + REPLY_WINDOW_US, true);
        let mut mask = 0;
        while let Some(frame) = st.seats[me].replies.pop_front() {
            if let Frame::Reply { ts, aid, data: payload } = frame {
                let _ = ts;
                if aid == 0 {
                    continue;
                }
                // Payloads pack at (aid-1)*1024 while the returned mask
                // uses the raw aid bit — mixing the two hands the host a
                // zeroed slot for its first client.
                let at = (aid as usize - 1) * 1024;
                data[at..at + payload.len()].copy_from_slice(&payload);
                mask |= 1 << aid;
                if mask & aidmask == aidmask {
                    break;
                }
            }
        }
        mask
    }
}

/// How far past a command round's timestamp a host waits for replies.
///
/// LocalMP also drops replies older than the command by 32 µs. That
/// horizon assumes both consoles run concurrently in wall-clock time;
/// a lockstepped pair's wifi clocks sit up to a video frame apart, so
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
    turn: usize,
}

impl Snapshot {
    /// Total bytes held, for callers budgeting a rollback buffer.
    pub fn size(&self) -> usize {
        self.consoles.iter().map(Vec::len).sum()
    }
}

/// Two DSes wired together over emulated local wireless.
pub struct Link {
    consoles: [Nds; 2],
    air: Arc<Air>,
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

        let mut consoles = [Nds::new(rom, saves[0], 0)?, Nds::new(rom, saves[1], 1)?];
        for nds in &mut consoles {
            nds.set_rtc(rtc.0, rtc.1, rtc.2, rtc.3, rtc.4, rtc.5);
            nds.boot();
        }

        let air = Arc::new(Air::default());
        *CURRENT.get_or_init(|| Mutex::new(None)).lock().unwrap() = Some(air.clone());
        Ok(Link { consoles, air })
    }

    /// Advance both consoles one video frame.
    pub fn tick(&mut self, inputs: [Input; 2]) {
        {
            let mut st = self.air.state.lock().unwrap();
            st.seats[0].frame_done = false;
            st.seats[1].frame_done = false;
            st.turn = 0;
        }
        let air = &self.air;
        std::thread::scope(|s| {
            for (i, nds) in self.consoles.iter_mut().enumerate() {
                s.spawn(move || {
                    // Console 0 opens every frame, and a console runs
                    // only while it holds the token, so the two
                    // interleave in one reproducible order.
                    air.acquire(i);
                    match inputs[i].touch {
                        Some((x, y)) => nds.touch(x, y),
                        None => nds.release_screen(),
                    }
                    nds.set_keys(inputs[i].keys);
                    nds.run_frame();
                    let mut st = air.state.lock().unwrap();
                    st.seats[i].frame_done = true;
                    // Hand off to the peer if it still owes a frame;
                    // otherwise keep the token so a peer blocked on us
                    // can see that we are done.
                    st.turn = if st.seats[1 - i].frame_done { i } else { 1 - i };
                    air.cv.notify_all();
                });
            }
        });
    }

    /// Capture the whole link — both consoles and the air between them.
    pub fn snapshot(&mut self) -> Result<Snapshot, melonds::Error> {
        let mut consoles = [Vec::new(), Vec::new()];
        for (nds, buf) in self.consoles.iter_mut().zip(consoles.iter_mut()) {
            nds.save_state(buf)?;
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
            turn: st.turn,
        })
    }

    /// Resume from a capture. Simulation continues from the frame *after*
    /// the one that had completed when the snapshot was taken.
    pub fn restore(&mut self, snap: &Snapshot) -> Result<(), melonds::Error> {
        for (nds, buf) in self.consoles.iter_mut().zip(snap.consoles.iter()) {
            nds.load_state(buf)?;
        }
        let mut st = self.air.state.lock().unwrap();
        for i in 0..2 {
            st.seats[i].incoming = snap.incoming[i].iter().cloned().collect();
            st.seats[i].replies = snap.replies[i].iter().cloned().collect();
            st.seats[i].progress = snap.progress[i];
            st.seats[i].attached = snap.attached[i];
            st.seats[i].parked = false;
            st.seats[i].frame_done = false;
        }
        st.turn = snap.turn;
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
        if let Some(slot) = CURRENT.get() {
            *slot.lock().unwrap() = None;
        }
    }
}
