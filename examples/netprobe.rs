//! Two-process netplay probe: the app-shaped test the single-process
//! examples can't be. Each process restores the same primed link,
//! drives its own [`Session`] as one seat, and cross-feeds real inputs
//! over TCP with whatever latency the wire has — so mispredictions,
//! rollbacks, speculation limits and the game's own custom-screen
//! resync all happen the way they do in a real match.
//!
//! The pass condition is the game's own: the emulated wireless session
//! must stay up. A protocol slip inside the simulation makes the game
//! tear its MP session down ("communication error"), which shows here
//! as `connected()` going false.
//!
//!     # terminal 1
//!     cargo run --release --example netprobe -- host 127.0.0.1:9155 <rom> <save> <primed.link> [ticks] [latency]
//!     # terminal 2
//!     cargo run --release --example netprobe -- join 127.0.0.1:9155 <rom> <save> <primed.link> [ticks] [latency]
//!
//! `latency_ms` (default 0) holds each received input for that long
//! before applying it — loopback TCP is far faster than a real wire,
//! and without held-back inputs prediction is almost never wrong,
//! which is exactly the stress a probe must not skip. The loop paces
//! at 60fps like a real host, so wall latency and tick latency agree.

use std::io::{Read, Write};

use melonds_rollback::session::Session;
use melonds_rollback::{Input, Link, Snapshot};

#[path = "common/script.rs"]
mod script;

fn main() {
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn")
        .join()
        .expect("netprobe thread panicked");
}

/// The local seat's input for a tick — deterministic, per seat, and
/// deliberately busy: chip-select mashing (A/B plus cursor movement)
/// exercises the custom screen's synchronized close, which idle probes
/// never touch.
fn scripted(seat: usize, tick: u32) -> Input {
    let phase = tick % 96;
    let keys = if seat == 0 {
        match phase {
            0..=7 => melonds::keys::A,
            24..=31 => melonds::keys::RIGHT,
            48..=55 => melonds::keys::A,
            72..=79 => melonds::keys::B,
            _ => 0,
        }
    } else {
        match phase {
            8..=15 => melonds::keys::A,
            32..=39 => melonds::keys::LEFT,
            56..=63 => melonds::keys::B,
            80..=87 => melonds::keys::A,
            _ => 0,
        }
    };
    Input::keys(keys)
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let role = &args[0];
    let addr = &args[1];
    let rom = std::fs::read(&args[2]).expect("failed to read rom");
    let save = std::fs::read(&args[3]).expect("failed to read save");
    // Either a cached primed link, or `walk <script0> <script1>` to do
    // the whole flow the way a real match does — boot cold and drive
    // the game's own menus into the battle before the session takes
    // over. The cached-restore path skips that seam entirely.
    let (primed, rest) = if args[4] == "walk" {
        (None, 7)
    } else {
        (
            Some(
                Snapshot::from_bytes(&std::fs::read(&args[4]).expect("failed to read primed link"))
                    .expect("primed link is malformed"),
            ),
            5,
        )
    };
    let ticks: u32 = args.get(rest).map(|s| s.parse().unwrap()).unwrap_or(3600);
    let latency = std::time::Duration::from_millis(args.get(rest + 1).map(|s| s.parse().unwrap()).unwrap_or(0));

    let local_player = match role.as_str() {
        "host" => 0,
        "join" => 1,
        other => panic!("role must be host or join, not {other:?}"),
    };

    let mut stream = if local_player == 0 {
        let listener = std::net::TcpListener::bind(addr).expect("bind");
        println!("netprobe: waiting for peer on {addr}");
        listener.accept().expect("accept").0
    } else {
        loop {
            match std::net::TcpStream::connect(addr) {
                Ok(s) => break s,
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(200)),
            }
        }
    };
    stream.set_nodelay(true).expect("nodelay");
    stream.set_nonblocking(true).expect("nonblocking");

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");
    match &primed {
        Some(primed) => link.restore(primed).expect("restore"),
        None => {
            let scripts = [
                script::parse(&std::fs::read_to_string(&args[5]).expect("script0")).0,
                script::parse(&std::fs::read_to_string(&args[6]).expect("script1")).0,
            ];
            let total = scripts[0].len().max(scripts[1].len());
            let start = std::time::Instant::now();
            for frame in 0..total {
                let inputs = [0, 1].map(|i| scripts[i].get(frame).copied().unwrap_or_default());
                link.tick(inputs);
            }
            println!("netprobe {role}: walked {total} frames in {:.1?}", start.elapsed());
        }
    }
    assert!(link.connected(), "the pair did not reach a connected battle");

    let mut session = Session::new(link, local_player, 2).expect("session");

    let mut rollbacks = 0u32;
    let mut deepest = 0u32;
    let mut sent = Vec::new();
    let mut rx = Vec::new();
    // (apply_at, keys, advantage): received inputs held until the wall
    // clock passes apply_at, simulating wire latency. Wall-based, not
    // tick-based: a stalled session must still receive inputs or the
    // stall never resolves.
    let mut held: std::collections::VecDeque<(std::time::Instant, u32, i16)> = std::collections::VecDeque::new();
    let mut tick = 0u32;
    let start = std::time::Instant::now();
    let mut next_frame = std::time::Instant::now();

    while tick < ticks {
        // Drain whatever the peer has sent: 7 bytes per input packet.
        let mut buf = [0u8; 1024];
        match stream.read(&mut buf) {
            // A peer that finished its run closes the socket; within
            // the final stretch that's a pass, not a failure.
            Ok(0) if tick + 600 >= ticks => break,
            Ok(0) => panic!("peer hung up at tick {tick}"),
            Ok(n) => rx.extend_from_slice(&buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset && tick + 600 >= ticks => break,
            Err(e) => panic!("read: {e}"),
        }
        while rx.len() >= 7 {
            let keys = u32::from_le_bytes(rx[0..4].try_into().unwrap());
            let advantage = i16::from_le_bytes(rx[4..6].try_into().unwrap());
            let _seq = rx[6];
            rx.drain(0..7);
            held.push_back((std::time::Instant::now() + latency, keys, advantage));
        }
        let now = std::time::Instant::now();
        while held.front().is_some_and(|(at, _, _)| *at <= now) {
            let (_, keys, advantage) = held.pop_front().unwrap();
            session.add_remote_input(Input::keys(keys), advantage);
        }

        // One frame per 60fps slot, the way a real host paces; skip
        // the advance (but keep draining the wire) while speculation
        // is capped and nothing is confirmable.
        if std::time::Instant::now() < next_frame {
            std::thread::sleep(std::time::Duration::from_millis(1));
            continue;
        }
        next_frame += std::time::Duration::from_micros(16_713);
        if next_frame < std::time::Instant::now() {
            next_frame = std::time::Instant::now();
        }
        if session.matchable() > 0 || session.local_queue_length() < 10 {
            let local = scripted(local_player, tick);
            let (outgoing, report) = session.advance(local).expect("advance");
            if report.rollback_depth > 0 {
                rollbacks += 1;
                deepest = deepest.max(report.rollback_depth);
            }
            sent.clear();
            sent.extend_from_slice(&outgoing.input.keys.to_le_bytes());
            sent.extend_from_slice(&outgoing.tick_advantage.to_le_bytes());
            sent.push(0);
            stream.write_all(&sent).expect("write");
            tick += 1;

            if tick % 600 == 0 {
                let connected = session.with_link(|l| l.connected());
                println!(
                    "tick {tick}: connected={connected} rollbacks={rollbacks} deepest={deepest} skew={}",
                    session.skew()
                );
                assert!(connected, "the emulated wireless session dropped (communication error)");
            }
        }
    }

    let connected = session.with_link(|l| l.connected());
    let elapsed = start.elapsed();
    println!(
        "netprobe {role}: {ticks} ticks in {elapsed:.2?} ({:.2}x realtime), {rollbacks} rollbacks (deepest {deepest}), connected={connected}",
        ticks as f64 / elapsed.as_secs_f64() / 59.8261,
    );
    assert!(connected, "the emulated wireless session dropped (communication error)");
}
