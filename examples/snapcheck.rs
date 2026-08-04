//! Exactness probe for incremental captures: a capture that moved only
//! the pages a console has written must be the capture that moved all
//! of them.
//!
//! A session snapshots every tick into a buffer it filled a rollback
//! window ago, and restores out of a buffer it filled itself, so both
//! sides of those copies already agree about nearly every page. Moving
//! only the rest is worth two thirds of a snapshot and more than half
//! of a restore — and it is a claim about what the console did, which
//! is exactly the kind of claim that is right until some seam quietly
//! makes it wrong. A page the record forgets is a byte a peer never
//! sees change: a desync, and an intermittent one, since which pages
//! get skipped depends on when the snapshot happened to be taken.
//!
//! So both directions are checked against a capture that assumes
//! nothing:
//!
//! * every tick, capture into a stale buffer *and* into a fresh one,
//!   and compare the bytes;
//! * every few ticks, restore from a stale buffer and check that the
//!   console then serializes to that buffer's own bytes.
//!
//! The buffers live in a ring, so captures land several ticks after the
//! state they are replacing — which is the case a session creates and
//! the one a single-tick check would miss.
//!
//!     cargo run --release --example snapcheck -- <rom.nds> <save.sav> <primed.link> [ticks]

use melonds_rollback::{Input, Link, Snapshot};

/// How many captures stay live at once — a rollback window's worth.
const WINDOW: usize = 8;
/// Ticks between restore checks.
const RESTORE_EVERY: u32 = 5;

fn main() {
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn")
        .join()
        .expect("snapcheck thread panicked");
}

/// Deterministic input variety, the same pattern `fbhash` drives.
fn inputs(tick: u32) -> [Input; 2] {
    [
        Input::keys(match tick % 97 {
            0..=2 => melonds::keys::A,
            10..=40 => melonds::keys::RIGHT,
            50..=52 => melonds::keys::B,
            60..=90 => melonds::keys::LEFT,
            _ => 0,
        }),
        Input::keys(match tick % 89 {
            0..=2 => melonds::keys::B,
            20..=45 => melonds::keys::UP,
            55..=57 => melonds::keys::A,
            65..=80 => melonds::keys::DOWN,
            _ => 0,
        }),
    ]
}

fn report(what: &str, tick: u32, player: usize, incremental: &[u8], whole: &[u8]) -> bool {
    if incremental == whole {
        return false;
    }
    let at = incremental.iter().zip(whole).position(|(a, b)| a != b);
    let differing = incremental.iter().zip(whole).filter(|(a, b)| a != b).count();
    println!(
        "  MISMATCH: {what}, tick {tick} console {player}: \
         {} vs {} bytes, first difference at {at:?}, {differing} differ",
        incremental.len(),
        whole.len(),
    );
    true
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let primed = Snapshot::from_bytes(&std::fs::read(&args[2]).expect("failed to read primed link"))
        .expect("primed link is malformed");
    let ticks: u32 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(600);

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");
    link.restore(&primed).expect("restore");
    assert!(link.connected(), "cached link is not in a battle");
    // Nothing here looks at pixels, and a blind tick is the one a
    // re-simulation runs.
    link.set_render([false, false]);

    let mut ring: Vec<Snapshot> = (0..WINDOW).map(|_| link.snapshot().expect("snapshot")).collect();
    let mut mismatches = 0u32;

    for tick in 0..ticks {
        link.tick(inputs(tick));

        // The whole-copy capture first, so the incremental one is the
        // one asked to reproduce it rather than the other way round.
        let slot = tick as usize % WINDOW;
        let stale = std::mem::replace(&mut ring[slot], link.snapshot().expect("whole capture"));
        let whole = ring[slot].clone();
        ring[slot] = link.snapshot_into(Some(stale)).expect("incremental capture");
        for player in 0..2 {
            if report(
                "capture",
                tick,
                player,
                ring[slot].console_bytes(player),
                whole.console_bytes(player),
            ) {
                mismatches += 1;
            }
        }

        if tick % RESTORE_EVERY == RESTORE_EVERY - 1 {
            // The oldest buffer in the ring: the deepest restore a
            // window this size can ask for.
            let from = ring[(tick as usize + 1) % WINDOW].clone();
            link.restore(&from).expect("restore");
            let after = link.snapshot().expect("whole capture");
            for player in 0..2 {
                if report(
                    "restore",
                    tick,
                    player,
                    after.console_bytes(player),
                    from.console_bytes(player),
                ) {
                    mismatches += 1;
                }
            }
        }
    }

    if mismatches > 0 {
        println!("{mismatches} mismatches over {ticks} ticks");
        std::process::exit(1);
    }
    println!("{ticks} ticks: every incremental capture and restore byte-identical");
}
