//! Pixel-exactness probe for renderer work: restores a primed link,
//! renders BOTH consoles, drives a deterministic input pattern through
//! the live battle, and folds every presented framebuffer into one
//! hash — plus the final whole-link snapshot bytes, which cover state
//! the compositor can reach through display capture. A renderer change
//! is only correct if both hashes come back bit-identical.
//!
//!     cargo run --release --example fbhash -- <rom.nds> <save.sav> <primed.link> [ticks]

use melonds_rollback::{Input, Link, Snapshot};
use sha2::Digest;

fn main() {
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn")
        .join()
        .expect("fbhash thread panicked");
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let primed = Snapshot::from_bytes(&std::fs::read(&args[2]).expect("failed to read primed link"))
        .expect("primed link is malformed");
    let ticks: u32 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1800);

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");
    link.restore(&primed).expect("restore");
    assert!(link.connected(), "primed link is not in a battle");
    link.set_render([true, true]);

    let mut hasher = sha2::Sha256::new();
    let start = std::time::Instant::now();

    for tick in 0..ticks {
        // Enough input variety to walk both sides through the battle:
        // taps, held directions, buster fire — each on its own period so
        // the scenes keep changing.
        let p0 = Input::keys(match tick % 97 {
            0..=2 => melonds::keys::A,
            10..=40 => melonds::keys::RIGHT,
            50..=52 => melonds::keys::B,
            60..=90 => melonds::keys::LEFT,
            _ => 0,
        });
        let p1 = Input::keys(match tick % 89 {
            0..=2 => melonds::keys::B,
            20..=45 => melonds::keys::UP,
            55..=57 => melonds::keys::A,
            65..=80 => melonds::keys::DOWN,
            _ => 0,
        });
        link.tick([p0, p1]);

        for player in 0..2 {
            if let Some((top, bottom)) = link.console(player).framebuffers() {
                for screen in [top, bottom] {
                    for px in screen {
                        hasher.update(px.to_le_bytes());
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let fb_hash: String = hasher.finalize()[..12].iter().map(|b| format!("{b:02x}")).collect();

    let snap = link.snapshot().expect("snapshot");
    let mut state = sha2::Sha256::new();
    state.update(snap.to_bytes());
    let state_hash: String = state.finalize()[..12].iter().map(|b| format!("{b:02x}")).collect();

    println!("framebuffer hash: {fb_hash}");
    println!("final state hash: {state_hash}");
    println!(
        "{} ticks in {:.2?} ({:.2}ms/tick, both consoles rendering), connected={}",
        ticks,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / ticks as f64,
        link.connected(),
    );
}
