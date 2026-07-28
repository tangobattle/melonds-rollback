//! Runs a rollback [`Session`] over a link that has been walked into a
//! live netbattle, feeding the remote side late and wrong on purpose so
//! the engine has to roll back and re-simulate.
//!
//!     cargo run --release --example session -- <rom.nds> <save.sav> <script0> <script1> [prime_frames]
//!
//! Scripts use the same grammar as the `wireless` example. They are
//! replayed first to prime the link into a battle; the session then
//! takes over and drives it.

use melonds_rollback::session::Session;
use melonds_rollback::{Input, Link};

#[path = "common/script.rs"]
mod script;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let scripts = [script::parse(&args[2]).0, script::parse(&args[3]).0];
    let session_ticks: u32 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(300);
    // Walking the menus into a battle takes minutes, and the result is
    // just a link snapshot — so cache it and restore instead.
    let cache = args.get(5).map(std::path::PathBuf::from);
    // How often the simulated peer changes its input. Real play holds a
    // button for many frames at a time, so repeat-last prediction is
    // usually right; a small period here is a deliberately hostile test.
    let flip: u32 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(7);

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");

    // Prime: restore a cached primed link if there is one, else replay
    // the scripted menu walk until both consoles are in the battle.
    let start = std::time::Instant::now();
    let cached = cache
        .as_ref()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|bytes| melonds_rollback::Snapshot::from_bytes(&bytes));
    match cached {
        Some(snap) => {
            link.restore(&snap).expect("restore primed link");
            println!("restored primed link in {:.1?}, connected={}", start.elapsed(), link.connected());
        }
        None => {
            let prime = scripts[0].len().max(scripts[1].len());
            for frame in 0..prime {
                let inputs = [0, 1].map(|i| scripts[i].get(frame).copied().unwrap_or_default());
                link.tick(inputs);
            }
            println!(
                "primed {} frames in {:.1?}, connected={}",
                prime,
                start.elapsed(),
                link.connected()
            );
            if let Some(path) = &cache {
                let snap = link.snapshot().expect("snapshot");
                std::fs::write(path, snap.to_bytes()).expect("write cache");
                println!("cached primed link to {}", path.display());
            }
        }
    }
    assert!(link.connected(), "priming did not reach a connected battle");

    // Hand the primed link to the rollback engine as player 0.
    let mut session = Session::new(link, 0, 2).expect("session");

    let mut rollbacks = 0;
    let mut deepest = 0;
    let mut pending: Vec<(u32, Input)> = Vec::new();
    let start = std::time::Instant::now();

    for tick in 0..session_ticks {
        // Hold A every other stretch, so the local side has real input.
        let local = Input::keys(if (tick / 30) % 2 == 0 { melonds::keys::A } else { 0 });
        let (outgoing, report) = session.advance(local).expect("advance");
        if report.rollback_depth > 0 {
            rollbacks += 1;
            deepest = deepest.max(report.rollback_depth);
        }

        // The peer's input arrives three ticks late and differs from the
        // repeat-last prediction half the time, which is exactly the
        // case rollback exists for.
        pending.push((
            outgoing.tick,
            Input::keys(if (tick / flip) % 2 == 0 { melonds::keys::B } else { 0 }),
        ));
        if pending.len() > 3 {
            let (_, input) = pending.remove(0);
            session.add_remote_input(input, 0);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "session: {} ticks in {:.2?} ({:.2}x realtime), {} rollbacks (deepest {}), skew {}",
        session_ticks,
        elapsed,
        session_ticks as f64 / elapsed.as_secs_f64() / 59.8261,
        rollbacks,
        deepest,
        session.skew(),
    );
    println!("still connected: {}", session.with_link(|l| l.connected()));
}
