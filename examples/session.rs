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

#[path = "script.rs"]
mod script;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let scripts = [script::parse(&args[2]).0, script::parse(&args[3]).0];
    let session_ticks: u32 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(300);

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");

    // Prime: replay the scripted menu walk until both consoles are in
    // the battle.
    let prime = scripts[0].len().max(scripts[1].len());
    let start = std::time::Instant::now();
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
            Input::keys(if (tick / 7) % 2 == 0 { melonds::keys::B } else { 0 }),
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
