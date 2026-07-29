//! Regression proof for overlapping links: a stale [`Link`] dropping
//! AFTER a newer one boots must not cut the newer one off the air.
//!
//! That is exactly what a host does across a session swap — the old
//! session's audio pull keeps its link alive until the device rebinds,
//! so the old link's drop lands after the new match's link exists. The
//! air routing is per-link serial now; before that, the drop cleared
//! the process-global routing and the new pair's wireless went dead
//! mid-priming.
//!
//!     cargo run --release --example overlap -- <rom.nds> <save.sav> <primed.link>

use melonds_rollback::{Input, Link, Snapshot};

fn main() {
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn")
        .join()
        .expect("overlap thread panicked");
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let primed = Snapshot::from_bytes(&std::fs::read(&args[2]).expect("failed to read primed link"))
        .expect("primed link is malformed");

    // The stale link: booted, then kept alive across the new link's
    // creation — the session-swap shape.
    let stale = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");

    let mut live = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");
    drop(stale);

    live.restore(&primed).expect("restore");
    assert!(live.connected(), "primed link is not in a battle");

    // A battle only stays connected if the air still routes: dead
    // airwaves feed the client "host gone" and the game tears the
    // session down within a few frames.
    for _ in 0..300 {
        live.tick([Input::default(); 2]);
    }
    assert!(
        live.connected(),
        "the battle dropped: a stale link's teardown severed the live link's air"
    );
    println!("overlap: stale link dropped, live link still connected after 300 ticks");
}
