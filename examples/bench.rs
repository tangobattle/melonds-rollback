//! Breaks a session tick into its parts — plain simulation, snapshot,
//! restore — so optimization work aims at the part that actually costs.
//!
//!     cargo run --release --example bench -- <rom.nds> <save.sav> <primed.link>

use melonds_rollback::{Input, Link, Snapshot};

fn main() {
    // Run on a thread with a large stack: melonDS's construction path
    // puts sizeable temporaries on the caller's stack, and the default
    // main-thread stack is not obviously enough for it.
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(run)
        .expect("spawn")
        .join()
        .expect("bench thread panicked");
}

fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let primed = Snapshot::from_bytes(&std::fs::read(&args[2]).expect("failed to read primed link"))
        .expect("primed link is malformed");

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");
    link.restore(&primed).expect("restore");
    assert!(link.connected(), "cached link is not in a battle");

    const N: usize = 60;

    let start = std::time::Instant::now();
    for _ in 0..N {
        link.tick([Input::default(); 2]);
    }
    let tick = start.elapsed() / N as u32;

    // Reuse one buffer set, the way the session's recycling pool does.
    let mut recycled = Some(link.snapshot().expect("snapshot"));
    let start = std::time::Instant::now();
    for _ in 0..N {
        recycled = Some(link.snapshot_into(recycled.take()).expect("snapshot"));
    }
    let snapshot = start.elapsed() / N as u32;
    let snap = recycled.unwrap();

    let start = std::time::Instant::now();
    for _ in 0..N {
        link.restore(&snap).expect("restore");
    }
    let restore = start.elapsed() / N as u32;

    println!("per tick:     {tick:.2?}  ({:.1} fps)", 1.0 / tick.as_secs_f64());
    println!("per snapshot: {snapshot:.2?}  ({} MiB)", snap.size() >> 20);
    println!("per restore:  {restore:.2?}");
    println!(
        "a session tick that saves once costs ~{:.2?} = {:.2}x realtime",
        tick + snapshot,
        1.0 / ((tick + snapshot).as_secs_f64() * 59.8261)
    );
}
