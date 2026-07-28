//! Drives a [`melonds_rollback::Link`] through a scripted wireless
//! session — the harness that proved BN5 Double Team DS netbattle works
//! between two in-process consoles, and that the link rolls back.
//!
//!     cargo run --release --example wireless -- [--rollback <frame>[:<len>]] \
//!         <rom.nds> <save.sav> <script0> <script1> <outdir>
//!
//! Scripts are comma-separated steps: `<frames>x<keys>` with keys
//! `+`-joined (A B X Y L R START SELECT UP DOWN LEFT RIGHT), or
//! `<frames>xT<x>:<y>` to hold the stylus. `@tag` on a step dumps that
//! console's screens when the step ends.
//!
//! `--rollback <frame>[:<len>]` snapshots the whole link at `frame`,
//! runs `len` frames and digests both consoles' RAM, restores, replays
//! the same span, and compares — a live wireless session must come back
//! bit-identical.

use melonds_rollback::Link;
use sha2::Digest;

#[path = "common/script.rs"]
mod script;

use script::parse as parse_script;

fn dump(link: &mut Link, player: usize, path: &std::path::Path) {
    let (w, h) = (melonds::SCREEN_WIDTH as u32, melonds::SCREEN_HEIGHT as u32);
    let mut img = image::RgbaImage::new(w, h * 2);
    if let Some((top, bottom)) = link.console(player).framebuffers() {
        for (i, screen) in [top, bottom].into_iter().enumerate() {
            for y in 0..h {
                for x in 0..w {
                    let [b, g, r, _] = screen[(y * w + x) as usize].to_le_bytes();
                    img.put_pixel(x, y + i as u32 * h, image::Rgba([r, g, b, 0xff]));
                }
            }
        }
    }
    img.save(path).expect("failed to write png");
    println!("dumped {}", path.display());
}

fn digest(link: &mut Link) -> String {
    let mut hasher = sha2::Sha256::new();
    for player in 0..2 {
        hasher.update(&*link.console(player).main_ram());
    }
    hasher.finalize()[..8].iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // --rollback <frame>[:<replay_len>], replay_len defaults to 120.
    let rollback = args.iter().position(|a| a == "--rollback").map(|i| {
        let spec = args[i + 1].clone();
        args.drain(i..=i + 1);
        match spec.split_once(':') {
            Some((at, len)) => (at.parse().unwrap(), len.parse().unwrap()),
            None => (spec.parse().unwrap(), 120usize),
        }
    });

    let rom = std::fs::read(&args[0]).expect("failed to read rom");
    let save = std::fs::read(&args[1]).expect("failed to read save");
    let scripts = [parse_script(&args[2]), parse_script(&args[3])];
    let outdir = std::path::PathBuf::from(&args[4]);
    std::fs::create_dir_all(&outdir).unwrap();

    let mut link = Link::new(&rom, [Some(&save), Some(&save)], (2026, 1, 1, 0, 0, 0)).expect("cart rejected");

    let total = scripts[0].0.len().max(scripts[1].0.len());
    let start = std::time::Instant::now();
    let mut snapshot = None;
    let mut first_digest = None;
    let mut frame = 0usize;

    while frame < total {
        let inputs = [0, 1].map(|i| scripts[i].0.get(frame).copied().unwrap_or_default());
        link.tick(inputs);

        for (i, (_, tags)) in scripts.iter().enumerate() {
            for (_, tag) in tags.iter().filter(|(at, _)| *at == frame) {
                dump(&mut link, i, &outdir.join(format!("i{i}_{tag}.png")));
            }
        }

        if let Some((at, len)) = rollback {
            if frame == at && snapshot.is_none() {
                let snap = link.snapshot().expect("snapshot");
                println!(
                    "rollback: captured link at frame {frame} ({} MiB, digest {})",
                    snap.size() >> 20,
                    digest(&mut link)
                );
                snapshot = Some(snap);
            }
            if frame == at + len {
                let d = digest(&mut link);
                match first_digest {
                    None => {
                        println!("rollback: first pass digest {d}");
                        link.restore(snapshot.as_ref().unwrap()).expect("restore");
                        first_digest = Some(d);
                        // The snapshot was taken AFTER frame `at` ran,
                        // so the replay resumes at the next frame —
                        // resuming at `at` would re-run a frame the
                        // first pass never ran and diverge by
                        // construction.
                        frame = at + 1;
                        continue;
                    }
                    Some(ref first) => println!(
                        "rollback: replay digest {d} -> {}",
                        if *first == d { "OK (bit-identical)" } else { "MISMATCH" }
                    ),
                }
            }
        }

        if frame % 2000 == 1999 {
            println!("frame {frame}: connected={}", link.connected());
        }
        frame += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "{} frames in {:.2?} ({:.2}x realtime/link), connected={}",
        total,
        elapsed,
        total as f64 / elapsed.as_secs_f64() / 59.8261,
        link.connected(),
    );
}
