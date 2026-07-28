# melonds-rollback

Rollback netplay over a *link* of two emulated Nintendo DSes, built on
[melonds-rs](https://github.com/tangobattle/melonds-rs). The DS analogue of
`mgba-rollback`.

- **`Link`** — two consoles in one process talking over emulated local
  wireless. The games run their real wireless protocol; nothing is spoofed.
  Exactly one console executes at a time and every handoff is a function of
  emulated state, so a link is a pure function of its inputs and snapshots
  as a unit (both consoles *plus* the frames in flight on the air).
- **`session::Session`** — the [getgud](https://github.com/tangobattle/getgud)
  rollback loop over that link: predict the peer, simulate ahead, restore and
  re-simulate on a misprediction.

Verified on MegaMan Battle Network 5: Double Team DS — two consoles complete
the game's own NetBattle flow into a link battle, the link replays
bit-identically across a snapshot/restore, and the session rolls back live
mispredictions without dropping the connection.

License: GPL-3.0-or-later (the melonDS core's license governs the combined work).
