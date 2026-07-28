//! Shared script grammar for the examples: comma-separated
//! `<frames>x<keys>` steps, keys `+`-joined, `<frames>xT<x>:<y>` for a
//! stylus hold, and `@tag` to mark a step boundary.

use melonds_rollback::Input;

pub fn parse_keys(s: &str) -> u32 {
    s.split('+')
        .filter(|k| !k.is_empty())
        .map(|k| match k {
            "A" => melonds::keys::A,
            "B" => melonds::keys::B,
            "X" => melonds::keys::X,
            "Y" => melonds::keys::Y,
            "L" => melonds::keys::L,
            "R" => melonds::keys::R,
            "START" => melonds::keys::START,
            "SELECT" => melonds::keys::SELECT,
            "UP" => melonds::keys::UP,
            "DOWN" => melonds::keys::DOWN,
            "LEFT" => melonds::keys::LEFT,
            "RIGHT" => melonds::keys::RIGHT,
            other => panic!("unknown key {other:?}"),
        })
        .fold(0, |a, b| a | b)
}

pub fn parse(script: &str) -> (Vec<Input>, Vec<(usize, String)>) {
    let mut inputs = Vec::new();
    let mut tags = Vec::new();
    for step in script.split(',') {
        let (count, rest) = step.split_once('x').expect("step must be <frames>x<keys>");
        let count: usize = count.parse().expect("bad frame count");
        let (keys, tag) = match rest.split_once('@') {
            Some((k, t)) => (k, Some(t)),
            None => (rest, None),
        };
        let input = match keys.strip_prefix('T') {
            Some(xy) => {
                let (x, y) = xy.split_once(':').expect("touch step is T<x>:<y>");
                Input::touch(x.parse().unwrap(), y.parse().unwrap())
            }
            None => Input::keys(parse_keys(keys)),
        };
        inputs.extend(std::iter::repeat(input).take(count));
        if let Some(tag) = tag {
            tags.push((inputs.len() - 1, tag.to_owned()));
        }
    }
    (inputs, tags)
}
