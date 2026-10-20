//! End to end through the receiver core: the Day 1 generator's byte
//! stream is framed and tracked, clean, with an injected gap, and with
//! the simulator's loss and reorder knobs turned on.

use itch_feed::msg::Message;
use itch_feed::recv::{Receiver, RetransmitRequest};
use itch_feed::sim::{apply_loss, generate_stream, StreamConfig};

fn cfg() -> StreamConfig {
    StreamConfig {
        start_seq: 1,
        seed: 9,
        count: 2000,
        base_timestamp_ns: 1_700_000_000_000_000_000,
    }
}

/// Feed messages one datagram at a time, draining after each, the way
/// the pipe path drives the receiver.
fn feed(rx: &mut Receiver, msgs: &[Message]) {
    for m in msgs {
        rx.handle_datagram(&m.encode_to_vec()).unwrap();
        rx.drain();
    }
}

/// The dropped seqs a receiver can know about, grouped into ranges. A
/// gap only opens once a later message arrives, so drops past the last
/// delivered message stay invisible; they are the tail.
fn visible_drops(dropped: &[u64], last_delivered: u64) -> (Vec<u64>, Vec<(u64, u64)>) {
    let vis: Vec<u64> = dropped
        .iter()
        .copied()
        .filter(|&s| s < last_delivered)
        .collect();
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for &s in &vis {
        match ranges.last_mut() {
            Some((_, hi)) if *hi + 1 == s => *hi = s,
            _ => ranges.push((s, s)),
        }
    }
    (vis, ranges)
}

#[test]
fn generator_stream_recovers_clean_with_zero_gaps() {
    let stream = generate_stream(&cfg());
    let mut rx = Receiver::new();
    for m in &stream {
        let n = rx
            .handle_datagram(&m.encode_to_vec())
            .expect("valid datagram must frame");
        assert_eq!(n, 1); // sender emits one message per datagram
        rx.drain();
    }
    let s = rx.stats();
    assert_eq!(s.datagrams, 2000);
    assert_eq!(s.received, 2000);
    assert_eq!(s.duplicates, 0);
    assert_eq!(s.gaps_seen, 0);
    assert_eq!(s.open_gaps, 0);
    assert_eq!(s.decode_errors, 0);
    assert_eq!(s.last_seq, 2000);
    assert!(rx.gap_ranges().is_empty());
}

#[test]
fn injected_gap_is_reported() {
    let stream = generate_stream(&cfg());
    let mut rx = Receiver::new();
    for m in &stream {
        let seq = m.header().seq;
        if (500..=509).contains(&seq) {
            continue; // drop ten messages mid-stream
        }
        rx.handle_datagram(&m.encode_to_vec()).unwrap();
        rx.drain();
    }
    let s = rx.stats();
    assert_eq!(s.received, 1990);
    assert_eq!(s.duplicates, 0);
    assert_eq!(s.gaps_seen, 1);
    assert_eq!(s.open_gaps, 10);
    assert_eq!(s.decode_errors, 0);
    assert_eq!(s.last_seq, 2000);
    assert_eq!(rx.gap_ranges(), &[(500, 509)]);
}

#[test]
fn late_fill_after_the_gap_closes_it() {
    let stream = generate_stream(&cfg());
    let mut rx = Receiver::new();
    let mut held = Vec::new();
    for m in &stream {
        let seq = m.header().seq;
        if (500..=509).contains(&seq) {
            held.push(m.encode_to_vec());
            continue;
        }
        rx.handle_datagram(&m.encode_to_vec()).unwrap();
        rx.drain();
    }
    assert_eq!(rx.stats().open_gaps, 10);
    for d in held {
        rx.handle_datagram(&d).unwrap(); // the "retransmit" arrives late
        rx.drain();
    }
    let s = rx.stats();
    assert_eq!(s.received, 2000);
    assert_eq!(s.gaps_seen, 1);
    assert_eq!(s.open_gaps, 0);
    assert_eq!(s.duplicates, 0);
}

#[test]
fn lossy_stream_requests_each_gap_and_counts_the_loss() {
    let loss = apply_loss(generate_stream(&cfg()), 5, 0, 9);
    assert!(!loss.dropped.is_empty());
    let last = loss.messages.last().unwrap().header().seq;
    let (vis, ranges) = visible_drops(&loss.dropped, last);

    let mut rx = Receiver::new();
    feed(&mut rx, &loss.messages);
    let s = rx.stats();
    assert_eq!(s.decode_errors, 0);
    assert_eq!(s.duplicates, 0);
    assert_eq!(s.received as usize, loss.messages.len());
    // one request per visible dropped range, matching it exactly
    let want: Vec<RetransmitRequest> = ranges
        .iter()
        .map(|&(start, end)| RetransmitRequest { start, end })
        .collect();
    assert_eq!(rx.requests(), want.as_slice());
    assert_eq!(s.retransmit_requests, ranges.len() as u64);
    assert_eq!(s.retransmit_requests, s.gaps_seen);
    assert_eq!(s.messages_recovered, 0);
    assert_eq!(s.messages_lost as usize, vis.len());
    assert_eq!(s.open_gaps as usize, vis.len());
    // accounting: generated = delivered + lost + the invisible tail
    let tail = (loss.dropped.len() - vis.len()) as u64;
    assert_eq!(s.received + s.messages_lost + tail, 2000);
}

#[test]
fn retransmit_answers_close_the_accounting() {
    let full = generate_stream(&cfg());
    let loss = apply_loss(generate_stream(&cfg()), 5, 0, 9);
    let last = loss.messages.last().unwrap().header().seq;
    let (vis, _) = visible_drops(&loss.dropped, last);

    let mut rx = Receiver::new();
    feed(&mut rx, &loss.messages);
    // Answer the requests in-process: generation is a pure function of
    // the seed, so the asked-for messages can be rebuilt exactly and
    // sent back, the way a retransmit source would resend them.
    let asked: Vec<u64> = rx.requests().iter().flat_map(|r| r.start..=r.end).collect();
    assert_eq!(asked, vis);
    for m in &full {
        if asked.contains(&m.header().seq) {
            rx.handle_datagram(&m.encode_to_vec()).unwrap();
            rx.drain();
        }
    }
    let s = rx.stats();
    assert_eq!(s.decode_errors, 0);
    assert_eq!(s.duplicates, 0);
    assert_eq!(s.messages_recovered as usize, vis.len());
    assert_eq!(s.messages_lost, 0);
    assert_eq!(s.open_gaps, 0);
    // delivered plus recovered is everything but the invisible tail
    assert_eq!(s.received as usize, loss.messages.len() + vis.len());
}

#[test]
fn reordered_stream_fills_its_own_gaps() {
    let loss = apply_loss(generate_stream(&cfg()), 0, 8, 9);
    assert!(loss.dropped.is_empty());
    assert!(loss.moved > 0);

    let mut rx = Receiver::new();
    feed(&mut rx, &loss.messages);
    let s = rx.stats();
    // The tracker takes its starting point from the first arrival, so
    // whatever the first chunk shuffled in ahead of seq 1 reads as
    // duplicates, not as a gap. Everything after that is accounted for.
    let early = loss.messages[0].header().seq - 1;
    assert_eq!(s.duplicates, early);
    assert_eq!(s.received, 2000 - early);
    assert_eq!(s.received + s.duplicates, 2000);
    assert_eq!(s.decode_errors, 0);
    assert_eq!(s.open_gaps, 0);
    assert_eq!(s.messages_lost, 0);
    // the shuffling opened gaps that the stragglers then filled, and
    // every gap produced exactly one request
    assert!(s.gaps_seen > 0);
    assert!(s.messages_recovered > 0);
    assert_eq!(s.retransmit_requests, s.gaps_seen);
}
