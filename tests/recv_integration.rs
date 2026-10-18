//! End to end through the receiver core: the Day 1 generator's byte
//! stream is framed and tracked, with and without an injected gap.

use itch_feed::recv::Receiver;
use itch_feed::sim::{generate_stream, StreamConfig};

fn cfg() -> StreamConfig {
    StreamConfig {
        start_seq: 1,
        seed: 9,
        count: 2000,
        base_timestamp_ns: 1_700_000_000_000_000_000,
    }
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
    }
    assert_eq!(rx.stats().open_gaps, 10);
    for d in held {
        rx.handle_datagram(&d).unwrap(); // the "retransmit" arrives late
    }
    let s = rx.stats();
    assert_eq!(s.received, 2000);
    assert_eq!(s.gaps_seen, 1);
    assert_eq!(s.open_gaps, 0);
    assert_eq!(s.duplicates, 0);
}
