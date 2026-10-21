//! Benchmarks for the three hot paths: decoding, the ring, and the whole
//! receive pipeline.
//!
//! Every input is built before the clock starts: the streams are generated
//! and encoded up front, and the receiver bench builds its `Receiver`
//! outside the timed section. Only the operation itself is measured.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use itch_feed::msg::Message;
use itch_feed::recv::Receiver;
use itch_feed::ring::RingBuffer;
use itch_feed::sim::{generate_stream, StreamConfig};
use std::time::Instant;

fn stream(count: usize, seed: u64) -> Vec<Message> {
    generate_stream(&StreamConfig {
        start_seq: 1,
        seed,
        count,
        base_timestamp_ns: 1_700_000_000_000_000_000,
    })
}

/// Decode a pre-encoded buffer holding a representative mix of all ten
/// message types, back to back, the way a packed datagram would carry them.
fn bench_decode(c: &mut Criterion) {
    let msgs = stream(1024, 7);
    let mut wire = Vec::new();
    for m in &msgs {
        wire.extend_from_slice(&m.encode_to_vec());
    }
    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Elements(msgs.len() as u64));
    group.bench_function("message_mix", |b| {
        b.iter(|| {
            let mut rest = &wire[..];
            let mut sum = 0u64;
            while !rest.is_empty() {
                let (m, n) = Message::decode(rest).unwrap();
                sum += m.header().seq;
                rest = &rest[n..];
            }
            black_box(sum)
        })
    });
    group.finish();
}

/// Push a batch through the ring and pop it back off, single-threaded.
/// The ring returns to empty at the end of every iteration, so iterations
/// measure the same thing.
fn bench_ring(c: &mut Criterion) {
    let msgs = stream(1024, 11);
    let ring = RingBuffer::new(4096);
    let mut group = c.benchmark_group("ring");
    group.throughput(Throughput::Elements(msgs.len() as u64));
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            for m in &msgs {
                ring.try_push(*m).unwrap();
            }
            let mut last = 0u64;
            while let Some(m) = ring.try_pop() {
                last = m.header().seq;
            }
            black_box(last)
        })
    });
    group.finish();
}

/// The full receive path for a clean stream: frame each datagram, queue
/// the message in the ring, drain into the sequence tracker. Same cadence
/// as the socket loop. The `Receiver` is built outside the timed section;
/// feeding it the same stream twice would change what it does (the second
/// pass would be all duplicates), so each timed run gets a fresh one.
fn bench_receiver(c: &mut Criterion) {
    let msgs = stream(4096, 42);
    let datagrams: Vec<Vec<u8>> = msgs.iter().map(|m| m.encode_to_vec()).collect();
    let mut group = c.benchmark_group("receiver");
    group.throughput(Throughput::Elements(msgs.len() as u64));
    group.bench_function("clean_stream", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut rx = Receiver::new();
                let start = Instant::now();
                for d in &datagrams {
                    rx.handle_datagram(d).unwrap();
                    rx.drain();
                }
                total += start.elapsed();
                black_box(rx.stats().received);
            }
            total
        })
    });
    group.finish();
}

criterion_group!(benches, bench_decode, bench_ring, bench_receiver);
criterion_main!(benches);
