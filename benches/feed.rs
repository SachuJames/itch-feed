//! Benchmarks for the hot paths: the codec both ways, the ring on one
//! thread and on two, the sequence tracker, and the whole receive
//! pipeline.
//!
//! Every input is built before the clock starts: the streams are generated
//! and encoded up front, and the receiver bench builds its `Receiver`
//! outside the timed section. Only the operation itself is measured.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use itch_feed::msg::Message;
use itch_feed::recv::{Receiver, SeqTracker};
use itch_feed::ring::RingBuffer;
use itch_feed::sim::{generate_stream, StreamConfig};
use std::time::{Duration, Instant};

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

/// Encode a prepared batch into one reused scratch buffer, the way a
/// sender with a fixed transmit buffer would.
fn bench_encode(c: &mut Criterion) {
    let msgs = stream(1024, 3);
    let mut group = c.benchmark_group("encode");
    group.throughput(Throughput::Elements(msgs.len() as u64));
    group.bench_function("message_mix", |b| {
        let mut buf = vec![0u8; 4096];
        b.iter(|| {
            let mut written = 0usize;
            for m in &msgs {
                written += m.encode(&mut buf).unwrap();
            }
            black_box(written)
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

/// The ring doing the job it exists for: a producer thread pushing while
/// the consumer thread (this one) pops, a fixed count per run. Both sides
/// spin on yield when the ring is full or empty, like the socket loop and
/// its consumer would.
fn bench_ring_threads(c: &mut Criterion) {
    use std::sync::Arc;
    const N: usize = 50_000;
    let msgs = stream(N, 23);
    let mut group = c.benchmark_group("ring");
    group.throughput(Throughput::Elements(N as u64));
    group.bench_function("two_thread", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let ring = Arc::new(RingBuffer::new(4096));
                let prod_ring = Arc::clone(&ring);
                let prod_msgs = msgs.clone();
                let start = Instant::now();
                let producer = std::thread::spawn(move || {
                    for m in &prod_msgs {
                        let mut item = *m;
                        loop {
                            match prod_ring.try_push(item) {
                                Ok(()) => break,
                                Err(back) => {
                                    item = back;
                                    std::thread::yield_now();
                                }
                            }
                        }
                    }
                });
                let mut popped = 0usize;
                let mut last = 0u64;
                while popped < N {
                    match ring.try_pop() {
                        Some(m) => {
                            last = m.header().seq;
                            popped += 1;
                        }
                        None => std::thread::yield_now(),
                    }
                }
                producer.join().unwrap();
                total += start.elapsed();
                black_box(last);
            }
            total
        })
    });
    group.finish();
}

/// Sequence tracking on its own. Two streams of the same length: one
/// arrives in order, and one delivers every block of 500 rotated, first
/// message of the block last, so each block opens a gap and the late
/// arrival then fills it. The trackers are built by the harness outside
/// the timed section; only the `observe` calls are measured.
fn bench_tracker(c: &mut Criterion) {
    const N: u64 = 8192;
    let in_order: Vec<u64> = (1..=N).collect();
    // seq 1 first, to set the tracker's window; after that, rotated blocks
    let mut gapped: Vec<u64> = vec![1];
    let mut block_start = 2u64;
    while block_start <= N {
        let block_end = (block_start + 499).min(N);
        if block_end > block_start {
            gapped.extend((block_start + 1)..=block_end);
            gapped.push(block_start);
        } else {
            gapped.push(block_start);
        }
        block_start = block_end + 1;
    }
    let mut group = c.benchmark_group("tracker");
    group.throughput(Throughput::Elements(N));
    for (name, seqs) in [("in_order", &in_order), ("gapped", &gapped)] {
        group.bench_function(name, |b| {
            b.iter_batched(
                SeqTracker::new,
                |mut tracker| {
                    let mut requests = 0u64;
                    for &seq in seqs.iter() {
                        if tracker.observe(seq).is_some() {
                            requests += 1;
                        }
                    }
                    black_box(requests)
                },
                BatchSize::SmallInput,
            )
        });
    }
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

criterion_group!(
    benches,
    bench_decode,
    bench_encode,
    bench_ring,
    bench_ring_threads,
    bench_tracker,
    bench_receiver
);
criterion_main!(benches);
