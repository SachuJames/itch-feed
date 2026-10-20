//! UDP sender for a synthetic ITCH-like stream.
//!
//! Each message goes out as one UDP datagram to --addr. The default target
//! is the multicast group 239.0.0.1:5001; where the host does not route
//! multicast, pass --addr 127.0.0.1:5001 and listen locally instead.

use itch_feed::sim::{apply_loss, generate_stream, StreamConfig};
use std::env;
use std::net::UdpSocket;
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn usage() -> ! {
    eprintln!("usage: sim [--addr HOST:PORT] [--count N] [--rate N] [--seed N] [--drop-rate P] [--reorder W] [--stdout]");
    eprintln!("  --addr      destination, default 239.0.0.1:5001");
    eprintln!("  --count     messages to send, default 10000");
    eprintln!("  --rate      messages per second, default 1000");
    eprintln!("  --seed      generator seed, default is derived from the clock");
    eprintln!("  --drop-rate percent of messages to drop, 0 to 100, default 0");
    eprintln!("  --reorder   shuffle window for out-of-order delivery, default 0 (off)");
    eprintln!("  --stdout    write encoded bytes to stdout instead of UDP");
    exit(2);
}

fn parse_u64(s: &str) -> u64 {
    s.parse().unwrap_or_else(|_| usage())
}

fn parse_usize(s: &str) -> usize {
    s.parse().unwrap_or_else(|_| usage())
}

fn main() {
    let mut addr = String::from("239.0.0.1:5001");
    let mut count: usize = 10_000;
    let mut rate: u64 = 1_000;
    let mut seed: Option<u64> = None;
    let mut drop_rate: u64 = 0;
    let mut reorder: usize = 0;
    let mut stdout = false;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                addr = args[i].clone();
            }
            "--count" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                count = parse_usize(&args[i]);
            }
            "--rate" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                rate = parse_u64(&args[i]);
            }
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                seed = Some(parse_u64(&args[i]));
            }
            "--drop-rate" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                drop_rate = parse_u64(&args[i]);
                if drop_rate > 100 {
                    usage()
                }
            }
            "--reorder" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                reorder = parse_usize(&args[i]);
            }
            "--stdout" => stdout = true,
            _ => usage(),
        }
        i += 1;
    }
    if rate == 0 {
        usage();
    }

    let base_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seed = seed.unwrap_or_else(|| base_ns ^ (std::process::id() as u64));
    let generated = generate_stream(&StreamConfig {
        start_seq: 1,
        seed,
        count,
        base_timestamp_ns: base_ns,
    });
    // The knobs work on the generated list, so the stdout and UDP paths
    // lose and shuffle exactly the same messages for a given seed.
    let loss = apply_loss(generated, drop_rate, reorder, seed);
    if drop_rate > 0 || reorder > 1 {
        eprintln!(
            "sim: loss knobs: {} dropped ({}%), {} moved (window {})",
            loss.dropped.len(),
            drop_rate,
            loss.moved,
            reorder
        );
    }
    let stream = loss.messages;
    let count = stream.len();

    let mut buf = [0u8; 64]; // largest message is 52 bytes

    if stdout {
        // raw wire bytes on stdout, status on stderr so the pipe stays clean
        use std::io::Write;
        let mut out = std::io::stdout().lock();
        for m in &stream {
            let n = m
                .encode(&mut buf)
                .expect("64-byte buffer fits every message");
            if let Err(e) = out.write_all(&buf[..n]) {
                eprintln!("sim: stdout write failed: {e}");
                exit(1);
            }
        }
        eprintln!("sim: wrote {count} messages to stdout (seed {seed})");
        return;
    }

    let sock = UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|e| {
        eprintln!("sim: bind failed: {e}");
        exit(1);
    });

    eprintln!("sim: sending {count} messages to {addr} at ~{rate}/s (seed {seed})");
    let gap = Duration::from_nanos(1_000_000_000 / rate);
    for m in &stream {
        let n = m
            .encode(&mut buf)
            .expect("64-byte buffer fits every message");
        if let Err(e) = sock.send_to(&buf[..n], &addr) {
            eprintln!("sim: send failed: {e}");
            exit(1);
        }
        std::thread::sleep(gap);
    }
    eprintln!("sim: done, {count} messages sent");
}
