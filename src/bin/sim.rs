//! UDP sender for a synthetic ITCH-like stream.
//!
//! Each message goes out as one UDP datagram to --addr. The default target
//! is the multicast group 239.0.0.1:5001; where the host does not route
//! multicast, pass --addr 127.0.0.1:5001 and listen locally instead.

use itch_feed::sim::{generate_stream, StreamConfig};
use std::env;
use std::net::UdpSocket;
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn usage() -> ! {
    eprintln!("usage: sim [--addr HOST:PORT] [--count N] [--rate N] [--seed N]");
    eprintln!("  --addr   destination, default 239.0.0.1:5001");
    eprintln!("  --count  messages to send, default 10000");
    eprintln!("  --rate   messages per second, default 1000");
    eprintln!("  --seed   generator seed, default is derived from the clock");
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
    let stream = generate_stream(&StreamConfig {
        start_seq: 1,
        seed,
        count,
        base_timestamp_ns: base_ns,
    });

    let sock = UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|e| {
        eprintln!("sim: bind failed: {e}");
        exit(1);
    });

    let gap = Duration::from_nanos(1_000_000_000 / rate);
    let mut buf = [0u8; 64]; // largest message is 52 bytes
    println!("sim: sending {count} messages to {addr} at ~{rate}/s (seed {seed})");
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
    println!("sim: done, {count} messages sent");
}
