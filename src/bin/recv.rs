//! Receiver for the synthetic ITCH-like stream.
//!
//! Two ways to run it:
//!
//! - multicast/unicast mode: bind a UDP socket on the port of --addr and
//!   join the group when --addr is multicast (the default
//!   239.0.0.1:5001, matching sim). Stops on --count, an idle timeout,
//!   or Ctrl-C.
//! - pipe mode: --pipe reads raw datagram bytes from stdin instead of the
//!   network. The sim binary can produce them with --stdout, so the
//!   receiver stays exercisable where UDP is blocked.
//!
//! Run stats print to stdout at exit, one fact per line.

use itch_feed::recv::{run_socket_loop, Receiver, SocketOpts};
use std::env;
use std::io::Read;
use std::process::exit;

fn usage() -> ! {
    eprintln!("usage: recv [--addr HOST:PORT] [--pipe] [--count N] [--idle-secs N]");
    eprintln!("  --addr       listen target, default 239.0.0.1:5001");
    eprintln!("  --pipe       read datagram bytes from stdin instead of UDP");
    eprintln!("  --count      stop after N received messages, default none");
    eprintln!("  --idle-secs  stop after N quiet seconds, default 5");
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
    let mut pipe = false;
    let mut count: Option<usize> = None;
    let mut idle_secs: u64 = 5;

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
            "--pipe" => pipe = true,
            "--count" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                count = Some(parse_usize(&args[i]));
            }
            "--idle-secs" => {
                i += 1;
                if i >= args.len() {
                    usage()
                }
                idle_secs = parse_u64(&args[i]);
            }
            _ => usage(),
        }
        i += 1;
    }

    let stats = if pipe {
        let mut stdin = std::io::stdin().lock();
        let mut bytes = Vec::new();
        if let Err(e) = stdin.read_to_end(&mut bytes) {
            eprintln!("recv: stdin read failed: {e}");
            exit(1);
        }
        let mut rx = Receiver::new();
        if let Err(e) = rx.handle_datagram(&bytes) {
            eprintln!("recv: frame error: {e}");
        }
        rx.drain();
        rx.stats()
    } else {
        match run_socket_loop(&addr, &SocketOpts { count, idle_secs }) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("recv: socket loop failed: {e}");
                exit(1);
            }
        }
    };

    println!("recv: datagrams: {}", stats.datagrams);
    println!("recv: messages received: {}", stats.received);
    println!("recv: duplicates: {}", stats.duplicates);
    println!("recv: gaps seen: {}", stats.gaps_seen);
    println!("recv: open gaps: {}", stats.open_gaps);
    println!("recv: decode errors: {}", stats.decode_errors);
    println!("recv: ring overflows: {}", stats.ring_overflows);
    println!("recv: last sequence: {}", stats.last_seq);
}
