# itch-feed

An ITCH-style market data feed handler in Rust. A UDP multicast simulator
publishes sequenced binary messages; the handler consumes them through a
lock-free SPSC ring buffer and watches for sequence gaps instead of
silently losing data. Retransmission requests are still open work.

## Still open

Sequence-gap detection works and reports gaps, but nothing asks for
retransmission yet: a request channel off the gap detector, plus counters
for retransmits requested and messages recovered or lost, is the remaining
piece.

## Status

The codec, the simulator, framing, and the sequence tracker are in.
Decoded messages flow through a lock-free SPSC ring buffer between the
socket loop and the consumer: fixed capacity, no locks, no allocation
after construction. A full ring refuses the push and counts an overflow
instead of blocking or overwriting queued data.

## Wire format

Big-endian, fixed length per message. Each message starts with a 1-byte
type tag, an 8-byte sequence number, and an 8-byte nanosecond timestamp:

| tag | type                  | bytes | payload                                                  |
| --- | --------------------- | ----- | -------------------------------------------------------- |
| S   | SystemEvent           | 18    | event_code u8                                            |
| R   | StockDirectory        | 23    | symbol 6B                                                |
| A   | AddOrder              | 44    | order_id u64, side u8 (B/S), qty u32, price u64, symbol 6B |
| F   | AddOrderMPID          | 48    | A fields plus mpid 4B                                    |
| E   | OrderExecuted         | 45    | order_id u64, qty u32, price u64, match_id u64            |
| C   | OrderExecutedWithPrice| 46    | E fields plus printable u8 (Y/N)                         |
| X   | OrderCancel           | 29    | order_id u64, qty u32                                    |
| D   | OrderDelete           | 25    | order_id u64                                             |
| U   | OrderReplace          | 45    | old id u64, new id u64, qty u32, price u64                |
| P   | Trade                 | 52    | order_id u64, side u8, qty u32, price u64, match_id u64, symbol 6B |

Decode lives in `src/msg.rs`: it never allocates, and short, unknown or
malformed input comes back as an error instead of panicking.

## Running the simulator

```sh
cargo run --bin sim -- --addr 127.0.0.1:5001 --count 1000 --rate 500
```

The default target is the multicast group 239.0.0.1:5001. Where the host
does not route multicast, pass `--addr 127.0.0.1:5001` and listen on that
port locally. `--seed` pins the generated stream for repeat runs.
`--stdout` writes the encoded bytes to stdout instead of UDP, which is what
the receiver's pipe mode reads.

## Running the receiver

```sh
cargo run --bin recv -- --addr 239.0.0.1:5001
```

This binds the port, joins the multicast group when the address is one,
frames every datagram, queues the decoded messages into the ring, and
drains the ring into the sequence tracker until `--count` messages arrive
or the socket sits quiet for `--idle-secs` seconds (default 5). Where UDP
is not available, pipe the simulator's bytes in instead:

```sh
cargo run --bin sim -- --stdout --count 5000 --seed 42 | cargo run --bin recv -- --pipe
```

At exit it prints the run stats, one fact per line: messages received,
duplicates, gaps seen, open gaps (still missing, with the ranges kept
internally), decode errors, ring overflows, and the last sequence number. A late arrival
that lands inside a known gap closes it in the accounting; a gap that is
seen is still reported even after it closes. Detection only: nothing here
asks for retransmission yet.

## License

MIT
