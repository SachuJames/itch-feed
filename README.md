# itch-feed

An ITCH-style market data feed handler in Rust. A UDP multicast simulator
publishes sequenced binary messages; the handler consumes them through a
lock-free SPSC ring buffer, watches for sequence gaps, and requests
retransmission instead of silently losing data.

## Plan

- Binary message codec for a small ITCH-like message set (system event,
  stock directory, add, add with MPID, execute, execute with price,
  cancel, delete, replace, trade).
- Multicast simulator that replays a message stream over UDP, with knobs
  for packet loss and reordering so the recovery paths actually get used.
- Lock-free SPSC ring buffer between the receive loop and the consumer.
- Sequence-gap detection with a retransmit request channel, plus counters
  for gaps seen, retransmits requested, and messages recovered or lost.

## Status

The binary codec for the ten message types is in, and a UDP simulator
emits a sequenced synthetic stream. The ring buffer and gap recovery are
still open work.

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
and feeds every datagram through framing plus the sequence tracker until
`--count` messages arrive or the socket sits quiet for `--idle-secs`
seconds (default 5). Where UDP is not available, pipe the simulator's
bytes in instead:

```sh
cargo run --bin sim -- --stdout --count 5000 --seed 42 | cargo run --bin recv -- --pipe
```

At exit it prints the run stats, one fact per line: messages received,
duplicates, gaps seen, open gaps (still missing, with the ranges kept
internally), decode errors, and the last sequence number. A late arrival
that lands inside a known gap closes it in the accounting; a gap that is
seen is still reported even after it closes. Detection only: nothing here
asks for retransmission yet.

## License

MIT
