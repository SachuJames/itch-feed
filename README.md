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

## License

MIT
