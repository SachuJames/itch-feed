# itch-feed

An ITCH-style market data feed handler in Rust. A UDP multicast simulator
publishes sequenced binary messages; the handler consumes them through a
lock-free SPSC ring buffer, watches for sequence gaps, and asks for the
missing ranges to be resent instead of silently losing data.

## Status

The codec, the simulator, framing, and the sequence tracker are in.
Decoded messages flow through a lock-free SPSC ring buffer between the
socket loop and the consumer: fixed capacity, no locks, no allocation
after construction. A full ring refuses the push and counts an overflow
instead of blocking or overwriting queued data.

## Gap recovery

When an arriving message jumps past the next expected sequence number,
the tracker opens a gap and emits one retransmit request for exactly the
missing range. It never asks twice for the same range: there is no
re-request policy, so a range nobody answers stays open and its messages
count as lost when the input ends. A late arrival that lands inside an
open range closes that slot and counts as recovered.

Three counters keep the accounting honest:

- `retransmit requests`: ranges asked for, one per gap.
- `messages recovered`: late arrivals that filled a requested slot
  (every open range was requested when it opened, so every fill counts).
- `messages lost`: sequence numbers still missing when the stats are
  read. A gap is only visible once a later message arrives, so messages
  dropped at the very tail of a stream cannot be told apart from the
  stream simply ending, and never show up here.

The channel is deliberately transport-agnostic: the tracker decides what
is missing, and whoever holds the requests decides how to answer them.
On a real feed the answer travels over a separate TCP session. Here the
tests answer in-process by regenerating the requested messages, which
works because the simulator's stream is a pure function of its seed.

One consequence of the tracker's window: it takes its starting point
from the first message it sees. Under heavy reordering at the very start
of a stream, a few early messages can arrive after that point and read
as duplicates rather than as a gap. Nothing later in the stream is
affected.

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

Two knobs rough up the stream so the receiver's gap handling has
something to work on:

```sh
cargo run --bin sim -- --stdout --count 3000 --seed 7 --drop-rate 2 --reorder 8
```

`--drop-rate P` drops P percent of the messages; `--reorder W` shuffles
delivery inside windows of W consecutive messages, so a message can
arrive early or late but never further than its own window. Both run off
the same seeded generator as the stream itself: identical seed, count,
and knob settings reproduce identical drops and shuffles, on the stdout
and UDP paths alike.

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
internally), retransmit requests, messages recovered, messages lost,
decode errors, ring overflows, and the last sequence number. In pipe
mode it also lists each retransmit request as it stands at exit, in the
order the gaps opened. A late arrival that lands inside a known gap
closes it in the accounting; a gap that is seen is still reported even
after it closes.

## License

MIT
