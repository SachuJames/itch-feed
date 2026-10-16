# itch-feed

An ITCH-style market data feed handler in Rust. A UDP multicast simulator
publishes sequenced binary messages; the handler consumes them through a
lock-free SPSC ring buffer, watches for sequence gaps, and requests
retransmission instead of silently losing data.

## Plan

- Binary message codec for a small ITCH-like message set (add, execute,
  cancel, delete, replace, trade).
- Multicast simulator that replays a message stream over UDP, with knobs
  for packet loss and reordering so the recovery paths actually get used.
- Lock-free SPSC ring buffer between the receive loop and the consumer.
- Sequence-gap detection with a retransmit request channel, plus counters
  for gaps seen, retransmits requested, and messages recovered or lost.

## Status

Early days: the codec and the simulator come first, then the ring buffer
and gap recovery.

## License

MIT
