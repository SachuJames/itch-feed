//! Receive-side core: datagram framing, sequence tracking, and the socket loop.
//!
//! A datagram carries one or more back-to-back messages in the Day 1 wire
//! format (1-byte tag, fixed length, big-endian). The sender emits exactly
//! one message per datagram, but framing accepts several: it decodes until
//! the bytes run out and errors on anything left over, instead of quietly
//! dropping it.
//!
//! Everything here is transport independent except [`run_socket_loop`]:
//! [`Receiver`] eats raw byte slices, so framing and the sequence tracker
//! stay unit testable without a socket.

use crate::msg::{DecodeError, Message};
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

/// What went wrong while splitting a datagram into messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// A message inside the datagram failed to decode.
    Decode(DecodeError),
    /// Bytes were left over after the last decodable message.
    TrailingGarbage { left: usize },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Decode(e) => write!(f, "frame decode failed: {e}"),
            FrameError::TrailingGarbage { left } => {
                write!(
                    f,
                    "trailing garbage: {left} unconsumed bytes at datagram end"
                )
            }
        }
    }
}

impl std::error::Error for FrameError {}

/// Split a datagram into its messages.
///
/// Decodes back-to-back until the bytes run out. Anything left over after at
/// least one full message is trailing garbage and errors out instead of
/// being silently dropped; a datagram that never yields a whole message
/// comes back with the codec's own error.
pub fn decode_datagram(buf: &[u8]) -> Result<Vec<Message>, FrameError> {
    let mut out = Vec::new();
    let mut rest = buf;
    while !rest.is_empty() {
        match Message::decode(rest) {
            Ok((m, n)) => {
                out.push(m);
                rest = &rest[n..];
            }
            Err(e) if out.is_empty() => return Err(FrameError::Decode(e)),
            Err(_) => {
                return Err(FrameError::TrailingGarbage { left: rest.len() });
            }
        }
    }
    Ok(out)
}

/// Tracks the message stream's sequence numbers in arrival order.
///
/// The first message seen sets the window; nothing before it is treated as
/// a gap. A jump forward opens a gap (a missing range); a late arrival that
/// lands inside a known missing range closes that part of the gap. A message
/// already seen, and not missing, counts as a duplicate. Recovery and
/// retransmit are out of scope: this only detects and reports.
#[derive(Debug, Default)]
pub struct SeqTracker {
    started: bool,
    expected: u64,
    received: u64,
    duplicates: u64,
    gaps_seen: u64,
    /// Sorted, disjoint ranges of sequence numbers known to be missing.
    missing: Vec<(u64, u64)>,
    last_seq: u64,
}

impl SeqTracker {
    pub fn new() -> Self {
        SeqTracker::default()
    }

    /// Observe one message sequence number.
    pub fn observe(&mut self, seq: u64) {
        if !self.started {
            self.started = true;
            self.expected = seq.wrapping_add(1);
            self.received = 1;
            self.last_seq = seq;
            return;
        }
        if self.last_seq < seq {
            self.last_seq = seq;
        }
        if let Some(idx) = self.find_missing(seq) {
            // Late arrival inside a known gap: close that slot.
            self.remove_missing(idx, seq);
            self.received += 1;
            return;
        }
        if seq < self.expected {
            self.duplicates += 1;
            return;
        }
        if seq == self.expected {
            self.expected = seq.wrapping_add(1);
            self.received += 1;
            return;
        }
        // seq > expected: a gap opened between expected and seq.
        self.missing.push((self.expected, seq.wrapping_sub(1)));
        self.gaps_seen += 1;
        self.expected = seq.wrapping_add(1);
        self.received += 1;
    }

    fn find_missing(&self, seq: u64) -> Option<usize> {
        self.missing
            .iter()
            .position(|&(lo, hi)| lo <= seq && seq <= hi)
    }

    fn remove_missing(&mut self, idx: usize, seq: u64) {
        let (lo, hi) = self.missing[idx];
        if lo == hi {
            self.missing.remove(idx);
        } else if seq == lo {
            self.missing[idx] = (lo + 1, hi);
        } else if seq == hi {
            self.missing[idx] = (lo, hi - 1);
        } else {
            self.missing[idx] = (lo, seq - 1);
            self.missing.insert(idx + 1, (seq + 1, hi));
        }
    }

    /// Messages accepted, in order or late fills. Duplicates excluded.
    pub fn received(&self) -> u64 {
        self.received
    }

    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }

    /// How many times the stream jumped forward, opening a gap.
    pub fn gaps_seen(&self) -> u64 {
        self.gaps_seen
    }

    /// Sequence numbers still missing, i.e. gaps never filled.
    pub fn open_gaps(&self) -> u64 {
        self.missing.iter().map(|&(lo, hi)| hi - lo + 1).sum()
    }

    /// The still-missing ranges, ascending.
    pub fn gap_ranges(&self) -> &[(u64, u64)] {
        &self.missing
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn expected_seq(&self) -> u64 {
        self.expected
    }
}

/// A snapshot of the receiver's run statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvStats {
    pub datagrams: u64,
    pub received: u64,
    pub duplicates: u64,
    pub gaps_seen: u64,
    pub open_gaps: u64,
    pub decode_errors: u64,
    pub last_seq: u64,
}

/// Transport-independent receiver: feed it datagram bytes, it frames and
/// tracks. Used by the socket loop, the pipe mode, and the tests.
#[derive(Debug, Default)]
pub struct Receiver {
    tracker: SeqTracker,
    datagrams: u64,
    decode_errors: u64,
}

impl Receiver {
    pub fn new() -> Self {
        Receiver::default()
    }

    /// Frame one datagram and feed its messages to the sequence tracker.
    /// Returns the number of messages in the datagram. A framing failure
    /// counts a decode error and returns the error.
    pub fn handle_datagram(&mut self, buf: &[u8]) -> Result<usize, FrameError> {
        match decode_datagram(buf) {
            Ok(msgs) => {
                for m in &msgs {
                    self.tracker.observe(m.header().seq);
                }
                self.datagrams += 1;
                Ok(msgs.len())
            }
            Err(e) => {
                self.decode_errors += 1;
                Err(e)
            }
        }
    }

    pub fn stats(&self) -> RecvStats {
        RecvStats {
            datagrams: self.datagrams,
            received: self.tracker.received(),
            duplicates: self.tracker.duplicates(),
            gaps_seen: self.tracker.gaps_seen(),
            open_gaps: self.tracker.open_gaps(),
            decode_errors: self.decode_errors,
            last_seq: self.tracker.last_seq(),
        }
    }

    pub fn gap_ranges(&self) -> &[(u64, u64)] {
        self.tracker.gap_ranges()
    }
}

/// Options for the UDP socket loop.
pub struct SocketOpts {
    /// Stop after this many received messages (None = run until idle or Ctrl-C).
    pub count: Option<usize>,
    /// Stop after this many quiet seconds with no datagrams.
    pub idle_secs: u64,
}

/// Bind to `addr` (joining the multicast group when it is one) and feed
/// every datagram into a fresh [`Receiver`] until the count or the idle
/// timeout fires.
pub fn run_socket_loop(addr: &str, opts: &SocketOpts) -> io::Result<RecvStats> {
    let dest: SocketAddr = addr
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let bind = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED), dest.port());
    let sock = UdpSocket::bind(bind)?;
    if let std::net::IpAddr::V4(group) = dest.ip() {
        if group.is_multicast() {
            sock.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)?;
        }
    }
    sock.set_read_timeout(Some(Duration::from_secs(opts.idle_secs.max(1))))?;

    let mut rx = Receiver::new();
    let mut buf = [0u8; 65535];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                let _ = rx.handle_datagram(&buf[..n]);
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                break; // idle timeout
            }
            Err(e) => return Err(e),
        }
        if let Some(count) = opts.count {
            if rx.stats().received >= count as u64 {
                break;
            }
        }
    }
    Ok(rx.stats())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{symbol_bytes, Header};

    fn hdr(seq: u64) -> Header {
        Header {
            seq,
            timestamp_ns: 1_700_000_000_000_000_000 + seq,
        }
    }

    fn delete(seq: u64) -> Message {
        Message::OrderDelete {
            header: hdr(seq),
            order_id: seq,
        }
    }

    #[test]
    fn framing_decodes_back_to_back_messages() {
        let mut datagram = delete(1).encode_to_vec();
        datagram.extend_from_slice(&delete(2).encode_to_vec());
        let msgs = decode_datagram(&datagram).expect("two messages must decode");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].header().seq, 1);
        assert_eq!(msgs[1].header().seq, 2);
    }

    #[test]
    fn framing_errors_on_trailing_garbage() {
        let mut datagram = delete(1).encode_to_vec();
        datagram.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        match decode_datagram(&datagram) {
            Err(FrameError::TrailingGarbage { left }) => assert_eq!(left, 3),
            other => panic!("expected TrailingGarbage, got {other:?}"),
        }
    }

    #[test]
    fn framing_errors_on_a_broken_message() {
        let datagram = vec![0x7Fu8; 17]; // unknown tag, header-sized
        assert!(matches!(
            decode_datagram(&datagram),
            Err(FrameError::Decode(_))
        ));
    }

    #[test]
    fn tracker_accepts_a_clean_stream() {
        let mut t = SeqTracker::new();
        for seq in 1..=10 {
            t.observe(seq);
        }
        assert_eq!(t.received(), 10);
        assert_eq!(t.duplicates(), 0);
        assert_eq!(t.gaps_seen(), 0);
        assert_eq!(t.open_gaps(), 0);
        assert_eq!(t.last_seq(), 10);
    }

    #[test]
    fn tracker_opens_a_gap_on_a_jump() {
        let mut t = SeqTracker::new();
        for seq in [1, 2, 3, 7] {
            t.observe(seq);
        }
        assert_eq!(t.received(), 4);
        assert_eq!(t.gaps_seen(), 1);
        assert_eq!(t.open_gaps(), 3);
        assert_eq!(t.gap_ranges(), &[(4, 6)]);
        assert_eq!(t.last_seq(), 7);
    }

    #[test]
    fn tracker_counts_duplicates() {
        let mut t = SeqTracker::new();
        for seq in [1, 2, 3, 3, 2] {
            t.observe(seq);
        }
        assert_eq!(t.received(), 3);
        assert_eq!(t.duplicates(), 2);
        assert_eq!(t.gaps_seen(), 0);
    }

    #[test]
    fn tracker_late_fill_closes_the_gap() {
        let mut t = SeqTracker::new();
        for seq in [1, 2, 3, 7] {
            t.observe(seq);
        }
        assert_eq!(t.open_gaps(), 3);
        // late arrivals fill the missing slots one by one
        for seq in [5, 4, 6] {
            t.observe(seq);
        }
        assert_eq!(t.open_gaps(), 0);
        assert!(t.gap_ranges().is_empty());
        assert_eq!(t.received(), 7);
        assert_eq!(t.duplicates(), 0);
        assert_eq!(t.gaps_seen(), 1); // the gap was still seen and reported
                                      // duplicates after the fill are still duplicates
        t.observe(4);
        assert_eq!(t.duplicates(), 1);
    }

    #[test]
    fn receiver_counts_datagrams_and_decode_errors() {
        let mut rx = Receiver::new();
        let n = rx.handle_datagram(&delete(1).encode_to_vec()).unwrap();
        assert_eq!(n, 1);
        assert!(rx.handle_datagram(&[0x7Fu8; 17]).is_err());
        let s = rx.stats();
        assert_eq!(s.datagrams, 1);
        assert_eq!(s.received, 1);
        assert_eq!(s.decode_errors, 1);
    }

    #[test]
    fn framing_decodes_a_stock_directory() {
        let m = Message::StockDirectory {
            header: hdr(42),
            symbol: symbol_bytes("AAPL"),
        };
        let msgs = decode_datagram(&m.encode_to_vec()).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].header().seq, 42);
    }
}
