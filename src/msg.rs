//! Binary wire codec for a small ITCH-like message set.
//!
//! Ten message types, following the Nasdaq ITCH 5.0 type bytes: system
//! events, stock directory, add order (with and without MPID attribution),
//! order executed (with and without an explicit execution price), cancel,
//! delete, replace, and trade.
//!
//! Layout on the wire, everything big-endian:
//!
//! ```text
//! tag: u8 | seq: u64 | timestamp_ns: u64 | payload (fixed size per tag)
//! ```
//!
//! Decoding never allocates and never panics: short, unknown or malformed
//! input comes back as an error.

use std::fmt;

/// One-byte type tags.
pub const TAG_SYSTEM_EVENT: u8 = b'S';
pub const TAG_STOCK_DIRECTORY: u8 = b'R';
pub const TAG_ADD_ORDER: u8 = b'A';
pub const TAG_ADD_ORDER_MPID: u8 = b'F';
pub const TAG_ORDER_EXECUTED: u8 = b'E';
pub const TAG_ORDER_EXECUTED_PRICE: u8 = b'C';
pub const TAG_ORDER_CANCEL: u8 = b'X';
pub const TAG_ORDER_DELETE: u8 = b'D';
pub const TAG_ORDER_REPLACE: u8 = b'U';
pub const TAG_TRADE: u8 = b'P';

/// System event codes, from the ITCH spec.
pub const EVENT_START_OF_MESSAGES: u8 = b'O';
pub const EVENT_START_OF_SYSTEM_HOURS: u8 = b'S';
pub const EVENT_START_OF_MARKET_HOURS: u8 = b'Q';
pub const EVENT_END_OF_MARKET_HOURS: u8 = b'M';
pub const EVENT_END_OF_SYSTEM_HOURS: u8 = b'E';
pub const EVENT_END_OF_MESSAGES: u8 = b'C';

/// Symbol field is 6 bytes, space-padded. MPID attribution is 4 bytes.
pub const SYMBOL_LEN: usize = 6;
pub const MPID_LEN: usize = 4;

pub const HEADER_LEN: usize = 1 + 8 + 8;
pub const SYSTEM_EVENT_LEN: usize = HEADER_LEN + 1;
pub const STOCK_DIRECTORY_LEN: usize = HEADER_LEN + SYMBOL_LEN;
pub const ADD_ORDER_LEN: usize = HEADER_LEN + 8 + 1 + 4 + 8 + SYMBOL_LEN;
pub const ADD_ORDER_MPID_LEN: usize = HEADER_LEN + 8 + 1 + 4 + 8 + MPID_LEN + SYMBOL_LEN;
pub const ORDER_EXECUTED_LEN: usize = HEADER_LEN + 8 + 4 + 8 + 8;
pub const ORDER_EXECUTED_PRICE_LEN: usize = HEADER_LEN + 8 + 4 + 8 + 8 + 1;
pub const ORDER_CANCEL_LEN: usize = HEADER_LEN + 8 + 4;
pub const ORDER_DELETE_LEN: usize = HEADER_LEN + 8;
pub const ORDER_REPLACE_LEN: usize = HEADER_LEN + 8 + 8 + 4 + 8;
pub const TRADE_LEN: usize = HEADER_LEN + 8 + 1 + 4 + 8 + 8 + SYMBOL_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    fn to_byte(self) -> u8 {
        match self {
            Side::Buy => b'B',
            Side::Sell => b'S',
        }
    }

    fn from_byte(b: u8) -> Option<Side> {
        match b {
            b'B' => Some(Side::Buy),
            b'S' => Some(Side::Sell),
            _ => None,
        }
    }
}

/// Header shared by every message: stream sequence and capture time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub seq: u64,
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
    SystemEvent {
        header: Header,
        event_code: u8,
    },
    StockDirectory {
        header: Header,
        symbol: [u8; SYMBOL_LEN],
    },
    AddOrder {
        header: Header,
        order_id: u64,
        side: Side,
        qty: u32,
        price: u64,
        symbol: [u8; SYMBOL_LEN],
    },
    AddOrderMPID {
        header: Header,
        order_id: u64,
        side: Side,
        qty: u32,
        price: u64,
        mpid: [u8; MPID_LEN],
        symbol: [u8; SYMBOL_LEN],
    },
    OrderExecuted {
        header: Header,
        order_id: u64,
        qty: u32,
        price: u64,
        match_id: u64,
    },
    OrderExecutedWithPrice {
        header: Header,
        order_id: u64,
        qty: u32,
        price: u64,
        match_id: u64,
        printable: bool,
    },
    OrderCancel {
        header: Header,
        order_id: u64,
        qty: u32,
    },
    OrderDelete {
        header: Header,
        order_id: u64,
    },
    OrderReplace {
        header: Header,
        old_order_id: u64,
        new_order_id: u64,
        qty: u32,
        price: u64,
    },
    Trade {
        header: Header,
        order_id: u64,
        side: Side,
        qty: u32,
        price: u64,
        match_id: u64,
        symbol: [u8; SYMBOL_LEN],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Truncated { needed: usize, got: usize },
    UnknownTag(u8),
    BadValue,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated { needed, got } => {
                write!(f, "truncated message: need {needed} bytes, got {got}")
            }
            DecodeError::UnknownTag(t) => write!(f, "unknown message tag {t:#04x}"),
            DecodeError::BadValue => write!(f, "invalid field value in message"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    BufferTooSmall { needed: usize, got: usize },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::BufferTooSmall { needed, got } => {
                write!(f, "buffer too small: need {needed} bytes, got {got}")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

fn write_u64(out: &mut [u8], off: usize, v: u64) {
    out[off..off + 8].copy_from_slice(&v.to_be_bytes());
}

fn write_u32(out: &mut [u8], off: usize, v: u32) {
    out[off..off + 4].copy_from_slice(&v.to_be_bytes());
}

fn read_u64(buf: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[off..off + 8]);
    u64::from_be_bytes(b)
}

fn read_u32(buf: &[u8], off: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[off..off + 4]);
    u32::from_be_bytes(b)
}

fn read_symbol(buf: &[u8], off: usize) -> [u8; SYMBOL_LEN] {
    let mut s = [0u8; SYMBOL_LEN];
    s.copy_from_slice(&buf[off..off + SYMBOL_LEN]);
    s
}

fn read_mpid(buf: &[u8], off: usize) -> [u8; MPID_LEN] {
    let mut m = [0u8; MPID_LEN];
    m.copy_from_slice(&buf[off..off + MPID_LEN]);
    m
}

/// Pad or truncate a symbol to the 6-byte wire field.
pub fn symbol_bytes(s: &str) -> [u8; SYMBOL_LEN] {
    let mut out = [b' '; SYMBOL_LEN];
    let bytes = s.as_bytes();
    let n = bytes.len().min(SYMBOL_LEN);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

impl Message {
    pub fn tag(&self) -> u8 {
        match self {
            Message::SystemEvent { .. } => TAG_SYSTEM_EVENT,
            Message::StockDirectory { .. } => TAG_STOCK_DIRECTORY,
            Message::AddOrder { .. } => TAG_ADD_ORDER,
            Message::AddOrderMPID { .. } => TAG_ADD_ORDER_MPID,
            Message::OrderExecuted { .. } => TAG_ORDER_EXECUTED,
            Message::OrderExecutedWithPrice { .. } => TAG_ORDER_EXECUTED_PRICE,
            Message::OrderCancel { .. } => TAG_ORDER_CANCEL,
            Message::OrderDelete { .. } => TAG_ORDER_DELETE,
            Message::OrderReplace { .. } => TAG_ORDER_REPLACE,
            Message::Trade { .. } => TAG_TRADE,
        }
    }

    pub fn header(&self) -> &Header {
        match self {
            Message::SystemEvent { header, .. } => header,
            Message::StockDirectory { header, .. } => header,
            Message::AddOrder { header, .. } => header,
            Message::AddOrderMPID { header, .. } => header,
            Message::OrderExecuted { header, .. } => header,
            Message::OrderExecutedWithPrice { header, .. } => header,
            Message::OrderCancel { header, .. } => header,
            Message::OrderDelete { header, .. } => header,
            Message::OrderReplace { header, .. } => header,
            Message::Trade { header, .. } => header,
        }
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            Message::SystemEvent { .. } => SYSTEM_EVENT_LEN,
            Message::StockDirectory { .. } => STOCK_DIRECTORY_LEN,
            Message::AddOrder { .. } => ADD_ORDER_LEN,
            Message::AddOrderMPID { .. } => ADD_ORDER_MPID_LEN,
            Message::OrderExecuted { .. } => ORDER_EXECUTED_LEN,
            Message::OrderExecutedWithPrice { .. } => ORDER_EXECUTED_PRICE_LEN,
            Message::OrderCancel { .. } => ORDER_CANCEL_LEN,
            Message::OrderDelete { .. } => ORDER_DELETE_LEN,
            Message::OrderReplace { .. } => ORDER_REPLACE_LEN,
            Message::Trade { .. } => TRADE_LEN,
        }
    }

    /// Encode into a caller-provided buffer. Returns the bytes written.
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, EncodeError> {
        let need = self.encoded_len();
        if out.len() < need {
            return Err(EncodeError::BufferTooSmall {
                needed: need,
                got: out.len(),
            });
        }
        out[0] = self.tag();
        write_u64(out, 1, self.header().seq);
        write_u64(out, 9, self.header().timestamp_ns);
        let o = HEADER_LEN;
        match self {
            Message::SystemEvent { event_code, .. } => {
                out[o] = *event_code;
            }
            Message::StockDirectory { symbol, .. } => {
                out[o..o + SYMBOL_LEN].copy_from_slice(symbol);
            }
            Message::AddOrderMPID {
                order_id,
                side,
                qty,
                price,
                mpid,
                symbol,
                ..
            } => {
                write_u64(out, o, *order_id);
                out[o + 8] = side.to_byte();
                write_u32(out, o + 9, *qty);
                write_u64(out, o + 13, *price);
                out[o + 21..o + 25].copy_from_slice(mpid);
                out[o + 25..o + 31].copy_from_slice(symbol);
            }
            Message::OrderExecutedWithPrice {
                order_id,
                qty,
                price,
                match_id,
                printable,
                ..
            } => {
                write_u64(out, o, *order_id);
                write_u32(out, o + 8, *qty);
                write_u64(out, o + 12, *price);
                write_u64(out, o + 20, *match_id);
                out[o + 28] = if *printable { b'Y' } else { b'N' };
            }
            Message::AddOrder {
                order_id,
                side,
                qty,
                price,
                symbol,
                ..
            } => {
                write_u64(out, o, *order_id);
                out[o + 8] = side.to_byte();
                write_u32(out, o + 9, *qty);
                write_u64(out, o + 13, *price);
                out[o + 21..o + 27].copy_from_slice(symbol);
            }
            Message::OrderExecuted {
                order_id,
                qty,
                price,
                match_id,
                ..
            } => {
                write_u64(out, o, *order_id);
                write_u32(out, o + 8, *qty);
                write_u64(out, o + 12, *price);
                write_u64(out, o + 20, *match_id);
            }
            Message::OrderCancel { order_id, qty, .. } => {
                write_u64(out, o, *order_id);
                write_u32(out, o + 8, *qty);
            }
            Message::OrderDelete { order_id, .. } => {
                write_u64(out, o, *order_id);
            }
            Message::OrderReplace {
                old_order_id,
                new_order_id,
                qty,
                price,
                ..
            } => {
                write_u64(out, o, *old_order_id);
                write_u64(out, o + 8, *new_order_id);
                write_u32(out, o + 16, *qty);
                write_u64(out, o + 20, *price);
            }
            Message::Trade {
                order_id,
                side,
                qty,
                price,
                match_id,
                symbol,
                ..
            } => {
                write_u64(out, o, *order_id);
                out[o + 8] = side.to_byte();
                write_u32(out, o + 9, *qty);
                write_u64(out, o + 13, *price);
                write_u64(out, o + 21, *match_id);
                out[o + 29..o + 35].copy_from_slice(symbol);
            }
        }
        Ok(need)
    }

    /// Convenience encode that allocates exactly the right size.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = vec![0u8; self.encoded_len()];
        let _ = self.encode(&mut v); // sized exactly, cannot fail
        v
    }

    /// Decode one message. Returns the message and the bytes consumed, so
    /// trailing bytes stay available to the caller.
    pub fn decode(buf: &[u8]) -> Result<(Message, usize), DecodeError> {
        if buf.len() < HEADER_LEN {
            return Err(DecodeError::Truncated {
                needed: HEADER_LEN,
                got: buf.len(),
            });
        }
        let tag = buf[0];
        let want = match tag {
            TAG_SYSTEM_EVENT => SYSTEM_EVENT_LEN,
            TAG_STOCK_DIRECTORY => STOCK_DIRECTORY_LEN,
            TAG_ADD_ORDER => ADD_ORDER_LEN,
            TAG_ADD_ORDER_MPID => ADD_ORDER_MPID_LEN,
            TAG_ORDER_EXECUTED => ORDER_EXECUTED_LEN,
            TAG_ORDER_EXECUTED_PRICE => ORDER_EXECUTED_PRICE_LEN,
            TAG_ORDER_CANCEL => ORDER_CANCEL_LEN,
            TAG_ORDER_DELETE => ORDER_DELETE_LEN,
            TAG_ORDER_REPLACE => ORDER_REPLACE_LEN,
            TAG_TRADE => TRADE_LEN,
            _ => return Err(DecodeError::UnknownTag(tag)),
        };
        if buf.len() < want {
            return Err(DecodeError::Truncated {
                needed: want,
                got: buf.len(),
            });
        }
        let header = Header {
            seq: read_u64(buf, 1),
            timestamp_ns: read_u64(buf, 9),
        };
        let o = HEADER_LEN;
        let msg = match tag {
            TAG_SYSTEM_EVENT => Message::SystemEvent {
                header,
                event_code: buf[o],
            },
            TAG_STOCK_DIRECTORY => Message::StockDirectory {
                header,
                symbol: read_symbol(buf, o),
            },
            TAG_ADD_ORDER_MPID => Message::AddOrderMPID {
                header,
                order_id: read_u64(buf, o),
                side: Side::from_byte(buf[o + 8]).ok_or(DecodeError::BadValue)?,
                qty: read_u32(buf, o + 9),
                price: read_u64(buf, o + 13),
                mpid: read_mpid(buf, o + 21),
                symbol: read_symbol(buf, o + 25),
            },
            TAG_ORDER_EXECUTED_PRICE => Message::OrderExecutedWithPrice {
                header,
                order_id: read_u64(buf, o),
                qty: read_u32(buf, o + 8),
                price: read_u64(buf, o + 12),
                match_id: read_u64(buf, o + 20),
                printable: match buf[o + 28] {
                    b'Y' => true,
                    b'N' => false,
                    _ => return Err(DecodeError::BadValue),
                },
            },
            TAG_ADD_ORDER => Message::AddOrder {
                header,
                order_id: read_u64(buf, o),
                side: Side::from_byte(buf[o + 8]).ok_or(DecodeError::BadValue)?,
                qty: read_u32(buf, o + 9),
                price: read_u64(buf, o + 13),
                symbol: read_symbol(buf, o + 21),
            },
            TAG_ORDER_EXECUTED => Message::OrderExecuted {
                header,
                order_id: read_u64(buf, o),
                qty: read_u32(buf, o + 8),
                price: read_u64(buf, o + 12),
                match_id: read_u64(buf, o + 20),
            },
            TAG_ORDER_CANCEL => Message::OrderCancel {
                header,
                order_id: read_u64(buf, o),
                qty: read_u32(buf, o + 8),
            },
            TAG_ORDER_DELETE => Message::OrderDelete {
                header,
                order_id: read_u64(buf, o),
            },
            TAG_ORDER_REPLACE => Message::OrderReplace {
                header,
                old_order_id: read_u64(buf, o),
                new_order_id: read_u64(buf, o + 8),
                qty: read_u32(buf, o + 16),
                price: read_u64(buf, o + 20),
            },
            _ => Message::Trade {
                header,
                order_id: read_u64(buf, o),
                side: Side::from_byte(buf[o + 8]).ok_or(DecodeError::BadValue)?,
                qty: read_u32(buf, o + 9),
                price: read_u64(buf, o + 13),
                match_id: read_u64(buf, o + 21),
                symbol: read_symbol(buf, o + 29),
            },
        };
        Ok((msg, want))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(seq: u64) -> Header {
        Header {
            seq,
            timestamp_ns: 1_700_000_000_000_000_000 + seq,
        }
    }

    fn every_message() -> Vec<Message> {
        vec![
            Message::SystemEvent {
                header: hdr(0),
                event_code: EVENT_START_OF_MARKET_HOURS,
            },
            Message::StockDirectory {
                header: hdr(0),
                symbol: symbol_bytes("AAPL"),
            },
            Message::AddOrder {
                header: hdr(1),
                order_id: 42,
                side: Side::Buy,
                qty: 500,
                price: 152_340,
                symbol: symbol_bytes("AAPL"),
            },
            Message::AddOrderMPID {
                header: hdr(1),
                order_id: 43,
                side: Side::Sell,
                qty: 250,
                price: 152_350,
                mpid: *b"GSCO",
                symbol: symbol_bytes("MSFT"),
            },
            Message::OrderExecuted {
                header: hdr(2),
                order_id: 42,
                qty: 200,
                price: 152_340,
                match_id: 9001,
            },
            Message::OrderExecutedWithPrice {
                header: hdr(2),
                order_id: 42,
                qty: 100,
                price: 152_341,
                match_id: 9003,
                printable: true,
            },
            Message::OrderCancel {
                header: hdr(3),
                order_id: 43,
                qty: 100,
            },
            Message::OrderDelete {
                header: hdr(4),
                order_id: 44,
            },
            Message::OrderReplace {
                header: hdr(5),
                old_order_id: 45,
                new_order_id: 46,
                qty: 700,
                price: 152_500,
            },
            Message::Trade {
                header: hdr(6),
                order_id: 47,
                side: Side::Sell,
                qty: 300,
                price: 152_310,
                match_id: 9002,
                symbol: symbol_bytes("MSFT"),
            },
        ]
    }

    #[test]
    fn roundtrip_every_message_type() {
        for m in every_message() {
            let bytes = m.encode_to_vec();
            assert_eq!(bytes.len(), m.encoded_len());
            let (decoded, n) = Message::decode(&bytes).expect("valid encoding must decode");
            assert_eq!(n, bytes.len());
            assert_eq!(decoded, m);
        }
    }

    #[test]
    fn header_fields_survive_the_trip() {
        let m = Message::OrderDelete {
            header: Header {
                seq: 12345,
                timestamp_ns: 987_654_321,
            },
            order_id: 7,
        };
        let (d, _) = Message::decode(&m.encode_to_vec()).unwrap();
        assert_eq!(d.header().seq, 12345);
        assert_eq!(d.header().timestamp_ns, 987_654_321);
    }

    #[test]
    fn truncated_input_errors() {
        for m in every_message() {
            let bytes = m.encode_to_vec();
            for cut in 0..bytes.len() {
                match Message::decode(&bytes[..cut]) {
                    Err(DecodeError::Truncated { .. }) => {}
                    other => panic!("expected Truncated for {m:?} cut at {cut}, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn unknown_tag_errors() {
        let mut bytes = vec![0x7Fu8];
        bytes.extend_from_slice(&[0u8; HEADER_LEN - 1]);
        assert_eq!(Message::decode(&bytes), Err(DecodeError::UnknownTag(0x7F)));
    }

    #[test]
    fn bad_side_byte_errors_instead_of_panicking() {
        let m = Message::AddOrder {
            header: hdr(9),
            order_id: 1,
            side: Side::Buy,
            qty: 10,
            price: 100,
            symbol: symbol_bytes("AAPL"),
        };
        let mut bytes = m.encode_to_vec();
        bytes[HEADER_LEN + 8] = b'?';
        assert_eq!(Message::decode(&bytes), Err(DecodeError::BadValue));
    }

    #[test]
    fn bad_printable_byte_errors_instead_of_panicking() {
        let m = Message::OrderExecutedWithPrice {
            header: hdr(9),
            order_id: 1,
            qty: 10,
            price: 100,
            match_id: 5,
            printable: true,
        };
        let mut bytes = m.encode_to_vec();
        let last = bytes.len() - 1;
        bytes[last] = b'?';
        assert_eq!(Message::decode(&bytes), Err(DecodeError::BadValue));
    }

    #[test]
    fn trailing_bytes_are_left_for_the_caller() {
        let m = every_message().into_iter().next().unwrap();
        let mut bytes = m.encode_to_vec();
        bytes.extend_from_slice(&[0xAA, 0xBB]);
        let (decoded, n) = Message::decode(&bytes).unwrap();
        assert_eq!(decoded, m);
        assert_eq!(n, m.encoded_len());
    }

    #[test]
    fn encode_rejects_a_short_buffer() {
        let m = every_message().into_iter().next().unwrap();
        let mut tiny = [0u8; 4];
        assert!(matches!(
            m.encode(&mut tiny),
            Err(EncodeError::BufferTooSmall { .. })
        ));
    }

    #[test]
    fn symbol_padding_and_truncation() {
        assert_eq!(symbol_bytes("AAPL"), *b"AAPL  ");
        assert_eq!(symbol_bytes("GOOGLONGER"), *b"GOOGLO");
    }
}
