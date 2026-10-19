//! Synthetic message stream generator.
//!
//! Builds a `Vec<Message>` with strictly increasing sequence numbers from a
//! fixed seed. It is a pure function with no network involved, so the stream
//! is testable on its own. The sender bin lives in src/bin/sim.rs.

use crate::msg::{symbol_bytes, Header, Message, Side};

const SYMBOLS: [&str; 6] = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"];
const MPIDS: [&str; 4] = ["GSCO", "MSCO", "JPMX", "UBSS"];

pub struct StreamConfig {
    pub start_seq: u64,
    pub seed: u64,
    pub count: usize,
    pub base_timestamp_ns: u64,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64, good enough for synthetic market data
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct LiveOrder {
    id: u64,
    qty: u32,
    price: u64,
    side: Side,
    symbol: [u8; 6],
}

fn random_side(rng: &mut Rng) -> Side {
    if rng.below(2) == 0 {
        Side::Buy
    } else {
        Side::Sell
    }
}

fn random_symbol(rng: &mut Rng) -> [u8; 6] {
    symbol_bytes(SYMBOLS[rng.below(SYMBOLS.len() as u64) as usize])
}

fn random_mpid(rng: &mut Rng) -> [u8; 4] {
    let b = MPIDS[rng.below(MPIDS.len() as u64) as usize].as_bytes();
    [b[0], b[1], b[2], b[3]]
}

fn live_index(rng: &mut Rng, live: &[LiveOrder]) -> usize {
    rng.below(live.len() as u64) as usize
}

fn add_order(
    rng: &mut Rng,
    live: &mut Vec<LiveOrder>,
    next_id: &mut u64,
    seq: u64,
    ts: u64,
) -> Message {
    let id = *next_id;
    *next_id += 1;
    let side = random_side(rng);
    let qty = (100 + rng.below(9900)) as u32;
    let price = 10_000 + rng.below(490_000);
    let symbol = random_symbol(rng);
    live.push(LiveOrder {
        id,
        qty,
        price,
        side,
        symbol,
    });
    let header = Header {
        seq,
        timestamp_ns: ts,
    };
    // About one add in five carries MPID attribution, like the real feed.
    if rng.below(5) == 0 {
        Message::AddOrderMPID {
            header,
            order_id: id,
            side,
            qty,
            price,
            mpid: random_mpid(rng),
            symbol,
        }
    } else {
        Message::AddOrder {
            header,
            order_id: id,
            side,
            qty,
            price,
            symbol,
        }
    }
}

fn execute_order(rng: &mut Rng, live: &mut Vec<LiveOrder>, seq: u64, ts: u64) -> Message {
    let idx = live_index(rng, live);
    let q = (1 + rng.below(live[idx].qty as u64)) as u32;
    let order_id;
    let price;
    {
        let lo = &mut live[idx];
        lo.qty -= q;
        order_id = lo.id;
        price = lo.price;
    }
    if live[idx].qty == 0 {
        live.swap_remove(idx);
    }
    let header = Header {
        seq,
        timestamp_ns: ts,
    };
    // Some fills come back with an explicit execution price and a
    // printable flag, others as plain executions.
    if rng.below(3) == 0 {
        Message::OrderExecutedWithPrice {
            header,
            order_id,
            qty: q,
            price,
            match_id: rng.next(),
            printable: rng.below(2) == 0,
        }
    } else {
        Message::OrderExecuted {
            header,
            order_id,
            qty: q,
            price,
            match_id: rng.next(),
        }
    }
}

fn cancel_order(rng: &mut Rng, live: &mut Vec<LiveOrder>, seq: u64, ts: u64) -> Message {
    let idx = live_index(rng, live);
    let q = (1 + rng.below(live[idx].qty as u64)) as u32;
    let order_id;
    {
        let lo = &mut live[idx];
        lo.qty -= q;
        order_id = lo.id;
    }
    if live[idx].qty == 0 {
        live.swap_remove(idx);
    }
    Message::OrderCancel {
        header: Header {
            seq,
            timestamp_ns: ts,
        },
        order_id,
        qty: q,
    }
}

fn delete_order(rng: &mut Rng, live: &mut Vec<LiveOrder>, seq: u64, ts: u64) -> Message {
    let idx = live_index(rng, live);
    let order_id = live[idx].id;
    live.swap_remove(idx);
    Message::OrderDelete {
        header: Header {
            seq,
            timestamp_ns: ts,
        },
        order_id,
    }
}

fn replace_order(
    rng: &mut Rng,
    live: &mut Vec<LiveOrder>,
    next_id: &mut u64,
    seq: u64,
    ts: u64,
) -> Message {
    let idx = live_index(rng, live);
    let old_order_id = live[idx].id;
    live.swap_remove(idx);
    let new_id = *next_id;
    *next_id += 1;
    let side = random_side(rng);
    let qty = (100 + rng.below(9900)) as u32;
    let price = 10_000 + rng.below(490_000);
    let symbol = random_symbol(rng);
    live.push(LiveOrder {
        id: new_id,
        qty,
        price,
        side,
        symbol,
    });
    Message::OrderReplace {
        header: Header {
            seq,
            timestamp_ns: ts,
        },
        old_order_id,
        new_order_id: new_id,
        qty,
        price,
    }
}

fn trade(rng: &mut Rng, live: &mut Vec<LiveOrder>, seq: u64, ts: u64) -> Message {
    let idx = live_index(rng, live);
    let q = (1 + rng.below(live[idx].qty as u64)) as u32;
    let order_id;
    let side;
    let price;
    let symbol;
    {
        let lo = &mut live[idx];
        lo.qty -= q;
        order_id = lo.id;
        side = lo.side;
        price = lo.price;
        symbol = lo.symbol;
    }
    if live[idx].qty == 0 {
        live.swap_remove(idx);
    }
    Message::Trade {
        header: Header {
            seq,
            timestamp_ns: ts,
        },
        order_id,
        side,
        qty: q,
        price,
        match_id: rng.next(),
        symbol,
    }
}

/// Build a synthetic stream of `count` messages. Sequence numbers run
/// `start_seq`, `start_seq + 1`, ... with no gaps; timestamps drift forward
/// by a small random jitter per message.
pub fn generate_stream(cfg: &StreamConfig) -> Vec<Message> {
    let mut rng = Rng(cfg.seed | 1);
    let mut out = Vec::with_capacity(cfg.count);
    let mut live: Vec<LiveOrder> = Vec::new();
    let mut next_id: u64 = 1;
    let mut ts = cfg.base_timestamp_ns;

    for seq in (cfg.start_seq..).take(cfg.count) {
        let msg = if live.len() < 8 {
            // warm up the book before any fills or cancels can happen
            add_order(&mut rng, &mut live, &mut next_id, seq, ts)
        } else {
            match rng.below(100) {
                0..=29 => add_order(&mut rng, &mut live, &mut next_id, seq, ts),
                30..=54 => execute_order(&mut rng, &mut live, seq, ts),
                55..=64 => cancel_order(&mut rng, &mut live, seq, ts),
                65..=74 => delete_order(&mut rng, &mut live, seq, ts),
                75..=84 => replace_order(&mut rng, &mut live, &mut next_id, seq, ts),
                _ => trade(&mut rng, &mut live, seq, ts),
            }
        };
        out.push(msg);
        ts += rng.below(2000);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::Message;

    fn cfg() -> StreamConfig {
        StreamConfig {
            start_seq: 500,
            seed: 7,
            count: 300,
            base_timestamp_ns: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn seq_numbers_increase_by_one() {
        let s = generate_stream(&cfg());
        assert_eq!(s.len(), 300);
        for (i, m) in s.iter().enumerate() {
            assert_eq!(m.header().seq, 500 + i as u64);
        }
    }

    #[test]
    fn same_seed_same_stream() {
        assert_eq!(generate_stream(&cfg()), generate_stream(&cfg()));
    }

    #[test]
    fn empty_count_gives_empty_stream() {
        let c = StreamConfig { count: 0, ..cfg() };
        assert!(generate_stream(&c).is_empty());
    }

    #[test]
    fn every_generated_message_roundtrips() {
        for m in generate_stream(&cfg()) {
            let bytes = m.encode_to_vec();
            let (d, n) = Message::decode(&bytes).unwrap();
            assert_eq!(n, bytes.len());
            assert_eq!(d, m);
        }
    }
}
