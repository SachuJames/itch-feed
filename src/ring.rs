//! Lock-free bounded SPSC ring buffer for the receive path.
//!
//! The socket side decodes datagrams and pushes messages in; the consumer
//! side pops them for sequence tracking. Fixed capacity, no locks, no
//! allocation after construction. The element type is `Message`, which
//! derives `Copy` (plain value data), so slots are plain array entries and
//! nothing here needs `MaybeUninit` juggling.

use crate::msg::Message;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pad a value to its own cache line, so the producer's head index and the
/// consumer's tail index never share one.
#[repr(align(64))]
struct Padded<T>(T);

/// Bounded single-producer single-consumer queue.
///
/// Indices run unbounded and mask into the slot array, so the only shared
/// state is the two atomics. A push writes the slot and then publishes the
/// new head with Release; a pop that sees the head with Acquire then reads
/// the slot, which orders the item's bytes before the index update.
///
/// This is only sound with exactly one producer thread calling `try_push`
/// and exactly one consumer thread calling `try_pop`: the producer writes
/// only the slot at `head & mask` while `head - tail < capacity`, the
/// consumer reads only the slot at `tail & mask` while `tail < head`, and
/// those slot sets never overlap. `Sync` is implemented manually on that
/// basis; sharing it any other way is a bug.
pub struct RingBuffer {
    head: Padded<AtomicUsize>, // producer index, written by the producer only
    tail: Padded<AtomicUsize>, // consumer index, written by the consumer only
    mask: usize,
    slots: Box<[UnsafeCell<Message>]>,
}

// SAFETY: see the struct docs. The two atomics are the only shared mutable
// state across threads, and the SPSC discipline keeps slot accesses
// disjoint.
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    /// Create a ring holding up to `capacity` messages.
    ///
    /// Panics when `capacity` is zero or not a power of two, since only
    /// powers of two work with the mask. Slot contents are meaningless
    /// until written; the consumer never reads a slot it has not seen
    /// published through the head index.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "ring capacity must be a non-zero power of two"
        );
        let slots: Vec<UnsafeCell<Message>> = (0..capacity)
            .map(|_| {
                UnsafeCell::new(Message::SystemEvent {
                    header: crate::msg::Header {
                        seq: 0,
                        timestamp_ns: 0,
                    },
                    event_code: 0,
                })
            })
            .collect();
        RingBuffer {
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
            mask: capacity - 1,
            slots: slots.into_boxed_slice(),
        }
    }

    /// The maximum number of queued messages.
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Messages currently queued. Approximate if the other side is
    /// pushing or popping concurrently.
    pub fn len(&self) -> usize {
        self.head
            .0
            .load(Ordering::Acquire)
            .wrapping_sub(self.tail.0.load(Ordering::Acquire))
    }

    pub fn is_empty(&self) -> bool {
        self.tail.0.load(Ordering::Relaxed) == self.head.0.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    /// Push one message. On a full ring the item comes back in the error
    /// untouched; the call never blocks and never overwrites queued data.
    pub fn try_push(&self, item: Message) -> Result<(), Message> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head.wrapping_sub(tail) == self.capacity() {
            return Err(item);
        }
        // The full check above means the consumer cannot have this slot in
        // flight, so writing it here races with nothing.
        unsafe {
            *self.slots[head & self.mask].get() = item;
        }
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pop the oldest message, or None when the ring is empty.
    pub fn try_pop(&self) -> Option<Message> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail == head {
            return None;
        }
        // tail < head means the producer published this slot with Release,
        // which we observed with Acquire, so the item is fully written.
        let item = unsafe { *self.slots[tail & self.mask].get() };
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::Header;

    fn msg(seq: u64) -> Message {
        Message::OrderDelete {
            header: Header {
                seq,
                timestamp_ns: 1_700_000_000_000_000_000 + seq,
            },
            order_id: seq,
        }
    }

    #[test]
    fn pushes_and_pops_preserve_fifo_order() {
        let r = RingBuffer::new(64);
        for i in 0..50 {
            r.try_push(msg(i)).unwrap();
        }
        assert_eq!(r.len(), 50);
        for i in 0..50 {
            let m = r.try_pop().expect("ring should not be empty");
            assert_eq!(m.header().seq, i);
        }
        assert!(r.is_empty());
    }

    #[test]
    fn empty_pop_returns_none() {
        let r = RingBuffer::new(8);
        assert!(r.try_pop().is_none());
        r.try_push(msg(1)).unwrap();
        r.try_pop().unwrap();
        assert!(r.try_pop().is_none());
    }

    #[test]
    fn wraps_around_the_capacity_boundary_repeatedly() {
        let r = RingBuffer::new(8);
        // cycle through the slots many times; sequence numbers must stay
        // exact or something overwrote a live slot
        for next in 0u64..10_000 {
            r.try_push(msg(next)).unwrap();
            let m = r.try_pop().expect("just pushed, must pop");
            assert_eq!(m.header().seq, next);
        }
        assert!(r.is_empty());
    }

    #[test]
    fn full_ring_returns_the_item_back_and_keeps_queued_items() {
        let r = RingBuffer::new(4);
        for i in 0..4 {
            r.try_push(msg(i)).unwrap();
        }
        assert!(r.is_full());
        let back = r.try_push(msg(99)).expect_err("full ring must refuse");
        assert_eq!(back.header().seq, 99);
        // nothing was overwritten: the queued items are still in order
        for i in 0..4 {
            let m = r.try_pop().unwrap();
            assert_eq!(m.header().seq, i);
        }
        // room for one again after a single pop
        r.try_push(msg(100)).unwrap();
        assert_eq!(r.try_pop().unwrap().header().seq, 100);
    }

    #[test]
    #[should_panic]
    fn zero_capacity_panics() {
        let _ = RingBuffer::new(0);
    }

    #[test]
    #[should_panic]
    fn non_power_of_two_capacity_panics() {
        let _ = RingBuffer::new(100);
    }

    #[test]
    fn producer_and_consumer_agree_across_threads() {
        use std::sync::Arc;
        const N: u64 = 100_000;
        let r = Arc::new(RingBuffer::new(1024));
        let prod = Arc::clone(&r);
        let handle = std::thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                match prod.try_push(msg(i)) {
                    Ok(()) => i += 1,
                    Err(_) => std::thread::yield_now(), // ring full, wait a beat
                }
            }
        });
        for i in 0..N {
            loop {
                match r.try_pop() {
                    Some(m) => {
                        assert_eq!(m.header().seq, i, "lost or reordered item");
                        break;
                    }
                    None => std::thread::yield_now(),
                }
            }
        }
        handle.join().unwrap();
        assert!(r.is_empty());
    }
}
