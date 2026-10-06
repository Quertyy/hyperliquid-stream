//! Transactions seen recently, shared by all peer sessions so that each
//! bundle is decoded and emitted once, by whichever peer delivers it first.

use hyperliquid_protocol::Hash;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

/// Upper bound on remembered transactions, whatever the retention.
const MAX_ENTRIES: usize = 2_000_000;

#[derive(Debug)]
pub(crate) struct SeenTransactions {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    entries: HashMap<Hash, Entry>,
    /// Hashes in insertion order, for expiry.
    order: VecDeque<(u64, Hash)>,
    pending: usize,
    retention_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    first_seen_ms: u64,
    committed: bool,
}

impl SeenTransactions {
    pub(crate) fn new(retention: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: VecDeque::new(),
                pending: 0,
                retention_ms: u64::try_from(retention.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a relayed transaction. Returns `true` the first time a hash
    /// is seen; transactions already committed are not pending again.
    pub(crate) fn insert(&self, hash: Hash, now_ms: u64) -> bool {
        let mut inner = self.lock();
        if inner.entries.contains_key(&hash) {
            return false;
        }
        inner.add(
            hash,
            Entry {
                first_seen_ms: now_ms,
                committed: false,
            },
        );
        inner.pending += 1;
        true
    }

    /// Marks the transactions of a block as committed. Returns, for each one
    /// that was pending, how long it spent in the mempool.
    pub(crate) fn commit(&self, hashes: &[Hash], now_ms: u64) -> Vec<u64> {
        let mut inner = self.lock();
        let mut delays = Vec::new();
        for hash in hashes {
            match inner.entries.get_mut(hash) {
                Some(entry) if !entry.committed => {
                    entry.committed = true;
                    delays.push(now_ms.saturating_sub(entry.first_seen_ms));
                    inner.pending -= 1;
                }
                Some(_) => {}
                // Remember it so that a late relay is not reported as pending.
                None => inner.add(
                    *hash,
                    Entry {
                        first_seen_ms: now_ms,
                        committed: true,
                    },
                ),
            }
        }
        delays
    }

    /// Forgets transactions first seen more than the retention ago.
    pub(crate) fn expire(&self, now_ms: u64) {
        let mut inner = self.lock();
        let cutoff = now_ms.saturating_sub(inner.retention_ms);
        while let Some(&(seen_ms, _)) = inner.order.front() {
            if seen_ms >= cutoff {
                break;
            }
            inner.pop_oldest();
        }
    }

    /// Transactions seen but not committed.
    pub(crate) fn pending(&self) -> usize {
        self.lock().pending
    }
}

impl Inner {
    fn add(&mut self, hash: Hash, entry: Entry) {
        if self.order.len() >= MAX_ENTRIES {
            self.pop_oldest();
        }
        self.entries.insert(hash, entry);
        self.order.push_back((entry.first_seen_ms, hash));
    }

    fn pop_oldest(&mut self) {
        let Some((_, hash)) = self.order.pop_front() else {
            return;
        };
        if let Some(entry) = self.entries.remove(&hash)
            && !entry.committed
        {
            self.pending -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduplicates_and_tracks_commitment() {
        let seen = SeenTransactions::new(Duration::from_secs(60));
        let (a, b, c) = (Hash([1; 32]), Hash([2; 32]), Hash([3; 32]));
        assert!(seen.insert(a, 1_000));
        assert!(!seen.insert(a, 1_100));
        assert!(seen.insert(b, 1_200));
        assert_eq!(seen.pending(), 2);

        assert_eq!(seen.commit(&[a, c], 1_500), [500]);
        assert_eq!(seen.pending(), 1);
        // A late relay of a committed transaction is not new.
        assert!(!seen.insert(c, 1_600));
        assert_eq!(seen.commit(&[a], 1_700), Vec::<u64>::new());
    }

    #[test]
    fn expires_old_entries() {
        let seen = SeenTransactions::new(Duration::from_secs(1));
        let (a, b) = (Hash([1; 32]), Hash([2; 32]));
        seen.insert(a, 1_000);
        seen.insert(b, 1_800);
        seen.expire(2_500);
        assert_eq!(seen.pending(), 1);
        assert!(seen.insert(a, 2_600));
        assert!(!seen.insert(b, 2_600));
    }
}
