//! Metrics recorded through the [`metrics`] facade.
//!
//! The library only records values; the application chooses the exporter,
//! for example `metrics-exporter-prometheus`. Without one, recording is a
//! no-op. Metric handles are created, and described, when
//! [`Client::connect`] runs, so install the recorder before connecting.
//!
//! Names are relative to [`PREFIX`], which the application applies to
//! every metric, for example with `metrics_util::layers::PrefixLayer`:
//! `transactions_total` is exported as `hl_stream_transactions_total`.
//!
//! [`Client::connect`]: crate::Client::connect

use crate::Transaction;
use hyperliquid_protocol::action::Action;
use metrics::{Counter, Gauge, Histogram};
use metrics_derive::Metrics;
use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError, atomic::AtomicBool},
};

/// Prefix of every metric name.
pub const PREFIX: &str = "hl_stream";

/// Histogram buckets, in seconds, for the inclusion delay and block lag
/// histograms.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 30.0, 60.0, 300.0,
];

/// Mempool transactions received by peer sessions.
#[derive(Clone, Metrics)]
#[metrics(scope = "transactions")]
pub(crate) struct TransactionMetrics {
    /// First-seen mempool transactions.
    pub(crate) total: Counter,
    /// Mempool transactions relayed again by another peer.
    pub(crate) duplicates_total: Counter,
    /// Bytes of first-seen mempool transactions.
    pub(crate) bytes_total: Counter,
}

/// Inclusion of mempool transactions in blocks.
#[derive(Clone, Metrics)]
#[metrics(scope = "transactions")]
pub(crate) struct InclusionMetrics {
    /// Mempool transactions seen but not yet committed.
    pub(crate) pending: Gauge,
    /// Time from first sight in the mempool to the committing block, in seconds.
    pub(crate) inclusion_delay_seconds: Histogram,
}

/// Actions of first-seen transactions; one instance per action kind.
#[derive(Clone, Metrics)]
#[metrics(scope = "actions")]
struct ActionMetrics {
    /// Actions of first-seen mempool transactions, by kind.
    total: Counter,
}

/// Decoding failures.
#[derive(Clone, Metrics)]
#[metrics(scope = "decode")]
pub(crate) struct DecodeMetrics {
    /// Decoding failures by stage.
    #[metric(rename = "errors_total", labels = [("stage", "message")])]
    pub(crate) message_errors: Counter,
    /// Decoding failures by stage.
    #[metric(rename = "errors_total", labels = [("stage", "unknown_message")])]
    pub(crate) unknown_messages: Counter,
    /// Decoding failures by stage.
    #[metric(rename = "errors_total", labels = [("stage", "bundle")])]
    pub(crate) bundle_errors: Counter,
    /// Decoding failures by stage.
    #[metric(rename = "errors_total", labels = [("stage", "action")])]
    pub(crate) action_errors: Counter,
}

/// Consensus blocks.
#[derive(Clone, Metrics)]
#[metrics(scope = "blocks")]
pub(crate) struct BlockMetrics {
    /// First-seen live consensus blocks.
    pub(crate) total: Counter,
    /// Blocks first seen later than the maximum lag, while a peer replays its backlog.
    pub(crate) stale_total: Counter,
    /// Transactions committed by first-seen live blocks.
    pub(crate) transactions_total: Counter,
    /// Committed transactions that were seen in the mempool first.
    pub(crate) transactions_seen_total: Counter,
    /// Delay between a block's consensus time and its reception, in seconds.
    pub(crate) lag_seconds: Histogram,
}

/// Peers and their sessions.
#[derive(Clone, Metrics)]
#[metrics(scope = "peers")]
pub(crate) struct PeerMetrics {
    /// Peers known from seeds, the cache and discovery.
    pub(crate) known: Gauge,
    /// Peers with an open stream session.
    pub(crate) active: Gauge,
    /// Active peers that relayed mempool transactions in their current session.
    pub(crate) relaying: Gauge,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "connected")])]
    pub(crate) connected: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "peer_full")])]
    pub(crate) peer_full: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "rate_limited")])]
    pub(crate) rate_limited: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "no_quorum")])]
    pub(crate) no_quorum: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "rejected")])]
    pub(crate) rejected: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "unreachable")])]
    pub(crate) unreachable: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "disconnected")])]
    pub(crate) disconnected: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "replaced")])]
    pub(crate) replaced: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "not_relaying")])]
    pub(crate) not_relaying: Counter,
    /// Peer session events by outcome.
    #[metric(rename = "sessions_total", labels = [("outcome", "surplus")])]
    pub(crate) surplus: Counter,
}

/// Bytes received from peers.
#[derive(Clone, Metrics)]
#[metrics(scope = "peers")]
pub(crate) struct TrafficMetrics {
    /// Frame bytes received from peers, as transported.
    pub(crate) received_bytes_total: Counter,
}

/// Peer discovery.
#[derive(Clone, Metrics)]
#[metrics(scope = "discovery")]
pub(crate) struct DiscoveryMetrics {
    /// Discovery requests by source and result.
    #[metric(rename = "requests_total", labels = [("source", "child_peers"), ("result", "ok")])]
    pub(crate) child_peers_ok: Counter,
    /// Discovery requests by source and result.
    #[metric(rename = "requests_total", labels = [("source", "child_peers"), ("result", "error")])]
    pub(crate) child_peers_error: Counter,
    /// Discovery requests by source and result.
    #[metric(rename = "requests_total", labels = [("source", "root_peers"), ("result", "ok")])]
    pub(crate) root_peers_ok: Counter,
    /// Discovery requests by source and result.
    #[metric(rename = "requests_total", labels = [("source", "root_peers"), ("result", "error")])]
    pub(crate) root_peers_error: Counter,
}

/// Callbacks answered for peers.
#[derive(Clone, Metrics)]
#[metrics(scope = "callbacks")]
pub(crate) struct CallbackMetrics {
    /// Peer callbacks answered, by service.
    #[metric(rename = "total", labels = [("service", "stream")])]
    pub(crate) stream: Counter,
    /// Peer callbacks answered, by service.
    #[metric(rename = "total", labels = [("service", "rpc")])]
    pub(crate) rpc: Counter,
}

/// Metrics of the supervisor.
#[derive(Debug, Default)]
pub(crate) struct SupervisorMetrics {
    pub(crate) blocks: BlockMetrics,
    pub(crate) inclusion: InclusionMetrics,
    pub(crate) peers: PeerMetrics,
    pub(crate) discovery: DiscoveryMetrics,
}

/// Most distinct action kinds labelled individually; further kinds are
/// counted as `other`. Action names come from unauthenticated peers, so the
/// label set must stay bounded.
const MAX_KIND_LABELS: usize = 256;
const MAX_KIND_LEN: usize = 48;

/// Metrics recorded by peer sessions, shared between them.
#[derive(Debug, Default)]
pub(crate) struct SessionMetrics {
    pub(crate) transactions: TransactionMetrics,
    pub(crate) decode: DecodeMetrics,
    pub(crate) traffic: TrafficMetrics,
    /// Whether an unknown message type was already reported in the logs.
    pub(crate) unknown_message_reported: AtomicBool,
    /// Counter handles by action kind, created on first use.
    kinds: Mutex<HashMap<String, Counter>>,
}

impl SessionMetrics {
    /// Records a first-seen transaction and its actions.
    pub(crate) fn transaction(&self, transaction: &Transaction) {
        self.transactions.total.increment(1);
        self.transactions
            .bytes_total
            .increment(transaction.payload.len() as u64);
        let Ok(bundle) = &transaction.bundle else {
            self.decode.bundle_errors.increment(1);
            return;
        };
        // Count per kind first: bundles hold hundreds of actions of few kinds.
        let mut counts: HashMap<&str, u64> = HashMap::new();
        let mut invalid = 0;
        for action in &bundle.actions {
            *counts.entry(action.kind()).or_default() += 1;
            if matches!(action.action, Action::Invalid { .. }) {
                invalid += 1;
            }
        }
        self.decode.action_errors.increment(invalid);
        let mut kinds = self.kinds.lock().unwrap_or_else(PoisonError::into_inner);
        for (kind, count) in counts {
            kind_counter(&mut kinds, kind).increment(count);
        }
    }
}

/// The counter for `kind`, or for `other` when the kind is implausible or
/// the label budget is spent.
fn kind_counter<'a>(kinds: &'a mut HashMap<String, Counter>, kind: &str) -> &'a Counter {
    let plausible = !kind.is_empty()
        && kind.len() <= MAX_KIND_LEN
        && kind.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    let label = if plausible && (kinds.contains_key(kind) || kinds.len() < MAX_KIND_LABELS) {
        kind
    } else {
        "other"
    };
    kinds
        .entry(label.to_owned())
        .or_insert_with(|| ActionMetrics::new_with_labels(&[("kind", label.to_owned())]).total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_labels_are_bounded() {
        let mut kinds = HashMap::new();
        kind_counter(&mut kinds, "cancelByCloid");
        kind_counter(&mut kinds, "bad kind");
        kind_counter(&mut kinds, &"x".repeat(MAX_KIND_LEN + 1));
        assert_eq!(kinds.len(), 2, "cancelByCloid and other");
        for i in 0..MAX_KIND_LABELS {
            kind_counter(&mut kinds, &format!("kind{i}"));
        }
        assert!(kinds.len() <= MAX_KIND_LABELS + 1);
        kind_counter(&mut kinds, "neverSeenBefore");
        assert!(!kinds.contains_key("neverSeenBefore"));
        assert!(kinds.contains_key("cancelByCloid"));
    }
}
