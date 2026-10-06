//! Events yielded by [`Client`](crate::Client).

use bytes::Bytes;
use hyperliquid_protocol::{
    Address, Hash,
    action::{Bundle, BundleError, SignedAction},
    keccak256,
};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{net::SocketAddr, sync::Arc};

/// An event of the mempool stream, in arrival order.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    /// A transaction seen in the mempool for the first time.
    Transaction(Arc<Transaction>),
    /// A consensus block seen for the first time, at most
    /// [`Config::max_lag`](crate::Config::max_lag) after its consensus time.
    Block(Arc<Block>),
    /// Periodic statistics.
    Stats(Arc<Stats>),
}

/// A mempool transaction: a signed action bundle relayed by a peer before it
/// is committed in a block.
///
/// Nothing is executed, so the exchange may still reject any of its actions.
#[derive(Debug)]
pub struct Transaction {
    /// Keccak-256 hash of the payload, the transaction's consensus hash.
    pub hash: Hash,
    /// When the first copy arrived, in Unix milliseconds.
    pub received_at_ms: u64,
    /// Peer that delivered the first copy.
    pub peer: SocketAddr,
    /// The raw bundle.
    pub payload: Bytes,
    /// The decoded bundle.
    pub bundle: Result<Bundle, BundleError>,
}

impl Transaction {
    /// Decodes a bundle received from `peer`.
    #[must_use]
    pub fn decode(payload: Bytes, peer: SocketAddr, received_at_ms: u64) -> Self {
        Self::decode_hashed(keccak256(&payload), payload, peer, received_at_ms)
    }

    /// Like [`Transaction::decode`], with the payload's hash already computed.
    pub(crate) fn decode_hashed(
        hash: Hash,
        payload: Bytes,
        peer: SocketAddr,
        received_at_ms: u64,
    ) -> Self {
        Self {
            hash,
            received_at_ms,
            peer,
            bundle: Bundle::decode(&payload),
            payload,
        }
    }

    /// The decoded actions; empty when the bundle did not decode.
    #[must_use]
    pub fn actions(&self) -> &[SignedAction] {
        self.bundle.as_ref().map_or(&[], |bundle| &bundle.actions)
    }
}

/// Serializes the transaction with its decoded actions, without the payload.
impl Serialize for Transaction {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("hash", &self.hash)?;
        map.serialize_entry("receivedAtMs", &self.received_at_ms)?;
        map.serialize_entry("peer", &self.peer)?;
        map.serialize_entry("size", &self.payload.len())?;
        match &self.bundle {
            Ok(bundle) => {
                map.serialize_entry("nonce", &bundle.nonce)?;
                map.serialize_entry("signature", &bundle.signature)?;
                map.serialize_entry("actions", &bundle.actions)?;
            }
            Err(error) => map.serialize_entry("error", &error.to_string())?,
        }
        map.end()
    }
}

/// A consensus block, and how the mempool anticipated it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    /// Consensus time, in Unix milliseconds.
    pub time_ms: u64,
    /// Validator that proposed the block.
    pub proposer: Address,
    /// When the block arrived, in Unix milliseconds.
    pub received_at_ms: u64,
    /// Arrival delay after the consensus time.
    pub lag_ms: u64,
    /// Peer that delivered the block first.
    pub peer: SocketAddr,
    /// Hashes of the committed transactions.
    pub tx_hashes: Vec<Hash>,
    /// How many committed transactions were seen in the mempool first.
    pub seen_in_mempool: usize,
    /// Longest time a committed transaction spent in the mempool.
    pub max_inclusion_delay_ms: Option<u64>,
}

/// Periodic statistics.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// Peers known from seeds, the cache and discovery.
    pub known_peers: usize,
    /// Peers with an open stream session.
    pub active_peers: Vec<PeerStats>,
    /// Active peers that relayed mempool transactions in their session.
    pub relaying_peers: usize,
    /// Transactions seen but not yet committed.
    pub pending: usize,
    /// First-seen transactions since start.
    pub transactions: u64,
    /// Transactions relayed again by another peer since start.
    pub duplicates: u64,
    /// First-seen live blocks since start.
    pub blocks: u64,
    /// Transactions committed by those blocks.
    pub block_transactions: u64,
    /// Committed transactions that were seen in the mempool first.
    pub block_transactions_seen: u64,
}

/// Statistics of one active peer.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerStats {
    /// Peer address.
    pub address: SocketAddr,
    /// Seconds since the session opened.
    pub connected_secs: u64,
    /// Arrival delay of the peer's latest block.
    pub lag_ms: Option<u64>,
    /// Transactions relayed by the peer, duplicates included.
    pub transactions: u64,
    /// Blocks relayed by the peer, duplicates included.
    pub blocks: u64,
}
