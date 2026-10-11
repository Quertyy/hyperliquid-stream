//! One stream session with one peer, from connection to disconnection.
//!
//! Sessions decode messages concurrently. The shared [`SeenTransactions`]
//! set ensures that a transaction relayed by several peers is decoded once.

use crate::{
    Transaction,
    connection::{OpenError, PeerConnection},
    metrics::SessionMetrics,
    seen::SeenTransactions,
};
use hyperliquid_protocol::{
    keccak256,
    wire::{
        greeting::{BroadcastGroup, GreetingResponse},
        message::{BlockHeader, StreamMessage},
    },
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{Instrument, debug, info_span, warn};

/// A message from a session to the supervisor.
#[derive(Debug)]
pub(crate) struct SessionUpdate {
    pub(crate) peer: SocketAddr,
    /// Identifies the session, so that late messages from a replaced
    /// session are not mistaken for the current one's.
    pub(crate) session: u64,
    pub(crate) kind: Update,
}

#[derive(Debug)]
pub(crate) enum Update {
    Connected,
    /// A transaction, `None` when another peer delivered it first.
    Transaction(Option<Arc<Transaction>>),
    Block {
        header: BlockHeader,
        received_at_ms: u64,
    },
    Ended(SessionEnd),
}

/// Why a session ended; selects how long to wait before retrying the peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionEnd {
    PeerFull,
    RateLimited,
    NoQuorum,
    Rejected,
    Unreachable,
    Disconnected,
    /// Ended by the supervisor for lag, or because a relay went silent.
    Replaced,
    /// Ended by the supervisor after relaying nothing during its trial.
    NotRelaying,
    /// Ended by the supervisor because enough relays are connected.
    Surplus,
}

impl SessionEnd {
    pub(crate) fn retry_after(self) -> Duration {
        Duration::from_secs(match self {
            Self::Disconnected | Self::Surplus => 10,
            Self::PeerFull | Self::RateLimited => 120,
            Self::NoQuorum | Self::Replaced => 300,
            Self::Rejected | Self::Unreachable => 900,
            Self::NotRelaying => 1800,
        })
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PeerFull => "peer_full",
            Self::RateLimited => "rate_limited",
            Self::NoQuorum => "no_quorum",
            Self::Rejected => "rejected",
            Self::Unreachable => "unreachable",
            Self::Disconnected => "disconnected",
            Self::Replaced => "replaced",
            Self::NotRelaying => "not_relaying",
            Self::Surplus => "surplus",
        }
    }
}

impl From<&OpenError> for SessionEnd {
    fn from(error: &OpenError) -> Self {
        match error {
            OpenError::Rejected(GreetingResponse::PeerFull) => Self::PeerFull,
            OpenError::Rejected(GreetingResponse::RateLimited) => Self::RateLimited,
            OpenError::Rejected(GreetingResponse::NoQuorum) => Self::NoQuorum,
            OpenError::Rejected(_) | OpenError::InvalidResponse(_) => Self::Rejected,
            OpenError::Connect(_) | OpenError::Timeout | OpenError::Frame(_) => Self::Unreachable,
        }
    }
}

/// Runs a session and reports its end.
///
/// The session stops when `stop` resolves, which includes the supervisor
/// dropping its sender. It only stops while waiting for the network, never
/// between recording a transaction as seen and delivering it, so that no
/// transaction is lost.
pub(crate) async fn run(
    peer: SocketAddr,
    session: u64,
    seen: Arc<SeenTransactions>,
    metrics: Arc<SessionMetrics>,
    updates: mpsc::Sender<SessionUpdate>,
    stop: oneshot::Receiver<()>,
) {
    let end = stream(peer, session, &seen, &metrics, &updates, stop)
        .instrument(info_span!("session", %peer, session))
        .await;
    let update = SessionUpdate {
        peer,
        session,
        kind: Update::Ended(end),
    };
    // The supervisor may already be gone during shutdown.
    let _ = updates.send(update).await;
}

async fn stream(
    peer: SocketAddr,
    session: u64,
    seen: &SeenTransactions,
    metrics: &SessionMetrics,
    updates: &mpsc::Sender<SessionUpdate>,
    mut stop: oneshot::Receiver<()>,
) -> SessionEnd {
    let send = |kind| {
        updates.send(SessionUpdate {
            peer,
            session,
            kind,
        })
    };
    let opened = tokio::select! {
        opened = PeerConnection::open(peer, BroadcastGroup::SplitClientBlocks) => opened,
        _ = &mut stop => return SessionEnd::Replaced,
    };
    let mut connection = match opened {
        Ok(connection) => connection,
        Err(error) => {
            debug!(%error, "connection failed");
            return SessionEnd::from(&error);
        }
    };
    if send(Update::Connected).await.is_err() {
        return SessionEnd::Disconnected;
    }
    let mut counted_bytes = 0;
    loop {
        let received = tokio::select! {
            received = connection.next_frame() => received,
            _ = &mut stop => return SessionEnd::Replaced,
        };
        let frame = match received {
            Ok(frame) => frame,
            Err(error) => {
                debug!(%error, "stream ended");
                return SessionEnd::Disconnected;
            }
        };
        let received_bytes = connection.received_bytes();
        metrics
            .traffic
            .received_bytes_total
            .increment(received_bytes - counted_bytes);
        counted_bytes = received_bytes;
        let received_at_ms = unix_ms();
        let update = match StreamMessage::decode(&frame) {
            Ok(StreamMessage::Transaction(payload)) => {
                let hash = keccak256(&payload);
                if seen.insert(hash, received_at_ms) {
                    let transaction =
                        Transaction::decode_hashed(hash, payload, peer, received_at_ms);
                    metrics.transaction(&transaction);
                    Update::Transaction(Some(Arc::new(transaction)))
                } else {
                    metrics.transactions.duplicates_total.increment(1);
                    Update::Transaction(None)
                }
            }
            Ok(StreamMessage::Block(header)) => Update::Block {
                header,
                received_at_ms,
            },
            // A message type this version does not know: a node upgrade may
            // have changed the protocol, so make it visible.
            Ok(message) => {
                metrics.decode.unknown_messages.increment(1);
                if metrics
                    .unknown_message_reported
                    .swap(true, Ordering::Relaxed)
                {
                    debug!(?message, "skipping unknown message");
                } else {
                    warn!(
                        ?message,
                        "skipping a message of unknown type; the peer protocol may have changed"
                    );
                }
                continue;
            }
            Err(error) => {
                debug!(%error, "skipping undecodable message");
                metrics.decode.message_errors.increment(1);
                continue;
            }
        };
        if send(update).await.is_err() {
            return SessionEnd::Disconnected;
        }
    }
}

/// Current time in Unix milliseconds.
pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
