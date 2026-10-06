use crate::connection::{BindError, CallbackConfig};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use thiserror::Error;

/// Default info API endpoint, used to find root peers.
pub const MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

/// Settings of a [`Client`](crate::Client).
///
/// [`Default`] targets mainnet with discovery enabled and callback
/// listeners on `0.0.0.0:4001` and `0.0.0.0:4002`.
#[derive(Clone, Debug)]
pub struct Config {
    /// Peers to try first, usually `IP:4001`.
    pub peers: Vec<SocketAddr>,
    /// Find more peers through the info API and `ChildPeerStatuses`.
    pub discovery: bool,
    /// Info API endpoint used by discovery.
    pub info_url: String,
    /// Most concurrent peer sessions, used while looking for relays. Once
    /// `min_relay_peers` peers relay, only they and one spare session stay
    /// open.
    pub max_peers: usize,
    /// Try to keep at least this many sessions with peers that relay
    /// mempool transactions: while fewer do, sessions that relay only blocks
    /// are replaced after a short trial. Zero disables the search.
    pub min_relay_peers: usize,
    /// How long a session may stream without relaying a transaction before
    /// it is replaced, while relays are missing. Relays send several
    /// transactions per second; a peer silent for three trials no longer
    /// counts as a relay.
    pub relay_trial: Duration,
    /// Replace peers whose blocks arrive later than this after their
    /// consensus time, for more than 20 seconds.
    pub max_lag: Duration,
    /// Callback listeners. Peers only stream to clients that answer their
    /// callbacks, so disable this only when another process answers them.
    pub callbacks: Option<CallbackConfig>,
    /// File storing good peers between runs.
    pub peers_cache: Option<PathBuf>,
    /// Interval between [`Event::Stats`](crate::Event::Stats) events.
    pub stats_interval: Duration,
    /// How long a transaction hash is remembered, for deduplication and
    /// inclusion tracking.
    pub retention: Duration,
    /// Events buffered for the consumer. When the buffer is full, peers are
    /// read more slowly instead of events being dropped.
    pub channel_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            discovery: true,
            info_url: MAINNET_INFO_URL.to_owned(),
            max_peers: 4,
            min_relay_peers: 2,
            relay_trial: Duration::from_secs(20),
            max_lag: Duration::from_secs(30),
            callbacks: Some(CallbackConfig::default()),
            peers_cache: None,
            stats_interval: Duration::from_secs(30),
            retention: Duration::from_secs(15 * 60),
            channel_capacity: 1024,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.peers.is_empty() && !self.discovery && self.peers_cache.is_none() {
            return Err(Error::NoPeers);
        }
        if self.max_peers == 0 {
            return Err(Error::InvalidConfig("max_peers must be positive"));
        }
        if self.channel_capacity == 0 {
            return Err(Error::InvalidConfig("channel_capacity must be positive"));
        }
        if self.max_lag.is_zero() {
            return Err(Error::InvalidConfig("max_lag must be positive"));
        }
        if self.min_relay_peers > self.max_peers {
            return Err(Error::InvalidConfig(
                "min_relay_peers cannot exceed max_peers",
            ));
        }
        if self.stats_interval.is_zero() {
            return Err(Error::InvalidConfig("stats_interval must be positive"));
        }
        Ok(())
    }
}

/// A failure to start a [`Client`](crate::Client).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Neither seed peers, a peer cache nor discovery were configured.
    #[error("no peers: configure seed peers, a peer cache or discovery")]
    NoPeers,
    /// A setting is out of range.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    /// A callback listener could not be bound.
    #[error(transparent)]
    Callback(#[from] BindError),
}
