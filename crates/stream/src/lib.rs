//! Stream what Hyperliquid nodes gossip to each other, directly from P2P
//! peers: the HyperCore mempool and consensus blocks.
//!
//! The mempool is everything observed before it is committed in a block:
//! signed action bundles (orders, cancels, transfers, EVM transactions…)
//! that nodes relay before validators include them. [`Client`] connects to
//! several peers, keeps the ones that relay the mempool, and yields each
//! transaction and block once, decoded, as soon as the first peer delivers
//! it.
//!
//! ```no_run
//! use hyperliquid_stream::{Config, Event, Client};
//!
//! # async fn run() -> Result<(), hyperliquid_stream::Error> {
//! let mut client = Client::connect(Config::default()).await?;
//! while let Some(event) = client.next_event().await {
//!     if let Event::Transaction(tx) = event {
//!         for action in tx.actions() {
//!             println!("{} {}", tx.hash, action.kind());
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Network requirements
//!
//! Peers check that a client is reachable before streaming to it: they
//! connect back to its TCP ports 4001 and 4002. The host must accept inbound
//! connections on both ports, and no other process may use them. See
//! [`CallbackConfig`].
//!
//! # What the mempool is not
//!
//! Transactions are decoded, not executed: an action seen in the mempool may
//! never be committed, or be committed and then rejected by the exchange.
//! Coverage depends on the peers: a node relays transactions only when every
//! node between it and the validators does, so some transactions only appear
//! in blocks.
//!
//! # Layers
//!
//! - [`Client`]: the complete stream, with peer management.
//! - [`connection`]: one peer connection and the callback listeners.
//! - [`protocol`]: the codecs, without I/O (the `hyperliquid-protocol`
//!   crate).
#![warn(missing_docs)]

mod client;
mod config;
pub mod connection;
mod discovery;
mod event;
pub mod metrics;
mod seen;
mod session;
mod supervisor;

pub use client::Client;
pub use config::{Config, Error, MAINNET_INFO_URL};
pub use connection::CallbackConfig;
pub use event::{Block, Event, PeerStats, Stats, Transaction};
pub use hyperliquid_protocol as protocol;
pub use hyperliquid_protocol::{Address, Bytes, Hash, Signature, action};
