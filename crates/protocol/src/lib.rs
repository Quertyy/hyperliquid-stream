//! Codecs for the Hyperliquid peer-to-peer protocol, without I/O.
//!
//! - [`wire`]: frames, the stream greeting, consensus stream messages and the
//!   gossip RPC used for discovery.
//! - [`action`]: signed action bundles, the transactions of the HyperCore
//!   mempool, and their action bodies.
//!
//! Decoders never trust a length before checking it against the bytes
//! present, and binary values share the input buffer instead of copying it.
//! The [`hyperliquid-stream`](https://docs.rs/hyperliquid-stream) crate
//! uses these codecs to stream the mempool from live peers.
#![warn(missing_docs)]

pub mod action;
mod types;
pub mod wire;

pub use bytes::Bytes;
pub use types::{Address, Cloid, Hash, ParseHexError, Signature, keccak256, to_hex};
