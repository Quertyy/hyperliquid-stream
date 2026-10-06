//! WebSocket subscriptions, independent of what is published.
//!
//! A [`Hub`] holds named topics. Publishers call [`Hub::publish`] with a
//! topic, the kinds the record carries (for example action names) and any
//! [`serde::Serialize`] record; [`WsServer`] lets clients subscribe, with
//! messages in the style of the Hyperliquid API:
//!
//! ```text
//! -> {"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order"]}}
//! <- {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order"]}}}
//! <- {"channel":"mempoolActions","data":{...}}
//! -> {"method":"unsubscribe","subscription":{"type":"mempoolActions","kinds":["order"]}}
//! -> {"method":"ping"}
//! <- {"channel":"pong"}
//! ```
//!
//! Connecting with `?compression=zstd` makes published messages arrive as
//! binary frames, each one complete zstd frame holding the same JSON.
//!
//! Design, for many clients on one high-rate feed:
//!
//! - a record is serialized and framed at most once per publication, and
//!   reaches each client once even when several of its subscriptions accept
//!   it;
//! - [`Hub::is_subscribed`] lets publishers skip preparing records nobody
//!   wants;
//! - a message is zstd-compressed at most once, only when a recipient asked,
//!   on the blocking pool, so other clients are never delayed;
//! - each client has a queue bounded in messages and bytes; a client that
//!   falls behind, stops reading or stops answering pings is disconnected
//!   instead of slowing the others or growing memory.
//!
//! ```no_run
//! use hyperliquid_ws::{Hub, WsConfig, WsServer};
//!
//! # async fn run() -> std::io::Result<()> {
//! let hub = Hub::new(&["blocks"]);
//! let server = WsServer::start(WsConfig::default(), hub.clone()).await?;
//! hub.publish("blocks", &[], &serde_json::json!({"height": 1}));
//! # Ok(())
//! # }
//! ```
//!
//! Metrics are recorded through the `metrics` facade under the `ws` scope
//! (`ws_connections`, `ws_subscriptions`, `ws_notifications_total`,
//! `ws_slow_clients_total`, `ws_compressed_connections_total`,
//! `ws_compressed_messages_total`, `ws_compression_duration_seconds`);
//! applications may prefix them.
#![warn(missing_docs)]

mod compression;
mod filter;
mod hub;
mod outbox;
mod server;

pub use filter::KindFilter;
pub use hub::Hub;
pub use server::{WsConfig, WsServer};
