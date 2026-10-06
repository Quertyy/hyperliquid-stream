//! `hl-stream serve`: the mempool and blocks over WebSocket subscriptions.
//!
//! | Subscription `type` | Messages | `kinds` selects |
//! | --- | --- | --- |
//! | `mempool` | `summary` record per transaction | any action of the transaction |
//! | `mempoolTransactions` | `transaction` record | any action of the transaction |
//! | `mempoolActions` | `actions` record per transaction, with the selected actions | the action (or its multi-signature inner action) |
//! | `blocks` | `block` record | (no filter) |
//! | `stats` | `stats` record | (no filter) |

use crate::{
    records::{self, ActionsRecord, SummaryRecord, accepts_action, transaction_kinds},
    shutdown_signal,
};
use anyhow::{Context, Result};
use clap::Args;
use hyperliquid_stream::{Client, Event, Transaction};
use hyperliquid_ws::{Hub, WsConfig, WsServer};
use std::net::SocketAddr;
use tracing::info;

const MEMPOOL: &str = "mempool";
const MEMPOOL_TRANSACTIONS: &str = "mempoolTransactions";
const MEMPOOL_ACTIONS: &str = "mempoolActions";
const BLOCKS: &str = "blocks";
const STATS: &str = "stats";
const TOPICS: &[&str] = &[
    MEMPOOL,
    MEMPOOL_TRANSACTIONS,
    MEMPOOL_ACTIONS,
    BLOCKS,
    STATS,
];

#[derive(Clone, Debug, Args)]
pub(crate) struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8546")]
    listen: SocketAddr,
    /// Concurrent client connections.
    #[arg(long, default_value_t = 1024)]
    max_connections: usize,
    /// Subscriptions per connection.
    #[arg(long, default_value_t = 64)]
    max_subscriptions: usize,
    /// Messages queued per connection before a slow client is disconnected.
    #[arg(long, default_value_t = 16_384)]
    queue: usize,
    /// Bytes queued per connection before a slow client is disconnected (MiB).
    #[arg(long, default_value_t = 64)]
    queue_mib: usize,
}

pub(crate) async fn serve(client: &mut Client, args: &ServeArgs) -> Result<()> {
    let hub = Hub::new(TOPICS);
    let config = WsConfig {
        addr: args.listen,
        max_connections: args.max_connections,
        max_subscriptions: args.max_subscriptions,
        queue_messages: args.queue,
        queue_bytes: args.queue_mib.saturating_mul(1024 * 1024),
        ..WsConfig::default()
    };
    let server = WsServer::start(config, hub.clone())
        .await
        .with_context(|| format!("listening on {}", args.listen))?;
    info!(addr = %server.local_addr(), topics = ?TOPICS, "serving WebSocket subscriptions");
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            event = client.next_event() => match event {
                Some(event) => publish(&hub, &event),
                None => break,
            },
            () = &mut shutdown => break,
        }
    }
    Ok(())
}

fn publish(hub: &Hub, event: &Event) {
    match event {
        Event::Transaction(tx) => publish_transaction(hub, tx),
        Event::Block(block) => {
            hub.publish(BLOCKS, &[], &records::block(block));
        }
        Event::Stats(stats) => {
            hub.publish(STATS, &[], &records::stats(stats));
        }
        _ => {}
    }
}

/// Publishes a transaction to the mempool topics, preparing only the
/// records that someone subscribed to.
fn publish_transaction(hub: &Hub, tx: &Transaction) {
    let topics = [MEMPOOL, MEMPOOL_TRANSACTIONS].map(|topic| hub.is_subscribed(topic));
    if topics.contains(&true) {
        let kinds = transaction_kinds(tx);
        if topics[0] {
            hub.publish(MEMPOOL, &kinds, &SummaryRecord::new(tx));
        }
        if topics[1] {
            hub.publish(MEMPOOL_TRANSACTIONS, &kinds, &records::transaction(tx));
        }
    }
    if hub.is_subscribed(MEMPOOL_ACTIONS) {
        // One message per transaction and group of subscribers, with the
        // actions their filter selects: far fewer and better compressed
        // messages than one per action.
        hub.publish_with(MEMPOOL_ACTIONS, |filter| {
            let selected: Vec<usize> = tx
                .actions()
                .iter()
                .enumerate()
                .filter(|(_, action)| accepts_action(filter, action))
                .map(|(index, _)| index)
                .collect();
            (!selected.is_empty()).then(|| ActionsRecord::new(tx, selected))
        });
    }
}
