//! Prints one line per mempool transaction with its action counts.
//!
//! ```sh
//! cargo run --example stream                       # discover peers
//! cargo run --example stream -- 1.2.3.4:4001       # start from a known peer
//! RUST_LOG=hyperliquid_stream=debug cargo run --example stream
//! ```
//!
//! Inbound TCP ports 4001 and 4002 must be reachable.

use hyperliquid_stream::{Client, Config, Event};
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hyperliquid_stream=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let peers = std::env::args()
        .skip(1)
        .map(|peer| peer.parse())
        .collect::<Result<_, _>>()?;
    let mut client = Client::connect(Config {
        peers,
        ..Config::default()
    })
    .await?;

    loop {
        let event = tokio::select! {
            event = client.next_event() => event,
            _ = tokio::signal::ctrl_c() => break,
        };
        match event {
            Some(Event::Transaction(tx)) => {
                let mut kinds = BTreeMap::<&str, usize>::new();
                for action in tx.actions() {
                    *kinds.entry(action.kind()).or_default() += 1;
                }
                println!("{} from {} {kinds:?}", tx.hash, tx.peer);
            }
            Some(Event::Block(block)) => println!(
                "block with {} transactions, {} seen in the mempool, lag {} ms",
                block.tx_hashes.len(),
                block.seen_in_mempool,
                block.lag_ms
            ),
            Some(_) => {}
            None => break,
        }
    }
    client.shutdown().await;
    Ok(())
}
