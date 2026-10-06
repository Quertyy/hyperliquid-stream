//! Subscribes to an `hl-stream serve` topic and prints the notifications.
//!
//! ```sh
//! cargo run --example subscribe -- ws://127.0.0.1:8546 mempoolActions order,cancel
//! cargo run --example subscribe -- ws://127.0.0.1:8546 blocks
//! cargo run --example subscribe -- 'ws://127.0.0.1:8546/?compression=zstd' mempoolActions
//! ```

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .unwrap_or_else(|| "ws://127.0.0.1:8546".to_owned());
    let topic = args.next().unwrap_or_else(|| "mempool".to_owned());
    let kinds: Vec<String> = args
        .next()
        .map(|kinds| kinds.split(',').map(str::to_owned).collect())
        .unwrap_or_default();

    let (mut socket, _) = connect_async(url.as_str()).await?;
    let request = json!({
        "method": "subscribe",
        "subscription": {"type": topic, "kinds": kinds},
    });
    socket.send(Message::text(request.to_string())).await?;

    let started = Instant::now();
    let mut received = 0_u64;
    while let Some(message) = socket.next().await {
        // With `?compression=zstd`, published messages arrive as binary zstd
        // frames holding the same JSON.
        let value: Value = match message? {
            Message::Text(text) => serde_json::from_str(text.as_str())?,
            Message::Binary(compressed) => {
                serde_json::from_slice(&zstd::decode_all(&*compressed)?)?
            }
            _ => continue,
        };
        match value["channel"].as_str() {
            Some("error") => return Err(format!("error: {}", value["data"]).into()),
            Some("subscriptionResponse") => {
                eprintln!("subscribed: {}", value["data"]["subscription"]);
                continue;
            }
            _ => {}
        }
        received += 1;
        println!("{}", value["data"]);
        if received.is_multiple_of(1000) {
            let rate = received as f64 / started.elapsed().as_secs_f64();
            eprintln!("{received} notifications, {rate:.0}/s");
        }
    }
    Ok(())
}
