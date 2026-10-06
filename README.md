# hyperliquid-stream

Stream the [Hyperliquid](https://hyperliquid.xyz) mempool and blocks directly
from P2P peers: orders, cancels, transfers and EVM transactions, before they
are committed in a block. No node, no API key.

![hl-stream streaming the mempool](docs/media/demo.gif)

- **CLI**: `hl-stream` writes JSON lines, or serves a WebSocket API in the
  style of the Hyperliquid API.
- **Rust library**: a typed async stream of decoded transactions and blocks.
- **Operations**: Prometheus metrics and a ready-made Grafana dashboard.

## Quick start

```sh
cargo install --path bin/hl-stream
hl-stream --peers-cache peers.json
```

Peers only stream to clients they can reach: **open inbound TCP ports 4001
and 4002**. Finding peers that relay the mempool can take a minute or two;
`--peers-cache` remembers them for the next start. See `hl-stream --help`
for all options.

## Output

`hl-stream` writes one JSON record per line: one per action by default,
`--format transactions` or `--format summary` for one per transaction.
`--kinds order,cancel,evmRawTx` filters by action kind, and `--blocks` adds
blocks.

```json
{"type":"summary","hash":"0xf7e1…","receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","size":126428,"actions":{"cancel":131,"cancelByCloid":316,"order":335}}
```

See [output records](docs/output.md).

## WebSocket

```sh
hl-stream serve --listen 0.0.0.0:8546
```

```text
client: {"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order"]}}
server: {"channel":"mempoolActions","data":{"tx":"0x…","actions":[{"kind":"order",…}],…}}
```

Subscriptions: `mempool`, `mempoolTransactions`, `mempoolActions`, `blocks`
and `stats`. Connect with `?compression=zstd` for compressed frames. See the
[WebSocket API](docs/websocket.md).

## Library

```rust,no_run
use hyperliquid_stream::{Client, Config, Event};

#[tokio::main]
async fn main() -> Result<(), hyperliquid_stream::Error> {
    let mut client = Client::connect(Config::default()).await?;
    while let Some(event) = client.next_event().await {
        if let Event::Transaction(tx) = event {
            for action in tx.actions() {
                println!("{} {}", tx.hash, action.kind());
            }
        }
    }
    Ok(())
}
```

## Metrics

`--metrics-addr 0.0.0.0:9100` serves Prometheus metrics, and
`docker compose -f deploy/docker-compose.yml up -d` starts `hl-stream` with
Prometheus and Grafana. See [metrics](docs/metrics.md).

## Limitations

- Hyperliquid's P2P protocol is undocumented and changes with node
  upgrades, which can break this tool without notice. Last validated on
  mainnet in October 2026.
- Mempool transactions are not executed: an action may never be committed,
  or be rejected by the exchange.
- Coverage depends on the peers found: some transactions only appear in
  blocks.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Libraries live in `crates/`, the binary in `bin/`. See
[how it works](docs/architecture.md).

## License

[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
