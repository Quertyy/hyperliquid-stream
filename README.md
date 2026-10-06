# hyperliquid-stream

Stream what [Hyperliquid](https://hyperliquid.xyz) nodes gossip to each other,
directly from P2P peers: the HyperCore mempool and consensus blocks.

- **Mempool**: everything observed before it is committed in a block. Nodes
  relay signed action bundles (orders, cancels, modifies, transfers, EVM
  transactions such as `evmRawTx`…) before validators include them.
- **Blocks**: committed blocks, with their transaction hashes, consensus
  time and lag, and how many of their transactions the mempool showed first.

`hyperliquid-stream` connects to several peers, keeps the ones that relay the
mempool, and delivers each transaction and block once, decoded, as soon as the
first peer sends it. It needs no node, no API key and no execution engine.

- **CLI** `hl-stream`: JSON lines on stdout, or subscriptions over
  WebSocket.
- **Rust library**: a typed async stream of decoded transactions and blocks.
- **Operations**: Prometheus metrics, JSON logs and a ready-made Grafana
  dashboard.

> Transactions in the mempool are not executed. An action may never be
> committed, or be committed and then rejected by the exchange.

> **Compatibility.** Hyperliquid does not document or version its P2P
> protocol, and node upgrades by the Hyperliquid team can change it at any
> time. This tool may then stop connecting, stop finding relays, or decode
> some messages incorrectly until it is updated. It was last validated
> against mainnet in October 2026. Watch `hl_stream_decode_errors_total`
> and the logs after each Hyperliquid upgrade, and report breakage with the
> node version and the error.

## Quick start

```sh
cargo install --path bin/hl-stream   # from a clone of this repository
hl-stream --peers-cache peers.json
```

Peers only stream to clients they can reach: **TCP ports 4001 and 4002 must
accept inbound connections** on the host, and no other process (such as a
local `hl-node`) may use them. Use `--callback-ip` to bind a specific
address.

The first start discovers peers through the info API and the peers'
own child lists. Few peers relay the mempool (a node forwards it only when
every node between it and the validators does), and most peers accept few
clients and answer `PeerFull`, so finding relays can take a few minutes.

- `--min-relay-peers N` (default 2, `0` to disable) is the number of relays
  to aim for: while fewer sessions relay transactions, every session that
  relayed only blocks for 20 seconds is replaced by the next candidate. The
  log warns when the count drops below the minimum and reports each peer
  found to relay; `hl_stream_peers_relaying` tracks it. It is a best effort:
  at times the network has fewer relays than requested, or none.
- `--peers-cache` remembers good peers, with how much each relayed, so known
  relays are tried first on the next start.

## Output

`hl-stream stream` (the default command) writes one JSON record per line.
Logs go to stderr.

| `--format` | Record |
| --- | --- |
| `actions` (default) | one `action` record per action |
| `transactions` | one `transaction` record per transaction, with all its actions |
| `summary` | one `summary` record per transaction, with its action count per kind |

`--kinds order,cancel,evmRawTx` keeps only those action kinds (the inner kind
of multi-signature actions also matches). `--blocks` adds a `block` record per
block, and `--stats` periodic `stats` records.

```json
{"type":"action","tx":"0x9f3c…","index":2,"receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","kind":"cancel","encoding":"msgpack","nonce":1789506279724,"vaultAddress":"0x7d0c59691fec7f3fea385cdc3126c37a41d91b28","isFrontend":false,"expiresAfter":null,"signature":"0x8873…","action":["cancel",[[159,546119392617]],true]}
{"type":"action","tx":"0x9f3c…","index":3,"receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","kind":"order","encoding":"compactOrders","nonce":1789506279753,"vaultAddress":null,"isFrontend":false,"expiresAfter":null,"signature":"0x0650…","action":{"grouping":"na","builder":null,"orders":[{"asset":0,"isBuy":false,"price":"75736","size":"0.01","reduceOnly":false,"orderType":{"limit":{"tif":"Alo"}},"cloid":"0x63f244e0ba3b49b3aca4922450490db2"}]}}
{"type":"summary","hash":"0xf7e1…","receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","size":126428,"actions":{"batchModify":6,"cancel":131,"cancelByCloid":316,"modify":2,"noop":8,"order":335}}
{"type":"block","timeMs":1791216060083,"proposer":"0xdf35…","receivedAtMs":1791216061121,"lagMs":1038,"peer":"46.250.251.246:4001","txHashes":["0x365f…"],"seenInMempool":1,"maxInclusionDelayMs":412}
```

Action fields:

- `kind`: the action name, as in the exchange API (`order`, `cancel`,
  `cancelByCloid`, `batchModify`, `evmRawTx`, `multiSig`…).
- `encoding`: `msgpack`, `compactOrders` or `multiSig`.
- `action`: the decoded body. Compact order batches are typed, with exact
  decimal strings. Other actions are rendered losslessly from MessagePack:
  most are positional arrays `[kind, fields…]`, exactly as transported, and
  binary values are hex strings. A body that fails to decode becomes
  `{"error", "body"}` while the rest of the transaction is kept.

`hash` (`tx` in action records) is the transaction's consensus hash: the
Keccak-256 of its bytes, which is also how blocks reference it.

## WebSocket

```sh
hl-stream serve --listen 0.0.0.0:8546
```

Messages are JSON objects in the style of the Hyperliquid API: requests
carry a `method`, server messages a `channel` and `data`.

```text
-> {"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
<- {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["cancel","order"]}}}
<- {"channel":"mempoolActions","data":{"type":"actions","tx":"0x…","receivedAtMs":…,"peer":"…","actions":[{"index":3,"kind":"order",...},{"index":7,"kind":"cancel",...}]}}
-> {"method":"unsubscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
<- {"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{...}}}
-> {"method":"ping"}
<- {"channel":"pong"}
<- {"channel":"error","data":"unknown subscription type: trades"}
```

| Subscription `type` | `data` | `kinds` selects |
| --- | --- | --- |
| `mempool` | `summary` record per transaction | transactions with a matching action |
| `mempoolTransactions` | `transaction` record | transactions with a matching action |
| `mempoolActions` | `actions` record per transaction, holding only the matching actions | matching actions (multi-signature inner kinds match too) |
| `blocks` | `block` record | — |
| `stats` | `stats` record | — |

To try it:
`cargo run --release --example subscribe -- ws://127.0.0.1:8546 mempoolActions order,cancel`.

**Compression.** Connect to `ws://<host>:8546/?compression=zstd` to receive
published messages as binary WebSocket frames, each one complete
[zstd](https://facebook.github.io/zstd/) frame holding the same JSON; replies
to requests (`subscriptionResponse`, `pong`, `error`) stay text. Decompress
every binary frame and parse it like a text frame. Messages shrink several
fold, which matters for broad subscriptions such as all `mempoolActions`.
`compression=none` is the default; another value is refused with HTTP 400.

A subscription is identified by its content: unsubscribe with the same
`subscription` object. The `subscriptionResponse` always arrives before the
subscription's first message, and a record matched by several of a
client's subscriptions is sent once: a client with several `mempoolActions`
subscriptions receives one message per transaction with the union of the
actions they select. Requests are validated strictly:
unknown methods, types or fields are reported on the `error` channel, as is
exceeding `--max-subscriptions`.

Each record is serialized once per channel, and nothing is serialized for
topics nobody subscribed to. A client is disconnected when it falls more than `--queue` messages or
`--queue-mib` MiB behind, stops reading for 10 seconds, or stops answering
pings, so one slow client cannot slow the others.

## Library

```toml
[dependencies]
hyperliquid-stream = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use hyperliquid_stream::{Client, Config, Event};

#[tokio::main]
async fn main() -> Result<(), hyperliquid_stream::Error> {
    let mut client = Client::connect(Config::default()).await?;
    while let Some(event) = client.next_event().await {
        match event {
            Event::Transaction(tx) => {
                for action in tx.actions() {
                    println!("{} {} {}", tx.hash, action.kind(), action.nonce);
                }
            }
            Event::Block(block) => println!("block: {} transactions", block.tx_hashes.len()),
            Event::Stats(stats) => println!("{} peers", stats.active_peers.len()),
            _ => {} // events may be added in future versions
        }
    }
    Ok(())
}
```

`Client` also implements `futures::Stream`. Events are buffered up to
`Config::channel_capacity`; when the consumer is slower, peers are read more
slowly rather than events being dropped. See
[`examples/stream.rs`](crates/stream/examples/stream.rs).

The codecs live in
[`hyperliquid-protocol`](crates/protocol), which
has no I/O or async runtime dependency.

## Metrics and dashboard

The library records metrics through the [`metrics`](https://docs.rs/metrics)
facade, declared with `metrics-derive`. Names are relative
(`transactions_total`); `hl-stream` prefixes them all with `hl_stream_`
through `metrics_util::layers::PrefixLayer`. Library users choose the
exporter, apply the prefix (`hyperliquid_stream::metrics::PREFIX`) and
install the recorder before `Client::connect`.

`--metrics-addr 0.0.0.0:9100` serves Prometheus metrics at `/metrics`, and
`--log-format json` writes one JSON object per log line for collectors such as
Loki. The log level is set with `RUST_LOG`.

[`deploy/`](deploy) runs `hl-stream serve` with Prometheus and Grafana:

```sh
docker compose -f deploy/docker-compose.yml up -d
# Grafana: http://localhost:3000 (admin / admin), dashboard "Hyperliquid stream"
```

The dashboard shows transactions and actions per second by kind, mempool
coverage (the share of committed transactions seen in the mempool first),
inclusion delay and block lag percentiles, bandwidth, peers and session
outcomes, decode errors and WebSocket clients.

| Metric | Type | Description |
| --- | --- | --- |
| `hl_stream_transactions_total` | counter | First-seen mempool transactions |
| `hl_stream_transactions_duplicates_total` | counter | Copies relayed by other peers |
| `hl_stream_transactions_bytes_total` | counter | Bytes of first-seen transactions |
| `hl_stream_transactions_pending` | gauge | Seen but not yet committed |
| `hl_stream_transactions_inclusion_delay_seconds` | histogram | Mempool to committing block |
| `hl_stream_actions_total{kind}` | counter | Actions of first-seen transactions |
| `hl_stream_decode_errors_total{stage}` | counter | Undecodable messages, bundles or actions |
| `hl_stream_blocks_total` | counter | Live blocks |
| `hl_stream_blocks_stale_total` | counter | Blocks replayed from a peer's backlog |
| `hl_stream_blocks_transactions_total` | counter | Transactions committed by live blocks |
| `hl_stream_blocks_transactions_seen_total` | counter | Of those, seen in the mempool first |
| `hl_stream_blocks_lag_seconds` | histogram | Consensus time to block reception |
| `hl_stream_peers_known` / `_active` / `_relaying` | gauge | Peer counts |
| `hl_stream_peers_sessions_total{outcome}` | counter | Sessions opened and ended, by reason |
| `hl_stream_peers_received_bytes_total` | counter | Frame bytes received from peers |
| `hl_stream_discovery_requests_total{source,result}` | counter | Discovery requests |
| `hl_stream_callbacks_total{service}` | counter | Peer callbacks answered |
| `hl_stream_ws_connections` / `_subscriptions` | gauge | WebSocket clients and subscriptions |
| `hl_stream_ws_notifications_total` / `_slow_clients_total` | counter | Notifications queued, clients dropped for lagging |
| `hl_stream_ws_compressed_connections_total` / `_compressed_messages_total` | counter | Connections opened with zstd, messages compressed (once each) |
| `hl_stream_ws_compression_duration_seconds` | histogram | Time to compress one message, off the publishing thread |
| `hl_stream_process_start_time_seconds`, `hl_stream_process_build_info{version}` | gauge | Process information |

## How it works

```text
            ┌───────────── hyperliquid-stream ─────────────┐
 peers ──►  │ sessions (one per peer, decode in parallel)   │
 :4001      │   └─ shared seen-set: decode each tx once     │ ──► Event stream ──► hl-stream
            │ supervisor: rank, replace, rotate, discover   │                      stdout / WebSocket
 peers ◄──  │ callback server :4001 / :4002                 │                      /metrics
            └───────────────────────────────────────────────┘
```

1. **Handshake.** The client sends a greeting asking for the split broadcast
   group, where peers send transactions separately from blocks. The peer
   connects back to the client's ports 4001 and 4002 before answering
   `Streaming`; the callback server answers these checks.
2. **Messages.** Each frame is a block or a standalone transaction. Only the
   leading block fields are decoded (proposer, transaction hashes,
   timestamp), so certificate format changes do not affect streaming.
3. **Deduplication.** Sessions share a set of transaction hashes: the first
   copy is decoded and emitted, later copies are counted as duplicates.
   Blocks mark their transactions as committed, which measures inclusion
   delay and coverage.
4. **Peer management.** Candidates come from seeds, the cache, the info API
   (`gossipRootIps`) and the gossip RPC `ChildPeerStatuses`. Peers that
   relayed transactions are preferred. A peer whose blocks lag more than
   `--max-lag` for 20 seconds is replaced. While fewer than
   `--min-relay-peers` peers relay transactions, every peer that streamed
   only blocks for 20 seconds is replaced, because a node forwards the
   mempool only if every node between it and the validators does.
5. **Backlog.** A newly connected peer first replays recent blocks. Blocks
   older than `--max-lag` still mark their transactions as committed but are
   not emitted.

All buffers are bounded: frames (16 MiB), the event channel, per-client
WebSocket queues, known peers and remembered hashes (`--retention`).

## Limitations

- Coverage depends on the peers found: some transactions only appear in
  blocks. Watch `hl_stream_blocks_transactions_seen_total` against
  `hl_stream_blocks_transactions_total`.
- Most actions are decoded generically from MessagePack; only compact order
  batches and multi-signature envelopes are typed.
- The protocol is undocumented and changes with Hyperliquid node upgrades,
  which can break this tool without notice (see the compatibility note at
  the top). A rise in
  `hl_stream_decode_errors_total` signals a format change.
- Peers do not authenticate themselves; signatures are reported, not
  verified.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Libraries live in `crates/`, the binary in `bin/`.

| Crate | Role |
| --- | --- |
| [`hyperliquid-protocol`](crates/protocol) | Codecs, no I/O |
| [`hyperliquid-stream`](crates/stream) | Peer sessions, discovery, supervisor, `Client` stream |
| [`hyperliquid-ws`](crates/ws) | WebSocket subscription hub and server, independent of the mempool |
| [`hl-stream`](bin/hl-stream) | The `hl-stream` binary |

Decoding tests run offline on bundles captured on mainnet
(`crates/protocol/tests/fixtures`). The end-to-end test
streams from local peers that speak the protocol.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
