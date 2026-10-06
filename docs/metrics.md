# Metrics and logs

`--metrics-addr 0.0.0.0:9100` serves Prometheus metrics at `/metrics`.
`--log-format json` writes one JSON object per log line, for collectors such
as Loki; the level is set with `RUST_LOG`.

[`deploy/`](../deploy) runs `hl-stream serve` with Prometheus and a
provisioned Grafana dashboard:

```sh
docker compose -f deploy/docker-compose.yml up -d
# Grafana: http://localhost:3000 (admin / admin), dashboard "Hyperliquid stream"
```

## Library users

The library records metrics through the [`metrics`](https://docs.rs/metrics)
facade, declared with `metrics-derive`. Names are relative
(`transactions_total`): apply the prefix
(`hyperliquid_stream::metrics::PREFIX`, for example with
`metrics_util::layers::PrefixLayer`) and install the recorder before
`Client::connect`.

## Reference

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
| `hl_stream_ws_notifications_total` / `_slow_clients_total` | counter | Messages queued, clients dropped for lagging |
| `hl_stream_ws_compressed_connections_total` / `_compressed_messages_total` | counter | Connections using zstd, messages compressed |
| `hl_stream_ws_compression_duration_seconds` | histogram | Time to compress one message |
| `hl_stream_process_start_time_seconds`, `hl_stream_process_build_info{version}` | gauge | Process information |

Mempool coverage, the share of committed transactions seen in the mempool
first, is `hl_stream_blocks_transactions_seen_total` over
`hl_stream_blocks_transactions_total`.
