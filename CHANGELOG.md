# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `hyperliquid-protocol`: codecs for frames, the stream greeting,
  stream messages, the `ChildPeerStatuses` RPC and signed action bundles.
- `hyperliquid-stream`: the `Client` stream of mempool transactions and blocks, with peer discovery, ranking,
  lag and relay management, cross-peer deduplication, inclusion tracking and
  metrics.
- `hyperliquid-ws`: a domain-independent WebSocket subscription hub
  and server, with Hyperliquid-style `channel`/`data` messages and bounded,
  serialize-once fan-out; clients connecting with `?compression=zstd`
  receive published messages as zstd-compressed binary frames.
- `hl-stream`: JSON lines output, a WebSocket server, Prometheus metrics
  declared with `metrics-derive`, and JSON logs.
- `--min-relay-peers` (`Config::min_relay_peers`): sessions that relay only
  blocks are replaced after a 20-second trial until enough peers relay the
  mempool, with logs when the minimum is lost or reached.
- Docker image, and a Prometheus and Grafana deployment with a dashboard.
