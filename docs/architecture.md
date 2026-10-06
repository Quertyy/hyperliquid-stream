# How it works

```text
            ┌──────────────── hyperliquid-stream ────────────────┐
 peers ──►  │ sessions (one per peer, decode in parallel)        │
 :4001      │   └─ shared seen-set: decode each transaction once │ ──► events ──► hl-stream
            │ supervisor: rank, replace, discover peers          │               stdout / WebSocket
 peers ◄──  │ callback server :4001 / :4002                      │               /metrics
            └────────────────────────────────────────────────────┘
```

1. **Handshake.** The client asks a peer for the split broadcast group, where
   transactions are sent separately from blocks. Before answering, the peer
   connects back to the client's ports 4001 and 4002; the callback server
   answers these checks.
2. **Messages.** Each frame is a block or a standalone transaction. Only the
   leading block fields are decoded (proposer, transaction hashes,
   timestamp), so certificate format changes do not affect streaming.
3. **Deduplication.** Sessions share a set of transaction hashes: the first
   copy is decoded and emitted, later copies are counted as duplicates.
   Blocks mark their transactions as committed, which measures inclusion
   delay and coverage. Hashes are forgotten after `--retention`.
4. **Peers.** Candidates come from seeds, the peer cache, the info API
   (`gossipRootIps`) and the gossip RPC `ChildPeerStatuses`. A node forwards
   the mempool only if every node between it and the validators does, so
   relays are rare. While fewer than `--min-relay-peers` sessions relay,
   sessions that relay only blocks are replaced after 20 seconds; once enough
   relay, only they and one spare session stay open. Peers whose blocks lag
   more than `--max-lag` are replaced.
5. **Backlog.** A newly connected peer first replays recent blocks. Blocks
   older than `--max-lag` still mark their transactions as committed but are
   not emitted.

All buffers are bounded: frames (16 MiB), bundles (4 MiB), the event
channel, per-client WebSocket queues, known peers and remembered hashes.

## Crates

| Crate | Role |
| --- | --- |
| [`hyperliquid-protocol`](../crates/protocol) | Codecs for frames, messages and action bundles, no I/O |
| [`hyperliquid-stream`](../crates/stream) | Peer sessions, discovery, supervisor, the `Client` stream |
| [`hyperliquid-ws`](../crates/ws) | WebSocket subscription hub and server, independent of the mempool |
| [`hl-stream`](../bin/hl-stream) | The command-line tool |
