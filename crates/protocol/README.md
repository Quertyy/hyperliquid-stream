# hyperliquid-protocol

Codecs for the Hyperliquid peer-to-peer protocol and HyperCore signed action
bundles, without I/O or async runtime.

- `wire`: frames (with LZ4), the consensus stream greeting, stream messages
  (standalone transactions and block headers) and the gossip RPC
  `ChildPeerStatuses`.
- `action`: signed action bundles and their bodies: MessagePack actions,
  compact order batches and multi-signature envelopes, serializable to JSON.

```rust
use hyperliquid_protocol::{Bytes, action::Bundle, keccak256};

fn print(payload: Bytes) {
    println!("transaction {}", keccak256(&payload));
    if let Ok(bundle) = Bundle::decode(&payload) {
        for action in &bundle.actions {
            println!("{} {}", action.kind(), serde_json::to_string(action).unwrap());
        }
    }
}
```

Decoders check every length against the bytes present before allocating,
bound nesting, and share binary values with the input buffer.

Used by [`hyperliquid-stream`](https://crates.io/crates/hyperliquid-stream)
to stream the mempool from live peers. Licensed under Apache-2.0 or MIT.
