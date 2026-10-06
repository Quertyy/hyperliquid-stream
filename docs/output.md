# Output records

`hl-stream stream` writes one JSON record per line on stdout; logs go to
stderr. Every record has a `type`.

| `--format` | Records |
| --- | --- |
| `actions` (default) | one `action` record per action |
| `transactions` | one `transaction` record per transaction, with all its actions |
| `summary` | one `summary` record per transaction, with its action count per kind |

`--kinds order,cancel,evmRawTx` keeps only those action kinds (the inner kind
of multi-signature actions also matches). `--blocks` adds `block` records and
`--stats` periodic `stats` records.

```json
{"type":"action","tx":"0x9f3c…","index":3,"receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","kind":"order","encoding":"compactOrders","nonce":1789506279753,"vaultAddress":null,"isFrontend":false,"expiresAfter":null,"signature":"0x0650…","action":{"grouping":"na","builder":null,"orders":[{"asset":0,"isBuy":false,"price":"75736","size":"0.01","reduceOnly":false,"orderType":{"limit":{"tif":"Alo"}},"cloid":"0x63f2…"}]}}
{"type":"summary","hash":"0xf7e1…","receivedAtMs":1791216081747,"peer":"181.41.140.238:4001","size":126428,"actions":{"batchModify":6,"cancel":131,"cancelByCloid":316,"modify":2,"noop":8,"order":335}}
{"type":"block","timeMs":1791216060083,"proposer":"0xdf35…","receivedAtMs":1791216061121,"lagMs":1038,"peer":"46.250.251.246:4001","txHashes":["0x365f…"],"seenInMempool":1,"maxInclusionDelayMs":412}
```

## Action fields

- `kind`: the action name, as in the exchange API (`order`, `cancel`,
  `cancelByCloid`, `batchModify`, `evmRawTx`, `multiSig`…).
- `encoding`: `msgpack`, `compactOrders` or `multiSig`.
- `action`: the decoded body. Compact order batches are typed, with exact
  decimal strings. Other actions are rendered losslessly from MessagePack:
  most are positional arrays `[kind, fields…]`, exactly as transported, and
  binary values are hex strings. A body that fails to decode becomes
  `{"error", "body"}` while the rest of the transaction is kept.

`hash` (`tx` in action records) is the transaction's consensus hash, the
Keccak-256 of its bytes, which is also how blocks reference it.
