# WebSocket API

```sh
hl-stream serve --listen 0.0.0.0:8546
```

Messages are JSON objects in the style of the Hyperliquid API: requests carry
a `method`, server messages a `channel` and `data`.

```text
client: {"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
server: {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["cancel","order"]}}}
server: {"channel":"mempoolActions","data":{"type":"actions","tx":"0x…","receivedAtMs":…,"peer":"…","actions":[{"index":3,"kind":"order",…},{"index":7,"kind":"cancel",…}]}}
client: {"method":"unsubscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
server: {"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{…}}}
client: {"method":"ping"}
server: {"channel":"pong"}
server: {"channel":"error","data":"unknown subscription type: trades"}
```

## Subscriptions

| `type` | `data` | `kinds` selects |
| --- | --- | --- |
| `mempool` | `summary` record per transaction | transactions with a matching action |
| `mempoolTransactions` | `transaction` record | transactions with a matching action |
| `mempoolActions` | `actions` record per transaction, with only the matching actions | matching actions (multi-signature inner kinds match too) |
| `blocks` | `block` record | — |
| `stats` | `stats` record | — |

Records have the same fields as the [stdout output](output.md).

- A subscription is identified by its content: unsubscribe with the same
  `subscription` object.
- The `subscriptionResponse` always arrives before the subscription's first
  message.
- A record matched by several of a client's subscriptions is sent once. With
  several `mempoolActions` subscriptions, a client receives one message per
  transaction with the union of the actions they select.
- Requests are validated strictly: unknown methods, types or fields, and
  exceeding `--max-subscriptions`, are reported on the `error` channel.

## Compression

Connect to `ws://<host>:8546/?compression=zstd` to receive published messages
as binary WebSocket frames, each one complete [zstd](https://facebook.github.io/zstd/)
frame holding the same JSON. Replies to requests (`subscriptionResponse`,
`pong`, `error`) stay text. Decompress every binary frame and parse it like a
text frame. `mempoolTransactions` and `mempoolActions` shrink about fivefold.
`compression=none` is the default; another value is refused with HTTP 400.

## Slow clients

Each record is serialized once per group of subscribers, and nothing is
serialized for subscriptions nobody holds. A client is disconnected when it
falls more than `--queue` messages or `--queue-mib` MiB behind, stops reading
for 10 seconds, or stops answering pings, so one slow client cannot slow the
others.

## Trying it

```sh
cargo run --release --example subscribe -- ws://127.0.0.1:8546 mempoolActions order,cancel
cargo run --release --example subscribe -- 'ws://127.0.0.1:8546/?compression=zstd' blocks
```
