//! WebSocket server for a [`Hub`].
//!
//! Requests and messages are JSON objects, in the style of the Hyperliquid
//! API:
//!
//! ```text
//! -> {"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
//! <- {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"mempoolActions","kinds":["cancel","order"]}}}
//! <- {"channel":"mempoolActions","data":{...}}
//! -> {"method":"unsubscribe","subscription":{"type":"mempoolActions","kinds":["order","cancel"]}}
//! <- {"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{...}}}
//! -> {"method":"ping"}
//! <- {"channel":"pong"}
//! <- {"channel":"error","data":"unknown subscription type: nope"}
//! ```
//!
//! A subscription is identified by its content. The subscription response
//! is sent before the first message of the subscription.
//!
//! A client that connects with `?compression=zstd` receives published
//! messages as binary frames, each one complete zstd frame holding the same
//! JSON; replies to its requests stay text. `compression=none` is the
//! default, and any other value is refused with HTTP 400.
//!
//! Each client has a queue bounded in messages and bytes. A client that
//! falls behind, stops reading or stops answering pings is disconnected
//! instead of slowing the others or growing memory.

use crate::{
    KindFilter,
    hub::Hub,
    outbox::{Outbox, Outgoing},
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::tungstenite::{
    self, Message,
    handshake::server::{self, ErrorResponse, Response},
    http::StatusCode,
    protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
};
use tracing::{debug, warn};

/// A request from a client.
#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "camelCase", deny_unknown_fields)]
enum Request {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
    Ping,
}

/// What a client subscribes to: a topic and, optionally, the kinds of
/// records it wants.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    #[serde(rename = "type")]
    topic: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    kinds: Vec<String>,
}

/// Server address and limits. [`Default`] listens on `127.0.0.1:8546`.
#[derive(Clone, Debug)]
pub struct WsConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// Concurrent client connections.
    pub max_connections: usize,
    /// Subscriptions per connection.
    pub max_subscriptions: usize,
    /// Messages queued per connection before it is disconnected.
    pub queue_messages: usize,
    /// Bytes queued per connection before it is disconnected.
    pub queue_bytes: usize,
    /// Largest accepted request.
    pub max_request_bytes: usize,
    /// Interval between pings; a client silent for two intervals is dropped.
    pub ping_interval: Duration,
    /// Longest wait for a client to complete the handshake or accept data.
    pub io_timeout: Duration,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 8546)),
            max_connections: 1024,
            max_subscriptions: 64,
            queue_messages: 16_384,
            queue_bytes: 64 * 1024 * 1024,
            max_request_bytes: 64 * 1024,
            ping_interval: Duration::from_secs(30),
            io_timeout: Duration::from_secs(10),
        }
    }
}

/// A running server; dropping it stops accepting connections.
#[derive(Debug)]
pub struct WsServer {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl WsServer {
    /// Binds `config.addr` and starts serving `hub`.
    pub async fn start(config: WsConfig, hub: Hub) -> io::Result<Self> {
        let listener = TcpListener::bind(config.addr).await?;
        let local_addr = listener.local_addr()?;
        let task = tokio::spawn(accept_loop(listener, Arc::new(config), hub));
        Ok(Self { local_addr, task })
    }

    /// Address the server listens on.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for WsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn accept_loop(listener: TcpListener, config: Arc<WsConfig>, hub: Hub) {
    let active = Arc::new(AtomicUsize::new(0));
    let mut next_connection = 0_u64;
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                warn!(%error, "accepting a WebSocket connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        if active.load(Ordering::Relaxed) >= config.max_connections {
            debug!(%addr, "connection limit reached");
            continue;
        }
        next_connection += 1;
        let connection = next_connection;
        let (hub, config, active) = (hub.clone(), Arc::clone(&config), Arc::clone(&active));
        let count = active.fetch_add(1, Ordering::Relaxed) + 1;
        hub.metrics().connections.set(count as f64);
        tokio::spawn(async move {
            debug!(%addr, connection, "client connected");
            if let Err(error) = serve_connection(connection, stream, &hub, &config).await {
                debug!(%addr, connection, %error, "client connection failed");
            }
            let count = active.fetch_sub(1, Ordering::Relaxed) - 1;
            hub.metrics().connections.set(count as f64);
        });
    }
}

/// Runs `operation`, failing if it takes longer than `limit`.
async fn timed<T>(
    limit: Duration,
    operation: impl Future<Output = Result<T, tungstenite::Error>>,
) -> Result<T, tungstenite::Error> {
    timeout(limit, operation).await.map_err(|_| {
        tungstenite::Error::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "client did not keep up",
        ))
    })?
}

async fn serve_connection(
    connection: u64,
    stream: TcpStream,
    hub: &Hub,
    config: &WsConfig,
) -> Result<(), tungstenite::Error> {
    stream.set_nodelay(true)?;
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.max_request_bytes))
        .max_frame_size(Some(config.max_request_bytes));
    let mut compressed = false;
    #[allow(
        clippy::result_large_err,
        reason = "the error type is set by tungstenite"
    )]
    let negotiate =
        |request: &server::Request, response: Response| match compression(request.uri().query()) {
            Ok(zstd) => {
                compressed = zstd;
                Ok(response)
            }
            Err(()) => {
                let mut refusal = ErrorResponse::new(Some("unsupported compression".to_owned()));
                *refusal.status_mut() = StatusCode::BAD_REQUEST;
                Err(refusal)
            }
        };
    let accept =
        tokio_tungstenite::accept_hdr_async_with_config(stream, negotiate, Some(ws_config));
    let ws = timed(config.io_timeout, accept).await?;
    if compressed {
        hub.metrics().compressed_connections_total.increment(1);
    }
    let (mut sink, mut source) = ws.split();
    let (queue, mut queued) = mpsc::channel::<Outgoing>(config.queue_messages);
    let mut client = Client {
        hub,
        config,
        outbox: Arc::new(Outbox::new(
            connection,
            queue,
            config.queue_bytes,
            compressed,
            hub,
        )),
        subscriptions: 0,
    };
    let result = client.run(&mut sink, &mut source, &mut queued).await;
    hub.remove_client(&client.outbox);
    let close = if client.outbox.is_closed() {
        CloseFrame {
            code: CloseCode::Again,
            reason: "client too slow".into(),
        }
    } else {
        CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        }
    };
    let _ = timed(config.io_timeout, sink.send(Message::Close(Some(close)))).await;
    result
}

/// State of one client connection.
struct Client<'a> {
    hub: &'a Hub,
    config: &'a WsConfig,
    outbox: Arc<Outbox>,
    subscriptions: usize,
}

impl Client<'_> {
    async fn run<S, R>(
        &mut self,
        sink: &mut S,
        source: &mut R,
        queued: &mut mpsc::Receiver<Outgoing>,
    ) -> Result<(), tungstenite::Error>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
        R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
    {
        let interval = self.config.ping_interval;
        let mut ping = tokio::time::interval_at(Instant::now() + interval, interval);
        let mut last_heard = Instant::now();
        let outbox = Arc::clone(&self.outbox);
        while !outbox.is_closed() {
            tokio::select! {
                message = queued.recv() => {
                    let Some(message) = message else { break };
                    self.write(sink, queued, message).await?;
                }
                _ = ping.tick() => {
                    if last_heard.elapsed() > 2 * interval {
                        debug!(connection = outbox.connection, "client stopped answering pings");
                        break;
                    }
                    timed(self.config.io_timeout, sink.send(Message::Ping(Default::default()))).await?;
                }
                message = source.next() => {
                    last_heard = Instant::now();
                    match message.transpose()? {
                        Some(Message::Text(text)) => self.handle(text.as_str()),
                        Some(Message::Close(_)) | None => break,
                        Some(_) => {}
                    }
                }
                () = outbox.closing.notified() => {}
            }
        }
        Ok(())
    }

    /// Writes `first` and everything already queued, then flushes once.
    async fn write<S>(
        &self,
        sink: &mut S,
        queued: &mut mpsc::Receiver<Outgoing>,
        first: Outgoing,
    ) -> Result<(), tungstenite::Error>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        let limit = self.config.io_timeout;
        let mut next = Some(first);
        while let Some(outgoing) = next {
            self.outbox.sent(outgoing.len());
            let message = outgoing.message().await;
            timed(limit, sink.feed(message)).await?;
            next = queued.try_recv().ok();
        }
        timed(limit, sink.flush()).await
    }

    /// Handles one request.
    fn handle(&mut self, text: &str) {
        let reply = match serde_json::from_str::<Request>(text) {
            Ok(Request::Subscribe { subscription }) => self.subscribe(subscription),
            Ok(Request::Unsubscribe { subscription }) => self.unsubscribe(subscription),
            Ok(Request::Ping) => Some(json!({"channel": "pong"}).to_string()),
            Err(error) => Some(error_message(&format!("invalid request: {error}"))),
        };
        if let Some(reply) = reply {
            self.outbox.push(Outgoing::text(reply));
        }
    }

    /// Registers a subscription. Returns an error message, or `None` when
    /// the response was queued.
    fn subscribe(&mut self, subscription: Subscription) -> Option<String> {
        if self.subscriptions >= self.config.max_subscriptions {
            return Some(error_message("subscription limit reached"));
        }
        let filter = KindFilter::new(subscription.kinds);
        let reply = response("subscribe", &subscription.topic, &filter);
        match self.hub.subscribe(
            &self.outbox,
            &subscription.topic,
            filter,
            Outgoing::text(reply),
        ) {
            Ok(added) => {
                self.subscriptions += usize::from(added);
                None
            }
            Err(_) => Some(unknown_topic(&subscription.topic)),
        }
    }

    fn unsubscribe(&mut self, subscription: Subscription) -> Option<String> {
        let filter = KindFilter::new(subscription.kinds);
        if self
            .hub
            .unsubscribe(&self.outbox, &subscription.topic, &filter)
        {
            self.subscriptions -= 1;
            Some(response("unsubscribe", &subscription.topic, &filter))
        } else if self.hub.topics().contains(&subscription.topic.as_str()) {
            Some(error_message("not subscribed"))
        } else {
            Some(unknown_topic(&subscription.topic))
        }
    }
}

/// Whether the handshake query asks for zstd: `compression=zstd`. Other
/// parameters are ignored; another compression is refused.
fn compression(query: Option<&str>) -> Result<bool, ()> {
    let mut zstd = false;
    for (key, value) in query
        .into_iter()
        .flat_map(|query| query.split('&'))
        .filter_map(|pair| pair.split_once('='))
    {
        if key == "compression" {
            zstd = match value {
                "zstd" => true,
                "none" => false,
                _ => return Err(()),
            };
        }
    }
    Ok(zstd)
}

/// A `subscriptionResponse` echoing the normalized subscription.
fn response(method: &str, topic: &str, filter: &KindFilter) -> String {
    let subscription = Subscription {
        topic: topic.to_owned(),
        kinds: filter.kinds().map(str::to_owned).collect(),
    };
    json!({
        "channel": "subscriptionResponse",
        "data": {"method": method, "subscription": subscription},
    })
    .to_string()
}

fn unknown_topic(topic: &str) -> String {
    error_message(&format!("unknown subscription type: {topic}"))
}

fn error_message(message: &str) -> String {
    json!({"channel": "error", "data": message}).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn start(config: WsConfig) -> (Hub, WsServer, String) {
        let hub = Hub::new(&["blocks", "stats", "mempoolActions"]);
        let config = WsConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            ..config
        };
        let server = WsServer::start(config, hub.clone()).await.unwrap();
        let url = format!("ws://{}", server.local_addr());
        (hub, server, url)
    }

    async fn request(socket: &mut Socket, request: Value) -> Value {
        socket
            .send(Message::text(request.to_string()))
            .await
            .unwrap();
        next(socket).await
    }

    async fn next(socket: &mut Socket) -> Value {
        loop {
            if let Message::Text(text) = socket.next().await.unwrap().unwrap() {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    }

    fn subscribe(subscription: Value) -> Value {
        json!({"method": "subscribe", "subscription": subscription})
    }

    #[tokio::test]
    async fn subscribes_receives_and_unsubscribes() {
        let (hub, _server, url) = start(WsConfig::default()).await;
        let (mut socket, _) = connect_async(&url).await.unwrap();
        let orders = json!({"type": "mempoolActions", "kinds": ["order", "cancel"]});
        let reply = request(&mut socket, subscribe(orders.clone())).await;
        assert_eq!(
            reply,
            json!({
                "channel": "subscriptionResponse",
                "data": {
                    "method": "subscribe",
                    "subscription": {"type": "mempoolActions", "kinds": ["cancel", "order"]},
                },
            })
        );

        assert_eq!(
            hub.publish("mempoolActions", &["noop"], &json!({"n": 1})),
            0
        );
        assert_eq!(
            hub.publish("mempoolActions", &["order"], &json!({"n": 2})),
            1
        );
        assert_eq!(
            next(&mut socket).await,
            json!({"channel": "mempoolActions", "data": {"n": 2}})
        );

        let unsubscribe = json!({"method": "unsubscribe", "subscription": orders});
        let reply = request(&mut socket, unsubscribe).await;
        assert_eq!(reply["data"]["method"], "unsubscribe");
        assert!(!hub.is_subscribed("mempoolActions"));
        assert_eq!(
            request(&mut socket, json!({"method": "ping"})).await,
            json!({"channel": "pong"})
        );
    }

    #[tokio::test]
    async fn reports_invalid_requests() {
        let config = WsConfig {
            max_subscriptions: 1,
            ..WsConfig::default()
        };
        let (_hub, _server, url) = start(config).await;
        let (mut socket, _) = connect_async(&url).await.unwrap();
        let error = |reply: Value| {
            assert_eq!(reply["channel"], "error", "{reply}");
            reply["data"].as_str().unwrap().to_owned()
        };
        socket.send(Message::text("{")).await.unwrap();
        assert!(error(next(&mut socket).await).starts_with("invalid request"));
        let reply = request(&mut socket, json!({"method": "nope"})).await;
        assert!(error(reply).starts_with("invalid request"));
        let reply = request(
            &mut socket,
            subscribe(json!({"type": "blocks", "coin": "BTC"})),
        )
        .await;
        assert!(error(reply).contains("unknown field"));
        let reply = request(&mut socket, subscribe(json!({"type": "nope"}))).await;
        assert_eq!(error(reply), "unknown subscription type: nope");
        let unsubscribe = json!({"method": "unsubscribe", "subscription": {"type": "stats"}});
        assert_eq!(
            error(request(&mut socket, unsubscribe).await),
            "not subscribed"
        );
        let reply = request(&mut socket, subscribe(json!({"type": "blocks"}))).await;
        assert_eq!(reply["channel"], "subscriptionResponse");
        let reply = request(&mut socket, subscribe(json!({"type": "stats"}))).await;
        assert_eq!(error(reply), "subscription limit reached");
    }

    #[tokio::test]
    async fn sends_published_messages_zstd_compressed_on_request() {
        let (hub, _server, url) = start(WsConfig::default()).await;
        let (mut socket, _) = connect_async(format!("{url}/?compression=zstd"))
            .await
            .unwrap();
        // Replies stay text.
        let reply = request(&mut socket, subscribe(json!({"type": "blocks"}))).await;
        assert_eq!(reply["channel"], "subscriptionResponse");

        let record = json!({"txHashes": vec!["0x1234"; 200]});
        assert_eq!(hub.publish("blocks", &[], &record), 1);
        let Message::Binary(compressed) = socket.next().await.unwrap().unwrap() else {
            panic!("expected a binary message");
        };
        let message: Value =
            serde_json::from_slice(&zstd::decode_all(compressed.as_ref()).unwrap()).unwrap();
        assert_eq!(message, json!({"channel": "blocks", "data": record}));
        assert!(compressed.len() * 5 < message.to_string().len());

        let refused = connect_async(format!("{url}/?compression=gzip")).await;
        let Err(tungstenite::Error::Http(response)) = refused else {
            panic!("expected an HTTP refusal");
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            connect_async(format!("{url}/?compression=none"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn disconnects_slow_clients() {
        let config = WsConfig {
            queue_messages: 4,
            ..WsConfig::default()
        };
        let (hub, _server, url) = start(config).await;
        let (mut socket, _) = connect_async(&url).await.unwrap();
        request(&mut socket, subscribe(json!({"type": "stats"}))).await;
        // The client reads nothing while far more than four messages are
        // published, so its queue overflows.
        for n in 0..10_000 {
            hub.publish("stats", &[], &json!({ "n": n }));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!hub.is_subscribed("stats"));
    }
}
