//! The bounded queue of messages waiting to be written to one client.

use crate::{compression::Compression, hub::Hub};
use metrics::Counter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tracing::warn;

/// A message queued for a client.
#[derive(Debug)]
pub(crate) enum Outgoing {
    /// A message to write as is.
    Ready(Message),
    /// A published message whose compression may still be running. The
    /// writer waits for it, in order, and sends it as text if compression
    /// failed.
    Compressing { text: Utf8Bytes, zstd: Compression },
}

impl Outgoing {
    /// A text message.
    pub(crate) fn text(text: impl Into<Utf8Bytes>) -> Self {
        Self::Ready(Message::Text(text.into()))
    }

    /// Bytes charged to the queue: the uncompressed size, known up front.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Ready(message) => message.len(),
            Self::Compressing { text, .. } => text.len(),
        }
    }

    /// The message to write, once compressed if it is being compressed.
    pub(crate) async fn message(self) -> Message {
        match self {
            Self::Ready(message) => message,
            Self::Compressing { text, zstd } => match zstd.await {
                Some(bytes) => Message::Binary(bytes),
                None => Message::Text(text),
            },
        }
    }
}

/// Messages waiting to be written to one client, bounded in count and bytes.
#[derive(Debug)]
pub(crate) struct Outbox {
    pub(crate) connection: u64,
    queue: mpsc::Sender<Outgoing>,
    /// Whether the client asked for zstd-compressed published messages.
    compressed: bool,
    queued_bytes: AtomicUsize,
    max_bytes: usize,
    closed: AtomicBool,
    /// Wakes the connection task when the outbox closes.
    pub(crate) closing: Notify,
    slow_clients: Counter,
}

impl Outbox {
    pub(crate) fn new(
        connection: u64,
        queue: mpsc::Sender<Outgoing>,
        max_bytes: usize,
        compressed: bool,
        hub: &Hub,
    ) -> Self {
        Self {
            connection,
            queue,
            compressed,
            queued_bytes: AtomicUsize::new(0),
            max_bytes,
            closed: AtomicBool::new(false),
            closing: Notify::new(),
            slow_clients: hub.metrics().slow_clients_total.clone(),
        }
    }

    /// Whether published messages go out zstd-compressed.
    pub(crate) fn compressed(&self) -> bool {
        self.compressed
    }

    /// Queues a message. Returns `false`, and closes the outbox, when the
    /// client is too far behind.
    pub(crate) fn push(&self, message: Outgoing) -> bool {
        if self.is_closed() {
            return false;
        }
        let len = message.len();
        if self.queued_bytes.fetch_add(len, Ordering::Relaxed) + len > self.max_bytes
            || self.queue.try_send(message).is_err()
        {
            self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
            warn!(
                connection = self.connection,
                "client too slow, disconnecting"
            );
            self.slow_clients.increment(1);
            self.close();
            return false;
        }
        true
    }

    /// Records that a message of `len` bytes left the queue.
    pub(crate) fn sent(&self, len: usize) {
        self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closing.notify_one();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}
