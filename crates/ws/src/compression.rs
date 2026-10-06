//! zstd compression of published messages.
//!
//! A message is compressed once, for every recipient that asked, by the
//! first of them to write it: the work runs in that connection's task, with
//! no hand-off to another thread, and neither the publisher nor clients
//! without compression wait for it. Compressing a message takes tens of
//! microseconds, far less than waking a thread to do it.

use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use metrics::Histogram;
use std::{cell::RefCell, future, time::Instant};
use tokio_tungstenite::tungstenite::{Bytes, Utf8Bytes};
use tracing::warn;

/// A message being compressed, shared by its recipients; `None` if
/// compression failed.
pub(crate) type Compression = Shared<BoxFuture<'static, Option<Bytes>>>;

/// Level 1 compresses JSON messages about as well as higher levels, at the
/// lowest cost.
const LEVEL: i32 = 1;

thread_local! {
    /// Compression context of the current thread, reused across messages.
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
}

/// A compression of `message` into one zstd frame, run by its first poll
/// and timed in `duration`.
pub(crate) fn start(message: Utf8Bytes, duration: Histogram) -> Compression {
    future::ready(())
        .map(move |()| {
            let started = Instant::now();
            let compressed = compress(message.as_bytes());
            duration.record(started.elapsed().as_secs_f64());
            compressed
        })
        .boxed()
        .shared()
}

/// One zstd frame holding `input`.
fn compress(input: &[u8]) -> Option<Bytes> {
    COMPRESSOR.with_borrow_mut(|compressor| {
        if compressor.is_none() {
            *compressor = zstd::bulk::Compressor::new(LEVEL).ok();
        }
        match compressor.as_mut()?.compress(input) {
            Ok(bytes) => Some(Bytes::from(bytes)),
            Err(error) => {
                warn!(%error, "compressing a message failed");
                None
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_through_zstd() {
        let message = Utf8Bytes::from(r#"{"channel":"blocks","data":{"n":1}}"#.repeat(50));
        let compressed = start(message.clone(), Histogram::noop()).await.unwrap();
        assert!(compressed.len() < message.len() / 10);
        let decompressed = zstd::decode_all(compressed.as_ref()).unwrap();
        assert_eq!(decompressed, message.as_bytes());
    }
}
