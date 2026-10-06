//! A connection to one peer's consensus stream, and the callback listeners
//! peers require before they accept it.
//!
//! When a client sends its greeting, the peer connects back to the client's
//! TCP ports 4001 and 4002 to check that it is reachable, and asks port 4002
//! for the client's child peers. Without these callbacks the peer closes the
//! stream after a few seconds. [`CallbackServer`] answers them; one server can
//! serve any number of concurrent connections.

use crate::metrics::CallbackMetrics;
use bytes::Bytes;
use hyperliquid_protocol::wire::{
    DecodeError,
    frame::{self, FrameError, FrameHeader, HEADER_LEN, MAX_FRAME_SIZE},
    greeting::{self, BroadcastGroup, GreetingResponse},
    rpc::{CHILD_PEER_STATUSES_REQUEST, EMPTY_CHILD_PEER_STATUSES_RESPONSE},
};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tracing::{debug, trace, warn};

/// Port of the consensus stream.
pub const STREAM_PORT: u16 = 4001;
/// Port of the gossip RPC service.
pub const RPC_PORT: u16 = 4002;
/// A peer that sends no complete frame for this long is considered gone.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const GREETING_TIMEOUT: Duration = Duration::from_secs(15);
const GREETING_RESPONSE_LIMIT: usize = 64 * 1024;
const READ_BUFFER: usize = 64 * 1024;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(2);
const CALLBACK_REQUEST_LIMIT: usize = 64;
const MAX_CONCURRENT_CALLBACKS: usize = 32;

/// A failure to read or write a frame.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FrameIoError {
    /// The stream failed or closed mid-frame.
    #[error("frame I/O: {0}")]
    Io(#[from] io::Error),
    /// The frame was rejected.
    #[error(transparent)]
    Frame(#[from] FrameError),
}

/// Reads one frame and returns its decompressed body.
pub async fn read_frame<R>(reader: &mut R, limit: usize) -> Result<Bytes, FrameIoError>
where
    R: AsyncRead + Unpin,
{
    read_frame_sized(reader, limit).await.map(|(body, _)| body)
}

/// Reads one frame; also returns its size on the wire.
async fn read_frame_sized<R>(reader: &mut R, limit: usize) -> Result<(Bytes, usize), FrameIoError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0; HEADER_LEN];
    reader.read_exact(&mut header).await?;
    let header = FrameHeader::parse(header, limit)?;
    let mut body = vec![0; header.len()];
    reader.read_exact(&mut body).await?;
    Ok((header.decode_body(body)?, HEADER_LEN + header.len()))
}

/// Writes `body` as one uncompressed frame and flushes the stream.
pub async fn write_frame<W>(writer: &mut W, body: &[u8]) -> Result<(), FrameIoError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&frame::encode(body)?).await?;
    writer.flush().await?;
    Ok(())
}

/// A failure to open a stream.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The TCP connection failed.
    #[error("connecting: {0}")]
    Connect(#[source] io::Error),
    /// The connection or greeting did not complete in time.
    #[error("timed out")]
    Timeout,
    /// The greeting exchange failed.
    #[error("greeting: {0}")]
    Frame(#[from] FrameIoError),
    /// The greeting response is malformed.
    #[error("invalid greeting response: {0}")]
    InvalidResponse(#[from] DecodeError),
    /// The peer declined to stream.
    #[error("peer declined: {0:?}")]
    Rejected(GreetingResponse),
}

/// A failure to receive the next frame. The connection is unusable afterwards.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReceiveError {
    /// No complete frame arrived within [`IDLE_TIMEOUT`].
    #[error("no frame received for {}s", IDLE_TIMEOUT.as_secs())]
    Idle,
    /// The frame could not be read.
    #[error(transparent)]
    Frame(#[from] FrameIoError),
}

/// An open consensus stream from one peer.
#[derive(Debug)]
pub struct PeerConnection {
    reader: BufReader<TcpStream>,
    peer: SocketAddr,
    received_bytes: u64,
}

impl PeerConnection {
    /// Connects to `peer`, sends the greeting for `group` and waits until the
    /// peer accepts. The peer's callbacks must be answered meanwhile, for
    /// example by a running [`CallbackServer`].
    pub async fn open(peer: SocketAddr, group: BroadcastGroup) -> Result<Self, OpenError> {
        let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(peer))
            .await
            .map_err(|_| OpenError::Timeout)?
            .map_err(OpenError::Connect)?;
        stream.set_nodelay(true).map_err(OpenError::Connect)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER, stream);
        let response = timeout(GREETING_TIMEOUT, async {
            write_frame(reader.get_mut(), &greeting::encode(group)).await?;
            read_frame(&mut reader, GREETING_RESPONSE_LIMIT).await
        })
        .await
        .map_err(|_| OpenError::Timeout)??;
        match GreetingResponse::decode(&response)? {
            GreetingResponse::Streaming => Ok(Self {
                reader,
                peer,
                received_bytes: 0,
            }),
            response => Err(OpenError::Rejected(response)),
        }
    }

    /// Receives the next decompressed frame.
    ///
    /// Not cancel-safe: if the future is dropped mid-frame, the stream is out
    /// of sync and the connection must be dropped too.
    pub async fn next_frame(&mut self) -> Result<Bytes, ReceiveError> {
        let (body, wire_len) = timeout(
            IDLE_TIMEOUT,
            read_frame_sized(&mut self.reader, MAX_FRAME_SIZE),
        )
        .await
        .map_err(|_| ReceiveError::Idle)??;
        self.received_bytes += wire_len as u64;
        Ok(body)
    }

    /// Bytes received in frames since the stream opened, as transported
    /// (before decompression).
    #[must_use]
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes
    }

    /// Address of the peer.
    #[must_use]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

/// Where the callback listeners bind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallbackConfig {
    /// Local address to bind; peers must reach it on both ports.
    pub ip: IpAddr,
    /// Port of the reachability check; peers always use 4001.
    pub stream_port: u16,
    /// Port of the gossip RPC callback; peers always use 4002.
    pub rpc_port: u16,
}

impl Default for CallbackConfig {
    fn default() -> Self {
        Self {
            ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            stream_port: STREAM_PORT,
            rpc_port: RPC_PORT,
        }
    }
}

/// A callback listener that could not be bound.
#[derive(Debug, Error)]
#[error("binding callback listener {addr}: {source}")]
pub struct BindError {
    /// Address that could not be bound.
    pub addr: SocketAddr,
    /// Cause of the failure.
    #[source]
    pub source: io::Error,
}

/// Answers the reachability checks and `ChildPeerStatuses` requests that
/// peers send to a connecting client.
///
/// Any caller is answered: the reply advertises no child peers and reveals
/// nothing. Concurrent RPC replies are bounded, and each must complete
/// within two seconds.
#[derive(Debug)]
pub struct CallbackServer {
    stream: TcpListener,
    rpc: TcpListener,
    metrics: CallbackMetrics,
}

impl CallbackServer {
    /// Binds both listeners.
    pub async fn bind(config: CallbackConfig) -> Result<Self, BindError> {
        let bind = |port| async move {
            let addr = SocketAddr::new(config.ip, port);
            TcpListener::bind(addr)
                .await
                .map_err(|source| BindError { addr, source })
        };
        Ok(Self {
            stream: bind(config.stream_port).await?,
            rpc: bind(config.rpc_port).await?,
            metrics: CallbackMetrics::default(),
        })
    }

    /// Local addresses of the stream and RPC listeners.
    pub fn local_addrs(&self) -> io::Result<(SocketAddr, SocketAddr)> {
        Ok((self.stream.local_addr()?, self.rpc.local_addr()?))
    }

    /// Serves callbacks until the future is dropped.
    pub async fn run(self) {
        let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CALLBACKS));
        loop {
            tokio::select! {
                accepted = self.stream.accept() => match accepted {
                    // Accepting the connection is the whole reachability check.
                    Ok((_, addr)) => {
                        trace!(%addr, "reachability check");
                        self.metrics.stream.increment(1);
                    }
                    Err(error) => accept_failed(&error).await,
                },
                accepted = self.rpc.accept() => match accepted {
                    Ok((stream, addr)) => {
                        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                            debug!(%addr, "too many concurrent callbacks, closing");
                            continue;
                        };
                        let answered = self.metrics.rpc.clone();
                        tokio::spawn(async move {
                            match timeout(CALLBACK_TIMEOUT, reply(stream)).await {
                                Ok(Ok(())) => answered.increment(1),
                                Ok(Err(error)) => debug!(%addr, %error, "RPC callback failed"),
                                Err(_) => debug!(%addr, "RPC callback timed out"),
                            }
                            drop(permit);
                        });
                    }
                    Err(error) => accept_failed(&error).await,
                },
            }
        }
    }
}

/// Answers `ChildPeerStatuses` with an empty child list.
async fn reply(mut stream: TcpStream) -> Result<(), FrameIoError> {
    let request = read_frame(&mut stream, CALLBACK_REQUEST_LIMIT).await?;
    if request.as_ref() == CHILD_PEER_STATUSES_REQUEST {
        write_frame(&mut stream, &EMPTY_CHILD_PEER_STATUSES_RESPONSE).await?;
    } else {
        debug!(?request, "unsupported RPC callback");
    }
    Ok(())
}

/// Accept fails on transient conditions such as file descriptor exhaustion;
/// pausing avoids a busy loop.
async fn accept_failed(error: &io::Error) {
    warn!(%error, "accepting callback failed");
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperliquid_protocol::wire::rpc::decode_child_peers;

    fn local_config() -> CallbackConfig {
        CallbackConfig {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            stream_port: 0,
            rpc_port: 0,
        }
    }

    #[tokio::test]
    async fn answers_child_peer_statuses_with_an_empty_list() {
        let server = CallbackServer::bind(local_config()).await.unwrap();
        let (stream_addr, rpc_addr) = server.local_addrs().unwrap();
        let task = tokio::spawn(server.run());

        TcpStream::connect(stream_addr).await.unwrap();
        let mut rpc = TcpStream::connect(rpc_addr).await.unwrap();
        write_frame(&mut rpc, &CHILD_PEER_STATUSES_REQUEST)
            .await
            .unwrap();
        let response = read_frame(&mut rpc, 1024).await.unwrap();
        assert_eq!(decode_child_peers(&response), Ok(Vec::new()));
        task.abort();
    }

    #[tokio::test]
    async fn opens_a_stream_and_reports_rejections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            for response in [&[3][..], &[4][..]] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let greeting = read_frame(&mut socket, 16).await.unwrap();
                assert_eq!(greeting.as_ref(), [0, 1, 0]);
                write_frame(&mut socket, response).await.unwrap();
                write_frame(&mut socket, b"data").await.unwrap();
            }
        });
        let mut connection = PeerConnection::open(addr, BroadcastGroup::SplitClientBlocks)
            .await
            .unwrap();
        assert_eq!(connection.next_frame().await.unwrap().as_ref(), b"data");
        let rejected = PeerConnection::open(addr, BroadcastGroup::SplitClientBlocks).await;
        assert!(matches!(
            rejected,
            Err(OpenError::Rejected(GreetingResponse::PeerFull))
        ));
        peer.await.unwrap();
    }
}
