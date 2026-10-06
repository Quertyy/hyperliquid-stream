//! Peer discovery.
//!
//! Root peers come from the info API (`{"type": "gossipRootIps"}`). Further
//! peers come from asking known peers for their children with the gossip RPC
//! `ChildPeerStatuses`. Most peers serve few children and reject new ones
//! with `PeerFull`, so a wide candidate list matters.

use crate::connection::{FrameIoError, RPC_PORT, read_frame, write_frame};
use hyperliquid_protocol::wire::rpc::{
    CHILD_PEER_STATUSES_REQUEST, ChildPeer, RpcError, decode_child_peers,
};
use std::{io, net::IpAddr, time::Duration};
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_LIMIT: usize = 1024 * 1024;

/// A failed discovery request.
#[derive(Debug, Error)]
pub(crate) enum DiscoveryError {
    #[error("timed out")]
    Timeout,
    #[error("connecting: {0}")]
    Connect(#[from] io::Error),
    #[error(transparent)]
    Frame(#[from] FrameIoError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("info API: {0}")]
    Http(#[from] reqwest::Error),
}

/// Asks `ip` for the peers streaming from it.
pub(crate) async fn child_peers(ip: IpAddr) -> Result<Vec<ChildPeer>, DiscoveryError> {
    timeout(PROBE_TIMEOUT, async {
        let mut stream = TcpStream::connect((ip, RPC_PORT)).await?;
        write_frame(&mut stream, &CHILD_PEER_STATUSES_REQUEST).await?;
        let response = read_frame(&mut stream, RESPONSE_LIMIT).await?;
        Ok(decode_child_peers(&response)?)
    })
    .await
    .map_err(|_| DiscoveryError::Timeout)?
}

/// Fetches the root peers published by the info API.
pub(crate) async fn root_peers(
    http: &reqwest::Client,
    info_url: &str,
) -> Result<Vec<IpAddr>, DiscoveryError> {
    let ips: Vec<String> = http
        .post(info_url)
        .json(&serde_json::json!({ "type": "gossipRootIps" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(ips.iter().filter_map(|ip| ip.parse().ok()).collect())
}
