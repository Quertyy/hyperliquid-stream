//! Gossip RPC (port 4002): one request frame and one response frame per TCP
//! connection.
//!
//! Only `ChildPeerStatuses` (tag 1) is used here. Peers send it to a new
//! client during the handshake, and the client sends it to peers to discover
//! their children.

use super::{DecodeError, Reader};
use std::net::Ipv4Addr;
use thiserror::Error;

/// Body of a `ChildPeerStatuses` request.
pub const CHILD_PEER_STATUSES_REQUEST: [u8; 1] = [1];

/// Body of a `ChildPeerStatuses` response listing no children.
pub const EMPTY_CHILD_PEER_STATUSES_RESPONSE: [u8; 2] = [1, 0];

const CHILD_PEER_STATUSES: u32 = 1;
const ERROR: u32 = 3;
const IPV4: u32 = 0;
// IP tag, four address bytes, boolean, one-byte count.
const MIN_CHILD_PEER_SIZE: usize = 7;

/// A peer streaming from the queried node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildPeer {
    /// Address of the child.
    pub ip: Ipv4Addr,
    /// Whether the node verified the child's reachability.
    pub verified: bool,
    /// Number of connections the node reports for the child.
    pub connection_count: u64,
}

/// A `ChildPeerStatuses` response that does not list children.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RpcError {
    /// The peer answered with an error message.
    #[error("peer error: {0}")]
    Remote(String),
    /// The peer answered with a different response type.
    #[error("unexpected response tag {0}")]
    UnexpectedResponse(u32),
    /// The response is malformed.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// Decodes a `ChildPeerStatuses` response.
pub fn decode_child_peers(payload: &[u8]) -> Result<Vec<ChildPeer>, RpcError> {
    let mut reader = Reader::new(payload);
    match reader.u32()? {
        CHILD_PEER_STATUSES => {}
        ERROR => {
            let range = reader.blob()?;
            let message = String::from_utf8_lossy(&payload[range]).into_owned();
            return Err(RpcError::Remote(message));
        }
        tag => return Err(RpcError::UnexpectedResponse(tag)),
    }
    let count = reader.count(MIN_CHILD_PEER_SIZE)?;
    let mut peers = Vec::with_capacity(count);
    for _ in 0..count {
        let offset = reader.offset();
        if reader.u32()? != IPV4 {
            return Err(DecodeError::Invalid {
                field: "IP address family",
                offset,
            }
            .into());
        }
        peers.push(ChildPeer {
            ip: Ipv4Addr::from(reader.array::<4>()?),
            verified: reader.bool("verified flag")?,
            connection_count: reader.u64()?,
        });
    }
    reader.finish()?;
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_child_peers() {
        let payload = [1, 2, 0, 172, 32, 1, 83, 1, 1, 0, 10, 0, 0, 1, 0, 251, 0, 1];
        let peers = decode_child_peers(&payload).unwrap();
        assert_eq!(
            peers,
            [
                ChildPeer {
                    ip: Ipv4Addr::new(172, 32, 1, 83),
                    verified: true,
                    connection_count: 1,
                },
                ChildPeer {
                    ip: Ipv4Addr::new(10, 0, 0, 1),
                    verified: false,
                    connection_count: 256,
                },
            ]
        );
        assert_eq!(
            decode_child_peers(&EMPTY_CHILD_PEER_STATUSES_RESPONSE),
            Ok(Vec::new())
        );
    }

    #[test]
    fn reports_remote_errors_and_malformed_responses() {
        assert_eq!(
            decode_child_peers(b"\x03\x11Peer-only request"),
            Err(RpcError::Remote("Peer-only request".to_owned()))
        );
        assert_eq!(
            decode_child_peers(&[2, 0]),
            Err(RpcError::UnexpectedResponse(2))
        );
        assert!(decode_child_peers(&[1, 1, 1, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_child_peers(&[1, 9, 0]).is_err());
    }
}
