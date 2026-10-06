//! The greeting that opens a consensus stream on port 4001.
//!
//! The client sends three bytes, `[send_abci, broadcast_group, id]`, and the
//! peer answers with one frame whose first value is a response tag.

use super::{DecodeError, Reader};

/// The peer broadcast stream requested in the greeting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum BroadcastGroup {
    /// Blocks with their transactions attached; carries no mempool.
    Default = 0,
    /// Blocks and transactions sent separately. Peers relay standalone
    /// (not yet committed) transactions only in this group.
    #[default]
    SplitClientBlocks = 1,
}

/// Encodes the live-stream greeting for `group`.
#[must_use]
pub const fn encode(group: BroadcastGroup) -> [u8; 3] {
    // No state snapshot, the requested group, the live stream identifier.
    [0, group as u8, 0]
}

/// The peer's answer to a greeting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GreetingResponse {
    /// The peer accepted and starts streaming.
    Streaming,
    /// The peer has no quorum for its latest state.
    NoQuorum,
    /// The peer serves no more clients.
    PeerFull,
    /// The peer rejected the client for connecting too often.
    RateLimited,
    /// Any other response, such as a state snapshot.
    Other(u32),
}

impl GreetingResponse {
    /// Decodes a greeting response frame body.
    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(payload);
        let response = match reader.u32()? {
            2 => Self::NoQuorum,
            3 => Self::Streaming,
            4 => Self::PeerFull,
            5 => Self::RateLimited,
            // Other responses carry a payload this crate does not need.
            tag => return Ok(Self::Other(tag)),
        };
        reader.finish()?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_the_split_group_greeting() {
        assert_eq!(encode(BroadcastGroup::SplitClientBlocks), [0, 1, 0]);
        assert_eq!(encode(BroadcastGroup::Default), [0, 0, 0]);
    }

    #[test]
    fn decodes_responses() {
        assert_eq!(
            GreetingResponse::decode(&[3]),
            Ok(GreetingResponse::Streaming)
        );
        assert_eq!(
            GreetingResponse::decode(&[251, 3, 0]),
            Ok(GreetingResponse::Streaming)
        );
        assert_eq!(
            GreetingResponse::decode(&[4]),
            Ok(GreetingResponse::PeerFull)
        );
        assert_eq!(
            GreetingResponse::decode(&[0, 9, 9]),
            Ok(GreetingResponse::Other(0))
        );
        // A gossip RPC error ("Peer-only request") is not a streaming answer.
        assert!(GreetingResponse::decode(b"\x03\x11Peer-only request").is_err());
        assert!(GreetingResponse::decode(&[]).is_err());
    }
}
