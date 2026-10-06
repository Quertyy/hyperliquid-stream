//! Messages of the consensus stream (port 4001) after the greeting.
//!
//! ```text
//! tag 0  consensus block: signature, proposer, transaction hashes, timestamp,
//!        parent certificate, optional commit proof, optional bodies
//! tag 1  standalone transaction: one length-prefixed signed action bundle
//! ```
//!
//! Only the block fields this crate uses are decoded: everything after the
//! timestamp is skipped, so changes to certificates or proofs do not affect
//! mempool streaming.

use super::{DecodeError, Reader};
use crate::types::{Address, Hash};
use bytes::Bytes;

const BLOCK: u32 = 0;
const TRANSACTION: u32 = 1;

/// One message of the consensus stream.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamMessage {
    /// A signed action bundle relayed before it is committed in a block.
    /// The bytes share the frame's buffer.
    Transaction(Bytes),
    /// The leading fields of a consensus block.
    Block(BlockHeader),
    /// A message this crate does not decode.
    Other {
        /// Message tag.
        tag: u32,
    },
}

/// The leading fields of a consensus block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    /// Validator that proposed the block.
    pub proposer: Address,
    /// Consensus hashes of the transactions committed by the block.
    pub tx_hashes: Vec<Hash>,
    /// Consensus time of the block, in Unix milliseconds.
    pub time_ms: u64,
}

impl StreamMessage {
    /// Decodes one decompressed frame body.
    pub fn decode(frame: &Bytes) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(frame);
        match reader.u32()? {
            TRANSACTION => {
                let range = reader.blob()?;
                reader.finish()?;
                Ok(Self::Transaction(frame.slice(range)))
            }
            BLOCK => BlockHeader::decode(&mut reader).map(Self::Block),
            tag => Ok(Self::Other { tag }),
        }
    }
}

impl BlockHeader {
    fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        // Block signature: four `u64` limbs each for r and s, then v.
        for _ in 0..8 {
            reader.u64()?;
        }
        reader.byte()?;
        let proposer = Address(reader.array()?);
        let count = reader.count(Hash::LEN)?;
        let tx_hashes = (0..count)
            .map(|_| reader.array().map(Hash))
            .collect::<Result<_, _>>()?;
        let time_ms = timestamp_ms(reader)?;
        Ok(Self {
            proposer,
            tx_hashes,
            time_ms,
        })
    }
}

/// Reads zigzag-encoded seconds followed by nanoseconds.
fn timestamp_ms(reader: &mut Reader<'_>) -> Result<u64, DecodeError> {
    let offset = reader.offset();
    let zigzag = reader.u64()?;
    let nanos = reader.u32()?;
    let invalid = DecodeError::Invalid {
        field: "block timestamp",
        offset,
    };
    // Zigzag: odd values are negative times, which are not meaningful here.
    if zigzag & 1 == 1 {
        return Err(invalid);
    }
    let seconds = zigzag >> 1;
    // Up to two billion nanoseconds only during a leap second.
    let max_nanos = if seconds % 60 == 59 {
        2_000_000_000
    } else {
        1_000_000_000
    };
    if nanos >= max_nanos {
        return Err(invalid);
    }
    seconds
        .checked_mul(1000)
        .and_then(|ms| ms.checked_add(u64::from(nanos / 1_000_000)))
        .ok_or(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(time_secs: u64, hashes: &[[u8; 32]]) -> Vec<u8> {
        let mut bytes = vec![0];
        bytes.extend([1, 2, 3, 251, 0, 1, 5, 6, 7, 8, 27]); // signature limbs and v
        bytes.extend([0x11; 20]);
        bytes.push(u8::try_from(hashes.len()).unwrap());
        for hash in hashes {
            bytes.extend(hash);
        }
        bytes.push(252);
        bytes.extend(u32::try_from(time_secs * 2).unwrap().to_le_bytes());
        bytes.extend([252]);
        bytes.extend(500_000_000_u32.to_le_bytes());
        bytes.extend([0xde, 0xad]); // parent certificate and beyond: not decoded
        bytes
    }

    #[test]
    fn rejects_out_of_range_nanoseconds() {
        let decode = |seconds: u64, nanos: u32| {
            let mut bytes = vec![253];
            bytes.extend((seconds * 2).to_le_bytes());
            bytes.push(252);
            bytes.extend(nanos.to_le_bytes());
            timestamp_ms(&mut Reader::new(&bytes))
        };
        assert_eq!(decode(60, 999_999_999), Ok(60_999));
        assert!(decode(60, 1_000_000_000).is_err());
        assert_eq!(decode(59, 1_500_000_000), Ok(60_500));
    }

    #[test]
    fn decodes_block_header_prefix() {
        let frame = Bytes::from(block(1_790_000_000, &[[0x22; 32], [0x33; 32]]));
        let StreamMessage::Block(header) = StreamMessage::decode(&frame).unwrap() else {
            panic!("expected a block");
        };
        assert_eq!(header.proposer, Address([0x11; 20]));
        assert_eq!(header.tx_hashes, [Hash([0x22; 32]), Hash([0x33; 32])]);
        assert_eq!(header.time_ms, 1_790_000_000_500);
    }

    #[test]
    fn rejects_truncated_block_headers() {
        let bytes = block(1, &[[0x22; 32]]);
        for end in 1..bytes.len() - 2 {
            let frame = Bytes::copy_from_slice(&bytes[..end]);
            assert!(StreamMessage::decode(&frame).is_err(), "prefix {end}");
        }
    }

    #[test]
    fn shares_the_transaction_buffer() {
        let frame = Bytes::from_static(&[1, 3, 0xaa, 0xbb, 0xcc]);
        let StreamMessage::Transaction(payload) = StreamMessage::decode(&frame).unwrap() else {
            panic!("expected a transaction");
        };
        assert_eq!(payload.as_ref(), [0xaa, 0xbb, 0xcc]);
        assert_eq!(payload.as_ptr(), frame[2..].as_ptr());
        assert!(StreamMessage::decode(&Bytes::from_static(&[1, 3, 0xaa])).is_err());
        assert!(StreamMessage::decode(&Bytes::from_static(&[1, 1, 0xaa, 0])).is_err());
    }

    #[test]
    fn keeps_unknown_tags() {
        let frame = Bytes::from_static(&[7, 1, 2]);
        assert_eq!(
            StreamMessage::decode(&frame),
            Ok(StreamMessage::Other { tag: 7 })
        );
    }
}
