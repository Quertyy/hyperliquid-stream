//! Length-prefixed frames shared by every peer service.
//!
//! ```text
//! u32 big-endian   body length, excluding this five-byte header
//! u8               compression: 0 = none, 1 = LZ4 block
//! [body]           an LZ4 body starts with its decompressed length (u32 little-endian)
//! ```
//!
//! Reading a frame takes two steps, so that any reader (blocking or async)
//! can use this codec: parse the [`HEADER_LEN`]-byte header with
//! [`FrameHeader::parse`], read [`FrameHeader::len`] bytes, then call
//! [`FrameHeader::decode_body`].

use bytes::Bytes;
use thiserror::Error;

/// Largest encoded or decompressed frame accepted by default, 16 MiB.
///
/// This is a local resource limit, not a protocol rule.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Size of the frame header.
pub const HEADER_LEN: usize = 5;

const UNCOMPRESSED: u8 = 0;
const LZ4: u8 = 1;

/// A frame that cannot be accepted.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The encoded or decompressed size exceeds the limit.
    #[error("frame of {size} bytes exceeds the {limit}-byte limit")]
    TooLarge {
        /// Announced size in bytes.
        size: usize,
        /// Applicable limit in bytes.
        limit: usize,
    },
    /// The compression flag is neither 0 nor 1.
    #[error("unknown compression flag {0}")]
    UnknownCompression(u8),
    /// The LZ4 body is malformed or does not match its announced size.
    #[error("invalid LZ4 body")]
    InvalidLz4,
}

/// A parsed frame header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    len: usize,
    compressed: bool,
    limit: usize,
}

impl FrameHeader {
    /// Parses a header, rejecting bodies larger than `limit` before any
    /// buffer is allocated.
    pub fn parse(header: [u8; HEADER_LEN], limit: usize) -> Result<Self, FrameError> {
        let [l0, l1, l2, l3, compression] = header;
        let len = u32::from_be_bytes([l0, l1, l2, l3]) as usize;
        if len > limit {
            return Err(FrameError::TooLarge { size: len, limit });
        }
        let compressed = match compression {
            UNCOMPRESSED => false,
            LZ4 => true,
            flag => return Err(FrameError::UnknownCompression(flag)),
        };
        Ok(Self {
            len,
            compressed,
            limit,
        })
    }

    /// Length of the encoded body that follows the header.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the body is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Decompresses the body read after this header. The decompressed size
    /// is checked against the same limit before decompressing.
    pub fn decode_body(&self, body: Vec<u8>) -> Result<Bytes, FrameError> {
        debug_assert_eq!(body.len(), self.len);
        if !self.compressed {
            return Ok(Bytes::from(body));
        }
        let Some((prefix, block)) = body.split_first_chunk::<4>() else {
            return Err(FrameError::InvalidLz4);
        };
        let size = u32::from_le_bytes(*prefix) as usize;
        // LZ4 expands at most 255 times, so a larger size is a lie that
        // would only make the decoder allocate.
        if size > block.len().saturating_mul(255) {
            return Err(FrameError::InvalidLz4);
        }
        if size > self.limit {
            return Err(FrameError::TooLarge {
                size,
                limit: self.limit,
            });
        }
        let decoded =
            lz4_flex::block::decompress(block, size).map_err(|_| FrameError::InvalidLz4)?;
        if decoded.len() != size {
            return Err(FrameError::InvalidLz4);
        }
        Ok(Bytes::from(decoded))
    }
}

/// Encodes `body` as one uncompressed frame.
pub fn encode(body: &[u8]) -> Result<Vec<u8>, FrameError> {
    let len = u32::try_from(body.len())
        .ok()
        .filter(|_| body.len() <= MAX_FRAME_SIZE)
        .ok_or(FrameError::TooLarge {
            size: body.len(),
            limit: MAX_FRAME_SIZE,
        })?;
    let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.push(UNCOMPRESSED);
    frame.extend_from_slice(body);
    Ok(frame)
}

/// Decodes one complete frame held in memory; returns the body and the
/// number of bytes consumed.
pub fn decode(frame: &[u8], limit: usize) -> Result<Option<(Bytes, usize)>, FrameError> {
    let Some((header, rest)) = frame.split_first_chunk::<HEADER_LEN>() else {
        return Ok(None);
    };
    let header = FrameHeader::parse(*header, limit)?;
    let Some(body) = rest.get(..header.len()) else {
        return Ok(None);
    };
    let body = header.decode_body(body.to_vec())?;
    Ok(Some((body, HEADER_LEN + header.len())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lz4_frame(body: &[u8]) -> Vec<u8> {
        let compressed = lz4_flex::block::compress_prepend_size(body);
        let mut frame = u32::try_from(compressed.len())
            .unwrap()
            .to_be_bytes()
            .to_vec();
        frame.push(LZ4);
        frame.extend(compressed);
        frame
    }

    #[test]
    fn round_trips_uncompressed_frames() {
        let frame = encode(b"hello").unwrap();
        assert_eq!(frame, [0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']);
        let (body, used) = decode(&frame, MAX_FRAME_SIZE).unwrap().unwrap();
        assert_eq!((body.as_ref(), used), (&b"hello"[..], frame.len()));
        assert_eq!(decode(&frame[..7], MAX_FRAME_SIZE), Ok(None));
    }

    #[test]
    fn decompresses_lz4_frames() {
        let body = b"mempool mempool mempool mempool".repeat(10);
        let frame = lz4_frame(&body);
        let (decoded, _) = decode(&frame, MAX_FRAME_SIZE).unwrap().unwrap();
        assert_eq!(decoded.as_ref(), body.as_slice());
    }

    #[test]
    fn rejects_oversized_and_malformed_frames() {
        assert!(matches!(
            FrameHeader::parse([0xff, 0xff, 0xff, 0xff, 0], MAX_FRAME_SIZE),
            Err(FrameError::TooLarge { .. })
        ));
        assert_eq!(
            FrameHeader::parse([0, 0, 0, 1, 7], MAX_FRAME_SIZE),
            Err(FrameError::UnknownCompression(7))
        );
        let bomb = [0, 0, 0, 4, LZ4, 0xff, 0xff, 0xff, 0x7f];
        assert_eq!(decode(&bomb, MAX_FRAME_SIZE), Err(FrameError::InvalidLz4));
        let oversized = lz4_frame(&vec![0; 1024]);
        assert!(matches!(
            decode(&oversized, 512),
            Err(FrameError::TooLarge { .. })
        ));
        let short = [0, 0, 0, 2, LZ4, 1, 0];
        assert_eq!(decode(&short, MAX_FRAME_SIZE), Err(FrameError::InvalidLz4));
        let wrong_size = {
            let mut frame = lz4_frame(b"abcdef");
            frame[5] = 9; // announce more bytes than the block holds
            frame
        };
        assert_eq!(
            decode(&wrong_size, MAX_FRAME_SIZE),
            Err(FrameError::InvalidLz4)
        );
    }
}
