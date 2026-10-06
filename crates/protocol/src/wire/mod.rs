//! Codecs for the Hyperliquid peer protocol.
//!
//! Every peer service shares one [`frame`] format. Inside a frame,
//! peers use a compact bincode-style encoding: unsigned integers from 0 to 250
//! take one byte, while markers 251, 252 and 253 introduce little-endian
//! `u16`, `u32` and `u64` values. Options and booleans are a 0/1 byte.
//!
//! This module performs no network I/O apart from reading and writing frames
//! on a caller-supplied stream. Decoders are bounded by their input: no
//! allocation is sized from an untrusted length before that length has been
//! checked against the bytes actually present.

pub mod frame;
pub mod greeting;
pub mod message;
mod reader;
pub mod rpc;

pub(crate) use reader::Reader;

use thiserror::Error;

/// A payload that does not match the expected wire layout.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The payload ended before a field was complete.
    #[error("truncated payload at byte {offset}: {needed} more bytes needed")]
    Truncated {
        /// Offset of the incomplete field.
        offset: usize,
        /// Bytes missing to complete the field.
        needed: usize,
    },
    /// A field holds a value outside its allowed range.
    #[error("invalid {field} at byte {offset}")]
    Invalid {
        /// Name of the field.
        field: &'static str,
        /// Offset of the field.
        offset: usize,
    },
    /// Bytes remained after a complete value.
    #[error("{remaining} unexpected trailing bytes")]
    Trailing {
        /// Number of unread bytes.
        remaining: usize,
    },
}
