//! Fixed-size byte values displayed and serialized as `0x`-prefixed hexadecimal.

use serde::{Serialize, Serializer};
use sha3::{Digest, Keccak256};
use std::{fmt, str::FromStr};
use thiserror::Error;

/// A string that is not valid `0x`-prefixed hexadecimal of the expected length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("expected {expected} bytes of hexadecimal")]
pub struct ParseHexError {
    expected: usize,
}

macro_rules! fixed_bytes {
    ($(#[$meta:meta])* $name:ident, $len:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            /// Length in bytes.
            pub const LEN: usize = $len;

            /// Returns the raw bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $len] {
                &self.0
            }
        }

        impl From<[u8; $len]> for $name {
            fn from(bytes: [u8; $len]) -> Self {
                Self(bytes)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&Hex(&self.0), f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
        }

        impl FromStr for $name {
            type Err = ParseHexError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let mut bytes = [0; $len];
                parse_hex(s, &mut bytes)?;
                Ok(Self(bytes))
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(&Hex(&self.0))
            }
        }
    };
}

fixed_bytes!(
    /// A Keccak-256 hash, such as the consensus hash of a transaction.
    Hash,
    32
);
fixed_bytes!(
    /// A 20-byte account address.
    Address,
    20
);
fixed_bytes!(
    /// A 65-byte ECDSA signature exactly as transported.
    Signature,
    65
);
fixed_bytes!(
    /// A 16-byte client order ID.
    Cloid,
    16
);

/// Computes the Keccak-256 hash of `bytes`.
#[must_use]
pub fn keccak256(bytes: &[u8]) -> Hash {
    Hash(Keccak256::digest(bytes).into())
}

/// Encodes `bytes` as lowercase `0x`-prefixed hexadecimal.
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String {
    Hex(bytes).to_string()
}

fn parse_hex(s: &str, out: &mut [u8]) -> Result<(), ParseHexError> {
    let error = ParseHexError {
        expected: out.len(),
    };
    let digits = s.strip_prefix("0x").unwrap_or(s).as_bytes();
    if digits.len() != out.len() * 2 {
        return Err(error);
    }
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, &[high, low]) in out.iter_mut().zip(pairs) {
        let high = char::from(high).to_digit(16).ok_or(error)?;
        let low = char::from(low).to_digit(16).ok_or(error)?;
        // Both digits are below 16, so the value fits in a byte.
        *byte = u8::try_from((high << 4) | low).map_err(|_| error)?;
    }
    Ok(())
}

/// Serializes a byte slice as `0x`-prefixed hexadecimal.
///
/// Display and serialization write straight into the output, in chunks
/// encoded on the stack, without allocating.
pub(crate) struct Hex<'a>(pub(crate) &'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        const CHUNK: usize = 64;
        f.write_str("0x")?;
        let mut digits = [0; 2 * CHUNK];
        for chunk in self.0.chunks(CHUNK) {
            let (pairs, _) = digits.as_chunks_mut::<2>();
            for (pair, byte) in pairs.iter_mut().zip(chunk) {
                *pair = [
                    DIGITS[usize::from(byte >> 4)],
                    DIGITS[usize::from(byte & 0x0f)],
                ];
            }
            // Hexadecimal digits are ASCII.
            let text = std::str::from_utf8(&digits[..2 * chunk.len()]).map_err(|_| fmt::Error)?;
            f.write_str(text)?;
        }
        Ok(())
    }
}

impl Serialize for Hex<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let hash = keccak256(b"");
        let text = hash.to_string();
        assert_eq!(
            text,
            "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        assert_eq!(text.parse::<Hash>().unwrap(), hash);
        assert_eq!(text[2..].parse::<Hash>().unwrap(), hash);
        assert!("0x12".parse::<Hash>().is_err());
        assert!(format!("0x{}", "zz".repeat(32)).parse::<Hash>().is_err());
    }

    #[test]
    fn serializes_as_hex_string() {
        let address = Address([0xab; 20]);
        assert_eq!(
            serde_json::to_string(&address).unwrap(),
            format!("\"0x{}\"", "ab".repeat(20))
        );
        assert_eq!(serde_json::to_string(&Hex(&[1, 2])).unwrap(), "\"0x0102\"");
        // Longer than one stack chunk.
        let long: Vec<u8> = (0..=255).collect();
        let expected: String = std::iter::once("0x".to_owned())
            .chain(long.iter().map(|byte| format!("{byte:02x}")))
            .collect();
        assert_eq!(to_hex(&long), expected);
        assert_eq!(
            serde_json::to_string(&Hex(&long)).unwrap(),
            format!("\"{expected}\"")
        );
    }
}
