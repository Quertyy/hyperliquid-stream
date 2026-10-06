//! A bounded MessagePack decoder producing a lossless [`Value`] tree.
//!
//! Most HyperCore actions are MessagePack values whose schema this crate does
//! not model. Decoding them generically keeps the tool compatible with new
//! action types: every value is preserved and rendered as JSON.

use bytes::Bytes;
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use std::fmt;
use thiserror::Error;

/// Deepest nesting accepted, which bounds recursion.
const MAX_DEPTH: usize = 64;

/// Most values decoded from one input by [`Value::decode`], and from one
/// bundle by [`Bundle::decode`](super::Bundle::decode). A value takes about
/// 40 bytes in memory, so this bounds the memory one transaction can
/// claim; real bundles hold well under 100,000 values.
pub const MAX_VALUES: usize = 1_000_000;

/// A UTF-8 string that shares the buffer it was decoded from.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Text(Bytes);

impl Text {
    /// Wraps `bytes` if they are valid UTF-8.
    pub fn new(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Self(bytes))
    }

    /// The string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Validated on construction; checking again keeps the crate free of
        // `unsafe` and costs a scan of a short string.
        std::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl From<&'static str> for Text {
    fn from(text: &'static str) -> Self {
        Self(Bytes::from_static(text.as_bytes()))
    }
}

impl From<String> for Text {
    fn from(text: String) -> Self {
        Self(Bytes::from(text))
    }
}

impl std::ops::Deref for Text {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A decoded MessagePack value.
///
/// Serializing to JSON maps strings, integers, floats, booleans and nil to
/// their JSON counterparts. Binary data becomes a `0x`-prefixed hex string.
/// Maps with string keys become objects; other maps become arrays of
/// `[key, value]` pairs. Extension values become `{"extType", "data"}`.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// `nil`.
    Nil,
    /// A boolean.
    Bool(bool),
    /// A nonnegative integer.
    UInt(u64),
    /// A negative integer.
    Int(i64),
    /// A single-precision float.
    F32(f32),
    /// A double-precision float.
    F64(f64),
    /// A UTF-8 string, sharing the transaction's buffer.
    String(Text),
    /// Binary data, sharing the transaction's buffer.
    Binary(Bytes),
    /// An array.
    Array(Vec<Value>),
    /// A map, in wire order.
    Map(Vec<(Value, Value)>),
    /// An extension type and its data.
    Ext(i8, Bytes),
}

/// A malformed or excessively nested MessagePack value.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MsgpackError {
    /// The input ended inside a value.
    #[error("truncated MessagePack value at byte {0}")]
    Truncated(usize),
    /// A marker byte that MessagePack never uses (`0xc1`).
    #[error("invalid MessagePack marker {marker:#04x} at byte {offset}")]
    InvalidMarker {
        /// The marker byte.
        marker: u8,
        /// Its offset.
        offset: usize,
    },
    /// A string is not valid UTF-8.
    #[error("invalid UTF-8 string at byte {0}")]
    InvalidUtf8(usize),
    /// Arrays or maps are nested more than 64 levels deep.
    #[error("MessagePack nesting exceeds {MAX_DEPTH} levels")]
    TooDeep,
    /// Bytes remained after one complete value.
    #[error("{0} trailing bytes after MessagePack value")]
    Trailing(usize),
    /// The input holds more values than allowed.
    #[error("more than {MAX_VALUES} MessagePack values")]
    TooManyValues,
}

impl Value {
    /// Decodes exactly one value from `bytes`, holding at most
    /// [`MAX_VALUES`] values.
    pub fn decode(bytes: &Bytes) -> Result<Self, MsgpackError> {
        let mut budget = MAX_VALUES;
        Self::decode_budgeted(bytes, &mut budget)
    }

    /// Decodes one value, charging each decoded value to `budget`.
    pub(crate) fn decode_budgeted(bytes: &Bytes, budget: &mut usize) -> Result<Self, MsgpackError> {
        let mut decoder = Decoder {
            bytes,
            offset: 0,
            budget,
        };
        let value = decoder.value(0)?;
        match bytes.len() - decoder.offset {
            0 => Ok(value),
            remaining => Err(MsgpackError::Trailing(remaining)),
        }
    }

    /// Returns the string, if this is one.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Returns the value as a `u64`, if it is a nonnegative integer.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::UInt(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns the elements, if this is an array.
    #[must_use]
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    /// Looks up a string key, if this is a map.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::Map(entries) => entries
                .iter()
                .find_map(|(k, v)| (k.as_str() == Some(key)).then_some(v)),
            _ => None,
        }
    }

    /// The action name: the first element of an array, or the `type` field
    /// of a map. HyperCore encodes most actions as `[name, fields...]`.
    #[must_use]
    pub fn action_name(&self) -> Option<&str> {
        match self {
            Self::Array(values) => values.first()?.as_str(),
            Self::Map(_) => self.get("type")?.as_str(),
            _ => None,
        }
    }
}

struct Decoder<'a, 'b> {
    bytes: &'a Bytes,
    offset: usize,
    /// Values that may still be decoded.
    budget: &'b mut usize,
}

impl Decoder<'_, '_> {
    fn take(&mut self, len: usize) -> Result<std::ops::Range<usize>, MsgpackError> {
        if len > self.bytes.len() - self.offset {
            return Err(MsgpackError::Truncated(self.offset));
        }
        let start = self.offset;
        self.offset += len;
        Ok(start..self.offset)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MsgpackError> {
        let range = self.take(N)?;
        let mut array = [0; N];
        array.copy_from_slice(&self.bytes[range]);
        Ok(array)
    }

    fn byte(&mut self) -> Result<u8, MsgpackError> {
        self.array::<1>().map(|[byte]| byte)
    }

    fn len8(&mut self) -> Result<usize, MsgpackError> {
        self.byte().map(usize::from)
    }

    fn len16(&mut self) -> Result<usize, MsgpackError> {
        self.array().map(u16::from_be_bytes).map(usize::from)
    }

    fn len32(&mut self) -> Result<usize, MsgpackError> {
        // A 32-bit length always fits in `usize` on supported targets.
        self.array().map(u32::from_be_bytes).map(|len| len as usize)
    }

    fn value(&mut self, depth: usize) -> Result<Value, MsgpackError> {
        *self.budget = self
            .budget
            .checked_sub(1)
            .ok_or(MsgpackError::TooManyValues)?;
        let offset = self.offset;
        let marker = self.byte()?;
        Ok(match marker {
            0x00..=0x7f => Value::UInt(u64::from(marker)),
            0x80..=0x8f => self.map(usize::from(marker & 0x0f), depth)?,
            0x90..=0x9f => self.seq(usize::from(marker & 0x0f), depth)?,
            0xa0..=0xbf => self.string(usize::from(marker & 0x1f))?,
            0xc0 => Value::Nil,
            0xc2 => Value::Bool(false),
            0xc3 => Value::Bool(true),
            0xc4 => {
                let len = self.len8()?;
                self.binary(len)?
            }
            0xc5 => {
                let len = self.len16()?;
                self.binary(len)?
            }
            0xc6 => {
                let len = self.len32()?;
                self.binary(len)?
            }
            0xc7 => {
                let len = self.len8()?;
                self.ext(len)?
            }
            0xc8 => {
                let len = self.len16()?;
                self.ext(len)?
            }
            0xc9 => {
                let len = self.len32()?;
                self.ext(len)?
            }
            0xca => Value::F32(f32::from_be_bytes(self.array()?)),
            0xcb => Value::F64(f64::from_be_bytes(self.array()?)),
            0xcc => Value::UInt(u64::from(self.byte()?)),
            0xcd => Value::UInt(u64::from(u16::from_be_bytes(self.array()?))),
            0xce => Value::UInt(u64::from(u32::from_be_bytes(self.array()?))),
            0xcf => Value::UInt(u64::from_be_bytes(self.array()?)),
            0xd0 => int(i64::from(i8::from_be_bytes(self.array()?))),
            0xd1 => int(i64::from(i16::from_be_bytes(self.array()?))),
            0xd2 => int(i64::from(i32::from_be_bytes(self.array()?))),
            0xd3 => int(i64::from_be_bytes(self.array()?)),
            0xd4 => self.ext(1)?,
            0xd5 => self.ext(2)?,
            0xd6 => self.ext(4)?,
            0xd7 => self.ext(8)?,
            0xd8 => self.ext(16)?,
            0xd9 => {
                let len = self.len8()?;
                self.string(len)?
            }
            0xda => {
                let len = self.len16()?;
                self.string(len)?
            }
            0xdb => {
                let len = self.len32()?;
                self.string(len)?
            }
            0xdc => {
                let len = self.len16()?;
                self.seq(len, depth)?
            }
            0xdd => {
                let len = self.len32()?;
                self.seq(len, depth)?
            }
            0xde => {
                let len = self.len16()?;
                self.map(len, depth)?
            }
            0xdf => {
                let len = self.len32()?;
                self.map(len, depth)?
            }
            0xe0..=0xff => Value::Int(i64::from(marker.cast_signed())),
            0xc1 => return Err(MsgpackError::InvalidMarker { marker, offset }),
        })
    }

    fn string(&mut self, len: usize) -> Result<Value, MsgpackError> {
        let offset = self.offset;
        let range = self.take(len)?;
        Text::new(self.bytes.slice(range))
            .map(Value::String)
            .map_err(|_| MsgpackError::InvalidUtf8(offset))
    }

    fn binary(&mut self, len: usize) -> Result<Value, MsgpackError> {
        let range = self.take(len)?;
        Ok(Value::Binary(self.bytes.slice(range)))
    }

    fn ext(&mut self, len: usize) -> Result<Value, MsgpackError> {
        let [tag] = self.array()?;
        let range = self.take(len)?;
        Ok(Value::Ext(tag.cast_signed(), self.bytes.slice(range)))
    }

    /// Every element occupies at least one byte and one value of budget, so
    /// a count larger than either is rejected before allocating.
    fn check_count(&self, count: usize, item_size: usize) -> Result<(), MsgpackError> {
        let remaining = self.bytes.len() - self.offset;
        let values = count.saturating_mul(item_size);
        if values > remaining {
            return Err(MsgpackError::Truncated(self.offset));
        }
        if values > *self.budget {
            return Err(MsgpackError::TooManyValues);
        }
        Ok(())
    }

    fn seq(&mut self, len: usize, depth: usize) -> Result<Value, MsgpackError> {
        if depth >= MAX_DEPTH {
            return Err(MsgpackError::TooDeep);
        }
        self.check_count(len, 1)?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.value(depth + 1)?);
        }
        Ok(Value::Array(values))
    }

    fn map(&mut self, len: usize, depth: usize) -> Result<Value, MsgpackError> {
        if depth >= MAX_DEPTH {
            return Err(MsgpackError::TooDeep);
        }
        self.check_count(len, 2)?;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            let key = self.value(depth + 1)?;
            let value = self.value(depth + 1)?;
            entries.push((key, value));
        }
        Ok(Value::Map(entries))
    }
}

/// Stores nonnegative signed integers as [`Value::UInt`].
fn int(value: i64) -> Value {
    u64::try_from(value).map_or(Value::Int(value), Value::UInt)
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Nil => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::UInt(value) => serializer.serialize_u64(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::F32(value) => serializer.serialize_f32(*value),
            Self::F64(value) => serializer.serialize_f64(*value),
            Self::String(value) => serializer.serialize_str(value.as_str()),
            Self::Binary(bytes) => crate::types::Hex(bytes).serialize(serializer),
            Self::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(value)?;
                }
                seq.end()
            }
            Self::Map(entries) if entries.iter().all(|(k, _)| k.as_str().is_some()) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Self::Map(entries) => {
                let mut seq = serializer.serialize_seq(Some(entries.len()))?;
                for (key, value) in entries {
                    seq.serialize_element(&(key, value))?;
                }
                seq.end()
            }
            Self::Ext(tag, data) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("extType", tag)?;
                map.serialize_entry("data", &crate::types::Hex(data))?;
                map.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> Result<Value, MsgpackError> {
        Value::decode(&Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn decodes_scalars() {
        assert_eq!(decode(&[0x05]), Ok(Value::UInt(5)));
        assert_eq!(decode(&[0xff]), Ok(Value::Int(-1)));
        assert_eq!(decode(&[0xd0, 0x80]), Ok(Value::Int(-128)));
        assert_eq!(decode(&[0xd1, 0x00, 0x05]), Ok(Value::UInt(5)));
        assert_eq!(
            decode(&[0xcf, 0, 0, 0, 1, 0, 0, 0, 0]),
            Ok(Value::UInt(1 << 32))
        );
        assert_eq!(decode(&[0xc0]), Ok(Value::Nil));
        assert_eq!(decode(&[0xc3]), Ok(Value::Bool(true)));
        assert_eq!(
            decode(&[0xcb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0]),
            Ok(Value::F64(1.5))
        );
        assert_eq!(decode(b"\xa3abc"), Ok(Value::String("abc".into())));
        assert_eq!(
            decode(&[0xc4, 2, 0xaa, 0xbb]),
            Ok(Value::Binary(Bytes::from_static(&[0xaa, 0xbb])))
        );
        assert_eq!(
            decode(&[0xd4, 0xfe, 0x07]),
            Ok(Value::Ext(-2, Bytes::from_static(&[7])))
        );
    }

    #[test]
    fn renders_actions_as_json() {
        // ["cancel", [[3, 42]]]
        let value = decode(b"\x92\xa6cancel\x91\x92\x03\x2a").unwrap();
        assert_eq!(value.action_name(), Some("cancel"));
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"["cancel",[[3,42]]]"#
        );
        // {"type": "noop", 1: true}
        let value = decode(b"\x82\xa4type\xa4noop\x01\xc3").unwrap();
        assert_eq!(value.action_name(), Some("noop"));
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"[["type","noop"],[1,true]]"#
        );
        let value = decode(b"\x81\xa4type\xa4noop").unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), r#"{"type":"noop"}"#);
        let value = decode(&[0x92, 0xc4, 1, 0xab, 0xd4, 3, 1]).unwrap();
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"["0xab",{"extType":3,"data":"0x01"}]"#
        );
    }

    #[test]
    fn rejects_malformed_input_without_large_allocations() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0xc1]).is_err());
        assert!(decode(&[0xa2, b'a']).is_err());
        assert!(decode(&[0xa1, 0xff]).is_err());
        assert!(decode(&[0x01, 0x02]).is_err());
        // An array claiming four billion elements with no data.
        assert_eq!(
            decode(&[0xdd, 0xff, 0xff, 0xff, 0xff]),
            Err(MsgpackError::Truncated(5))
        );
        let nested = [0x91; MAX_DEPTH + 1];
        assert_eq!(decode(&nested), Err(MsgpackError::TooDeep));
        let mut ok = vec![0x91; MAX_DEPTH];
        ok.push(0xc0);
        assert!(decode(&ok).is_ok());
    }

    #[test]
    fn bounds_the_number_of_values() {
        let mut bytes = vec![0xdc, 0x00, 0x10];
        bytes.extend([0xc0; 16]);
        let bytes = Bytes::from(bytes);
        assert!(Value::decode_budgeted(&bytes, &mut 17).is_ok());
        assert_eq!(
            Value::decode_budgeted(&bytes, &mut 16),
            Err(MsgpackError::TooManyValues)
        );
        assert_eq!(
            Value::decode_budgeted(&bytes, &mut 5),
            Err(MsgpackError::TooManyValues)
        );
    }

    #[test]
    fn shares_strings_with_the_input() {
        let bytes = Bytes::from_static(b"\xa5order");
        let Value::String(text) = Value::decode(&bytes).unwrap() else {
            panic!("expected a string");
        };
        assert_eq!(text.as_str(), "order");
        assert_eq!(text.as_str().as_ptr(), bytes[1..].as_ptr());
    }

    #[test]
    fn shares_binary_data_with_the_input() {
        let bytes = Bytes::from_static(&[0xc4, 2, 0xaa, 0xbb]);
        let Value::Binary(data) = Value::decode(&bytes).unwrap() else {
            panic!("expected binary");
        };
        assert_eq!(data.as_ptr(), bytes[2..].as_ptr());
    }
}
