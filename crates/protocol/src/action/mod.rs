//! Signed action bundles: the transactions of the HyperCore mempool.
//!
//! A bundle groups the signed actions one node broadcasts:
//!
//! ```text
//! u32 BE           byte length of the size table
//! MessagePack      array with the byte length of each signed action
//! signed actions   concatenated
//! u64 BE           bundle nonce
//! [65]             bundle signature
//! ```
//!
//! Each signed action is laid out as follows:
//!
//! ```text
//! [65]       signature
//! u64 BE     nonce
//! u8 + [20]  vault address flag and address
//! u8         frontend flag
//! u8 + u64   expiry flag and value (BE)
//! u8         body encoding: 0 MessagePack, 1 compact orders, 2 multi-signature
//! [..]       body
//! ```
//!
//! A malformed bundle fails as a whole ([`BundleError`]). A malformed action
//! body only affects that action ([`Action::Invalid`]).

pub mod msgpack;
pub mod multisig;
pub mod orders;

use crate::types::{Address, Hex, Signature};
use bytes::Bytes;
use msgpack::{MsgpackError, Value};
use multisig::MultiSig;
use orders::{OrderBatch, OrderError};
use serde::{Serialize, Serializer, ser::SerializeMap};
use thiserror::Error;

const SIGNATURE_LEN: usize = Signature::LEN;
// Signature, nonce, three flags and the encoding byte.
const MIN_SIGNED_ACTION_LEN: usize = SIGNATURE_LEN + 8 + 3 + 1;
// Nonce and signature after the actions.
const TRAILER_LEN: usize = 8 + SIGNATURE_LEN;

/// Largest bundle decoded, 4 MiB. Real bundles stay well under 1 MiB; the
/// limit bounds the memory a single transaction can claim.
pub const MAX_BUNDLE_SIZE: usize = 4 * 1024 * 1024;

/// A bundle whose envelope does not match the expected layout.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum BundleError {
    /// The payload exceeds [`MAX_BUNDLE_SIZE`].
    #[error("bundle of {0} bytes exceeds {MAX_BUNDLE_SIZE} bytes")]
    TooLarge(usize),
    /// The payload ended inside a field.
    #[error("truncated bundle")]
    Truncated,
    /// The size table is not an array of action lengths.
    #[error("invalid action size table")]
    InvalidSizeTable,
    /// The action sizes do not account for the bytes present.
    #[error("action sizes cover {expected} bytes but {actual} are present")]
    SizeMismatch {
        /// Bytes implied by the size table and trailer.
        expected: usize,
        /// Bytes present after the size table.
        actual: usize,
    },
    /// A signed action has an invalid flag byte or ends inside a field.
    #[error("malformed signed action {index}")]
    MalformedAction {
        /// Position of the action in the bundle.
        index: usize,
    },
}

/// An action body that could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ActionError {
    /// The MessagePack body is malformed.
    #[error(transparent)]
    MessagePack(#[from] MsgpackError),
    /// The compact order batch is malformed.
    #[error(transparent)]
    Orders(#[from] OrderError),
    /// A field of a multi-signature envelope is missing or malformed.
    #[error("invalid multi-signature field `{0}`")]
    MultiSig(&'static str),
    /// A multi-signature envelope contains another one.
    #[error("nested multi-signature envelope")]
    NestedMultiSig,
}

/// A signed action bundle.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bundle {
    /// Nonce of the bundle.
    pub nonce: u64,
    /// Signature of the node that broadcast the bundle.
    pub signature: Signature,
    /// Signed actions, in order.
    pub actions: Vec<SignedAction>,
}

/// One action and the fields signed with it.
#[derive(Clone, Debug, PartialEq)]
pub struct SignedAction {
    /// Signature of the action's author.
    pub signature: Signature,
    /// Nonce chosen by the author, usually a millisecond timestamp.
    pub nonce: u64,
    /// Vault or subaccount the action is submitted for.
    pub vault_address: Option<Address>,
    /// Whether the action was submitted from the official frontend.
    pub is_frontend: bool,
    /// Time after which the action expires, in Unix milliseconds.
    pub expires_after: Option<u64>,
    /// The decoded action.
    pub action: Action,
}

/// The encoding of an action body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Encoding {
    /// A MessagePack value.
    MessagePack,
    /// A compact order batch.
    CompactOrders,
    /// A multi-signature envelope.
    MultiSig,
    /// An encoding this crate does not know.
    Unknown(u8),
}

/// A decoded action body.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Action {
    /// A MessagePack action, such as `["cancel", ...]` or `["evmRawTx", ...]`.
    MessagePack(Value),
    /// A compact order batch.
    Orders(OrderBatch),
    /// A multi-signature envelope.
    MultiSig(Box<MultiSig>),
    /// A body in an unknown encoding.
    Unknown {
        /// Encoding byte.
        encoding: u8,
        /// Raw body.
        body: Bytes,
    },
    /// A body that failed to decode.
    Invalid {
        /// Encoding of the body.
        encoding: Encoding,
        /// Raw body.
        body: Bytes,
        /// Why decoding failed.
        error: ActionError,
    },
}

impl From<u8> for Encoding {
    fn from(tag: u8) -> Self {
        match tag {
            0 => Self::MessagePack,
            1 => Self::CompactOrders,
            2 => Self::MultiSig,
            tag => Self::Unknown(tag),
        }
    }
}

impl Encoding {
    /// Name used in JSON output.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::MessagePack => "msgpack",
            Self::CompactOrders => "compactOrders",
            Self::MultiSig => "multiSig",
            Self::Unknown(_) => "unknown",
        }
    }
}

impl Bundle {
    /// Decodes a bundle; action bodies share the payload's buffer.
    ///
    /// All bodies together may hold at most
    /// [`MAX_VALUES`](msgpack::MAX_VALUES) MessagePack values; bodies past
    /// that budget become [`Action::Invalid`].
    pub fn decode(payload: &Bytes) -> Result<Self, BundleError> {
        if payload.len() > MAX_BUNDLE_SIZE {
            return Err(BundleError::TooLarge(payload.len()));
        }
        let mut budget = msgpack::MAX_VALUES;
        let mut cursor = Cursor::new(payload);
        let table_len = cursor.u32().ok_or(BundleError::Truncated)? as usize;
        let table = cursor.take(table_len).ok_or(BundleError::Truncated)?;
        let sizes = action_sizes(&payload.slice_ref(table), &mut budget)?;
        let actual = cursor.remaining();
        let expected = sizes
            .iter()
            .try_fold(TRAILER_LEN, |total, &size| total.checked_add(size))
            .ok_or(BundleError::InvalidSizeTable)?;
        if expected != actual {
            return Err(BundleError::SizeMismatch { expected, actual });
        }
        let mut actions = Vec::with_capacity(sizes.len());
        for (index, size) in sizes.into_iter().enumerate() {
            let bytes = cursor.take(size).ok_or(BundleError::Truncated)?;
            let action = SignedAction::decode(&payload.slice_ref(bytes), &mut budget)
                .ok_or(BundleError::MalformedAction { index })?;
            actions.push(action);
        }
        Ok(Self {
            nonce: cursor.u64().ok_or(BundleError::Truncated)?,
            signature: Signature(cursor.array().ok_or(BundleError::Truncated)?),
            actions,
        })
    }
}

fn action_sizes(table: &Bytes, budget: &mut usize) -> Result<Vec<usize>, BundleError> {
    let Ok(Value::Array(values)) = Value::decode_budgeted(table, budget) else {
        return Err(BundleError::InvalidSizeTable);
    };
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|size| usize::try_from(size).ok())
                .filter(|&size| size >= MIN_SIGNED_ACTION_LEN)
                .ok_or(BundleError::InvalidSizeTable)
        })
        .collect()
}

impl SignedAction {
    /// Decodes one signed action; `None` when a flag byte is invalid or a
    /// field is truncated.
    fn decode(bytes: &Bytes, budget: &mut usize) -> Option<Self> {
        let mut cursor = Cursor::new(bytes);
        let signature = Signature(cursor.array()?);
        let nonce = cursor.u64()?;
        let vault_address = if cursor.flag()? {
            Some(Address(cursor.array()?))
        } else {
            None
        };
        let is_frontend = cursor.flag()?;
        let expires_after = if cursor.flag()? {
            Some(cursor.u64()?)
        } else {
            None
        };
        let encoding = Encoding::from(cursor.byte()?);
        let body = bytes.slice(bytes.len() - cursor.remaining()..);
        Some(Self {
            signature,
            nonce,
            vault_address,
            is_frontend,
            expires_after,
            action: Action::decode_budgeted(encoding, body, budget),
        })
    }

    /// Name of the action, see [`Action::kind`].
    #[must_use]
    pub fn kind(&self) -> &str {
        self.action.kind()
    }
}

impl Action {
    /// Decodes a body in the given encoding.
    #[must_use]
    pub fn decode(encoding: Encoding, body: Bytes) -> Self {
        let mut budget = msgpack::MAX_VALUES;
        Self::decode_budgeted(encoding, body, &mut budget)
    }

    /// Decodes a body, charging MessagePack values to `budget`.
    pub(crate) fn decode_budgeted(encoding: Encoding, body: Bytes, budget: &mut usize) -> Self {
        let result = match encoding {
            Encoding::MessagePack => Value::decode_budgeted(&body, budget)
                .map(Self::MessagePack)
                .map_err(ActionError::from),
            Encoding::CompactOrders => OrderBatch::decode(&body)
                .map(Self::Orders)
                .map_err(ActionError::from),
            Encoding::MultiSig => {
                MultiSig::decode(&body, budget).map(|m| Self::MultiSig(Box::new(m)))
            }
            Encoding::Unknown(encoding) => return Self::Unknown { encoding, body },
        };
        result.unwrap_or_else(|error| Self::Invalid {
            encoding,
            body,
            error,
        })
    }

    /// The action name, as used by the exchange API: for example `order`,
    /// `cancel`, `cancelByCloid`, `batchModify`, `evmRawTx` or `multiSig`.
    ///
    /// Compact order batches are `order`. Bodies that did not decode are
    /// `invalid`, unknown encodings `unknown`, and MessagePack values without
    /// a name `unnamed`.
    #[must_use]
    pub fn kind(&self) -> &str {
        match self {
            Self::MessagePack(value) => value.action_name().unwrap_or("unnamed"),
            Self::Orders(_) => "order",
            Self::MultiSig(_) => "multiSig",
            Self::Unknown { .. } => "unknown",
            Self::Invalid { .. } => "invalid",
        }
    }

    /// The encoding the body was transported in.
    #[must_use]
    pub fn encoding(&self) -> Encoding {
        match self {
            Self::MessagePack(_) => Encoding::MessagePack,
            Self::Orders(_) => Encoding::CompactOrders,
            Self::MultiSig(_) => Encoding::MultiSig,
            Self::Unknown { encoding, .. } => Encoding::Unknown(*encoding),
            Self::Invalid { encoding, .. } => *encoding,
        }
    }
}

/// Serializes the decoded body: the MessagePack value, the order batch, the
/// multi-signature envelope, or `{"body", "error"?}` for raw bodies.
impl Serialize for Action {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::MessagePack(value) => value.serialize(serializer),
            Self::Orders(batch) => batch.serialize(serializer),
            Self::MultiSig(multisig) => multisig.serialize(serializer),
            Self::Unknown { body, .. } => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("body", &Hex(body))?;
                map.end()
            }
            Self::Invalid { body, error, .. } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("error", &error.to_string())?;
                map.serialize_entry("body", &Hex(body))?;
                map.end()
            }
        }
    }
}

impl SignedAction {
    /// Number of entries written by [`SignedAction::serialize_entries`].
    pub const ENTRIES: usize = 8;

    /// Writes the action's fields into `map`, so that a record embedding the
    /// action can add its own fields to the same JSON object without
    /// `#[serde(flatten)]`.
    pub fn serialize_entries<M: SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        map.serialize_entry("kind", self.kind())?;
        map.serialize_entry("encoding", self.action.encoding().name())?;
        map.serialize_entry("nonce", &self.nonce)?;
        map.serialize_entry("vaultAddress", &self.vault_address)?;
        map.serialize_entry("isFrontend", &self.is_frontend)?;
        map.serialize_entry("expiresAfter", &self.expires_after)?;
        map.serialize_entry("signature", &self.signature)?;
        map.serialize_entry("action", &self.action)
    }
}

impl Serialize for SignedAction {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(Self::ENTRIES))?;
        self.serialize_entries(&mut map)?;
        map.end()
    }
}

/// Big-endian cursor over fixed-width fields.
#[derive(Debug)]
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.bytes.split_at_checked(len)?;
        self.bytes = tail;
        Some(head)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, tail) = self.bytes.split_first_chunk::<N>()?;
        self.bytes = tail;
        Some(*head)
    }

    pub(crate) fn byte(&mut self) -> Option<u8> {
        self.array::<1>().map(|[byte]| byte)
    }

    /// A 0/1 flag byte; `None` for any other value or at the end.
    pub(crate) fn flag(&mut self) -> Option<bool> {
        match self.byte()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_be_bytes)
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_be_bytes)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Builds a signed action with the given encoding and body.
    pub(crate) fn signed_action(encoding: u8, body: &[u8], vault: bool) -> Vec<u8> {
        let mut bytes = vec![0x5a; SIGNATURE_LEN];
        bytes.extend(1_700_000_000_000_u64.to_be_bytes());
        bytes.push(u8::from(vault));
        if vault {
            bytes.extend([0x77; 20]);
        }
        bytes.extend([1, 1]); // frontend, expiry present
        bytes.extend(1_700_000_060_000_u64.to_be_bytes());
        bytes.push(encoding);
        bytes.extend(body);
        bytes
    }

    /// Builds a bundle from encoded signed actions.
    pub(crate) fn bundle(actions: &[Vec<u8>]) -> Vec<u8> {
        let mut table = vec![0x90 | u8::try_from(actions.len()).unwrap()];
        for action in actions {
            table.push(0xcd);
            table.extend(u16::try_from(action.len()).unwrap().to_be_bytes());
        }
        let mut bytes = u32::try_from(table.len()).unwrap().to_be_bytes().to_vec();
        bytes.extend(table);
        for action in actions {
            bytes.extend(action);
        }
        bytes.extend(42_u64.to_be_bytes());
        bytes.extend([0x99; SIGNATURE_LEN]);
        bytes
    }

    #[test]
    fn decodes_a_bundle_of_mixed_encodings() {
        let evm = b"\x92\xa8evmRawTx\xc4\x02\x02\xf8";
        let mut orders = vec![0, 0, 0, 0, 0];
        orders.extend(5_u32.to_be_bytes());
        orders.extend(100_000_000_u64.to_be_bytes());
        orders.extend(200_000_000_u64.to_be_bytes());
        orders.push(0x09); // buy, Gtc
        let payload = Bytes::from(bundle(&[
            signed_action(0, evm, true),
            signed_action(1, &orders, false),
            signed_action(7, b"??", false),
            signed_action(0, b"\xc1", false),
        ]));
        let bundle = Bundle::decode(&payload).unwrap();
        assert_eq!(bundle.nonce, 42);
        let kinds: Vec<_> = bundle.actions.iter().map(SignedAction::kind).collect();
        assert_eq!(kinds, ["evmRawTx", "order", "unknown", "invalid"]);
        let first = &bundle.actions[0];
        assert_eq!(first.vault_address, Some(Address([0x77; 20])));
        assert!(first.is_frontend);
        assert_eq!(first.expires_after, Some(1_700_000_060_000));

        let json = serde_json::to_value(first).unwrap();
        assert_eq!(json["kind"], "evmRawTx");
        assert_eq!(json["encoding"], "msgpack");
        assert_eq!(json["action"], serde_json::json!(["evmRawTx", "0x02f8"]));
        let json = serde_json::to_value(&bundle.actions[1]).unwrap();
        assert_eq!(json["action"]["orders"][0]["price"], "1");
        let json = serde_json::to_value(&bundle.actions[3]).unwrap();
        assert_eq!(json["action"]["body"], "0xc1");
    }

    #[test]
    fn action_bodies_share_the_payload_buffer() {
        let payload = Bytes::from(bundle(&[signed_action(9, b"body", false)]));
        let bundle = Bundle::decode(&payload).unwrap();
        let Action::Unknown { body, .. } = &bundle.actions[0].action else {
            panic!("expected an unknown encoding");
        };
        let range = payload.as_ptr_range();
        assert!(range.contains(&body.as_ptr()));
    }

    #[test]
    fn rejects_malformed_envelopes() {
        let valid = bundle(&[signed_action(0, b"\xc0", false)]);
        assert!(Bundle::decode(&Bytes::from(valid.clone())).is_ok());
        for end in 0..valid.len() {
            let payload = Bytes::copy_from_slice(&valid[..end]);
            assert!(Bundle::decode(&payload).is_err(), "prefix {end}");
        }
        let mut longer = valid.clone();
        longer.push(0);
        assert!(matches!(
            Bundle::decode(&Bytes::from(longer)),
            Err(BundleError::SizeMismatch { .. })
        ));
        let mut bad_flag = valid;
        bad_flag[4 + 4 + SIGNATURE_LEN + 8] = 2; // vault flag of the first action
        assert_eq!(
            Bundle::decode(&Bytes::from(bad_flag)),
            Err(BundleError::MalformedAction { index: 0 })
        );
    }
}
