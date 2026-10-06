//! Multi-signature envelopes (action encoding 2).
//!
//! The body is a MessagePack value holding, in order or by name:
//! `signature_chain_id`, `multi_sig_user`, `signatures`, `outer_signer` and
//! `inner_custom_vec`. The last one is binary data: an encoding byte followed
//! by the inner action body.

use super::{Action, ActionError, Encoding, msgpack::Value};
use bytes::Bytes;
use serde::Serialize;

/// An action authorized by several signers on behalf of one account.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiSig {
    /// Chain identifier used to build the signatures, as transported.
    pub signature_chain_id: String,
    /// Multi-signature account, as transported.
    pub multi_sig_user: String,
    /// Signer submitting the envelope, as transported.
    pub outer_signer: String,
    /// Signatures of the authorizing signers.
    pub signatures: Vec<MultiSigSignature>,
    /// Name of the inner action.
    pub inner_kind: String,
    /// The inner action.
    pub action: Action,
}

/// One signer's ECDSA signature, as transported.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MultiSigSignature {
    /// `r` scalar.
    pub r: String,
    /// `s` scalar.
    pub s: String,
    /// Recovery identifier.
    pub v: u64,
}

impl MultiSig {
    pub(super) fn decode(body: &Bytes, budget: &mut usize) -> Result<Self, ActionError> {
        let envelope = Value::decode_budgeted(body, budget)?;
        let field = |index: usize, name: &'static str| {
            match &envelope {
                Value::Array(values) => values.get(index),
                _ => envelope.get(name),
            }
            .ok_or(ActionError::MultiSig(name))
        };
        let string = |index, name| {
            field(index, name)?
                .as_str()
                .map(str::to_owned)
                .ok_or(ActionError::MultiSig(name))
        };
        let signatures = field(2, "signatures")?
            .as_array()
            .ok_or(ActionError::MultiSig("signatures"))?
            .iter()
            .map(signature)
            .collect::<Result<_, _>>()?;
        let Value::Binary(inner) = field(4, "inner_custom_vec")? else {
            return Err(ActionError::MultiSig("inner_custom_vec"));
        };
        let Some(&tag) = inner.first() else {
            return Err(ActionError::MultiSig("inner_custom_vec"));
        };
        let encoding = Encoding::from(tag);
        if encoding == Encoding::MultiSig {
            return Err(ActionError::NestedMultiSig);
        }
        let action = Action::decode_budgeted(encoding, inner.slice(1..), budget);
        Ok(Self {
            signature_chain_id: string(0, "signature_chain_id")?,
            multi_sig_user: string(1, "multi_sig_user")?,
            outer_signer: string(3, "outer_signer")?,
            signatures,
            inner_kind: action.kind().to_owned(),
            action,
        })
    }
}

fn signature(value: &Value) -> Result<MultiSigSignature, ActionError> {
    let field = |index: usize, name: &str| match value {
        Value::Array(values) => values.get(index),
        _ => value.get(name),
    };
    let error = ActionError::MultiSig("signatures");
    let string = |index, name| {
        field(index, name)
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    Ok(MultiSigSignature {
        r: string(0, "r").ok_or_else(|| error.clone())?,
        s: string(1, "s").ok_or_else(|| error.clone())?,
        v: field(2, "v").and_then(Value::as_u64).ok_or(error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(body: &Bytes) -> Result<MultiSig, ActionError> {
        let mut budget = crate::action::msgpack::MAX_VALUES;
        MultiSig::decode(body, &mut budget)
    }

    fn envelope(inner: &[u8]) -> Bytes {
        let mut bytes = vec![0x95, 0xa5];
        bytes.extend(b"0x3e7");
        bytes.extend([0xa4]);
        bytes.extend(b"0xaa");
        bytes.extend([0x91, 0x93, 0xa4]);
        bytes.extend(b"0x01");
        bytes.extend([0xa4]);
        bytes.extend(b"0x02");
        bytes.extend([27, 0xa4]);
        bytes.extend(b"0xbb");
        bytes.extend([0xc4, u8::try_from(inner.len()).unwrap()]);
        bytes.extend(inner);
        Bytes::from(bytes)
    }

    #[test]
    fn decodes_positional_envelopes_with_both_inner_encodings() {
        let multisig = decode(&envelope(b"\x00\x92\xa4noop\x90")).unwrap();
        assert_eq!(multisig.signature_chain_id, "0x3e7");
        assert_eq!(multisig.multi_sig_user, "0xaa");
        assert_eq!(multisig.outer_signer, "0xbb");
        assert_eq!(multisig.signatures[0].v, 27);
        assert_eq!(multisig.inner_kind, "noop");

        let multisig = decode(&envelope(&[1, 0, 0, 0, 0, 0])).unwrap();
        assert_eq!(multisig.inner_kind, "order");
    }

    #[test]
    fn rejects_nested_and_malformed_envelopes() {
        assert_eq!(
            decode(&envelope(&[2, 0xc0])),
            Err(ActionError::NestedMultiSig)
        );
        assert_eq!(
            decode(&envelope(&[])),
            Err(ActionError::MultiSig("inner_custom_vec"))
        );
        assert!(decode(&Bytes::from_static(&[0x90])).is_err());
    }
}
