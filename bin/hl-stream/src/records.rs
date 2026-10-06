//! JSON records written to stdout and sent to WebSocket subscribers.
//!
//! Every record carries a `type` field: `action`, `transaction`, `summary`,
//! `block` or `stats`.

use hyperliquid_stream::{
    Block, Hash, Stats, Transaction,
    action::{Action, SignedAction},
};
use hyperliquid_ws::KindFilter;
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

/// A value with a `type` field added.
#[derive(Debug, Serialize)]
pub(crate) struct Tagged<'a, T> {
    #[serde(rename = "type")]
    record: &'static str,
    #[serde(flatten)]
    value: &'a T,
}

pub(crate) fn transaction(tx: &Transaction) -> Tagged<'_, Transaction> {
    Tagged {
        record: "transaction",
        value: tx,
    }
}

pub(crate) fn block(block: &Block) -> Tagged<'_, Block> {
    Tagged {
        record: "block",
        value: block,
    }
}

pub(crate) fn stats(stats: &Stats) -> Tagged<'_, Stats> {
    Tagged {
        record: "stats",
        value: stats,
    }
}

/// One action, with the transaction it belongs to. It is the most frequent
/// record, so it is serialized by hand rather than with `#[serde(flatten)]`.
#[derive(Debug)]
pub(crate) struct ActionRecord<'a> {
    tx: Hash,
    index: usize,
    received_at_ms: u64,
    peer: SocketAddr,
    action: &'a SignedAction,
}

impl Serialize for ActionRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5 + SignedAction::ENTRIES))?;
        map.serialize_entry("type", "action")?;
        map.serialize_entry("tx", &self.tx)?;
        map.serialize_entry("index", &self.index)?;
        map.serialize_entry("receivedAtMs", &self.received_at_ms)?;
        map.serialize_entry("peer", &self.peer)?;
        self.action.serialize_entries(&mut map)?;
        map.end()
    }
}

impl<'a> ActionRecord<'a> {
    pub(crate) fn new(tx: &Transaction, index: usize, action: &'a SignedAction) -> Self {
        Self {
            tx: tx.hash,
            index,
            received_at_ms: tx.received_at_ms,
            peer: tx.peer,
            action,
        }
    }
}

/// Some actions of a transaction: the `mempoolActions` record.
#[derive(Debug)]
pub(crate) struct ActionsRecord<'a> {
    tx: &'a Transaction,
    /// Positions of the selected actions in the transaction.
    selected: Vec<usize>,
}

impl<'a> ActionsRecord<'a> {
    pub(crate) fn new(tx: &'a Transaction, selected: Vec<usize>) -> Self {
        Self { tx, selected }
    }
}

impl Serialize for ActionsRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let actions = self.tx.actions();
        let indexed: Vec<IndexedAction<'_>> = self
            .selected
            .iter()
            .filter_map(|&index| {
                Some(IndexedAction {
                    index,
                    action: actions.get(index)?,
                })
            })
            .collect();
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("type", "actions")?;
        map.serialize_entry("tx", &self.tx.hash)?;
        map.serialize_entry("receivedAtMs", &self.tx.received_at_ms)?;
        map.serialize_entry("peer", &self.tx.peer)?;
        map.serialize_entry("actions", &indexed)?;
        map.end()
    }
}

/// An action with its position in the transaction.
struct IndexedAction<'a> {
    index: usize,
    action: &'a SignedAction,
}

impl Serialize for IndexedAction<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1 + SignedAction::ENTRIES))?;
        map.serialize_entry("index", &self.index)?;
        self.action.serialize_entries(&mut map)?;
        map.end()
    }
}

/// A transaction with its action count per kind.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SummaryRecord<'a> {
    #[serde(rename = "type")]
    record: &'static str,
    hash: Hash,
    received_at_ms: u64,
    peer: SocketAddr,
    size: usize,
    actions: BTreeMap<&'a str, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<'a> SummaryRecord<'a> {
    pub(crate) fn new(tx: &'a Transaction) -> Self {
        let mut actions = BTreeMap::new();
        for action in tx.actions() {
            *actions.entry(action.kind()).or_default() += 1;
        }
        Self {
            record: "summary",
            hash: tx.hash,
            received_at_ms: tx.received_at_ms,
            peer: tx.peer,
            size: tx.payload.len(),
            actions,
            error: tx.bundle.as_ref().err().map(ToString::to_string),
        }
    }
}

/// Whether the filter selects the action, or the action inside a
/// multi-signature envelope.
pub(crate) fn accepts_action(filter: &KindFilter, action: &SignedAction) -> bool {
    filter.accepts(&action_kinds(action))
}

/// Whether the filter selects any action of the transaction.
pub(crate) fn accepts_transaction(filter: &KindFilter, tx: &Transaction) -> bool {
    filter.is_all()
        || tx
            .actions()
            .iter()
            .any(|action| accepts_action(filter, action))
}

/// The kind of an action, and the inner kind of a multi-signature action.
pub(crate) fn action_kinds(action: &SignedAction) -> Vec<&str> {
    match &action.action {
        Action::MultiSig(multisig) => vec![action.kind(), &multisig.inner_kind],
        _ => vec![action.kind()],
    }
}

/// The distinct kinds of a transaction's actions.
pub(crate) fn transaction_kinds(tx: &Transaction) -> Vec<&str> {
    let kinds: BTreeSet<&str> = tx.actions().iter().flat_map(action_kinds).collect();
    kinds.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction() -> Transaction {
        // An undecodable payload: no actions, an error.
        let payload = hyperliquid_stream::Bytes::from_static(b"not a bundle");
        Transaction::decode(payload, "1.2.3.4:4001".parse().unwrap(), 1_700_000_000_000)
    }

    #[test]
    fn records_carry_their_type() {
        let tx = transaction();
        let json = serde_json::to_value(super::transaction(&tx)).unwrap();
        assert_eq!(json["type"], "transaction");
        assert_eq!(json["peer"], "1.2.3.4:4001");
        assert_eq!(json["receivedAtMs"], 1_700_000_000_000_u64);
        assert!(json["error"].is_string());

        let json = serde_json::to_value(SummaryRecord::new(&tx)).unwrap();
        assert_eq!(json["type"], "summary");
        assert_eq!(json["size"], 12);
        assert_eq!(json["actions"], serde_json::json!({}));
    }

    #[test]
    fn filters_transactions_by_action_kind() {
        let tx = transaction();
        assert!(accepts_transaction(&KindFilter::default(), &tx));
        assert!(!accepts_transaction(
            &KindFilter::new(["order".to_owned()]),
            &tx
        ));
    }

    #[test]
    fn action_records_keep_their_fields_flat() {
        let payload = hyperliquid_stream::Bytes::from_static(include_bytes!(
            "../../../crates/protocol/tests/fixtures/bundle-orders-cancels.bin"
        ));
        let tx = Transaction::decode(payload, "1.2.3.4:4001".parse().unwrap(), 7);
        let action = &tx.actions()[0];
        let text = serde_json::to_string(&ActionRecord::new(&tx, 0, action)).unwrap();
        assert!(text.starts_with(r#"{"type":"action","tx":"#), "{text}");
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let mut keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut expected = [
            "type",
            "tx",
            "index",
            "receivedAtMs",
            "peer",
            "kind",
            "encoding",
            "nonce",
            "vaultAddress",
            "isFrontend",
            "expiresAfter",
            "signature",
            "action",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected);
        assert_eq!(json["type"], "action");
        assert_eq!(json["tx"], tx.hash.to_string());
        assert_eq!(json["kind"], action.kind());
        assert_eq!(
            json["action"],
            serde_json::to_value(&action.action).unwrap()
        );
    }

    #[test]
    fn actions_records_group_the_selected_actions() {
        let payload = hyperliquid_stream::Bytes::from_static(include_bytes!(
            "../../../crates/protocol/tests/fixtures/bundle-orders-cancels.bin"
        ));
        let tx = Transaction::decode(payload, "1.2.3.4:4001".parse().unwrap(), 7);
        let orders = KindFilter::new(["order".to_owned()]);
        let selected: Vec<usize> = (0..tx.actions().len())
            .filter(|&index| accepts_action(&orders, &tx.actions()[index]))
            .collect();
        assert_eq!(selected.len(), 2);
        let json = serde_json::to_value(ActionsRecord::new(&tx, selected.clone())).unwrap();
        assert_eq!(json["type"], "actions");
        assert_eq!(json["tx"], tx.hash.to_string());
        assert_eq!(json["peer"], "1.2.3.4:4001");
        let actions = json["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        for (record, index) in actions.iter().zip(selected) {
            assert_eq!(record["index"], index);
            assert_eq!(record["kind"], "order");
            assert!(record["signature"].is_string() && !record["action"].is_null());
        }
    }
}
