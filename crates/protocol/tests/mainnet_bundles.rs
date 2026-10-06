//! Decodes signed action bundles captured on mainnet.
//!
//! The fixtures are transaction bodies attached to consensus blocks, which use
//! the same layout as mempool transactions. Expected counts were produced by
//! an independent decoder.

use bytes::Bytes;
use hyperliquid_protocol::{
    action::{Action, Bundle, Encoding},
    keccak256,
};
use std::collections::BTreeMap;

fn fixture(name: &str) -> Bytes {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    Bytes::from(std::fs::read(path).expect("fixture"))
}

fn kind_counts(bundle: &Bundle) -> BTreeMap<&str, usize> {
    let mut counts = BTreeMap::new();
    for action in &bundle.actions {
        *counts.entry(action.kind()).or_default() += 1;
    }
    counts
}

#[test]
fn decodes_every_action_of_a_large_mixed_bundle() {
    let bundle = Bundle::decode(&fixture("bundle-mixed-multisig.bin")).unwrap();
    let expected = BTreeMap::from([
        ("batchModify", 10),
        ("cancel", 17),
        ("cancelByCloid", 97),
        ("multiSig", 1),
        ("noop", 7),
        ("order", 159),
        ("scheduleCancel", 1),
        ("updateIsolatedMargin", 3),
        ("userSetAbstraction", 1),
    ]);
    assert_eq!(kind_counts(&bundle), expected);
    assert!(
        bundle
            .actions
            .iter()
            .all(|action| !matches!(action.action, Action::Invalid { .. }))
    );

    let multisig = bundle
        .actions
        .iter()
        .find_map(|action| match &action.action {
            Action::MultiSig(multisig) => Some(multisig),
            _ => None,
        })
        .unwrap();
    assert_eq!(multisig.inner_kind, "perpDeploy");
    assert_eq!(multisig.signatures.len(), 4);
    assert!(multisig.multi_sig_user.starts_with("0x"));

    let compact = bundle
        .actions
        .iter()
        .filter(|action| action.action.encoding() == Encoding::CompactOrders)
        .count();
    assert!(compact > 0, "compact order batches are decoded");
}

#[test]
fn decodes_evm_transactions() {
    let bundle = Bundle::decode(&fixture("bundle-evm-raw-tx.bin")).unwrap();
    assert_eq!(kind_counts(&bundle), BTreeMap::from([("evmRawTx", 2)]));
    for action in &bundle.actions {
        let Action::MessagePack(value) = &action.action else {
            panic!("evmRawTx is MessagePack");
        };
        let json = serde_json::to_value(value).unwrap();
        let raw = json[1].as_str().unwrap();
        // EIP-2718 typed transaction, hex encoded.
        assert!(raw.starts_with("0x02"), "{raw}");
    }
}

#[test]
fn decodes_small_bundles() {
    let bundle = Bundle::decode(&fixture("bundle-orders-cancels.bin")).unwrap();
    assert_eq!(
        kind_counts(&bundle),
        BTreeMap::from([("cancel", 1), ("order", 2)])
    );
    let bundle = Bundle::decode(&fixture("bundle-set-global-action.bin")).unwrap();
    assert_eq!(
        kind_counts(&bundle),
        BTreeMap::from([("SetGlobalAction", 1)])
    );
}

#[test]
fn renders_actions_as_json() {
    let bundle = Bundle::decode(&fixture("bundle-orders-cancels.bin")).unwrap();
    for action in &bundle.actions {
        let json = serde_json::to_value(action).unwrap();
        assert_eq!(json["kind"], action.kind());
        assert!(json["nonce"].is_u64());
        assert_eq!(json["signature"].as_str().unwrap().len(), 2 + 65 * 2);
        assert!(!json["action"].is_null());
    }
}

#[test]
fn transaction_hashes_are_keccak_of_the_payload() {
    let payload = fixture("bundle-evm-raw-tx.bin");
    assert_eq!(keccak256(&payload).to_string().len(), 66);
    assert_ne!(keccak256(&payload), keccak256(&payload[1..]));
}
