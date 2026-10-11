//! Streams from local peers that speak the consensus stream protocol.

use hyperliquid_stream::{
    Client, Config, Event,
    protocol::{keccak256, wire::frame},
};
use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

/// A bundle with one MessagePack `["noop"]` action and the given nonce.
fn bundle(nonce: u64) -> Vec<u8> {
    let mut action = vec![0xab; 65]; // signature
    action.extend(nonce.to_be_bytes());
    action.extend([0, 0, 0, 0]); // no vault, not frontend, no expiry, MessagePack
    action.extend(b"\x91\xa4noop");
    let table = [0x91, u8::try_from(action.len()).unwrap()];
    let mut bundle = u32::try_from(table.len()).unwrap().to_be_bytes().to_vec();
    bundle.extend(table);
    bundle.extend(action);
    bundle.extend(7_u64.to_be_bytes());
    bundle.extend([0xcd; 65]);
    bundle
}

/// A standalone transaction message.
fn transaction_message(bundle: &[u8]) -> Vec<u8> {
    let mut message = vec![1, 251];
    message.extend(u16::try_from(bundle.len()).unwrap().to_le_bytes());
    message.extend(bundle);
    message
}

/// A block message committing `hashes`, timestamped now.
fn block_message(hashes: &[[u8; 32]]) -> Vec<u8> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let mut message = vec![0];
    message.extend([0; 8]); // signature limbs
    message.push(27);
    message.extend([0x11; 20]); // proposer
    message.push(u8::try_from(hashes.len()).unwrap());
    for hash in hashes {
        message.extend(hash);
    }
    message.push(252);
    message.extend(u32::try_from(now.as_secs() * 2).unwrap().to_le_bytes());
    message.push(252);
    message.extend(now.subsec_nanos().to_le_bytes());
    message.extend([0xee; 40]); // certificates: not decoded
    message
}

fn lz4_frame(body: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::block::compress_prepend_size(body);
    let mut frame = u32::try_from(compressed.len())
        .unwrap()
        .to_be_bytes()
        .to_vec();
    frame.push(1);
    frame.extend(compressed);
    frame
}

/// Starts a peer that accepts one client and sends `frames` after the
/// greeting, then keeps the connection open.
async fn peer(frames: Vec<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut greeting = [0; 8];
        socket.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [0, 0, 0, 3, 0, 0, 1, 0], "split group greeting");
        socket
            .write_all(&frame::encode(&[3]).unwrap())
            .await
            .unwrap();
        for frame in frames {
            socket.write_all(&frame).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    addr
}

async fn next_event(client: &mut Client) -> Event {
    timeout(Duration::from_secs(10), client.next_event())
        .await
        .expect("an event in time")
        .expect("an open stream")
}

#[tokio::test]
async fn emits_each_transaction_once_and_tracks_its_block() {
    let first = bundle(1);
    let second = bundle(2);
    let hash = keccak256(&first);
    let peer_a = peer(vec![frame::encode(&transaction_message(&first)).unwrap()]).await;
    let peer_b = peer(vec![
        lz4_frame(&transaction_message(&first)),
        // A message type a future node version might add: skipped.
        frame::encode(&[7, 1, 2]).unwrap(),
        frame::encode(&transaction_message(&second)).unwrap(),
        frame::encode(&block_message(&[hash.0])).unwrap(),
    ])
    .await;

    let mut client = Client::connect(Config {
        peers: vec![peer_a, peer_b],
        discovery: false,
        callbacks: None,
        stats_interval: Duration::from_secs(3600),
        ..Config::default()
    })
    .await
    .unwrap();

    let mut transactions = Vec::new();
    let block = loop {
        match next_event(&mut client).await {
            Event::Transaction(tx) => transactions.push(tx),
            Event::Block(block) => break block,
            _ => {}
        }
    };
    // The block may arrive before the second transaction.
    while transactions.len() < 2 {
        if let Event::Transaction(tx) = next_event(&mut client).await {
            transactions.push(tx);
        }
    }

    let hashes: Vec<_> = transactions.iter().map(|tx| tx.hash).collect();
    assert!(hashes.contains(&hash));
    assert!(hashes.contains(&keccak256(&second)));
    for tx in &transactions {
        assert_eq!(tx.actions().len(), 1);
        assert_eq!(tx.actions()[0].kind(), "noop");
    }
    assert_eq!(block.tx_hashes, [hash]);
    assert_eq!(block.seen_in_mempool, 1);
    assert!(block.lag_ms < 5_000);

    // Nothing else: the duplicate copy of the first transaction is dropped.
    assert!(
        timeout(Duration::from_millis(500), client.next_event())
            .await
            .is_err()
    );
    client.shutdown().await;
}

#[tokio::test]
async fn replaces_block_only_peers_until_one_relays() {
    let tx = bundle(3);
    let block_only = peer(Vec::new()).await;
    let relay = peer(vec![frame::encode(&transaction_message(&tx)).unwrap()]).await;
    // The cache ranks the block-only peer first; with one slot, the relay
    // is reached only once the block-only peer is replaced after its trial.
    let cache =
        std::env::temp_dir().join(format!("hl-stream-relay-test-{}.json", std::process::id()));
    std::fs::write(
        &cache,
        format!(r#"[{{"peer":"{block_only}","transactions":0,"blocks":100}},{{"peer":"{relay}","transactions":0,"blocks":1}}]"#),
    )
    .unwrap();
    let started = std::time::Instant::now();
    let mut client = Client::connect(Config {
        peers_cache: Some(cache.clone()),
        discovery: false,
        callbacks: None,
        max_peers: 1,
        min_relay_peers: 1,
        relay_trial: Duration::from_millis(500),
        stats_interval: Duration::from_secs(3600),
        ..Config::default()
    })
    .await
    .unwrap();
    let Event::Transaction(received) = next_event(&mut client).await else {
        panic!("expected the relay's transaction");
    };
    assert_eq!(received.hash, keccak256(&tx));
    assert_eq!(received.peer, relay);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "the block-only peer was tried first"
    );
    client.shutdown().await;
    let _ = std::fs::remove_file(cache);
}

#[tokio::test]
async fn keeps_only_relays_and_one_spare_session() {
    let tx = bundle(4);
    let relay = peer(vec![frame::encode(&transaction_message(&tx)).unwrap()]).await;
    let mut client = Client::connect(Config {
        peers: vec![peer(Vec::new()).await, peer(Vec::new()).await, relay],
        discovery: false,
        callbacks: None,
        max_peers: 3,
        min_relay_peers: 1,
        relay_trial: Duration::from_secs(5),
        stats_interval: Duration::from_millis(500),
        ..Config::default()
    })
    .await
    .unwrap();
    // All three sessions open while looking for the relay, then the
    // block-only one beyond the spare is closed.
    let mut most_active = 0;
    let stats = loop {
        if let Event::Stats(stats) = next_event(&mut client).await {
            most_active = most_active.max(stats.active_peers.len());
            if most_active == 3 && stats.relaying_peers == 1 && stats.active_peers.len() == 2 {
                break stats;
            }
        }
    };
    assert!(stats.active_peers.iter().any(|peer| peer.address == relay));
    client.shutdown().await;
}

#[tokio::test]
async fn requires_a_peer_source() {
    let result = Client::connect(Config {
        discovery: false,
        callbacks: None,
        ..Config::default()
    })
    .await;
    assert!(matches!(result, Err(hyperliquid_stream::Error::NoPeers)));
}
