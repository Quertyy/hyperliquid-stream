//! Chooses peers, keeps as few sessions open as the mempool needs, and merges
//! what they deliver into one event stream.
//!
//! Peer policy:
//! - candidates are ranked: peers that relayed transactions, then peers that
//!   streamed blocks, then seeds, then the least loaded discovered peers;
//! - a session that ends puts its peer on hold, from 10 seconds after a
//!   disconnection to 15 minutes for an unreachable peer;
//! - a peer whose blocks lag more than `max_lag` for 20 seconds is replaced;
//! - while fewer than `min_relay_peers` sessions relay transactions, up to
//!   `max_peers` sessions are open and every session that relayed nothing for
//!   `relay_trial` is replaced; such a peer is not retried for 30 minutes.
//!   Peers forward mempool transactions only when every hop above them does,
//!   and most do not;
//! - once enough sessions relay, only they and one spare session stay open,
//!   and discovery slows down;
//! - at most eight sessions start per minute.
//!
//! Each session costs its peer a client slot and an upload of every block and
//! transaction, so sessions beyond what the mempool needs are not kept.

use crate::{
    Block, Config, Event, PeerStats, Stats,
    connection::STREAM_PORT,
    discovery::{child_peers, root_peers},
    metrics::{SessionMetrics, SupervisorMetrics},
    seen::SeenTransactions,
    session::{self, SessionEnd, SessionUpdate, Update, unix_ms},
};
use hyperliquid_protocol::{
    Address,
    wire::{message::BlockHeader, rpc::ChildPeer},
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet, VecDeque},
    io,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval, interval_at},
};
use tracing::{debug, info, warn};

const MAINTAIN_INTERVAL: Duration = Duration::from_secs(2);
const CRAWL_INTERVAL: Duration = Duration::from_secs(2);
const ROOTS_INTERVAL: Duration = Duration::from_secs(300);
const MAX_CONCURRENT_PROBES: usize = 4;
/// Interval between probes of one peer while relays are missing, and once
/// enough sessions relay.
const REPROBE_AFTER: Duration = Duration::from_secs(600);
const SETTLED_REPROBE_AFTER: Duration = Duration::from_secs(3600);
/// Interval between root peer requests once enough sessions relay.
const SETTLED_ROOTS_INTERVAL: Duration = Duration::from_secs(3600);
/// Sessions kept beside the relays, to bridge a relay's disconnection.
const SPARE_SESSIONS: usize = 1;
const MAX_SESSION_STARTS_PER_MINUTE: usize = 8;
const LAG_GRACE: Duration = Duration::from_secs(20);
const MAX_KNOWN_PEERS: usize = 4096;
/// New peers accepted from one `ChildPeerStatuses` response, so that a
/// single peer cannot flood the candidate list.
const MAX_NEW_PEERS_PER_RESPONSE: usize = 64;
const MAX_CACHED_PEERS: usize = 512;
const RECENT_BLOCKS: usize = 1024;
const UPDATE_CAPACITY: usize = 4096;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Everything known about one peer.
#[derive(Debug, Default)]
struct PeerRecord {
    /// A seed or cached peer, preferred over discovered ones.
    preferred: bool,
    /// Connections the peer's parent reported for it.
    connection_count: Option<u64>,
    /// Do not start a session before this time.
    retry_at: Option<Instant>,
    /// Transactions and blocks relayed, over all sessions.
    transactions: u64,
    blocks: u64,
    /// State of the current session.
    connected_at: Option<Instant>,
    session_transactions: u64,
    session_blocks: u64,
    lag_ms: Option<u64>,
    lagging_since: Option<Instant>,
    /// When the current session last relayed a transaction.
    last_transaction_at: Option<Instant>,
}

#[derive(Debug)]
struct Session {
    id: u64,
    task: JoinHandle<()>,
    /// Dropping this sender asks the session to stop.
    _stop: oneshot::Sender<()>,
}

#[derive(Debug)]
enum Discovered {
    Roots(Vec<IpAddr>),
    RootsFailed,
    Children(Vec<ChildPeer>),
    ProbeFailed,
}

/// Blocks already emitted, identified by consensus time and proposer.
#[derive(Debug, Default)]
struct RecentBlocks {
    set: HashSet<(u64, Address)>,
    order: VecDeque<(u64, Address)>,
}

impl RecentBlocks {
    fn insert(&mut self, key: (u64, Address)) -> bool {
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > RECENT_BLOCKS
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        true
    }
}

pub(crate) struct Supervisor {
    config: Config,
    peers: HashMap<SocketAddr, PeerRecord>,
    sessions: HashMap<SocketAddr, Session>,
    next_session: u64,
    seen: Arc<SeenTransactions>,
    recent_blocks: RecentBlocks,
    totals: Stats,
    events: mpsc::Sender<Event>,
    updates: mpsc::Sender<SessionUpdate>,
    discovered: mpsc::Sender<Discovered>,
    probe_queue: VecDeque<IpAddr>,
    probed: HashMap<IpAddr, Instant>,
    probes_in_flight: usize,
    /// Whether fewer than `min_relay_peers` sessions relay transactions.
    relay_shortage: bool,
    /// When sessions started during the last minute.
    session_starts: VecDeque<Instant>,
    roots_fetched_at: Option<Instant>,
    /// Until when the consumer is considered slow; see [`Self::congested`].
    congested_until: Option<Instant>,
    http: reqwest::Client,
    metrics: SupervisorMetrics,
    session_metrics: Arc<SessionMetrics>,
}

impl Supervisor {
    /// Starts the supervisor. Sending on (or dropping) the returned channel
    /// stops it.
    pub(crate) fn spawn(
        config: Config,
        events: mpsc::Sender<Event>,
    ) -> (JoinHandle<()>, oneshot::Sender<()>) {
        let (stop_tx, stop) = oneshot::channel();
        let (updates_tx, updates) = mpsc::channel(UPDATE_CAPACITY);
        let (discovered_tx, discovered) = mpsc::channel(64);
        let mut peers = HashMap::new();
        for &peer in &config.peers {
            peers.entry(peer).or_insert_with(|| PeerRecord {
                preferred: true,
                ..PeerRecord::default()
            });
        }
        // Cached peers keep what they relayed before, so that known relays
        // are tried first.
        for cached in config
            .peers_cache
            .as_deref()
            .map(load_cache)
            .unwrap_or_default()
        {
            let record = peers.entry(cached.peer).or_default();
            record.preferred = true;
            record.transactions = cached.transactions;
            record.blocks = cached.blocks;
        }
        let supervisor = Self {
            seen: Arc::new(SeenTransactions::new(config.retention)),
            probe_queue: peers.keys().map(SocketAddr::ip).collect(),
            peers,
            sessions: HashMap::new(),
            next_session: 0,
            recent_blocks: RecentBlocks::default(),
            totals: Stats::default(),
            events,
            updates: updates_tx,
            discovered: discovered_tx,
            probed: HashMap::new(),
            probes_in_flight: 0,
            relay_shortage: false,
            session_starts: VecDeque::new(),
            roots_fetched_at: None,
            congested_until: None,
            metrics: SupervisorMetrics::default(),
            session_metrics: Arc::default(),
            http: reqwest::Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .unwrap_or_default(),
            config,
        };
        let task = tokio::spawn(supervisor.run(updates, discovered, stop));
        (task, stop_tx)
    }

    async fn run(
        mut self,
        mut updates: mpsc::Receiver<SessionUpdate>,
        mut discovered: mpsc::Receiver<Discovered>,
        mut stop: oneshot::Receiver<()>,
    ) {
        let stats_interval = self.config.stats_interval;
        let mut maintain = interval(MAINTAIN_INTERVAL);
        let mut crawl = interval(CRAWL_INTERVAL);
        let mut roots = interval(ROOTS_INTERVAL);
        let mut stats = interval_at(Instant::now() + stats_interval, stats_interval);
        for timer in [&mut maintain, &mut crawl, &mut roots, &mut stats] {
            timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        }
        let discovery = self.config.discovery;
        info!(
            known_peers = self.peers.len(),
            discovery, "mempool supervisor started"
        );
        loop {
            let running = tokio::select! {
                _ = &mut stop => false,
                Some(update) = updates.recv() => self.on_update(update).await,
                Some(found) = discovered.recv() => {
                    self.on_discovered(found);
                    true
                }
                _ = maintain.tick() => {
                    self.maintain();
                    true
                }
                _ = crawl.tick(), if discovery => {
                    self.crawl();
                    true
                }
                _ = roots.tick(), if discovery => {
                    self.fetch_roots(Instant::now());
                    true
                }
                _ = stats.tick() => self.publish_stats().await,
            };
            if !running {
                break;
            }
        }
        for (_, session) in self.sessions.drain() {
            session.task.abort();
        }
        if let Some((path, peers)) = self.cache_snapshot()
            && let Err(error) = write_atomically(&path, &peers)
        {
            warn!(path = %path.display(), %error, "saving the peer cache failed");
        }
        info!("mempool supervisor stopped");
    }

    /// Returns `false` once the consumer has gone away.
    async fn on_update(
        &mut self,
        SessionUpdate {
            peer,
            session,
            kind,
        }: SessionUpdate,
    ) -> bool {
        let current = self.sessions.get(&peer).is_some_and(|s| s.id == session);
        let event = match kind {
            Update::Connected if current => {
                let record = self.peers.entry(peer).or_default();
                record.connected_at = Some(Instant::now());
                self.metrics.peers.connected.increment(1);
                debug!(%peer, "streaming");
                return true;
            }
            Update::Ended(end) if current => {
                self.sessions.remove(&peer);
                self.hold(peer, end);
                return true;
            }
            Update::Connected | Update::Ended(_) => return true,
            Update::Transaction(transaction) => {
                let record = self.peers.entry(peer).or_default();
                record.transactions += 1;
                record.session_transactions += 1;
                record.last_transaction_at = Some(Instant::now());
                if record.session_transactions == 1 {
                    info!(%peer, "peer relays the mempool");
                }
                let Some(transaction) = transaction else {
                    self.totals.duplicates += 1;
                    return true;
                };
                self.totals.transactions += 1;
                Event::Transaction(transaction)
            }
            Update::Block {
                header,
                received_at_ms,
            } => match self.on_block(peer, header, received_at_ms) {
                Some(block) => Event::Block(Arc::new(block)),
                None => return true,
            },
        };
        if self.events.capacity() == 0 {
            self.congested_until = Some(Instant::now() + LAG_GRACE);
        }
        self.events.send(event).await.is_ok()
    }

    /// Whether the consumer recently left no room for events. Sessions then
    /// read more slowly, so block lag measures the consumer rather than the
    /// peers: lag is not held against peers and blocks are not dropped as
    /// stale.
    fn congested(&self) -> bool {
        self.congested_until
            .is_some_and(|until| Instant::now() < until)
    }

    /// Updates the peer's lag and the committed transactions. Returns the
    /// block if it is live and no peer delivered it before.
    ///
    /// A newly connected peer first replays blocks from up to several minutes
    /// earlier; those still mark their transactions as committed, but are
    /// not emitted.
    fn on_block(
        &mut self,
        peer: SocketAddr,
        header: BlockHeader,
        received_at_ms: u64,
    ) -> Option<Block> {
        let lag_ms = received_at_ms.saturating_sub(header.time_ms);
        let live = Duration::from_millis(lag_ms) <= self.config.max_lag || self.congested();
        let record = self.peers.entry(peer).or_default();
        record.blocks += 1;
        record.session_blocks += 1;
        record.lag_ms = Some(lag_ms);
        if live {
            record.lagging_since = None;
        } else {
            record.lagging_since.get_or_insert_with(Instant::now);
        }
        if !self.recent_blocks.insert((header.time_ms, header.proposer)) {
            return None;
        }
        let delays = self.seen.commit(&header.tx_hashes, received_at_ms);
        if !live {
            self.metrics.blocks.stale_total.increment(1);
            return None;
        }
        let blocks = &self.metrics.blocks;
        blocks.total.increment(1);
        blocks
            .transactions_total
            .increment(header.tx_hashes.len() as u64);
        blocks
            .transactions_seen_total
            .increment(delays.len() as u64);
        blocks.lag_seconds.record(seconds(lag_ms));
        let inclusion_delay = &self.metrics.inclusion.inclusion_delay_seconds;
        for &delay in &delays {
            inclusion_delay.record(seconds(delay));
        }
        self.totals.blocks += 1;
        self.totals.block_transactions += header.tx_hashes.len() as u64;
        self.totals.block_transactions_seen += delays.len() as u64;
        Some(Block {
            time_ms: header.time_ms,
            proposer: header.proposer,
            received_at_ms,
            lag_ms,
            peer,
            seen_in_mempool: delays.len(),
            max_inclusion_delay_ms: delays.iter().max().copied(),
            tx_hashes: header.tx_hashes,
        })
    }

    /// Puts a peer whose session ended on hold.
    fn hold(&mut self, peer: SocketAddr, end: SessionEnd) {
        let record = self.peers.entry(peer).or_default();
        record.retry_at = Some(Instant::now() + end.retry_after());
        record.connected_at = None;
        record.lagging_since = None;
        let metrics = &self.metrics.peers;
        match end {
            SessionEnd::PeerFull => &metrics.peer_full,
            SessionEnd::RateLimited => &metrics.rate_limited,
            SessionEnd::NoQuorum => &metrics.no_quorum,
            SessionEnd::Rejected => &metrics.rejected,
            SessionEnd::Unreachable => &metrics.unreachable,
            SessionEnd::Disconnected => &metrics.disconnected,
            SessionEnd::Replaced => &metrics.replaced,
            SessionEnd::NotRelaying => &metrics.not_relaying,
            SessionEnd::Surplus => &metrics.surplus,
        }
        .increment(1);
        debug!(%peer, reason = end.label(), "session ended");
    }

    /// Stops a session; dropping it signals the task, which finishes
    /// delivering what it has already read.
    fn stop_session(&mut self, peer: SocketAddr, end: SessionEnd) {
        self.sessions.remove(&peer);
        self.hold(peer, end);
    }

    /// Whether the peer relayed a transaction within three relay trials.
    fn is_relaying(&self, peer: &SocketAddr) -> bool {
        let window = self.config.relay_trial * 3;
        self.peers.get(peer).is_some_and(|record| {
            record
                .last_transaction_at
                .is_some_and(|at| at.elapsed() < window)
        })
    }

    fn maintain(&mut self) {
        let now = Instant::now();
        self.seen.expire(unix_ms());
        let congested = self.congested();
        let lagging: Vec<SocketAddr> = self
            .sessions
            .keys()
            .filter(|peer| {
                !congested
                    && self
                        .peers
                        .get(peer)
                        .and_then(|record| record.lagging_since)
                        .is_some_and(|since| now - since > LAG_GRACE)
            })
            .copied()
            .collect();
        for peer in lagging {
            info!(%peer, "replacing lagging peer");
            self.stop_session(peer, SessionEnd::Replaced);
        }

        let relaying = self.find_relays(now);
        let target = if self.relay_shortage {
            self.config.max_peers
        } else {
            self.config.max_peers.min(relaying + SPARE_SESSIONS)
        };
        self.close_surplus(target);

        self.session_starts
            .retain(|at| now - *at < Duration::from_secs(60));
        let free = target
            .saturating_sub(self.sessions.len())
            .min(MAX_SESSION_STARTS_PER_MINUTE.saturating_sub(self.session_starts.len()));
        let mut candidates: Vec<(&SocketAddr, &PeerRecord)> = self
            .peers
            .iter()
            .filter(|(peer, record)| {
                !self.sessions.contains_key(peer) && record.retry_at.is_none_or(|at| at <= now)
            })
            .collect();
        candidates.sort_by_key(|(_, record)| {
            (
                Reverse(record.transactions),
                Reverse(record.blocks),
                Reverse(record.preferred),
                record.connection_count.unwrap_or(u64::MAX),
            )
        });
        let chosen: Vec<SocketAddr> = candidates
            .iter()
            .take(free)
            .map(|(peer, _)| **peer)
            .collect();
        for peer in chosen {
            self.start(peer);
        }

        let relaying = self.relaying_sessions();
        self.metrics
            .inclusion
            .pending
            .set(self.seen.pending() as f64);
        let metrics = &self.metrics.peers;
        metrics.known.set(self.peers.len() as f64);
        metrics.active.set(self.sessions.len() as f64);
        metrics.relaying.set(relaying as f64);
    }

    fn relaying_sessions(&self) -> usize {
        self.sessions.keys().filter(|p| self.is_relaying(p)).count()
    }

    /// While fewer than `min_relay_peers` sessions relay transactions,
    /// stops every session that relayed nothing for `relay_trial`; free
    /// slots are then filled with the next candidates. Returns the number of
    /// relaying sessions.
    fn find_relays(&mut self, now: Instant) -> usize {
        let relaying = self.relaying_sessions();
        let wanted = self.config.min_relay_peers;
        if relaying >= wanted {
            if self.relay_shortage {
                info!(relaying, wanted, "enough peers relay the mempool");
                self.relay_shortage = false;
            }
            return relaying;
        }
        if !self.relay_shortage {
            warn!(
                relaying,
                wanted, "too few peers relay the mempool, looking for more"
            );
            self.relay_shortage = true;
        }
        let silent: Vec<(SocketAddr, bool)> = self
            .sessions
            .keys()
            .filter(|peer| !self.is_relaying(peer))
            .filter_map(|peer| {
                let record = self.peers.get(peer)?;
                let tried = record
                    .connected_at
                    .is_some_and(|at| now - at >= self.config.relay_trial);
                tried.then_some((*peer, record.session_transactions > 0))
            })
            .collect();
        for (peer, relayed_before) in silent {
            debug!(%peer, relaying, relayed_before, "replacing peer that relays no transactions");
            // A relay that went silent may resume, so it is retried sooner.
            let end = if relayed_before {
                SessionEnd::Replaced
            } else {
                SessionEnd::NotRelaying
            };
            self.stop_session(peer, end);
        }
        relaying
    }

    /// Stops sessions that do not relay until at most `target` remain;
    /// sessions still connecting go first, then the most lagging ones.
    fn close_surplus(&mut self, target: usize) {
        let excess = self.sessions.len().saturating_sub(target);
        if excess == 0 {
            return;
        }
        let mut idle: Vec<(SocketAddr, Option<u64>, bool)> = self
            .sessions
            .keys()
            .filter(|peer| !self.is_relaying(peer))
            .map(|peer| {
                let record = self.peers.get(peer);
                let connected = record.is_some_and(|r| r.connected_at.is_some());
                (*peer, record.and_then(|r| r.lag_ms), connected)
            })
            .collect();
        idle.sort_by_key(|&(_, lag, connected)| (connected, Reverse(lag)));
        for (peer, _, _) in idle.into_iter().take(excess) {
            debug!(%peer, "closing a session no longer needed");
            self.stop_session(peer, SessionEnd::Surplus);
        }
    }

    fn start(&mut self, peer: SocketAddr) {
        self.next_session += 1;
        let id = self.next_session;
        let record = self.peers.entry(peer).or_default();
        record.connected_at = None;
        record.session_transactions = 0;
        record.session_blocks = 0;
        record.lag_ms = None;
        record.lagging_since = None;
        record.last_transaction_at = None;
        self.session_starts.push_back(Instant::now());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(session::run(
            peer,
            id,
            Arc::clone(&self.seen),
            Arc::clone(&self.session_metrics),
            self.updates.clone(),
            stopped,
        ));
        self.sessions.insert(
            peer,
            Session {
                id,
                task,
                _stop: stop,
            },
        );
        debug!(%peer, "starting session");
    }

    /// Probes queued peers for their children, a few at a time.
    fn crawl(&mut self) {
        if self.probe_queue.is_empty() {
            let known: HashSet<IpAddr> = self.peers.keys().map(SocketAddr::ip).collect();
            self.probe_queue.extend(known);
        }
        let now = Instant::now();
        while self.probes_in_flight < MAX_CONCURRENT_PROBES {
            let Some(ip) = self.probe_queue.pop_front() else {
                break;
            };
            let reprobe_after = if self.relay_shortage {
                REPROBE_AFTER
            } else {
                SETTLED_REPROBE_AFTER
            };
            if self
                .probed
                .get(&ip)
                .is_some_and(|at| now - *at < reprobe_after)
            {
                continue;
            }
            self.probed.insert(ip, now);
            self.probes_in_flight += 1;
            let discovered = self.discovered.clone();
            tokio::spawn(async move {
                let found = match child_peers(ip).await {
                    Ok(children) => Discovered::Children(children),
                    Err(error) => {
                        debug!(%ip, %error, "ChildPeerStatuses failed");
                        Discovered::ProbeFailed
                    }
                };
                let _ = discovered.send(found).await;
            });
        }
    }

    /// Requests root peers; once enough sessions relay, at most hourly.
    fn fetch_roots(&mut self, now: Instant) {
        if !self.relay_shortage
            && self
                .roots_fetched_at
                .is_some_and(|at| now - at < SETTLED_ROOTS_INTERVAL)
        {
            return;
        }
        self.roots_fetched_at = Some(now);
        let (http, url, discovered) = (
            self.http.clone(),
            self.config.info_url.clone(),
            self.discovered.clone(),
        );
        tokio::spawn(async move {
            let found = match root_peers(&http, &url).await {
                Ok(ips) => Discovered::Roots(ips),
                Err(error) => {
                    warn!(%error, "fetching root peers failed");
                    Discovered::RootsFailed
                }
            };
            let _ = discovered.send(found).await;
        });
    }

    fn on_discovered(&mut self, found: Discovered) {
        match found {
            Discovered::Roots(ips) => {
                self.metrics.discovery.root_peers_ok.increment(1);
                debug!(count = ips.len(), "root peers");
                for ip in ips {
                    self.add_candidate(ip, None);
                }
            }
            Discovered::RootsFailed => self.metrics.discovery.root_peers_error.increment(1),
            Discovered::Children(children) => {
                self.probes_in_flight -= 1;
                self.metrics.discovery.child_peers_ok.increment(1);
                let mut added = 0;
                for child in children {
                    if added == MAX_NEW_PEERS_PER_RESPONSE {
                        break;
                    }
                    added += usize::from(
                        self.add_candidate(IpAddr::V4(child.ip), Some(child.connection_count)),
                    );
                }
            }
            Discovered::ProbeFailed => {
                self.probes_in_flight -= 1;
                self.metrics.discovery.child_peers_error.increment(1);
            }
        }
    }

    /// Adds a discovered peer; returns whether it was new.
    fn add_candidate(&mut self, ip: IpAddr, connection_count: Option<u64>) -> bool {
        if !is_public(ip) {
            return false;
        }
        let addr = SocketAddr::new(ip, STREAM_PORT);
        let new = !self.peers.contains_key(&addr);
        if new {
            if self.peers.len() >= MAX_KNOWN_PEERS && !self.evict_failed_peer() {
                return false;
            }
            self.probe_queue.push_back(ip);
        }
        let record = self.peers.entry(addr).or_default();
        if connection_count.is_some() {
            record.connection_count = connection_count;
        }
        new
    }

    /// Forgets one discovered peer that failed and never streamed, to make
    /// room for a new candidate.
    fn evict_failed_peer(&mut self) -> bool {
        let failed = self
            .peers
            .iter()
            .find(|(peer, record)| {
                !record.preferred
                    && record.blocks == 0
                    && record.retry_at.is_some()
                    && !self.sessions.contains_key(peer)
            })
            .map(|(peer, _)| *peer);
        failed.is_some_and(|peer| self.peers.remove(&peer).is_some())
    }

    /// Returns `false` once the consumer has gone away.
    async fn publish_stats(&mut self) -> bool {
        let now = Instant::now();
        let active_peers: Vec<PeerStats> = self
            .sessions
            .keys()
            .filter_map(|peer| {
                let record = self.peers.get(peer)?;
                Some(PeerStats {
                    address: *peer,
                    connected_secs: (now - record.connected_at?).as_secs(),
                    lag_ms: record.lag_ms,
                    transactions: record.session_transactions,
                    blocks: record.session_blocks,
                })
            })
            .collect();
        let stats = Stats {
            known_peers: self.peers.len(),
            relaying_peers: self.relaying_sessions(),
            active_peers,
            pending: self.seen.pending(),
            ..self.totals.clone()
        };
        info!(
            pending = stats.pending,
            streaming = stats.active_peers.len(),
            known = stats.known_peers,
            transactions = stats.transactions,
            blocks = stats.blocks,
            "stats"
        );
        self.save_cache();
        self.events
            .send(Event::Stats(Arc::new(stats)))
            .await
            .is_ok()
    }

    /// Saves the peer cache in the background.
    fn save_cache(&self) {
        if let Some((path, peers)) = self.cache_snapshot() {
            tokio::task::spawn_blocking(move || {
                if let Err(error) = write_atomically(&path, &peers) {
                    warn!(path = %path.display(), %error, "saving the peer cache failed");
                }
            });
        }
    }

    /// The cache path and the peers to save: those that streamed,
    /// transaction relays first.
    fn cache_snapshot(&self) -> Option<(PathBuf, Vec<CachedPeer>)> {
        let path = self.config.peers_cache.clone()?;
        let mut good: Vec<(&SocketAddr, &PeerRecord)> = self
            .peers
            .iter()
            .filter(|(_, record)| record.blocks > 0)
            .collect();
        if good.is_empty() {
            return None;
        }
        good.sort_by_key(|(_, record)| Reverse((record.transactions, record.blocks)));
        let peers = good
            .iter()
            .take(MAX_CACHED_PEERS)
            .map(|(peer, record)| CachedPeer {
                peer: **peer,
                transactions: record.transactions,
                blocks: record.blocks,
            })
            .collect();
        Some((path, peers))
    }
}

fn seconds(ms: u64) -> f64 {
    ms as f64 / 1000.0
}

/// A peer that streamed before, as stored in the peer cache, with what it
/// relayed over all runs.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CachedPeer {
    peer: SocketAddr,
    transactions: u64,
    blocks: u64,
}

/// Reads the peer cache, skipping invalid entries.
fn load_cache(path: &Path) -> Vec<CachedPeer> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            warn!(path = %path.display(), %error, "reading the peer cache failed");
            return Vec::new();
        }
    };
    match serde_json::from_slice::<Vec<serde_json::Value>>(&raw) {
        Ok(entries) => entries
            .into_iter()
            .filter_map(|entry| serde_json::from_value(entry).ok())
            .collect(),
        Err(error) => {
            warn!(path = %path.display(), %error, "ignoring an invalid peer cache");
            Vec::new()
        }
    }
}

fn write_atomically(path: &Path, peers: &[CachedPeer]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(peers)?)?;
    std::fs::rename(temporary, path)
}

/// Whether a discovered address may be a public peer.
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_documentation())
        }
        IpAddr::V6(ip) => !(ip.is_loopback() || ip.is_unspecified() || ip.is_multicast()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_blocks_are_bounded() {
        let mut recent = RecentBlocks::default();
        let proposer = Address([1; 20]);
        assert!(recent.insert((1, proposer)));
        assert!(!recent.insert((1, proposer)));
        for time in 2..=RECENT_BLOCKS as u64 + 1 {
            assert!(recent.insert((time, proposer)));
        }
        assert_eq!(recent.set.len(), RECENT_BLOCKS);
        assert!(recent.insert((1, proposer)));
    }

    #[test]
    fn filters_unroutable_discovered_addresses() {
        assert!(is_public("16.76.120.9".parse().unwrap()));
        for ip in [
            "10.0.0.1",
            "127.0.0.1",
            "192.168.1.1",
            "0.0.0.0",
            "169.254.0.1",
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn peer_cache_round_trips() {
        let path =
            std::env::temp_dir().join(format!("hl-stream-cache-{}.json", std::process::id()));
        let relay = CachedPeer {
            peer: "1.2.3.4:4001".parse().unwrap(),
            transactions: 7,
            blocks: 9,
        };
        write_atomically(&path, std::slice::from_ref(&relay)).unwrap();
        assert_eq!(load_cache(&path), [relay]);
        std::fs::write(
            &path,
            r#"[{"peer":"5.6.7.8:4001","transactions":1,"blocks":2},"garbage"]"#,
        )
        .unwrap();
        assert_eq!(load_cache(&path).len(), 1);
        std::fs::remove_file(&path).unwrap();
        assert!(load_cache(&path).is_empty());
    }
}
