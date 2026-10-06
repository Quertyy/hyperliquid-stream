//! Topic registry and fan-out, independent of what is published.
//!
//! Clients whose subscriptions to a topic select the same kinds form one
//! group, so each client receives a record at most once, whatever its
//! subscriptions overlap. A record is serialized and framed at most once per
//! publication (once per group with [`Hub::publish_with`]), and only when a
//! group wants it.

use crate::{
    KindFilter, compression,
    outbox::{Outbox, Outgoing},
};
use metrics::{Counter, Gauge, Histogram};
use metrics_derive::Metrics;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tracing::warn;

/// WebSocket server metrics.
#[derive(Clone, Metrics)]
#[metrics(scope = "ws")]
pub(crate) struct WsMetrics {
    /// Connected WebSocket clients.
    pub(crate) connections: Gauge,
    /// Active subscriptions over all clients.
    pub(crate) subscriptions: Gauge,
    /// Notifications queued for clients.
    notifications_total: Counter,
    /// Clients disconnected for falling behind.
    pub(crate) slow_clients_total: Counter,
    /// Connections opened with zstd compression.
    pub(crate) compressed_connections_total: Counter,
    /// Published messages compressed, once per message.
    compressed_messages_total: Counter,
    /// Time to compress one published message, off the publishing thread:
    /// the extra latency of compressed connections.
    compression_duration_seconds: Histogram,
}

/// One client's subscriptions to a topic.
#[derive(Debug)]
struct Member {
    outbox: Arc<Outbox>,
    filters: Vec<KindFilter>,
}

/// Clients whose subscriptions to a topic select the same kinds.
#[derive(Debug)]
struct Group {
    /// Union of each member's subscription filters.
    filter: KindFilter,
    subscribers: Vec<Arc<Outbox>>,
}

/// The subscriptions of one topic. Each client is in exactly one group, so
/// it receives every record once, whatever its subscriptions overlap.
#[derive(Debug, Default)]
struct Topic {
    members: Vec<Member>,
    groups: Vec<Group>,
}

impl Topic {
    /// Regroups members by the union of their filters, after a change.
    fn regroup(&mut self) {
        self.members.retain(|member| !member.filters.is_empty());
        let mut groups: Vec<Group> = Vec::new();
        for member in &self.members {
            let filter = KindFilter::union(&member.filters);
            match groups.iter_mut().find(|group| group.filter == filter) {
                Some(group) => group.subscribers.push(Arc::clone(&member.outbox)),
                None => groups.push(Group {
                    filter,
                    subscribers: vec![Arc::clone(&member.outbox)],
                }),
            }
        }
        self.groups = groups;
    }
}

#[derive(Debug)]
struct Inner {
    topics: RwLock<HashMap<&'static str, Topic>>,
    /// Size of the latest message of each topic, to size the next buffer.
    sizes: HashMap<&'static str, AtomicUsize>,
    metrics: WsMetrics,
}

/// A published message: `{"channel": ..., "data": ...}`.
#[derive(Serialize)]
struct Envelope<'a, T> {
    channel: &'a str,
    data: &'a T,
}

/// Serializes `record` with its envelope into one buffer, sized from the
/// topic's previous message.
fn encode(topic: &str, record: &impl Serialize, size: &AtomicUsize) -> Option<Utf8Bytes> {
    let previous = size.load(Ordering::Relaxed);
    let mut buffer = Vec::with_capacity(previous + previous / 4 + 64);
    let envelope = Envelope {
        channel: topic,
        data: record,
    };
    if let Err(error) = serde_json::to_writer(&mut buffer, &envelope) {
        warn!(%error, topic, "serializing a record failed");
        return None;
    }
    size.store(buffer.len(), Ordering::Relaxed);
    // serde_json writes UTF-8 only; the check is a fast scan.
    String::from_utf8(buffer).ok().map(Utf8Bytes::from)
}

/// The topic named in a subscription does not exist.
#[derive(Debug)]
pub(crate) struct UnknownTopic;

/// Topic registry and fan-out. Cloning shares the same hub.
#[derive(Clone, Debug)]
pub struct Hub {
    inner: Arc<Inner>,
}

impl Hub {
    /// A hub serving exactly these topics.
    #[must_use]
    pub fn new(topics: &[&'static str]) -> Self {
        Self {
            inner: Arc::new(Inner {
                topics: RwLock::new(
                    topics
                        .iter()
                        .map(|&topic| (topic, Topic::default()))
                        .collect(),
                ),
                sizes: topics
                    .iter()
                    .map(|&topic| (topic, AtomicUsize::new(0)))
                    .collect(),
                metrics: WsMetrics::default(),
            }),
        }
    }

    pub(crate) fn metrics(&self) -> &WsMetrics {
        &self.inner.metrics
    }

    /// Topic names, sorted.
    #[must_use]
    pub fn topics(&self) -> Vec<&'static str> {
        let mut topics: Vec<_> = self.read().keys().copied().collect();
        topics.sort_unstable();
        topics
    }

    /// Whether `topic` has any subscription; check before preparing records.
    #[must_use]
    pub fn is_subscribed(&self, topic: &str) -> bool {
        self.read()
            .get(topic)
            .is_some_and(|topic| !topic.members.is_empty())
    }

    /// Sends `record` as `{"channel": topic, "data": record}` to every client
    /// with a subscription of `topic` whose filter accepts `kinds`. The
    /// record is serialized once. Returns the number of clients reached.
    pub fn publish(&self, topic: &str, kinds: &[&str], record: &impl Serialize) -> usize {
        let (delivered, dropped) = {
            let topics = self.read();
            let Some(state) = topics.get(topic) else {
                return 0;
            };
            let recipients: Vec<&Arc<Outbox>> = state
                .groups
                .iter()
                .filter(|group| group.filter.accepts(kinds))
                .flat_map(|group| &group.subscribers)
                .collect();
            if recipients.is_empty() {
                return 0;
            }
            match self.encode(topic, record) {
                Some(message) => self.deliver(&message, recipients),
                None => (0, false),
            }
        };
        self.after_publish(delivered, dropped)
    }

    /// Sends each group of subscribers of `topic` the record built for the
    /// union of their filters, if any: `record` may select, for example, the
    /// part of an event that the filter accepts. Each record is serialized
    /// once per group. Returns the number of clients reached.
    pub fn publish_with<R: Serialize>(
        &self,
        topic: &str,
        mut record: impl FnMut(&KindFilter) -> Option<R>,
    ) -> usize {
        let (mut delivered, mut dropped) = (0, false);
        {
            let topics = self.read();
            let Some(state) = topics.get(topic) else {
                return 0;
            };
            for group in &state.groups {
                let Some(message) = record(&group.filter).and_then(|r| self.encode(topic, &r))
                else {
                    continue;
                };
                let (count, slow) = self.deliver(&message, group.subscribers.iter());
                delivered += count;
                dropped |= slow;
            }
        }
        self.after_publish(delivered, dropped)
    }

    fn encode(&self, topic: &str, record: &impl Serialize) -> Option<Utf8Bytes> {
        encode(topic, record, self.inner.sizes.get(topic)?)
    }

    /// Queues `message` for `recipients`, compressed once for those that
    /// asked. Returns how many accepted it and whether one was dropped.
    fn deliver<'a>(
        &self,
        message: &Utf8Bytes,
        recipients: impl IntoIterator<Item = &'a Arc<Outbox>> + Clone,
    ) -> (usize, bool) {
        let metrics = &self.inner.metrics;
        let zstd = recipients
            .clone()
            .into_iter()
            .any(|outbox| outbox.compressed())
            .then(|| {
                metrics.compressed_messages_total.increment(1);
                compression::start(
                    message.clone(),
                    metrics.compression_duration_seconds.clone(),
                )
            });
        let (mut delivered, mut dropped) = (0, false);
        for outbox in recipients {
            let outgoing = match &zstd {
                Some(zstd) if outbox.compressed() => Outgoing::Compressing {
                    text: message.clone(),
                    zstd: zstd.clone(),
                },
                _ => Outgoing::Ready(Message::Text(message.clone())),
            };
            if outbox.push(outgoing) {
                delivered += 1;
            } else {
                dropped = true;
            }
        }
        (delivered, dropped)
    }

    fn after_publish(&self, delivered: usize, dropped: bool) -> usize {
        self.inner
            .metrics
            .notifications_total
            .increment(delivered as u64);
        if dropped {
            self.update(|_, topic| {
                topic.members.retain(|member| !member.outbox.is_closed());
                true
            });
        }
        delivered
    }

    /// Registers a subscription and queues `reply` before any message of
    /// the subscription can be sent. Returns `false` if the client already
    /// had this subscription.
    pub(crate) fn subscribe(
        &self,
        outbox: &Arc<Outbox>,
        topic: &str,
        filter: KindFilter,
        reply: Outgoing,
    ) -> Result<bool, UnknownTopic> {
        let mut topics = self.write();
        let state = topics.get_mut(topic).ok_or(UnknownTopic)?;
        outbox.push(reply);
        let added = match state
            .members
            .iter_mut()
            .find(|member| Arc::ptr_eq(&member.outbox, outbox))
        {
            Some(member) if member.filters.contains(&filter) => false,
            Some(member) => {
                member.filters.push(filter);
                true
            }
            None => {
                state.members.push(Member {
                    outbox: Arc::clone(outbox),
                    filters: vec![filter],
                });
                true
            }
        };
        if added {
            state.regroup();
            self.record_subscriptions(&topics);
        }
        Ok(added)
    }

    /// Removes a client's subscription; returns whether it existed.
    pub(crate) fn unsubscribe(
        &self,
        outbox: &Arc<Outbox>,
        topic: &str,
        filter: &KindFilter,
    ) -> bool {
        self.update(|name, state| {
            if name != topic {
                return false;
            }
            let Some(member) = state
                .members
                .iter_mut()
                .find(|member| Arc::ptr_eq(&member.outbox, outbox))
            else {
                return false;
            };
            let before = member.filters.len();
            member.filters.retain(|known| known != filter);
            member.filters.len() != before
        })
    }

    /// Removes every subscription of a client.
    pub(crate) fn remove_client(&self, outbox: &Arc<Outbox>) {
        self.update(|_, state| {
            let before = state.members.len();
            state
                .members
                .retain(|member| !Arc::ptr_eq(&member.outbox, outbox));
            state.members.len() != before
        });
    }

    /// Applies `change` to every topic and regroups those it changed;
    /// returns whether any changed.
    fn update(&self, mut change: impl FnMut(&str, &mut Topic) -> bool) -> bool {
        let mut topics = self.write();
        let mut changed = false;
        for (name, state) in topics.iter_mut() {
            if change(name, state) {
                state.regroup();
                changed = true;
            }
        }
        self.record_subscriptions(&topics);
        changed
    }

    fn record_subscriptions(&self, topics: &HashMap<&'static str, Topic>) {
        let count: usize = topics
            .values()
            .flat_map(|topic| &topic.members)
            .map(|member| member.filters.len())
            .sum();
        self.inner.metrics.subscriptions.set(count as f64);
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<&'static str, Topic>> {
        self.inner
            .topics
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<&'static str, Topic>> {
        self.inner
            .topics
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    fn client(
        hub: &Hub,
        connection: u64,
        capacity: usize,
    ) -> (Arc<Outbox>, mpsc::Receiver<Outgoing>) {
        let (queue, queued) = mpsc::channel(capacity);
        (
            Arc::new(Outbox::new(connection, queue, 1 << 20, false, hub)),
            queued,
        )
    }

    fn next(queued: &mut mpsc::Receiver<Outgoing>) -> Value {
        let Outgoing::Ready(Message::Text(text)) = queued.try_recv().unwrap() else {
            panic!("expected a text message");
        };
        serde_json::from_str(text.as_str()).unwrap()
    }

    fn orders() -> KindFilter {
        KindFilter::new(["order".to_owned()])
    }

    #[test]
    fn filters_by_kind_and_delivers_once_per_client() {
        let hub = Hub::new(&["actions", "blocks"]);
        let (all, mut all_queue) = client(&hub, 1, 16);
        let (only_orders, mut orders_queue) = client(&hub, 2, 16);
        let reply = || Outgoing::text("{}");
        assert!(
            hub.subscribe(&all, "actions", KindFilter::default(), reply())
                .unwrap()
        );
        // Overlapping subscriptions of one client: each record arrives once.
        assert!(hub.subscribe(&all, "actions", orders(), reply()).unwrap());
        assert!(!hub.subscribe(&all, "actions", orders(), reply()).unwrap());
        assert!(
            hub.subscribe(&only_orders, "actions", orders(), reply())
                .unwrap()
        );
        assert!(
            hub.subscribe(&all, "nope", KindFilter::default(), reply())
                .is_err()
        );
        for queue in [&mut all_queue, &mut orders_queue] {
            while queue.try_recv().is_ok() {}
        }

        assert!(hub.is_subscribed("actions"));
        assert!(!hub.is_subscribed("blocks"));
        assert_eq!(hub.publish("actions", &["cancel"], &json!({"n": 1})), 1);
        assert_eq!(hub.publish("actions", &["order"], &json!({"n": 2})), 2);
        assert_eq!(hub.publish("blocks", &[], &json!({"n": 3})), 0);

        assert_eq!(
            next(&mut all_queue),
            json!({"channel": "actions", "data": {"n": 1}})
        );
        assert_eq!(next(&mut all_queue)["data"]["n"], 2);
        assert!(all_queue.try_recv().is_err());
        assert_eq!(next(&mut orders_queue)["data"]["n"], 2);

        assert!(hub.unsubscribe(&only_orders, "actions", &orders()));
        assert!(!hub.unsubscribe(&only_orders, "actions", &orders()));
        hub.remove_client(&all);
        assert_eq!(hub.publish("actions", &["order"], &json!({"n": 4})), 0);
        assert!(!hub.is_subscribed("actions"));
    }

    #[test]
    fn drops_clients_that_fall_behind() {
        let hub = Hub::new(&["blocks"]);
        let (slow, _queued) = client(&hub, 1, 2);
        hub.subscribe(&slow, "blocks", KindFilter::default(), Outgoing::text("{}"))
            .unwrap();
        for n in 0..3 {
            hub.publish("blocks", &[], &json!({ "n": n }));
        }
        assert!(slow.is_closed());
        assert!(!hub.is_subscribed("blocks"));
    }

    #[tokio::test]
    async fn compresses_once_for_clients_that_asked() {
        let hub = Hub::new(&["blocks"]);
        let (queue, mut zstd_queue) = mpsc::channel(4);
        let zstd = Arc::new(Outbox::new(1, queue, 1 << 20, true, &hub));
        let (plain, mut plain_queue) = client(&hub, 2, 4);
        for outbox in [&zstd, &plain] {
            hub.subscribe(
                outbox,
                "blocks",
                KindFilter::default(),
                Outgoing::text("{}"),
            )
            .unwrap();
        }
        assert!(matches!(
            zstd_queue.try_recv(),
            Ok(Outgoing::Ready(Message::Text(_)))
        ));
        plain_queue.try_recv().unwrap();

        let record = json!({"txHashes": vec!["0xab"; 100]});
        assert_eq!(hub.publish("blocks", &[], &record), 2);
        let Message::Binary(compressed) = zstd_queue.try_recv().unwrap().message().await else {
            panic!("expected a binary message");
        };
        let json: Value =
            serde_json::from_slice(&zstd::decode_all(compressed.as_ref()).unwrap()).unwrap();
        assert_eq!(json, next(&mut plain_queue));
        assert_eq!(json["data"], record);
    }

    #[test]
    fn publishes_the_part_each_group_selects() {
        let hub = Hub::new(&["actions"]);
        let (orders, mut orders_queue) = client(&hub, 1, 16);
        let (both, mut both_queue) = client(&hub, 2, 16);
        let cancels = KindFilter::new(["cancel".to_owned()]);
        hub.subscribe(
            &orders,
            "actions",
            super::tests::orders(),
            Outgoing::text("{}"),
        )
        .unwrap();
        hub.subscribe(
            &both,
            "actions",
            super::tests::orders(),
            Outgoing::text("{}"),
        )
        .unwrap();
        hub.subscribe(&both, "actions", cancels, Outgoing::text("{}"))
            .unwrap();
        for queue in [&mut orders_queue, &mut both_queue] {
            while queue.try_recv().is_ok() {}
        }
        let event = ["order", "cancel", "noop"];
        let delivered = hub.publish_with("actions", |filter| {
            let kinds: Vec<&str> = event
                .into_iter()
                .filter(|kind| filter.accepts(&[kind]))
                .collect();
            (!kinds.is_empty()).then_some(kinds)
        });
        assert_eq!(delivered, 2);
        assert_eq!(next(&mut orders_queue)["data"], json!(["order"]));
        // Overlapping subscriptions: one message with the union.
        assert_eq!(next(&mut both_queue)["data"], json!(["order", "cancel"]));
        assert!(both_queue.try_recv().is_err());
    }
}
