use crate::{Config, Error, Event, connection::CallbackServer, supervisor::Supervisor};
use futures_core::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// A live stream of the HyperCore mempool and consensus blocks.
///
/// Connecting starts background tasks that discover peers, keep sessions
/// open and decode what they relay. Events are buffered up to
/// [`Config::channel_capacity`]; when the buffer is full, peers are read
/// more slowly rather than events being dropped, so consume events promptly.
///
/// Dropping the stream stops every session. [`Client::shutdown`] also
/// waits until the peer cache has been saved.
#[derive(Debug)]
pub struct Client {
    events: mpsc::Receiver<Event>,
    supervisor: Option<JoinHandle<()>>,
    stop: Option<oneshot::Sender<()>>,
    callbacks: Option<JoinHandle<()>>,
}

impl Client {
    /// Binds the callback listeners and starts streaming.
    ///
    /// Must be called within a Tokio runtime.
    pub async fn connect(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let callbacks = match config.callbacks {
            Some(callbacks) => Some(tokio::spawn(CallbackServer::bind(callbacks).await?.run())),
            None => None,
        };
        let (events_tx, events) = mpsc::channel(config.channel_capacity);
        let (supervisor, stop) = Supervisor::spawn(config, events_tx);
        Ok(Self {
            events,
            supervisor: Some(supervisor),
            stop: Some(stop),
            callbacks,
        })
    }

    /// Waits for the next event. Returns `None` once the stream has stopped.
    pub async fn next_event(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// Returns the next event if one is ready, without waiting.
    pub fn try_next_event(&mut self) -> Option<Event> {
        self.events.try_recv().ok()
    }

    /// Stops streaming, and waits until the supervisor has closed every
    /// session and saved the peer cache. Short-lived discovery requests may
    /// finish shortly afterwards.
    pub async fn shutdown(mut self) {
        // Closing the receiver first unblocks a supervisor waiting for room.
        self.events.close();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.await;
        }
        if let Some(callbacks) = self.callbacks.take() {
            callbacks.abort();
        }
    }
}

impl Stream for Client {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.events.poll_recv(cx)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(callbacks) = self.callbacks.take() {
            callbacks.abort();
        }
    }
}
