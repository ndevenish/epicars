use std::marker::PhantomData;

use thiserror::Error;
use tokio::sync::{broadcast, watch};

use crate::dbr::{Dbr, DbrValue};

/// Custom error type for `MyReceiver::recv`
#[derive(Debug, Error)]
pub enum SubscriberRecvError {
    #[error("Channel closed")]
    Closed,
    #[error("Receiver lagged by {0} messages")]
    Lagged(u64),
    #[error("Could not convert Dbr to type")]
    NoConvert(Box<Dbr>),
}

impl From<broadcast::error::RecvError> for SubscriberRecvError {
    fn from(err: broadcast::error::RecvError) -> Self {
        match err {
            broadcast::error::RecvError::Closed => Self::Closed,
            broadcast::error::RecvError::Lagged(n) => Self::Lagged(n),
        }
    }
}

/// Custom error type for `MyReceiver::try_recv`
#[derive(Debug, Error)]
pub enum SubscriberTryRecvError {
    #[error("No messages outstanding")]
    Empty,
    #[error("Channel closed")]
    Closed,
    #[error("Receiver lagged by {0} messages")]
    Lagged(u64),
    #[error("Could not convert Dbr to type")]
    NoConvert(Box<Dbr>),
}

impl From<broadcast::error::TryRecvError> for SubscriberTryRecvError {
    fn from(err: broadcast::error::TryRecvError) -> Self {
        match err {
            broadcast::error::TryRecvError::Empty => Self::Empty,
            broadcast::error::TryRecvError::Closed => Self::Closed,
            broadcast::error::TryRecvError::Lagged(n) => Self::Lagged(n),
        }
    }
}
/// A wrapper around `tokio::sync::broadcast::Receiver` that delegates all methods
/// to the internal instance.
pub struct Subscription<T>
where
    T: for<'a> TryFrom<&'a DbrValue>,
{
    inner: broadcast::Receiver<Dbr>,
    _phantom: PhantomData<T>,
}

impl<T> Subscription<T>
where
    T: for<'a> TryFrom<&'a DbrValue>,
{
    pub fn new(inner: broadcast::Receiver<Dbr>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Blocking receive for synchronous contexts (maps error).
    pub fn blocking_recv(&mut self) -> Result<T, SubscriberRecvError> {
        let dbr = self.inner.blocking_recv()?;
        match dbr.value().try_into().ok() {
            Some(v) => Ok(v),
            None => Err(SubscriberRecvError::NoConvert(Box::new(dbr))),
        }
    }

    /// Receive the next value from the channel.
    pub async fn recv(&mut self) -> Result<T, SubscriberRecvError> {
        let dbr = self.inner.recv().await?;
        match dbr.value().try_into().ok() {
            Some(v) => Ok(v),
            None => Err(SubscriberRecvError::NoConvert(Box::new(dbr))),
        }
    }

    /// Try to receive a value without waiting.
    pub fn try_recv(&mut self) -> Result<T, SubscriberTryRecvError> {
        let dbr = self.inner.try_recv()?;
        match dbr.value().try_into().ok() {
            Some(v) => Ok(v),
            None => Err(SubscriberTryRecvError::NoConvert(Box::new(dbr))),
        }
    }

    /// True if the channel has been closed (all senders dropped).
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns `true` if the channel is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    /// Returns the number of messages currently in the channel.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Clones the receiver.
    pub fn resubscribe(&self) -> Self {
        Self {
            inner: self.inner.resubscribe(),
            _phantom: PhantomData,
        }
    }

    /// Returns true if two receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }

    /// Number of strong sender handles.
    pub fn sender_strong_count(&self) -> usize {
        self.inner.sender_strong_count()
    }

    /// Number of weak sender handles.
    pub fn sender_weak_count(&self) -> usize {
        self.inner.sender_weak_count()
    }
}

pub struct Watcher<T>
where
    T: Clone + for<'a> TryFrom<&'a DbrValue>,
{
    watcher: watch::Receiver<Option<Dbr>>,
    _phantom: PhantomData<T>,
}

#[derive(Error, Debug)]
pub enum WatcherError {
    #[error("Could not convert Dbr to type")]
    NoConvert(Box<Dbr>),
    #[error("The channel is uninitialised: Has not received it's initial value yet")]
    Uninitialised,
}

impl<T> Watcher<T>
where
    T: Clone + for<'a> TryFrom<&'a DbrValue>,
{
    pub fn new(inner: watch::Receiver<Option<Dbr>>) -> Self {
        Self {
            watcher: inner,
            _phantom: PhantomData,
        }
    }
    /// Fetch the most recently sent value, without marking it as seen.
    ///
    /// Note: This does not strictly borrow, unlike the [`watch::Receiver`]
    /// that it wraps. The naming is kept in an attempt to reduce API
    /// confusion.
    ///
    /// If the value cannot be converted into the chosen data type, then a
    /// [`WatcherError::NoConvert`] error is returned. If the watcher has not
    /// yet received it's initial value, then [`WatcherError::Uninitialised`]
    /// is returned.
    ///
    /// For details, see [`watch::Receiver::borrow`].
    pub fn borrow(&self) -> Result<T, WatcherError> {
        TryInto::<T>::try_into(
            self.watcher
                .borrow()
                .as_ref()
                .ok_or(WatcherError::Uninitialised)?
                .value(),
        )
        .map_err(|_| {
            WatcherError::NoConvert(Box::new(self.watcher.borrow().as_ref().unwrap().clone()))
        })
    }

    /// Fetch the most recently sent value, and mark the value as seen.
    ///
    /// Much like [`Watcher::borrow`], this also does not strictly borrow,
    /// beyond the internal conversion.
    ///
    /// If the value cannot be converted into the chosen data type, then a
    /// [`WatcherError::NoConvert`] error is returned. If the watcher has not
    /// yet received it's initial value, then [`WatcherError::Uninitialised`]
    /// is returned.
    ///
    /// For details, see [`watch::Receiver::borrow_and_update`].
    pub fn borrow_and_update(&mut self) -> Result<T, WatcherError> {
        let dbr = self.watcher.borrow_and_update();
        if dbr.is_none() {
            return Err(WatcherError::Uninitialised);
        }
        TryInto::<T>::try_into(dbr.clone().unwrap().value())
            .ok()
            .ok_or(WatcherError::NoConvert(Box::new(
                dbr.clone().unwrap().clone(),
            )))
    }

    /// Wait for a change notification, then mark the newest value as seen.
    ///
    /// This returns an error if and only if the [`watch::Sender`] is dropped.
    ///
    /// For details, see [`watch::Receiver::changed`].
    ///
    /// ## Cancel Safety
    ///
    /// This method is cancel safe.
    ///
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.watcher.changed().await
    }

    /// Check if the channel contains an unobserved message.
    ///
    /// Note that this will mark the message as changed, even if it compares
    /// equal.
    ///
    /// An error will be returned if the Sender has been dropped. and thus the
    /// channel is closed.
    ///
    /// For details, see [`watch::Receiver::has_changed`].
    pub fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.watcher.has_changed()
    }

    /// Mark the state has changed.
    ///
    /// This is useful for triggering an initial change notification after subscribing to synchronize new receivers.
    ///
    /// For details, see [`watch::Receiver::mark_changed`].
    pub fn mark_changed(&mut self) {
        self.watcher.mark_changed();
    }

    /// Mark the state as unchanged.
    ///
    /// This is useful if you are not interested in the current value visible in the receiver.
    ///
    /// For details, see [`watch::Receiver::mark_unchanged`].
    pub fn mark_unchanged(&mut self) {
        self.watcher.mark_unchanged();
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.watcher.same_channel(&other.watcher)
    }

    /// Wait for a value that satisfies the provided condition.
    ///
    /// If the condition is fulfilled already, then this will return
    /// immediately without awaiting. Once the closure returns `true`, the
    /// current value will be immediately returned.
    ///
    /// Inner values that would normally cause a `NoConvert` error because
    /// the type cannot be converted, will not raise an error for this
    /// method. Instead, these conditions will count as "false" for purposes
    /// of matching.
    ///
    /// If the channel is closed, then [`WatcherRecvError::Closed`] will be returned.
    ///
    /// For details, see [`watch::Receiver::wait_for`].
    ///
    /// ## Cancel safety
    ///
    /// This method is cancel safe.
    ///
    pub async fn wait_for(
        &mut self,
        mut f: impl FnMut(&T) -> bool,
    ) -> Result<T, watch::error::RecvError> {
        self.watcher
            .wait_for(|d| {
                let Some(dbr) = d else {
                    return false;
                };
                TryInto::<T>::try_into(dbr.value())
                    .map(|v| f(&v))
                    .unwrap_or(false)
            })
            .await
            // Note: Unwrap fine here as we already tried the conversion inside
            // the predicate and we only passed the event through if it converted
            .map(|v| {
                TryInto::<T>::try_into(v.clone().unwrap().value())
                    .ok()
                    .unwrap()
            })
    }
}

impl<T> Clone for Watcher<T>
where
    T: Clone + for<'a> TryFrom<&'a DbrValue>,
{
    fn clone(&self) -> Self {
        Self {
            watcher: self.watcher.clone(),
            _phantom: self._phantom,
        }
    }
}
