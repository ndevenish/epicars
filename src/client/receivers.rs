use std::marker::PhantomData;

use thiserror::Error;
use tokio::sync::broadcast;

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
    T: Clone + for<'a> TryFrom<&'a DbrValue>,
{
    inner: broadcast::Receiver<Dbr>,
    _phantom: PhantomData<T>,
}

impl<T: Clone> Subscription<T>
where
    T: Clone + for<'a> TryFrom<&'a DbrValue>,
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

pub struct Watcher {}
