// #![warn(missing_docs)]

//! Rust implementation of EPICS CA protocol and basic variable access layers.
//!
//! This crate is a pure-rust implementation of the [EPICS CA protocol]. It does not
//! depend on the C-based [epics-base] project at all.
//!
//! <div class="warning">This is a very early version of this library. Interfaces or
//! structure may be changed around wildly between versions until a comfortable
//! final design is settled upon.</div>
//!
//! Whereas the full model of IOC and the EPICS database is extremely flexible and
//! expansive, that comes at a complexity and tooling cost that is not necessary for a
//! lot of smaller projects that just want to expose the ability to have a couple of
//! variables exposed via what is likely the designated control layer for the facility.
//!
//! EPICArs approaches the problem by separating:
//!
//! - Mapping and serialization/deserialization of message types, in module [messages].
//! - Representing data for transferring back and forth (["DBR" types]) via CA in module
//!   [dbr].
//! - A [Server] that manages connection lifecycles and other connection protocols.
//! - [Provider], a trait that [Server] uses to talk to values in your application.
//! - Example [providers] that provide built-in simple approaches to managing exposed
//!   variables. Included at this time are:
//!   - [`providers::IntercomProvider`]: Provides access objects to access record data
//!     as a natively mapped data type. The access objects can be cloned and passed
//!     across thread boundaries, and retain access to the same data (internally stored
//!     in an `Arc<Mutex<dbr::DbrValue>>`).
//!
//! ## Example Client
//!
//! Here is an example of reading a single PV once, and subscribing to a different one:
//!
//! ```
//! use epicars::Client;
//!
//! # use epicars::{ServerBuilder, providers::IntercomProvider};
//! #
//! #[tokio::main]
//! async fn main() {
//! #   let mut provider = IntercomProvider::new();
//! #   provider.add_pv("NUMERIC_VALUE", 42i32).unwrap();
//! #   let _server = ServerBuilder::new(provider).start().await.unwrap();
//! #
//!     let mut client = Client::new().await.unwrap();
//!
//!     // Read once-off, directly
//!     let number = client.read_pv("NUMERIC_VALUE").await.unwrap();
//!     println!("Read NUMERIC_VALUE: {number:?}");
//!
//!     // Or get a subscriber to receieve all updates
//!     let (mut reader, _) = client.subscribe("NUMERIC_VALUE").await.unwrap();
//!
//!     while let Ok(value) = reader.recv().await {
//!         println!("Got update: {:?}", value.value());
//! #       break;
//!     }
//! }
//! ```
//!
//! ## Example Server
//!
//! Here is an example of exposing a basic single [i32] via the
//! [providers::IntercomProvider]. You can run this and then `caget NUMERIC_VALUE` or
//! `caput NUMERIC_VALUE <new_value>` from anywhere inside the same broadcast network:
//!
//! ```
//! # use std::time::Duration;
//! use epicars::{ServerBuilder, providers::IntercomProvider};
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut provider = IntercomProvider::new();
//!     let mut value = provider.add_pv("NUMERIC_VALUE", 42i32).unwrap();
//!     let _server = ServerBuilder::new(provider).start();
//!     loop {
//!         value.store(value.load() + 1);
//!         println!("Value is now: {}", value.load());
//! #       break
//!         tokio::time::sleep(Duration::from_secs(3)).await;
//!     }
//! }
//! ```
//!
//! [EPICS CA protocol]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html
//! [epics-base]: https://github.com/epics-base/epics-base
//! ["DBR" types]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types

// Everything Channel Access is behind the `ca` feature, on by default. `value` and
// `utils` are shared by both protocols and are always present; `value::ca` is itself
// gated, since it is the CA adapter.
#[cfg(feature = "ca")]
pub mod client;
#[cfg(feature = "ca")]
pub use crate::client::Client;

#[cfg(feature = "ca")]
pub mod dbr;
#[cfg(feature = "ca")]
pub mod messages;
pub mod value;

#[cfg(feature = "ca")]
pub use crate::providers::Provider;

#[cfg(feature = "ca")]
mod server;
#[cfg(feature = "ca")]
pub use crate::server::Server;
#[cfg(feature = "ca")]
pub use crate::server::ServerBuilder;
#[cfg(feature = "ca")]
pub use crate::server::ServerEvent;
#[cfg(feature = "ca")]
pub use crate::server::ServerHandle;

#[cfg(feature = "ca")]
pub mod providers;

#[cfg(feature = "pva")]
pub mod pva;

pub mod utils;
