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
//! ## Example
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
//!     let _server = ServerBuilder::new(provider).start().await.unwrap();
//!     loop {
//!         value.store(&(value.load() + 1));
//!         println!("Value is now: {}", value.load());
//! #       break
//!         tokio::time::sleep(Duration::from_secs(3)).await;
//!     }
//! }
//! ```
//!
//! ## Current Status of crate
//!
//! What is currently present:
//! - Replying to searches, accepting connections, and serving values to clients.
//! - Supporting `camonitor` functionality to receive updates of PV values.
//! - Translating values between different data types upon request; you can `caget` an
//!   e.g. [i8] as an [i32] and this will automatically convert. You can also do the
//!   inverse, and this will work as long as the data are representable as the target
//!   type without loss.
//! - Because of the [Provider] trait, it's reasonable simple to run the server
//!   asynchronously (currently required, and using tokio) but still allow access the
//!   the current values synchronously. The built-in example providers do this.
//! - Mapping Rust [String] to CA `CHAR` arrays back and forth.
//!
//! What this doesn't do (yet):
//! - Work as a client to connect to repeaters, other CA servers, and IOCs. This will be
//!   worked on when the server portions are satisfactorily working and ergonomic
//!   "enough" to use (this was the reason this implementaiton was written).
//! - Match exact behaviours of epics-base in terms of translation, error handling, and
//!   what it allows. This requires careful study of the EPICS code, and the building of
//!   an exhaustive test suite - this has not yet become a priority, but it may just be
//!   left as "good enough" until PVAccess becomes common eough that we don't need the
//!   CA interface any more.
//! - Completely implement data types. Notably, `ENUM` types are not well handled, the
//!   `CTRL` and `GR` data categories are not handled, and some of the precise behaviour
//!   of `STR` have yet to be nailed down. These will be tackled when the use cases are
//!   better understood.
//!
//!
//! [EPICS CA protocol]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html
//! [epics-base]: https://github.com/epics-base/epics-base
//! ["DBR" types]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types

#[doc(hidden)]
pub mod client;

pub mod dbr;
pub mod messages;

pub use crate::providers::Provider;

mod server;
pub use crate::server::Server;
pub use crate::server::ServerBuilder;

pub mod providers;
