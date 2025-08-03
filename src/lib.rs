// #![warn(missing_docs)]

//! Rust implementation of EPICS CA protocol and basic variable access layers.
//!
//! This crate is a pure-rust implementation of the [EPICS CA protocol]. It
//! does not depend on the C-based [epics-base] project at all.
//!
//! Whereas the full model of IOC and the EPICS database is extremely flexible
//! and expansive, that comes at a complexity and tooling cost that is not
//! necessary for a lot of smaller projects that just want to expose the ability
//! to have a couple of variables exposed via what is likely the designated
//! control layer for the facility.
//!
//! EPICArs approaches the problem by separating:
//!
//! - Mapping and serialization/deserialization of message types, in module [messages].
//! - Representing data for transferring back and forth (["DBR" types]) via CA in module [dbr].
//! - A [Server] that manages connection lifecycles and other connection protocols.
//! - A trait that [Server] uses to talk to values in your application, [Provider].
//! - Example [providers] that provide built-in simple approaches to managing exposed variables.
//!
//! [EPICS CA protocol]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html
//! [epics-base]: https://github.com/epics-base/epics-base
//! ["DBR" types]: https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types
pub mod client;
pub mod dbr;
pub mod messages;

pub use crate::providers::Provider;

mod server;
pub use crate::server::Server;
pub use crate::server::ServerBuilder;

pub mod providers;
