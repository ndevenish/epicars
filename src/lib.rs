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
//! - API representation of all the message types
//!
//! [EPICS CA protocol]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html
//! [epics-base]: https://github.com/epics-base/epics-base

pub mod client;
pub mod database;
pub mod messages;
pub mod provider;
pub mod server;
