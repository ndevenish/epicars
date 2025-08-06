# EPICArs: EPICS-CA Implementation in Pure Rust

[![Crates.io](https://img.shields.io/crates/v/epicars.svg)](https://crates.io/crates/epicars)
[![Docs.rs](https://docs.rs/epicars/badge.svg)](https://docs.rs/epicars)
[![CI](https://github.com/ndevenish/epicars/actions/workflows/rust.yml/badge.svg)](https://github.com/ndevenish/epics_cars/actions)
[![License](https://img.shields.io/crates/l/epicars)](https://crates.io/crates/epicars)

Rust implementation of EPICS CA protocol and basic variable access layers.

This crate is a pure-rust implementation of the [EPICS CA protocol]. It does not
depend on the C-based [epics-base] project at all.

> [!WARNING]
> This is a very early version of this library. Interfaces or structure may be
> changed around wildly between versions until a comfortable final design is
> settled upon.

Whereas the full model of IOC and the EPICS database is extremely flexible and
expansive, that comes at a complexity and tooling cost that is not necessary for a
lot of smaller projects that just want to expose the ability to have a couple of
variables exposed via what is likely the designated control layer for the facility.

EPICArs approaches the problem by separating:

- Mapping and serialization/deserialization of message types
- Data transfer Representation ("DBR" types)
- A Server that manages connection lifecycles and protocols
- `Provider` - a trait that the Server uses to talk to values in your application.

Basic example (but fully functional) providers are included:

- `IntercomProvider`: Provides access objects to access record
  data as a natively mapped data type. The access objects can be cloned and
  passed across thread boundaries, and retain access to the same data.

### Example

Here is an example of exposing a basic single `i32` via the `IntercomProvider`.
You can run this and then `caget NUMERIC_VALUE` or `caput NUMERIC_VALUE <new_value>`
from anywhere inside the same broadcast network:

```rust
use epicars::{ServerBuilder, providers::IntercomProvider};

#[tokio::main]
async fn main() {
    let mut provider = IntercomProvider::new();
    let mut value = provider.add_pv("NUMERIC_VALUE", 42i32).unwrap();
    let _server = ServerBuilder::new(provider).start().await.unwrap();
    loop {
        value.store(&(value.load() + 1));
        println!("Value is now: {}", value.load());
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
```

There are more examples in the `example/` folder of the repository.

### Current Status of crate

What is currently present:
- Replying to searches, accepting connections, and serving values to clients.
- Supporting `camonitor` functionality to receive updates of PV values.
- Translating values between different data types upon request; you can `caget` an
  e.g. `i8` as an `i32` and this will automatically convert. You can also do the
  inverse, and this will work as long as the data are representable as the target
  type without loss.
- Because of the `Provider` trait, it's reasonable simple to run the server
  asynchronously (currently required, and using tokio) but still allow access the
  the current values synchronously. The built-in example providers do this.
- Mapping Rust `String` to CA `CHAR` arrays back and forth.

What this doesn't do (yet):
- Work as a client to connect to repeaters, other CA servers, and IOCs. This will be
  worked on when the server portions are satisfactorily working and ergonomic
  "enough" to use (this was the reason this implementaiton was written).
- Match exact behaviours of epics-base in terms of translation, error handling, and
  what it allows. This requires careful study of the EPICS code, and the building of
  an exhaustive test suite - this has not yet become a priority, but it may just be
  left as "good enough" until PVAccess becomes common eough that we don't need the
  CA interface any more.
- Completely implement data types. Notably, `ENUM` types are not well handled, the
  `CTRL` and `GR` data categories are not handled, and some of the precise behaviour
  of `STR` have yet to be nailed down. These will be tackled when the use cases are
  better understood.
- Work at all without tokio. There are ambition to make this flexible over
  async runtime, specifically [Embassy] for embedded usage, but the only evidence
  of this so far is lightly in some of the design decisions.


[EPICS CA protocol]:
    https://docs.epics-controls.org/en/latest/internal/ca_protocol.html
[epics-base]: https://github.com/epics-base/epics-base
["DBR" types]:
    https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types
[Embassy]: https://embassy.dev/