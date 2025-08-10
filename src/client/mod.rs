#[allow(clippy::module_inception)]
mod client;
mod searcher;

pub use searcher::{Searcher, SearcherBuilder};

pub use client::Client;
