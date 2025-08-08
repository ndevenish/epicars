use std::time::Duration;

use epicars::client::SearcherBuilder;

use tracing::{info, level_filters::LevelFilter};

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // Make sure panics from threads cause the whole process to terminate
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    let searcher = SearcherBuilder::new().start().await.unwrap();

    println!("Started searcher. Requesting a PV.");
    println!(
        "Searching result: {:?}",
        searcher.search_for("SOMETHING").await
    );
}
