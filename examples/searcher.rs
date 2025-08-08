use std::env;

use epicars::client::SearcherBuilder;

use tracing::level_filters::LevelFilter;

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

    if env::args().len() == 1 {
        println!("Usage: searcher [[PV_NAME] ...]");
        return;
    }

    let searcher = SearcherBuilder::new().start().await.unwrap();

    println!("Started searcher. Requesting a PV.");
    for arg in env::args().skip(1) {
        println!("Searching for: {arg}");
        match searcher.search_for(&arg).await {
            Ok(addr) => println!("Found: {addr:?}"),
            Err(_) => println!("Could not find PV in search time"),
        }
    }
}
