use clap::Parser;
use epicars::client::Searcher;
use std::time::Duration;
use tracing::level_filters::LevelFilter;

#[derive(Parser)]
struct Options {
    /// PV names to search for
    #[clap(required = true, id = "PV_NAME")]
    names: Vec<String>,
    /// Show debug output
    #[clap(short)]
    verbose: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // Make sure panics from threads cause the whole process to terminate
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    let opts = Options::parse();

    tracing_subscriber::fmt()
        .with_max_level(if opts.verbose {
            LevelFilter::DEBUG
        } else {
            LevelFilter::INFO
        })
        .init();

    let searcher = Searcher::start().await.unwrap();

    println!("Started searcher. Requesting a PV.");
    for arg in opts.names {
        println!("Searching for: {arg}");
        match searcher.search_for(&arg).await {
            Some(addr) => println!("Found: {addr:?}"),
            None => println!(
                "Could not find PV in search time ({:.1} s)",
                searcher.timeout().unwrap().as_secs_f32()
            ),
        }
    }
    if opts.verbose {
        println!("Lingering to wait for unexpected extra messages...");
        std::thread::sleep(Duration::from_secs(3));
    }
}
