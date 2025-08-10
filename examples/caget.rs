use clap::Parser;
use epicars::client::Client;

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

    let mut client = Client::new().await.unwrap();
    for name in opts.names {
        println!("Fetching {name}");
        let res = client.read_pv("SOMETHING").await;
        println!("    Result: {res:?}");
    }
}
