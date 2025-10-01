use clap::Parser;
use epicars::{client::Client, dbr::DbrValue};

use tracing::level_filters::LevelFilter;

#[derive(Parser)]
struct Options {
    /// PV names to search for
    #[clap(required = true, id = "PV_NAME")]
    names: Vec<String>,
    /// Show debug output
    #[clap(short, action = clap::ArgAction::Count)]
    verbose: u8,
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
        .with_max_level(match opts.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            2.. => LevelFilter::TRACE,
        })
        .init();

    let mut client = Client::new().await.unwrap();
    for name in opts.names {
        println!("Fetching {name}");
        let res: Result<DbrValue, _> = client.get(&name).await;
        println!("    Result: {res:?}");
    }
}
