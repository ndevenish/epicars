use std::time::Duration;

use clap::Parser;
use epicars::{ServerBuilder, providers::IntercomProvider};
use tokio::select;
use tracing::{info, level_filters::LevelFilter};

#[derive(Parser)]
struct Options {
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

    let mut provider = IntercomProvider::new();
    let mut value = provider.add_pv("NUMERIC_VALUE", 42i32).unwrap();
    let _vecvalue = provider
        .add_vec_pv("something2", vec![0, 1, 2, 4, 5], Some(10))
        .unwrap();
    let _svalue = provider
        .add_string_pv("FILENAME", "c:\\some_file.cif", Some(32))
        .unwrap();

    let mut server = ServerBuilder::new(provider).start();

    loop {
        select! {
            _ = server.join() => break,
            _ = tokio::time::sleep(Duration::from_secs(3)) => (),
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl-C: Shutting down");
                break;
            },
        };

        let v2 = value.load() + 1;
        info!("Updating value to {v2}");
        value.store(&v2);
    }
    // Wait for shutdown, unless another ctrl-c
    select! {
        _ = server.stop() => (),
        _ = tokio::signal::ctrl_c() => println!("Terminating"),
    };
}
