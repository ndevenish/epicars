use std::time::Duration;

use clap::Parser;
use epicars::{client::Client, dbr::DbrValue};

use tracing::{info, level_filters::LevelFilter};

#[derive(Parser)]
struct Options {
    /// PV names to search for
    #[clap(required = true, id = "PV_NAME")]
    name: String,
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
    let (mut monitor, _) = client.subscribe(&opts.name).await.unwrap();
    while let Ok(reply) = monitor.recv().await {
        let display = match reply.value() {
            DbrValue::String(s) => s.join(" "),
            DbrValue::Int(v) => v
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<String>>()
                .join(" "),
            DbrValue::Char(v) => v
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<String>>()
                .join(" "),
            DbrValue::Long(v) => v
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<String>>()
                .join(" "),
            DbrValue::Double(v) => v
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<String>>()
                .join(" "),
            DbrValue::Float(v) => v
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<String>>()
                .join(" "),
            DbrValue::Enum(x) => format!("{x}"),
        };
        println!("{} {}", opts.name, display);
    }
    drop(monitor);
    tokio::time::sleep(Duration::from_secs(5)).await;
    info!("Disconnected.");
}
