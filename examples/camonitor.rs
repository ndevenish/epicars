use std::{ops::Sub, time::Duration};

use clap::Parser;
use epicars::{client::Client, dbr::DbrValue};

use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};

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

    let filter = EnvFilter::default()
        .add_directive(
            match opts.verbose {
                0 => LevelFilter::INFO,
                1 => LevelFilter::DEBUG,
                2.. => LevelFilter::TRACE,
            }
            .into(),
        )
        .add_directive("tokio=off".parse().unwrap())
        .add_directive("runtime=off".parse().unwrap());
    let fmt_layer = tracing_subscriber::fmt::layer()
        // .with_target(false)
        .with_level(true)
        .with_filter(filter);
    let console_layer = console_subscriber::spawn();
    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(console_layer);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");

    let client = Client::new().await.unwrap();
    let mut monitor = client.subscribe(&opts.name, epicars::dbr::DbrCategory::Basic);

    while let Ok(reply) = monitor.recv().await {
        let Some(reply) = reply else {
            println!("Lost connection to PV for monitor {}", &opts.name);
            continue;
        };
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
