#![warn(clippy::indexing_slicing)]

use std::{collections::HashMap, time::Duration};

use clap::Parser;
use colored::Colorize;
use epicars::{ServerBuilder, ServerEvent, providers::IntercomProvider};
use time::macros::format_description;
use tokio::{select, sync::broadcast::error::RecvError};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::fmt::time::LocalTime;

#[derive(Parser)]
struct Options {
    /// Show debug output
    #[clap(short, action = clap::ArgAction::Count)]
    verbose: u8,
}

async fn watch_lifecycle(mut recv: tokio::sync::broadcast::Receiver<ServerEvent>) {
    let mut peers = HashMap::new();
    let mut client_id: HashMap<u64, String> = HashMap::new();
    let mut channel_names = HashMap::new();
    loop {
        let event = match recv.recv().await {
            Ok(event) => event,
            Err(RecvError::Closed) => break,
            Err(RecvError::Lagged(n)) => {
                info!("Warning: Lagged behind in server event listening (missed {n} events)");
                continue;
            }
        };

        match event {
            ServerEvent::CircuitOpened { id, peer } => {
                peers.insert(id, peer);
            }
            ServerEvent::CircuitClose { id } => {
                // let addr = peers[&id];
                let user_str = &client_id[&id];
                info!("Client {user_str} disconnected");
                peers.remove(&id);
                client_id.remove(&id);
                channel_names.remove(&id);
            }
            ServerEvent::ClientIdentified {
                circuit_id,
                client_hostname,
                client_username,
            } => {
                let addr = peers[&circuit_id];
                let user_str = format!("{}@{}", client_username.magenta(), client_hostname.blue());
                info!(
                    "Client {user_str} connected from {}",
                    addr.to_string().blue()
                );
                client_id.insert(circuit_id, user_str);
                channel_names.insert(circuit_id, HashMap::new());
            }
            ServerEvent::CreateChannel {
                circuit_id,
                channel_id,
                channel_name,
            } => {
                channel_names
                    .get_mut(&circuit_id)
                    .unwrap()
                    .insert(channel_id, channel_name);
            }
            ServerEvent::ClearChannel {
                circuit_id,
                channel_id,
            } => {
                // channel_names.remove(&(circuit_id, channel_id));
                channel_names
                    .get_mut(&circuit_id)
                    .unwrap()
                    .remove(&channel_id);
            }
            ServerEvent::Read {
                circuit_id,
                channel_id,
                success,
            } => {
                let user_str = &client_id[&circuit_id];
                let channel_name = &channel_names[&circuit_id][&channel_id];
                info!(
                    "PV: {name}: {:5} by {user_str}",
                    if success {
                        "Read".green()
                    } else {
                        "Read (failed) ".red()
                    },
                    name = channel_name.bold(),
                );
            }
            ServerEvent::Write {
                circuit_id,
                channel_id,
                success,
            } => {
                let user_str = &client_id[&circuit_id];
                let channel_name = &channel_names[&circuit_id][&channel_id];
                info!(
                    "PV: {}: {:5} by {user_str}",
                    channel_name.bold(),
                    if success {
                        "Write".yellow()
                    } else {
                        "Write (failed)".red()
                    },
                );
            }
            ServerEvent::Subscribe {
                circuit_id,
                channel_id,
            } => {
                let user_str = &client_id[&circuit_id];
                let channel_name = &channel_names[&circuit_id][&channel_id];
                info!("{user_str} subscribed to {}", channel_name.bold());
            }
        }
    }
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
        .with_ansi(false)
        .with_timer(LocalTime::new(format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"
        )))
        .with_target(opts.verbose != 0)
        .with_level(opts.verbose != 0)
        .init();

    let mut provider = IntercomProvider::new();
    let value = provider.add_pv("NUMERIC_VALUE", 42i32).unwrap();
    let _vecvalue = provider
        .add_pv("something2", vec![0i16, 1, 2, 4, 5])
        .unwrap();
    let _svalue = provider
        .build_pv("FILENAME", "c:\\some_file.cif".to_string())
        .minimum_length(128)
        .build()
        .unwrap();

    let mut server = ServerBuilder::new(provider).start().await.unwrap();

    // Set up output so we print info about events
    let listen = server.listen_to_events();
    tokio::spawn(async move {
        watch_lifecycle(listen).await;
    });

    loop {
        select! {
            _ = server.join() => break,
            _ = tokio::time::sleep(Duration::from_secs(3)) => (),
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C: Shutting down");
                break;
            },
        };

        let v2 = value.load() + 1;
        info!("Updating value to {v2}");
        value.store(v2);
    }
    // Wait for shutdown, unless another ctrl-c
    select! {
        _ = server.stop() => (),
        _ = tokio::signal::ctrl_c() => info!("Terminating"),
    };
}
