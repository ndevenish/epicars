//! Channel Access server integration tests.
#![cfg(feature = "ca")]

use core::panic;
use std::time::Duration;

use epicars::providers::intercom::Intercom;
use epicars::{Client, Provider, ServerBuilder, ServerHandle, providers::IntercomProvider};
use tokio::{select, sync::broadcast};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::fmt::TestWriter;
/// Create a client and server instance, connected to each other via random port
pub async fn connected_client_server<T>(provider: T) -> (Client, ServerHandle)
where
    T: Provider,
{
    let server = ServerBuilder::new(provider)
        .connection_port(0)
        .search_port(0)
        .beacons(false)
        .start()
        .await
        .unwrap();
    info!(
        "Server ports: {} {}",
        server.connection_port(),
        server.search_port()
    );
    let client = Client::new_with(
        server.search_port(),
        Some(vec![
            format!("127.0.0.1:{}", server.search_port())
                .parse()
                .unwrap(),
        ]),
    )
    .await
    .unwrap();
    (client, server)
}

#[tokio::test]
async fn test_events() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_writer(TestWriter::new())
        .try_init();
    // Start up a simple server
    let mut provider = IntercomProvider::new();
    let pv = provider.add_pv("TEST", 42i16).unwrap();
    let (mut client, server) = connected_client_server(provider).await;

    // Validate this works
    assert_eq!(client.read_pv("TEST").await.unwrap(), 42i16.into());
    let (mut sub, sub_token) = client.subscribe("TEST").await.unwrap();
    assert_eq!(sub.try_recv().unwrap().value(), &42i16.into());
    pv.store(413i16);
    select! {
        _ = tokio::time::sleep(Duration::from_secs(4)) => panic!("Did not get subscription event"),
        v = sub.recv() => {
            assert_eq!(v.unwrap().value(), &413i16.into());
        }
    }

    // Now, unsubscribe
    client.unsubscribe(sub_token).await;
    // Make sure that if we try to listen again we fail
    assert_eq!(
        sub.recv().await.unwrap_err(),
        broadcast::error::RecvError::Closed
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.stop().await.unwrap();
}

/// Two servers sharing one provider must not steal each other's subscriptions.
///
/// Subscriber IDs used to be computed by the *server*, as `(circuit_id << 32) |
/// channel_id`, with the circuit counter restarting at 0 for each `Server`. Two servers
/// over one provider therefore both asked for key 0, and the second silently overwrote
/// the first's trigger sender - so one of the two clients below would wait forever. The
/// provider allocates the IDs now, which is the only place that can guarantee they are
/// unique. This test fails against a server-computed ID.
#[tokio::test]
async fn two_servers_over_one_provider_each_get_their_own_monitors() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_writer(TestWriter::new())
        .try_init();

    let mut provider = IntercomProvider::new();
    let pv = provider.add_pv("SHARED", 0i32).unwrap();
    // Two independent servers, both serving the same PV out of the same provider
    let (mut first_client, _first) = connected_client_server(provider.clone()).await;
    let (mut second_client, _second) = connected_client_server(provider).await;

    let (mut first_sub, _first_token) = first_client.subscribe("SHARED").await.unwrap();
    let (mut second_sub, _second_token) = second_client.subscribe("SHARED").await.unwrap();
    // Each subscription opens with the current value
    assert_eq!(first_sub.recv().await.unwrap().value(), &0i32.into());
    assert_eq!(second_sub.recv().await.unwrap().value(), &0i32.into());

    pv.store(413i32);

    for (name, sub) in [("first", &mut first_sub), ("second", &mut second_sub)] {
        select! {
            _ = tokio::time::sleep(Duration::from_secs(4)) => {
                panic!("{name} server's subscriber never got the update")
            }
            value = sub.recv() => assert_eq!(value.unwrap().value(), &413i32.into()),
        }
    }
}

#[tokio::test]
async fn test_read_written_strings() {
    // Note: Have seen cases where see to get rogue null bytes in the PV?
    // This was to track this down, but seems to have failed. Keep here so that
    // we can look again later.
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_writer(TestWriter::new())
        .try_init();
    let mut provider = IntercomProvider::new();
    let pv: Intercom<String> = provider.add_pv("TEST", "".to_string()).unwrap();
    let (mut client, _server) = connected_client_server(provider).await;
    client.write_pv("TEST", "Atest".to_string()).await.unwrap();
    assert_eq!(pv.load(), "Atest");
}
