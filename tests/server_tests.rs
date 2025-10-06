use core::panic;
use std::time::Duration;

use epicars::providers::IntercomProvider;
use epicars::providers::intercom::Intercom;
use epicars::utils::connected_client_server;
use tokio::select;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::TestWriter;

#[tokio::test]
async fn test_events() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_writer(TestWriter::new())
        .try_init();
    // Start up a simple server
    let mut provider = IntercomProvider::new();
    let pv = provider.add_pv("TEST", 42i16).unwrap();
    let (client, server) = connected_client_server(provider).await;

    // Validate this works
    assert_eq!(client.get::<i16>("TEST").await.unwrap(), 42i16);
    let mut sub = client.subscribe::<i16>("TEST");
    // Validate we get an initial message without changing
    assert_eq!(sub.recv().await.unwrap(), 42i16);
    pv.store(413i16);
    select! {
        _ = tokio::time::sleep(Duration::from_secs(4)) => panic!("Did not get subscription event"),
        v = sub.recv() => {
            assert_eq!(v.unwrap(), 413i16);
        }
    }

    // Used to test unsubscribe flow here... this is not possible now as
    // by reconnection design it is impossible to hold a broadcast
    // receiver that will never receive anything
    //
    // The following just ensures log messages mentioning this are printed
    drop(sub);
    pv.store(100i16);
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.stop().await.unwrap();
}

#[tokio::test]
async fn test_read_written_strings() {
    // Note: Have seen cases where see to get rogue null bytes in the PV?
    // This was to track this down, but seems to have failed. Keep here so that
    // we can look again later.
    // let _ = tracing_subscriber::fmt()
    //     .with_max_level(LevelFilter::TRACE)
    //     .with_writer(TestWriter::new())
    //     .try_init();
    let mut provider = IntercomProvider::new();
    let pv: Intercom<String> = provider.add_pv("TEST", "".to_string()).unwrap();
    let (client, _server) = connected_client_server(provider).await;
    client.put("TEST", "Atest").await.unwrap();
    assert_eq!(pv.load(), "Atest");
}
