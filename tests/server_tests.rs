use std::time::Duration;

use epicars::{Client, ServerBuilder, providers::IntercomProvider};
use tokio::{select, sync::broadcast};
use tracing::{info, level_filters::LevelFilter};

#[tokio::test]
async fn test_events() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();
    // Start up a simple server
    let mut provider = IntercomProvider::new();
    let mut pv = provider.add_pv("TEST", 42i16).unwrap();
    let server = ServerBuilder::new(provider)
        .connection_port(0)
        .beacons(false)
        .start()
        .await
        .unwrap();
    info!(
        "Server ports: {} {}",
        server.connection_port(),
        server.search_port()
    );
    let mut client = Client::new_with(
        server.search_port(),
        Some(vec![
            format!("127.0.0.1:{}", server.search_port())
                .parse()
                .unwrap(),
        ]),
    )
    .await
    .unwrap();

    // Validate this works
    assert_eq!(client.read_pv("TEST").await.unwrap(), 42i16.into());
    let (mut sub, sub_token) = client.subscribe("TEST").await.unwrap();
    assert_eq!(sub.try_recv().unwrap().value(), &42i16.into());
    pv.store(&413i16);
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
