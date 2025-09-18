use std::time::Duration;

use epicars::{Client, ServerBuilder, providers::IntercomProvider};
use tokio::select;

#[tokio::test]
async fn test_events() {
    // Start up a simple server
    let mut provider = IntercomProvider::new();
    let mut pv = provider.add_pv("TEST", 42i16).unwrap();
    let server = ServerBuilder::new(provider)
        .connection_port(0)
        .beacons(false)
        .start()
        .await
        .unwrap();
    println!(
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
    let mut sub = client.subscribe("TEST").await.unwrap();
    assert_eq!(sub.try_recv().unwrap().value(), &42i16.into());
    pv.store(&413i16);
    select! {
        _ = tokio::time::sleep(Duration::from_secs(4)) => panic!("Did not get subscription event"),
        v = sub.recv() => {
            assert_eq!(v.unwrap().value(), &413i16.into());
        }
    }

    // Now, unsubscribe
}
