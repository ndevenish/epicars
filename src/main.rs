use std::time::Duration;

use epics::server::{AddBuilderPV, ServerBuilder};

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let _server = ServerBuilder::new()
        .beacon_port(5065)
        .add_pv("something", 42)
        .start()
        .await
        .unwrap();

    println!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
}
