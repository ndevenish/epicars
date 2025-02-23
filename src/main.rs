use std::time::Duration;

use epics::server::Server;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    Server::new(5065).await.unwrap();
    println!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
}
