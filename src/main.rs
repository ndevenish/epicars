use epics::server::Server;
use tokio::task::yield_now;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    Server::new(5065).await.unwrap();
    loop {
        yield_now().await;
    }
}
