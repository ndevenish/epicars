use epics::server::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    Server::new(5065).await.unwrap();
}
