use epics::server::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let server = Server::new(5070);
    server.listen().await;
}
