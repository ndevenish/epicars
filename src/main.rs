use std::time::Duration;

use epics::{provider::IntercomProvider, server::ServerBuilder};

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // Make sure panics from threads cause the whole process to terminate
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let mut provider = IntercomProvider::new();
    let mut value = provider.add_pv("something", 42i32).unwrap();

    // let provider = BasicProvider {};
    let _server = ServerBuilder::new(provider)
        .beacon_port(5065)
        .start()
        .await
        .unwrap();

    println!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let v2 = value.load() + 1;
        println!("Updating value to {v2}");
        value.store(&v2);
    }
}
