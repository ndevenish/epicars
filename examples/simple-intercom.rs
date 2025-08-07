use std::time::Duration;

use epicars::{ServerBuilder, providers::IntercomProvider};
use tracing::{info, level_filters::LevelFilter};

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // Make sure panics from threads cause the whole process to terminate
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    let mut provider = IntercomProvider::new();
    let mut value = provider.add_pv("something", 42i32).unwrap();
    let _vecvalue = provider
        .add_vec_pv("something2", vec![0, 1, 2, 4, 5], Some(10))
        .unwrap();
    let _svalue = provider
        .add_string_pv("FILENAME", "c:\\some_file.cif", Some(32))
        .unwrap();

    let _server = ServerBuilder::new(provider)
        .beacon_port(5065)
        .start()
        .await
        .unwrap();

    info!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let v2 = value.load() + 1;
        info!("Updating value to {v2}");
        value.store(&v2);
    }
}
