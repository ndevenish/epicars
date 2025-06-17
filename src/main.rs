use std::time::Duration;

use epics::{
    database::{Dbr, NumericDBR, SingleOrVec},
    provider::Provider,
    server::ServerBuilder,
};

#[derive(Clone)]
struct BasicProvider;

impl Provider for BasicProvider {
    fn get_value(
        &self,
        pv_name: &str,
        _requested_type: Option<epics::database::DBRType>,
    ) -> Option<epics::database::Dbr> {
        println!("Provider got asked for value of {pv_name}");
        if pv_name == "something" {
            Some(Dbr::Long(NumericDBR {
                value: SingleOrVec::Single(42),
                ..Default::default()
            }))
        } else {
            None
        }
    }
    fn provides(&self, pv_name: &str) -> bool {
        println!("Provider got asked if has \"{pv_name}\"\n");
        pv_name == "something"
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let provider = BasicProvider {};
    let _server = ServerBuilder::new(provider)
        .beacon_port(5065)
        .start()
        .await
        .unwrap();

    println!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
}
