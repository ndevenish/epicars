use std::time::Duration;

use epics::{
    database::{Dbr, NumericDBR, SingleOrVec},
    provider::Provider,
    server::{AddBuilderPV, ServerBuilder},
};

struct BasicProvider;

impl Provider for BasicProvider {
    fn get_value(
        &self,
        pv_name: String,
        _requested_type: epics::database::DBRType,
    ) -> Result<epics::database::Dbr, ()> {
        println!("Provider got asked for value of {pv_name}");
        if pv_name == "something" {
            Ok(Dbr::Long(NumericDBR {
                value: SingleOrVec::Single(42),
                ..Default::default()
            }))
        } else {
            Err(())
        }
    }
    fn provides(&self, pv_name: String) -> bool {
        println!("Provider got asked if has {pv_name}\n");
        pv_name == "something"
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let _server = ServerBuilder::new()
        .beacon_port(5065)
        // .add_pv("something", 42)
        .add_provider(Box::new(BasicProvider {}))
        .start()
        .await
        .unwrap();

    println!("Entering main() infinite loop");
    loop {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
}
