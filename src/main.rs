use std::time::{Duration, SystemTime};

use epics::{
    database::{DBRType, Dbr, DbrValue, Status},
    messages::{ErrorCondition, MonitorMask},
    provider::Provider,
    server::ServerBuilder,
};
use tokio::sync::broadcast;

#[derive(Clone)]
struct BasicProvider;

impl Provider for BasicProvider {
    fn read_value(
        &self,
        pv_name: &str,
        _requested_type: Option<epics::database::DBRType>,
    ) -> Result<Dbr, ErrorCondition> {
        println!("Provider got asked for value of '{pv_name}'");
        if pv_name == "something" {
            Ok(Dbr::Basic(epics::database::DbrValue::Long(vec![42])))
        } else if pv_name == "something2" {
            Ok(Dbr::Basic(vec![42, 56].into()))
        } else {
            Err(ErrorCondition::GetFail)
        }
    }

    // }
    fn provides(&self, pv_name: &str) -> bool {
        //        println!("Provider got asked if has \"{pv_name}\"");
        pv_name == "something" || pv_name == "something2"
    }

    fn get_access_right(
        &self,
        _pv_name: &str,
        _client_user_name: Option<&str>,
        _client_host_name: Option<&str>,
    ) -> epics::messages::AccessRight {
        epics::messages::AccessRight::ReadWrite
    }

    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        println!("BasicProvider: Got Write '{pv_name}' request with: {value:?}");
        Err(ErrorCondition::PutFail)
    }

    fn monitor_value(
        &mut self,
        _pv_name: &str,
        _data_type: DBRType,
        _data_count: usize,
        _mask: MonitorMask,
        trigger: tokio::sync::mpsc::Sender<String>,
    ) -> Result<tokio::sync::broadcast::Receiver<Dbr>, ErrorCondition> {
        let (sender, recv) = broadcast::channel::<Dbr>(1);
        sender
            .send(Dbr::Time {
                status: Status::default(),
                timestamp: SystemTime::now(),
                value: DbrValue::Long(vec![42]),
            })
            .unwrap();

        tokio::spawn(async move {
            let mut val = 0i32;
            let sender = sender;
            let trigger = trigger;
            trigger.send("something".to_string()).await.unwrap();

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                println!("Sending monitor update instance");
                sender
                    .send(Dbr::Time {
                        status: Status::default(),
                        timestamp: SystemTime::now(),
                        value: DbrValue::Long(vec![42 + val]),
                    })
                    .unwrap();
                trigger.send("something".to_string()).await.unwrap();
                val += 1;
            }
        });

        Ok(recv)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // Make sure panics from threads cause the whole process to terminate
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

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
