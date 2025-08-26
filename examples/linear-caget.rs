//< linear-caget: Basic manual workflow for testing existing server behaviour

use std::time::Duration;

use epicars::{
    Client,
    client::SearcherBuilder,
    dbr::{DBR_BASIC_STRING, DbrBasicType, DbrType},
    messages::{
        ClientMessage, ClientName, CreateChannel, EventAdd, EventCancel, HostName, Message,
        MonitorMask, Version,
    },
};
use tokio::{io::split, net::TcpStream, time::Instant};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;
use tracing::{info, level_filters::LevelFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();

    let channel_name = "CS-CS-MSTAT-01:SCROLLM";
    let s = SearcherBuilder::new().start().await.unwrap();
    let ioc = s.search_for(channel_name).await.unwrap();
    info!("Connecting to {ioc}");
    let (tcp_rx, mut tcp_tx) = split(TcpStream::connect(ioc).await.unwrap());
    let mut framed = FramedRead::with_capacity(tcp_rx, ClientMessage::default(), 16384usize);
    Message::write_all_messages(
        &vec![
            Version::default().into(),
            ClientName::new("testclient").into(),
            HostName::new("bl24i-sc-gh-02").into(),
            CreateChannel {
                client_id: 0,
                channel_name: channel_name.to_string(),
                ..Default::default()
            }
            .into(),
        ],
        &mut tcp_tx,
    )
    .await
    .unwrap();
    info!("{:?}", framed.next().await.unwrap());
    info!("{:?}", framed.next().await.unwrap());
    let ClientMessage::CreateChannelResponse(chan) = framed.next().await.unwrap().unwrap() else {
        panic!("Got unexpected message");
    };
    info!("{chan:?}");
    Message::write_all_messages(
        &vec![
            EventAdd {
                data_type: DBR_BASIC_STRING,
                data_count: 1,
                server_id: chan.server_id,
                subscription_id: 0,
                mask: MonitorMask::default(),
            }
            .into(),
            // EventAdd {
            //     data_type: DBR_BASIC_STRING,
            //     data_count: 1,
            //     server_id: chan.server_id,
            //     subscription_id: 0,
            //     mask: MonitorMask::default(),
            // }
            // .into(),
            EventAdd {
                data_type: DbrType {
                    basic_type: DbrBasicType::String,
                    category: epicars::dbr::DbrCategory::Time,
                },
                data_count: 1,
                server_id: chan.server_id,
                subscription_id: 0,
                mask: MonitorMask::default(),
            }
            .into(),
        ],
        &mut tcp_tx,
    )
    .await
    .unwrap();
    let after_wait = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::select! {
            message = framed.next() => {
                info!("Received: {:?}", message.unwrap().unwrap());
            },
            _ = tokio::time::sleep_until(after_wait) => break,
        }
    }
    // Send an EventCancel to see what happens
    Message::write_all_messages(
        &vec![
            EventCancel {
                data_type: DbrType {
                    basic_type: DbrBasicType::String,
                    category: epicars::dbr::DbrCategory::Time,
                },
                data_count: 1,
                server_id: chan.server_id,
                subscription_id: 0,
            }
            .into(),
        ],
        &mut tcp_tx,
    )
    .await
    .unwrap();

    while let Some(message) = framed.next().await {
        info!("Received: {:?}", message.unwrap());
    }

    //     EventAdd {
    //     data_type: DBR_BASIC_STRING,
    //     data_count: 0,
    //     server_id: todo!(),
    //     subscription_id: todo!(),
    //     mask: todo!(),
    // }
    // .into(),
}
