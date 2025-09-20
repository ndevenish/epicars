use core::panic;
use std::{
    any, io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use epicars::{
    Client, Provider, ServerBuilder, ServerHandle,
    dbr::DbrType,
    messages::{
        ClientMessage, ClientName, CreateChannel, EventAdd, EventCancel, HostName, Message,
        MonitorMask, Version,
    },
    providers::IntercomProvider,
};
use futures::{sink::SinkExt, stream};
use futures_sink::Sink;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf, split},
    net::TcpStream,
    select,
    sync::broadcast,
};
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::{info, level_filters::LevelFilter};

#[tokio::test]
async fn test_events() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();
    // Start up a simple server
    let mut provider = IntercomProvider::new();
    let mut pv = provider.add_pv("TEST", 42i16).unwrap();
    let server = ServerBuilder::new(provider)
        .connection_port(0)
        .search_port(0)
        .beacons(false)
        .start()
        .await
        .unwrap();
    info!(
        "Server ports: {} {}",
        server.connection_port(),
        server.search_port()
    );
    let mut client = Client::new_with(
        server.search_port(),
        Some(vec![
            format!("127.0.0.1:{}", server.search_port())
                .parse()
                .unwrap(),
        ]),
    )
    .await
    .unwrap();

    // Validate this works
    assert_eq!(client.read_pv("TEST").await.unwrap(), 42i16.into());
    let (mut sub, sub_token) = client.subscribe("TEST").await.unwrap();
    assert_eq!(sub.try_recv().unwrap().value(), &42i16.into());
    pv.store(&413i16);
    select! {
        _ = tokio::time::sleep(Duration::from_secs(4)) => panic!("Did not get subscription event"),
        v = sub.recv() => {
            assert_eq!(v.unwrap().value(), &413i16.into());
        }
    }

    // Now, unsubscribe
    client.unsubscribe(sub_token).await;
    // Make sure that if we try to listen again we fail
    assert_eq!(
        sub.recv().await.unwrap_err(),
        broadcast::error::RecvError::Closed
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.stop().await.unwrap();
}

/// Convenience class to hold separate write/read frame decoders
struct SplitFramed<T, D, E>
where
    T: AsyncRead + AsyncWrite,
{
    write: FramedWrite<tokio::io::WriteHalf<T>, E>,
    read: FramedRead<tokio::io::ReadHalf<T>, D>,
}
impl<T, D, E> SplitFramed<T, D, E>
where
    T: AsyncRead + AsyncWrite,
    D: Decoder,
    E: Encoder<E>,
{
    fn new(stream: T, decoder: D, encoder: E) -> SplitFramed<T, D, E> {
        let (tcp_rx, tcp_tx) = split(stream);
        SplitFramed {
            write: FramedWrite::new(tcp_tx, encoder),
            read: FramedRead::new(tcp_rx, decoder),
        }
    }
}
impl<T, D, E> Stream for SplitFramed<T, D, E>
where
    T: AsyncRead + AsyncWrite,
    FramedRead<ReadHalf<T>, D>: Stream,
{
    type Item = <FramedRead<ReadHalf<T>, D> as Stream>::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.read).poll_next(cx)
    }
}

impl<T, D, E, Message> Sink<Message> for SplitFramed<T, D, E>
where
    T: AsyncRead + AsyncWrite,
    FramedWrite<WriteHalf<T>, E>: Sink<Message>,
{
    type Error = <FramedWrite<WriteHalf<T>, E> as Sink<Message>>::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.write).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.write).poll_close(cx)
    }
}

async fn bare_test_client(port: u16) -> SplitFramed<TcpStream, ClientMessage, Message> {
    // let (tcp_rx, tcp_tx) = split(TcpStream::connect(server).await.unwrap());

    let addr: SocketAddr = ([127, 0, 0, 1], port).try_into().unwrap();
    // let mut reader = tokio_util::codec::FramedRead::new(tcp_rx, ClientMessage::default());
    SplitFramed::new(
        TcpStream::connect(addr).await.unwrap(),
        ClientMessage::default(),
        Message::default(),
    )
}

impl<T, D> SplitFramed<T, ClientMessage, D>
where
    T: AsyncRead + AsyncWrite,
    D: Encoder<Message>,
    <D as Encoder<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    async fn send_messages<I>(&mut self, messages: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = Message>,
    {
        let mut stream = stream::iter(messages.into_iter().map(Ok));
        self.send_all(&mut stream).await?;
        Ok(())
    }

    async fn do_handshake(&mut self) -> anyhow::Result<()> {
        self.send_messages([
            Version::default().into(),
            ClientName::new("testclient").into(),
            HostName::new("test-hostname").into(),
        ])
        .await
        .unwrap();

        let ClientMessage::Version(_) = self.next().await.unwrap().unwrap() else {
            panic!("Didn't get expected version packet");
        };
        Ok(())
    }
    async fn create_channel(&mut self, name: &str, client_id: u32) -> u32 {
        self.send(
            CreateChannel {
                client_id,
                channel_name: name.to_string(),
                ..Default::default()
            }
            .into(),
        )
        .await
        .unwrap();
        // Skip the access rights
        self.next().await.unwrap().unwrap();
        let ClientMessage::CreateChannelResponse(chan) = self.next().await.unwrap().unwrap() else {
            panic!("Failed to connect to channel");
        };
        chan.server_id
    }
}

async fn test_server<T>(provider: T) -> ServerHandle
where
    T: Provider,
{
    ServerBuilder::new(provider)
        .connection_port(0)
        .beacons(false)
        .start()
        .await
        .unwrap()
}

/// Test subscription cancellation, without a epicars Client
#[tokio::test]
async fn test_server_raw() {
    let mut provider = IntercomProvider::new();
    let mut pv = provider.add_pv("TEST", 42i16).unwrap();
    let server = test_server(provider).await;

    let mut client = bare_test_client(server.connection_port()).await;
    let _ = client.do_handshake().await.unwrap();
    let server_id = client.create_channel("TEST", 0).await;
    // Request subscription
    client
        .send(
            EventAdd {
                data_type: "DBR_TIME_INT".parse().unwrap(),
                data_count: 1,
                server_id,
                subscription_id: 0,
                mask: MonitorMask::default(),
            }
            .into(),
        )
        .await
        .unwrap();
    let evr = client.next().await.unwrap().unwrap();
    println!("Got EventAdd response: {evr:?}");
    pv.store(&(pv.load() + 1));
    let ClientMessage::EventAddResponse(_resp) = client.next().await.unwrap().unwrap() else {
        panic!("Failed to get stream update");
    };
    // Send a cancel
    client
        .send(
            EventCancel {
                data_type: "DBR_TIME_INT".parse().unwrap(),
                data_count: 1,
                server_id,
                subscription_id: 0,
            }
            .into(),
        )
        .await
        .unwrap();
    // Wait for the cancel response
    match client.next().await.unwrap().unwrap() {
        ClientMessage::EventCancelResponse(_) => (),
        m => panic!("Got unexpected response to cancel: {m:?}"),
    };

    // Change the PV and check we don't get a reply
    pv.store(&22i16);
    select! {
        _ = tokio::time::sleep(Duration::from_millis(100)) => (),
        m = client.next() => panic!("Got response: {m:?} after cancelling sub"),
    }
}
