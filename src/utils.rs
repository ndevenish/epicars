use num::{FromPrimitive, traits::WrappingAdd};
use pnet::datalink;
use socket2::{Domain, Protocol, Type};
use std::{
    env,
    io::{self},
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::net::UdpSocket;
use tracing::{debug, warn};

pub(crate) fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Increments a mutable reference in place, and returns the original value
pub(crate) fn wrapping_inplace_add<T: WrappingAdd + FromPrimitive + Copy>(value: &mut T) -> T {
    let id = *value;
    *value = value.wrapping_add(&T::from_u8(1).unwrap());
    id
}

/// Get the server listen port, either from environment or default 5064
pub fn get_default_server_port() -> u16 {
    env::var("EPICS_CA_SERVER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(5064u16)
        .max(5000u16)
}

/// Get the beacon broadcast port, either from environment or default 5065
pub fn get_default_beacon_port() -> u16 {
    env::var("EPICS_CA_REPEATER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(5065u16)
        .max(5000u16)
}

/// Get the target list of broadcast IPs, by reading the environment and interfaces
///
/// Hostnames are resolved if in the environment setting, so this will re-resolve
pub fn get_target_broadcast_ips(default_search_port: u16) -> Vec<SocketAddr> {
    // let interfaces = ;
    let mut ips = Vec::new();
    // Work out if we want to automatically include all local broadcast
    let use_auto_address = env::var("EPICS_CA_AUTO_ADDR_LIST")
        .map(|v| !v.eq_ignore_ascii_case("no"))
        .unwrap_or(true);
    if use_auto_address {
        ips.extend(
            datalink::interfaces()
                .into_iter()
                .filter(|i| !i.is_loopback())
                .flat_map(|i| i.ips.into_iter())
                .filter(|i| i.is_ipv4())
                .flat_map(|f| (f.broadcast(), default_search_port).to_socket_addrs())
                .flatten(),
        );
    }
    if let Ok(addr_list) = env::var("EPICS_CA_ADDR_LIST") {
        for add in addr_list.split_ascii_whitespace() {
            if add.contains(":") {
                match add.to_socket_addrs() {
                    Ok(addr) => {
                        debug!("Adding search IP: {add} => {addr:?}");
                        ips.extend(addr);
                    }
                    Err(e) => {
                        warn!("Failed to convert '{add}' to address: {e}");
                        continue;
                    }
                }
            } else {
                match (add, default_search_port).to_socket_addrs() {
                    Ok(addr) => {
                        debug!("Adding search IP: {add} => {addr:?}");
                        ips.extend(addr);
                    }
                    Err(e) => {
                        warn!("Failed to convert '{add}' to address: {e}");
                        continue;
                    }
                }
            }
        }
    }
    // The user might have explicitly requested some
    ips
}

pub fn get_default_connection_timeout() -> f32 {
    env::var("EPICS_CA_CONN_TMO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30.0f32)
        .max(0.1f32)
}

pub fn get_default_beacon_period() -> f32 {
    env::var("EPICS_CA_BEACON_PERIOD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15.0f32)
        .max(0.1f32)
}

pub fn get_default_max_search_interval() -> f32 {
    env::var("EPICS_CA_MAX_SEARCH_PERIOD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300.0f32)
        .max(60f32)
}

/// Test utilities
#[cfg(test)]
pub mod test {
    use core::panic;
    use std::{
        net::SocketAddr,
        pin::Pin,
        task::{Context, Poll},
    };

    use crate::{
        Provider, ServerBuilder, ServerHandle,
        messages::{ClientMessage, ClientName, CreateChannel, HostName, Message, Version},
    };
    use futures::{sink::SinkExt, stream};
    use futures_sink::Sink;
    use tokio::{
        io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf, split},
        net::TcpStream,
    };
    use tokio_stream::{Stream, StreamExt};
    use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

    /// Convenience class to hold separate write/read frame decoders
    pub struct SplitFramed<T, D, E>
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

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.write).poll_ready(cx)
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            Pin::new(&mut self.write).start_send(item)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.write).poll_flush(cx)
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Pin::new(&mut self.write).poll_close(cx)
        }
    }

    pub async fn bare_test_client(port: u16) -> SplitFramed<TcpStream, ClientMessage, Message> {
        // let (tcp_rx, tcp_tx) = split(TcpStream::connect(server).await.unwrap());

        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        // let mut reader = tokio_util::codec::FramedRead::new(tcp_rx, ClientMessage::default());
        SplitFramed::new(
            TcpStream::connect(addr).await.unwrap(),
            ClientMessage::default(),
            Message::default(),
        )
    }

    pub async fn test_server<T>(provider: T) -> ServerHandle
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

    impl<T, D> SplitFramed<T, ClientMessage, D>
    where
        T: AsyncRead + AsyncWrite,
        D: Encoder<Message>,
        <D as Encoder<Message>>::Error: std::error::Error + Send + Sync + 'static,
    {
        pub async fn send_messages<I>(&mut self, messages: I) -> anyhow::Result<()>
        where
            I: IntoIterator<Item = Message>,
        {
            let mut stream = stream::iter(messages.into_iter().map(Ok));
            self.send_all(&mut stream).await?;
            Ok(())
        }

        pub async fn do_handshake(&mut self) -> anyhow::Result<()> {
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
        pub async fn create_channel(&mut self, name: &str, client_id: u32) -> u32 {
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
            let ClientMessage::CreateChannelResponse(chan) = self.next().await.unwrap().unwrap()
            else {
                panic!("Failed to connect to channel");
            };
            chan.server_id
        }
    }
}
