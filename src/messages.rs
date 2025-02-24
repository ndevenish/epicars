#![allow(dead_code)]

use std::{
    io::{self, Cursor, Write},
    net::Ipv4Addr,
};

use nom::{
    bytes::complete::take,
    combinator::all_consuming,
    error::{Error, ErrorKind},
    multi::many0,
    number::complete::{be_u16, be_u32},
    Err, Finish, IResult, Parser,
};
use thiserror::Error;
use tokio::{io::AsyncReadExt, net::TcpStream};

const EPICS_VERSION: u16 = 13;

/// A basic trait to tie nom parseability to the struct without a
/// plethora of named functions.
/// Also adds common interface for writing a message struct to a writer.
pub trait CAMessage: TryFrom<RawMessage> {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized;

    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()>;
}

#[derive(Default)]
pub struct RawMessage {
    command: u16,
    field_1_data_type: u16,
    field_2_data_count: u32,
    field_3_parameter_1: u32,
    #[allow(dead_code)]
    field_4_parameter_2: u32,
    payload: Vec<u8>,
}

impl RawMessage {
    /// Parse an entire message, but check that it matches the expected tag
    fn parse_id(command_id: u16, input: &[u8]) -> IResult<&[u8], RawMessage> {
        let (input, result) = RawMessage::parse(input)?;
        if result.command != command_id {
            return Err(Err::Error(Error::new(input, ErrorKind::Tag)));
        }
        Ok((input, result))
    }

    async fn read(source: &mut TcpStream) -> Result<RawMessage, MessageError> {
        let mut data = vec![0u8; 16];
        source.read_exact(data.as_mut_slice()).await?;
        let (input, (command, payload_size, field_1)) = (
            be_u16::<&[u8], nom::error::Error<&[u8]>>,
            be_u16,
            be_u16::<&[u8], nom::error::Error<&[u8]>>,
        )
            .parse(data.as_slice())?;

        // Handle packets that could be large
        if payload_size == 0xFFFF {
            let (_, (_, field_3, field_4, payload_size, field_2)) = (
                take::<usize, &[u8], nom::error::Error<&[u8]>>(2usize),
                be_u32,
                be_u32,
                be_u32,
                be_u32,
            )
                .parse(input)?;
            let mut payload = vec![0u8; payload_size as usize];
            source.read_exact(&mut payload).await?;
            Ok(RawMessage {
                command,
                field_1_data_type: field_1,
                field_2_data_count: field_2,
                field_3_parameter_1: field_3,
                field_4_parameter_2: field_4,
                payload,
            })
        } else {
            let (_, (field_2, field_3, field_4)) =
                (be_u16::<&[u8], nom::error::Error<&[u8]>>, be_u32, be_u32).parse(input)?;
            let mut payload = vec![0u8; payload_size as usize];
            source.read_exact(&mut payload).await?;
            Ok(RawMessage {
                command,
                field_1_data_type: field_1,
                field_2_data_count: field_2 as u32,
                field_3_parameter_1: field_3,
                field_4_parameter_2: field_4,
                payload,
            })
        }
    }
    fn payload_as_string(&self) -> String {
        let input = self.payload.as_slice();
        padded_string(input.len())(input).unwrap().1
    }
    fn payload_size(&self) -> usize {
        self.payload.len()
    }
    fn expect_id(&self, id: u16) -> Result<(), MessageError> {
        match self.command {
            16 => Ok(()),
            _ => Err(MessageError::IncorrectCommandId(id)),
        }
    }
}

impl CAMessage for RawMessage {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Ensure that the payload is padded out to 8 byte multiple -
        // the protocol requires this.
        let payload_size = self.payload.len().div_ceil(8);

        writer.write_all(&self.command.to_be_bytes())?;
        if payload_size < 0xFFFF && self.field_2_data_count <= 0xFFFF {
            writer.write_all(&(payload_size as u16).to_be_bytes())?;
            writer.write_all(&self.field_1_data_type.to_be_bytes())?;
            writer.write_all(&(self.field_2_data_count as u16).to_be_bytes())?;
            writer.write_all(&self.field_3_parameter_1.to_be_bytes())?;
            writer.write_all(&self.field_4_parameter_2.to_be_bytes())?;
        } else {
            writer.write_all(&0xFFFFu32.to_be_bytes())?;
            writer.write_all(&self.field_1_data_type.to_be_bytes())?;
            writer.write_all(&[0x0000])?;
            writer.write_all(&self.field_3_parameter_1.to_be_bytes())?;
            writer.write_all(&self.field_4_parameter_2.to_be_bytes())?;
            writer.write_all(&payload_size.to_be_bytes())?;
            writer.write_all(&self.field_2_data_count.to_be_bytes())?;
        }
        writer.write_all(&self.payload)?;
        let extra_bytes = payload_size - self.payload.len();
        if extra_bytes > 0 {
            writer.write_all(&vec![0; extra_bytes])?;
        }

        Ok(())
    }
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, command) = be_u16(input)?;
        let (input, payload_size) = be_u16(input)?;
        // "Data Type" is always here, even in large packet headers
        let (input, field_1) = be_u16(input)?;

        // Handle packets that could be large
        if payload_size == 0xFFFF {
            let (input, _) = take(2usize)(input)?;
            let (input, field_3) = be_u32(input)?;
            let (input, field_4) = be_u32(input)?;
            let (input, payload_size) = be_u32(input)?;
            let (input, field_2) = be_u32(input)?;
            let (input, payload) = take(payload_size)(input)?;

            Ok((
                input,
                RawMessage {
                    command,
                    field_1_data_type: field_1,
                    field_2_data_count: field_2,
                    field_3_parameter_1: field_3,
                    field_4_parameter_2: field_4,
                    payload: payload.to_vec(),
                },
            ))
        } else {
            let (input, field_2) = be_u16(input)?;
            let (input, field_3) = be_u32(input)?;
            let (input, field_4) = be_u32(input)?;
            let (input, payload) = take(payload_size)(input)?;
            Ok((
                input,
                RawMessage {
                    command,
                    field_1_data_type: field_1,
                    field_2_data_count: field_2 as u32,
                    field_3_parameter_1: field_3,
                    field_4_parameter_2: field_4,
                    payload: payload.to_vec(),
                },
            ))
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Version(Version),
    RsrvIsUp(RsrvIsUp),
    Search(Search),
    SearchResponse(SearchResponse),
    CreateChannel(CreateChannel),
    CreateChannelResponse(CreateChannelResponse),
    AccessRights(AccessRights),
    ClientName(ClientName),
    HostName(HostName),
    ServerDisconnect(ServerDisconnect),
    Echo,
}

#[derive(Error, Debug)]
pub enum MessageError {
    #[error("IO Error Occured")]
    IO(#[from] io::Error),
    #[error("An error occured parsing a message")]
    ParsingError(#[from] nom::Err<nom::error::Error<Vec<u8>>>),
    #[error("Unknown command ID: {0}")]
    UnknownCommandId(u16),
    #[error("Got a valid message but is not valid at this state: {0:?}")]
    UnexpectedMessage(Message),
    #[error("Message command ID does not match expected")]
    IncorrectCommandId(u16),
    #[error("Invalid message field: {0} == {1}")]
    InvalidField(String, String),
}

impl From<nom::Err<nom::error::Error<&[u8]>>> for MessageError {
    fn from(err: nom::Err<nom::error::Error<&[u8]>>) -> Self {
        MessageError::ParsingError(err.to_owned())
    }
}

impl Message {
    /// Parse message sent to the server, directly from a TCP stream
    ///
    /// Handles any message that could be sent to the server, not
    /// messages that could be sent to a client. This is because some
    /// response messages have the same command ID but different fields,
    /// so it is impossible to tell which is which purely from the
    /// contents of the message.
    pub async fn read_server_message(source: &mut TcpStream) -> Result<Self, MessageError> {
        let message = RawMessage::read(source).await?;

        Ok(match message.command {
            0 => Self::Version(message.try_into()?),
            6 => Self::Search(message.try_into()?),
            18 => Self::CreateChannel(message.try_into()?),
            23 => Self::Echo,
            20 => Self::ClientName(message.try_into()?),
            21 => Self::HostName(message.try_into()?),
            unknown => Err(MessageError::UnknownCommandId(unknown))?,
        })
    }
    // pub async fn read_client_message(source: &mut TcpStream) -> Result<Self, MessageError> {
    //     // let message = RawMessage::
    // }

    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::Echo => Echo.write(writer),
            Self::Version(msg) => msg.write(writer),
            Self::RsrvIsUp(msg) => msg.write(writer),
            Self::Search(msg) => msg.write(writer),
            Self::SearchResponse(msg) => msg.write(writer),
            Self::CreateChannel(msg) => msg.write(writer),
            Self::CreateChannelResponse(msg) => msg.write(writer),
            Self::AccessRights(msg) => msg.write(writer),
            Self::ClientName(msg) => msg.write(writer),
            Self::HostName(msg) => msg.write(writer),
            Self::ServerDisconnect(msg) => msg.write(writer),
        }
    }
}

/// Basic DBR Data types, independent of category
#[derive(Debug, Copy, Clone)]
pub enum DBRBasicType {
    String = 0,
    Int = 1,
    Float = 2,
    Enum = 3,
    Char = 4,
    Long = 5,
    Double = 6,
}
impl TryFrom<u16> for DBRBasicType {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::String as u16 => Ok(Self::String),
            x if x == Self::Int as u16 => Ok(Self::Int),
            x if x == Self::Float as u16 => Ok(Self::Float),
            x if x == Self::Enum as u16 => Ok(Self::Enum),
            x if x == Self::Char as u16 => Ok(Self::Char),
            x if x == Self::Long as u16 => Ok(Self::Long),
            x if x == Self::Double as u16 => Ok(Self::Double),
            _ => Err(()),
        }
    }
}
/// Mapping of DBR categories
#[derive(Debug, Copy, Clone)]
pub enum DBRCategory {
    Basic = 0,
    Status = 1,
    Time = 2,
    Graphics = 3,
    Control = 4,
}
impl TryFrom<u16> for DBRCategory {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::Basic as u16 => Ok(Self::Basic),
            x if x == Self::Status as u16 => Ok(Self::Status),
            x if x == Self::Time as u16 => Ok(Self::Time),
            x if x == Self::Graphics as u16 => Ok(Self::Graphics),
            x if x == Self::Control as u16 => Ok(Self::Control),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DBRType {
    basic_type: DBRBasicType,
    category: DBRCategory,
}

impl TryFrom<u16> for DBRType {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(Self {
            basic_type: (value % 7).try_into()?,
            category: (value / 7).try_into()?,
        })
    }
}

impl From<DBRType> for u16 {
    fn from(value: DBRType) -> Self {
        value.category as u16 * 7 + value.basic_type as u16
    }
}

fn check_known_protocol<I>(version: u16, input: I) -> Result<(), Err<nom::error::Error<I>>> {
    if version != EPICS_VERSION {
        Err(Err::Failure(Error::new(input, ErrorKind::Tag)))
    } else {
        Ok(())
    }
}

fn padded_string(length: usize) -> impl for<'a> FnMut(&'a [u8]) -> IResult<&'a [u8], String> {
    move |input| {
        let (input, raw_string) = take(length)(input)?;
        let strlen = raw_string.iter().position(|&c| c == 0x00).unwrap_or(length);
        Ok((
            input,
            String::from_utf8_lossy(&raw_string[0..strlen]).into_owned(),
        ))
    }
}

fn pad_string(string: &str) -> Vec<u8> {
    let mut bytes = string.as_bytes().to_vec();
    let padded_len = (bytes.len() + 1).div_ceil(8);
    bytes.resize(padded_len, 0);
    bytes
}

/// Message CA_PROTO_RSRV_IS_UP.
///
/// Beacon sent by a server when it becomes available. Beacons are also
/// sent out periodically to announce the server is still alive. Another
/// function of beacons is to allow detection of changes in network
/// topology. Sent over UDP.
#[derive(Debug, Default)]
pub struct RsrvIsUp {
    pub server_port: u16,
    pub beacon_id: u32,
    pub server_ip: Option<Ipv4Addr>,
    pub protocol_version: u16,
}

pub trait AsBytes {
    fn as_bytes(&self) -> Vec<u8>;
}
impl<T> AsBytes for T
where
    T: CAMessage,
{
    fn as_bytes(&self) -> Vec<u8> {
        let mut buffer = Cursor::new(Vec::new());
        self.write(&mut buffer).unwrap();
        buffer.into_inner()
    }
}

impl TryFrom<RawMessage> for RsrvIsUp {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(0x0D)?;
        Ok(RsrvIsUp {
            server_port: value.field_2_data_count as u16,
            beacon_id: value.field_3_parameter_1,
            server_ip: match value.field_4_parameter_2 {
                0u32 => None,
                _ => Some(Ipv4Addr::from(value.field_4_parameter_2)),
            },
            protocol_version: value.field_1_data_type,
        })
    }
}

impl CAMessage for RsrvIsUp {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, raw) = RawMessage::parse_id(0x0D, input)?;
        Ok((
            input,
            RsrvIsUp {
                server_port: raw.field_2_data_count as u16,
                beacon_id: raw.field_3_parameter_1,
                server_ip: match raw.field_4_parameter_2 {
                    0u32 => None,
                    _ => Some(Ipv4Addr::from(raw.field_4_parameter_2)),
                },
                protocol_version: raw.field_1_data_type,
            },
        ))
    }

    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&13_u16.to_be_bytes())?;
        writer.write_all(&0_u16.to_be_bytes())?;
        writer.write_all(&EPICS_VERSION.to_be_bytes())?;
        writer.write_all(&self.server_port.to_be_bytes())?;
        writer.write_all(&self.beacon_id.to_be_bytes())?;
        if let Some(ip) = &self.server_ip {
            writer.write_all(&ip.octets())?;
        } else {
            writer.write_all(&0u32.to_be_bytes())?;
        }
        Ok(())
    }
}

/// Message CA_PROTO_VERSION.
///
/// Exchanges client and server protocol versions and desired circuit
/// priority. MUST be the first message sent, by both client and server,
/// when a new TCP (Virtual Circuit) connection is established. It is
/// also sent as the first message in UDP search messages.
#[derive(Debug)]
pub struct Version {
    pub priority: u16,
    pub protocol_version: u16,
}
impl Default for Version {
    fn default() -> Self {
        Version {
            priority: 0,
            protocol_version: EPICS_VERSION,
        }
    }
}
impl TryFrom<RawMessage> for Version {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(0)?;
        Ok(Version {
            priority: value.field_1_data_type,
            protocol_version: value.field_2_data_count as u16,
        })
    }
}
impl CAMessage for Version {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(0x00, input)?;
        Ok((
            input,
            Version {
                priority: header.field_1_data_type,
                protocol_version: header.field_2_data_count as u16,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 0,
            field_1_data_type: self.priority,
            field_2_data_count: EPICS_VERSION as u32,
            ..Default::default()
        }
        .write(writer)
    }
}

/// Message CA_PROTO_SEARCH.
///
/// Searches for a given channel name. Sent over UDP or TCP.
#[derive(Debug)]
pub struct Search {
    pub search_id: u32,
    pub channel_name: String,
    /// Indicating whether failed search response should be returned.
    pub should_reply: bool,
    pub protocol_version: u16,
}
impl Search {
    /// Construct a search response. is_udp required because field is
    /// only present when the intended target is UDP.
    pub fn respond(
        &self,
        server_ip: Option<Ipv4Addr>,
        port_number: u16,
        is_udp: bool,
    ) -> SearchResponse {
        SearchResponse {
            port_number,
            server_ip,
            search_id: self.search_id,
            protocol_version: if is_udp { Some(EPICS_VERSION) } else { None },
        }
    }
}
impl TryFrom<RawMessage> for Search {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(6)?;
        Ok(Search {
            should_reply: value.field_1_data_type == 10,
            protocol_version: value.field_2_data_count as u16,
            search_id: value.field_3_parameter_1,
            channel_name: value.payload_as_string(),
        })
    }
}
impl CAMessage for Search {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 6,
            field_1_data_type: if self.should_reply { 10 } else { 5 },
            field_2_data_count: EPICS_VERSION as u32,
            field_3_parameter_1: self.search_id,
            field_4_parameter_2: self.search_id,
            payload: pad_string(&self.channel_name),
        }
        .write(writer)
    }
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let raw = RawMessage::parse_id(0x06, input)?.1;
        Ok((
            input,
            Search {
                should_reply: raw.field_1_data_type == 10,
                protocol_version: raw.field_2_data_count as u16,
                search_id: raw.field_3_parameter_1,
                channel_name: raw.payload_as_string(),
            },
        ))
    }
}

#[derive(Debug)]
pub struct SearchResponse {
    pub port_number: u16,
    pub search_id: u32,
    /// Server to connect to, if different from the message sender
    pub server_ip: Option<Ipv4Addr>,
    /// Protocol version only present if this is being sent as UDP
    pub protocol_version: Option<u16>,
}

impl TryFrom<RawMessage> for SearchResponse {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(6)?;
        assert!(value.payload_size() == 0 || value.payload_size() == 8);
        Ok(SearchResponse {
            port_number: value.field_1_data_type,
            server_ip: match value.field_3_parameter_1 {
                0xFFFFFFFFu32 => None,
                i => Some(Ipv4Addr::from(i)),
            },
            search_id: value.field_4_parameter_2,
            protocol_version: if value.payload_size() == 0 {
                None
            } else {
                Some(
                    be_u16::<&[u8], nom::error::Error<&[u8]>>(value.payload.as_slice())
                        .unwrap()
                        .1,
                )
            },
        })
    }
}

impl CAMessage for SearchResponse {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(0x06, input)?;

        let mut response = SearchResponse {
            port_number: header.field_1_data_type,
            server_ip: match header.field_3_parameter_1 {
                0xFFFFFFFFu32 => None,
                i => Some(Ipv4Addr::from(i)),
            },
            search_id: header.field_4_parameter_2,
            protocol_version: None,
        };
        assert!(header.payload_size() == 0 || header.payload_size() == 8);
        if header.payload_size() == 8 {
            let (_, version) =
                be_u16::<&[u8], nom::error::Error<&[u8]>>(header.payload.as_slice()).unwrap();
            // let (input, _) = take(6usize)(input)?;
            response.protocol_version = Some(version);
            Ok((input, response))
        } else {
            Ok((input, response))
        }
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 0x06,
            field_1_data_type: self.port_number,
            field_2_data_count: 0,
            field_3_parameter_1: match self.server_ip {
                None => 0xFFFFFFFFu32,
                Some(ip) => ip.to_bits(),
            },
            field_4_parameter_2: self.search_id,
            payload: match self.protocol_version {
                None => Vec::new(),
                Some(v) => v.to_be_bytes().to_vec(),
            },
        }
        .write(writer)
    }
}

pub fn parse_search_packet(input: &[u8]) -> Result<Vec<Search>, nom::error::Error<&[u8]>> {
    // Starts with a version packet
    let (input, _) = Version::parse(input).finish()?;
    // Then a stream of multiple messages
    let (_, messages) = all_consuming(many0(Search::parse)).parse(input).finish()?;

    Ok(messages)
}

/// Message CA_PROTO_CREATE_CHAN.
///
/// Requests creation of channel. Server will allocate required
/// resources and return initialized SID. Sent over TCP.
#[derive(Debug)]
pub struct CreateChannel {
    pub client_id: u32,
    pub protocol_version: u32,
    pub channel_name: String,
}

impl TryFrom<RawMessage> for CreateChannel {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(18)?;
        Ok(CreateChannel {
            client_id: value.field_3_parameter_1,
            protocol_version: value.field_4_parameter_2,
            channel_name: value.payload_as_string(),
        })
    }
}
impl CAMessage for CreateChannel {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(18, input)?;
        Ok((
            input,
            CreateChannel {
                client_id: header.field_3_parameter_1,
                protocol_version: header.field_4_parameter_2,
                channel_name: header.payload_as_string(),
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 18,
            field_1_data_type: 0,
            field_2_data_count: 0,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.protocol_version,
            payload: pad_string(&self.channel_name),
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct CreateChannelResponse {
    data_type: DBRType,
    data_count: u32,
    client_id: u32,
    server_id: u32,
}

impl TryFrom<RawMessage> for CreateChannelResponse {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(18)?;
        Ok(CreateChannelResponse {
            data_type: value.field_1_data_type.try_into().unwrap(),
            data_count: value.field_2_data_count,
            client_id: value.field_3_parameter_1,
            server_id: value.field_4_parameter_2,
        })
    }
}

impl CAMessage for CreateChannelResponse {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(18, input)?;
        Ok((
            input,
            CreateChannelResponse {
                data_type: header.field_1_data_type.try_into().unwrap(),
                data_count: header.field_2_data_count,
                client_id: header.field_3_parameter_1,
                server_id: header.field_4_parameter_2,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 18,
            field_1_data_type: self.data_type.into(),
            field_2_data_count: self.data_count,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.server_id,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug, Copy, Clone)]
enum AccessRight {
    None = 0,
    Read = 1,
    Write = 2,
    ReadWrite = 3,
}

impl TryFrom<u32> for AccessRight {
    type Error = MessageError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AccessRight::None),
            1 => Ok(AccessRight::Read),
            2 => Ok(AccessRight::Write),
            3 => Ok(AccessRight::ReadWrite),
            _ => Err(MessageError::InvalidField(
                "AccessRight".to_owned(),
                format!("{}", value),
            )),
        }
    }
}

/// Message CA_PROTO_ACCESS_RIGHTS
///
/// Notifies of access rights for a channel. This value is determined
/// based on host and client name and may change during runtime. Client
/// cannot change access rights nor can it explicitly query its value,
/// so last received value must be stored.
#[derive(Debug)]
pub struct AccessRights {
    client_id: u32,
    access_rights: AccessRight,
}

impl TryFrom<RawMessage> for AccessRights {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(22)?;
        Ok(Self {
            client_id: value.field_3_parameter_1,
            access_rights: value.field_4_parameter_2.try_into()?,
        })
    }
}

impl CAMessage for AccessRights {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(22, input)?;
        Ok((
            input,
            AccessRights {
                client_id: header.field_3_parameter_1,
                access_rights: header
                    .field_4_parameter_2
                    .try_into()
                    .map_err(|_| Err::Error(Error::new(input, ErrorKind::Verify)))?,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 22,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.access_rights as u32,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Default)]
pub struct Echo;

impl TryFrom<RawMessage> for Echo {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(23)?;
        Ok(Echo {})
    }
}

impl CAMessage for Echo {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, _) = RawMessage::parse_id(23, input)?;
        Ok((input, Echo))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 23,
            ..Default::default()
        }
        .write(writer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClientName {
    pub name: String,
}

impl TryFrom<RawMessage> for ClientName {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(20)?;
        Ok(Self {
            name: value.payload_as_string(),
        })
    }
}

impl CAMessage for ClientName {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, message) = RawMessage::parse_id(20, input)?;
        // Unwrapping here as otherwise it might return a reference to the (local) message
        // - we _know_ that the data is limited by vec length so this should not be an issue...
        let (_, client_name) = padded_string(message.payload.len())(&message.payload).unwrap();

        Ok((input, ClientName { name: client_name }))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 20,
            payload: pad_string(&self.name),
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct HostName {
    pub name: String,
}
impl TryFrom<RawMessage> for HostName {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(21)?;
        Ok(Self {
            name: value.payload_as_string(),
        })
    }
}
impl CAMessage for HostName {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, message) = RawMessage::parse_id(21, input)?;
        // Unwrapping here as otherwise it might return a reference to the (local) message
        // - we _know_ that the data is limited by vec length so this should not be an issue...
        let (_, client_name) = padded_string(message.payload.len())(&message.payload).unwrap();

        Ok((input, HostName { name: client_name }))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 21,
            payload: pad_string(&self.name),
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct ServerDisconnect {
    client_id: u32,
}
impl TryFrom<RawMessage> for ServerDisconnect {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(27)?;
        Ok(Self {
            client_id: value.field_3_parameter_1,
        })
    }
}
impl CAMessage for ServerDisconnect {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = RawMessage::parse_id(27, input)?;
        Ok((
            input,
            ServerDisconnect {
                client_id: header.field_3_parameter_1,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 27,
            field_3_parameter_1: self.client_id,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
struct ClearChannel {}

enum ErrorSeverity {
    Warning = 0,
    Success = 1,
    Error = 2,
    Info = 3,
    Severe = 4,
}

enum ErrorCondition {
    Normal = 0,
    AllocMem = 6,
    TooLarge = 9,
    Timeout = 10,
    BadType = 14,
    Internal = 17,
    DblClFail = 18,
    GetFail = 19,
    PutFail = 20,
    BadCount = 22,
    BadStr = 23,
    Disconn = 24,
    EvDisallow = 26,
    BadMonId = 30,
    BadMask = 41,
    IoDone = 42,
    IoInProgress = 43,
    BadSyncGrp = 44,
    PutCbInProg = 45,
    NoRdAccess = 46,
    NoWtAccess = 47,
    Anachronism = 48,
    NoSearchAddr = 49,
    NoConvert = 50,
    BadChId = 51,
    BadFuncPtr = 52,
    IsAttached = 53,
    UnavailInServ = 54,
    ChanDestroy = 55,
    BadPriority = 56,
    NotThreaded = 57,
    Array16kClient = 58,
    ConnSeqTmo = 59,
    UnrespTmo = 60,
}

impl ErrorCondition {
    fn get_severity(&self) -> ErrorSeverity {
        match self {
            Self::Normal => ErrorSeverity::Success,
            Self::AllocMem => ErrorSeverity::Warning,
            Self::TooLarge => ErrorSeverity::Warning,
            Self::Timeout => ErrorSeverity::Warning,
            Self::BadType => ErrorSeverity::Error,
            Self::Internal => ErrorSeverity::Severe,
            Self::DblClFail => ErrorSeverity::Warning,
            Self::GetFail => ErrorSeverity::Warning,
            Self::PutFail => ErrorSeverity::Warning,
            Self::BadCount => ErrorSeverity::Warning,
            Self::BadStr => ErrorSeverity::Error,
            Self::Disconn => ErrorSeverity::Warning,
            Self::EvDisallow => ErrorSeverity::Error,
            Self::BadMonId => ErrorSeverity::Error,
            Self::BadMask => ErrorSeverity::Error,
            Self::IoDone => ErrorSeverity::Info,
            Self::IoInProgress => ErrorSeverity::Info,
            Self::BadSyncGrp => ErrorSeverity::Error,
            Self::PutCbInProg => ErrorSeverity::Error,
            Self::NoRdAccess => ErrorSeverity::Warning,
            Self::NoWtAccess => ErrorSeverity::Warning,
            Self::Anachronism => ErrorSeverity::Error,
            Self::NoSearchAddr => ErrorSeverity::Warning,
            Self::NoConvert => ErrorSeverity::Warning,
            Self::BadChId => ErrorSeverity::Error,
            Self::BadFuncPtr => ErrorSeverity::Error,
            Self::IsAttached => ErrorSeverity::Warning,
            Self::UnavailInServ => ErrorSeverity::Warning,
            Self::ChanDestroy => ErrorSeverity::Warning,
            Self::BadPriority => ErrorSeverity::Error,
            Self::NotThreaded => ErrorSeverity::Error,
            Self::Array16kClient => ErrorSeverity::Warning,
            Self::ConnSeqTmo => ErrorSeverity::Warning,
            Self::UnrespTmo => ErrorSeverity::Warning,
        }
    }
}
impl std::fmt::Display for ErrorCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",         match self {
            Self::Normal => "Normal successful completion",
            Self::AllocMem => "Unable to allocate additional dynamic memory",
            Self::TooLarge => "The requested data transfer is greater than available memory or EPICS_CA_MAX_ARRAY_BYTES",
            Self::Timeout => "User specified timeout on IO operation expired",
            Self::BadType => "The data type specified is invalid",
            Self::Internal => "Channel Access Internal Failure",
            Self::DblClFail => "The requested local DB operation failed",
            Self::GetFail => "Channel read request failed",
            Self::PutFail => "Channel write request failed",
            Self::BadCount => "Invalid element count requested",
            Self::BadStr => "Invalid string",
            Self::Disconn => "Virtual circuit disconnect",
            Self::EvDisallow => "Request inappropriate within subscription (monitor) update callback",
            Self::BadMonId => "Bad event subscription (monitor) identifier",
            Self::BadMask => "Invalid event selection mask",
            Self::IoDone => "IO operations have completed",
            Self::IoInProgress => "IO operations are in progress",
            Self::BadSyncGrp => "Invalid synchronous group identifier",
            Self::PutCbInProg => "Put callback timed out",
            Self::NoRdAccess => "Read access denied",
            Self::NoWtAccess => "Write access denied",
            Self::Anachronism => "Requested feature is no longer supported",
            Self::NoSearchAddr => "Empty PV search address list",
            Self::NoConvert => "No reasonable data conversion between client and server types",
            Self::BadChId => "Invalid channel identifier",
            Self::BadFuncPtr => "Invalid function pointer",
            Self::IsAttached => "Thread is already attached to a client context",
            Self::UnavailInServ => "Not supported by attached service",
            Self::ChanDestroy => "User destroyed channel",
            Self::BadPriority => "Invalid channel priority",
            Self::NotThreaded => "Preemptive callback not enabled - additional threads may not join context",
            Self::Array16kClient => "Client’s protocol revision does not support transfers exceeding 16k bytes",
            Self::ConnSeqTmo => "Virtual circuit connection sequence aborted",
            Self::UnrespTmo => "?",
        })
    }
}

struct ECAError {
    error_message: String,
    client_id: u32,
    condition: ErrorCondition,
    original_request: Message,
}

// impl CAMessage for ECAError {
//     fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
//         // Work out how big our message payload is
//     }
// }
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek};

    #[test]
    fn parse_beacon() {
        let raw_beacon = b"\x00\x0d\x00\x00\x00\x0d\x92\x32\x00\x06\xde\xde\xac\x17\x7c\xcf";
        // let mut reader = Cursor::new(raw_beacon);
        // let beacon: CA_PROTO_RSRV_IS_UP = reader.read_be().unwrap();
        let (_, beacon) = RsrvIsUp::parse(raw_beacon).unwrap();
        assert_eq!(beacon.server_port, 37426);
        assert_eq!(beacon.beacon_id, 450270);
        assert_eq!(
            beacon.server_ip,
            Some("172.23.124.207".parse::<Ipv4Addr>().unwrap())
        );
        println!("Beacon: {:?}", beacon);

        // Now try converting it back
        let mut writer = Cursor::new(Vec::new());
        beacon.write(&mut writer).unwrap();
        assert_eq!(writer.stream_position().unwrap(), 16);
        assert_eq!(writer.into_inner(), raw_beacon);
    }
    #[test]
    fn parse_version() {
        let raw = b"\x00\x00\x00\x00\x00\x01\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x00";
        // let mut reader = Cursor::new(raw);
        let (_, ver) = Version::parse(raw).unwrap();
        println!("Version: {:?}", ver);
        assert_eq!(ver.priority, 1);
        let mut writer = Cursor::new(Vec::new());
        ver.write(&mut writer).unwrap();
        assert_eq!(writer.stream_position().unwrap(), 16);
        assert_eq!(writer.into_inner(), raw);
    }

    #[test]
    fn parse_search() {
        let raw = b"\x00\x06\x00 \x00\x05\x00\r\x00\x00\x00\x01\x00\x00\x00\x01ME02P-MO-ALIGN-01:Z:TEMPAAAAAAA\x00";
        let (_, search) = Search::parse(raw).unwrap();
        assert_eq!(search.channel_name, "ME02P-MO-ALIGN-01:Z:TEMPAAAAAAA");
        assert!(!search.should_reply);
        assert_eq!(search.search_id, 1);
        // Check parsing something that isn't a search
        let raw = b"\x00\x00\x00 \x00\x05\x00\r\x00\x00\x00\x01\x00";
        assert!(Search::parse(raw).is_err());
        // let raw = []
        // Saw this fail?
        let raw = [
            0x0u8, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0xd, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0,
            0x0u8, 0x6, 0x0, 0x8, 0x0, 0x5, 0x0, 0xd, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x73,
            0x6f, 0x6d, 0x65, 0x0, 0x0, 0x0, 0x0,
        ];
        let searches = parse_search_packet(&raw).unwrap();
    }
}
