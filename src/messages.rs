#![allow(dead_code)]

use std::{
    io::{self, Cursor, Write},
    net::Ipv4Addr,
};

use nom::{
    bytes::complete::take,
    combinator::all_consuming,
    error::{ErrorKind, ParseError},
    multi::many0,
    number::complete::{be_f32, be_u16, be_u32},
    Err, IResult, Parser,
};
use thiserror::Error;
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::database::DBRType;

const EPICS_VERSION: u16 = 13;

/// A basic trait to tie nom parseability to the struct without a
/// plethora of named functions.
/// Also adds common interface for writing a message struct to a writer.
pub trait CAMessage: TryFrom<RawMessage> {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (i, raw) = RawMessage::parse(input)?;
        let converted: Self = raw
            .try_into()
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, ErrorKind::IsNot)))?;

        Ok((i, converted))
    }
}

/// Represents the content of any message
///
/// This is via header parsing (accounting for large headers) and then
/// automatically reading the payload. When writing, it will
/// automatically pad the output payload to the correct multiple of 8
/// bytes (although this will not avoid the necessity of adding a zero
/// byte to the end of String).
///
/// Other messages can be parsed from a RawMessage with TryFrom<RawMessage>.
#[derive(Default, Debug)]
pub struct RawMessage {
    pub command: u16,
    field_1_data_type: u16,
    field_2_data_count: u32,
    field_3_parameter_1: u32,
    field_4_parameter_2: u32,
    payload: Vec<u8>,
}

impl RawMessage {
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
        if self.command == id {
            Ok(())
        } else {
            Err(MessageError::IncorrectCommandId(self.command, id))
        }
    }
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = MessageHeader::parse(input)?;
        let (input, payload) = take(header.payload_size)(input)?;
        Ok((
            input,
            RawMessage {
                command: header.command,
                payload: payload.to_vec(),
                field_1_data_type: header.field_1_data_type,
                field_2_data_count: header.field_2_data_count,
                field_3_parameter_1: header.field_3_parameter_1,
                field_4_parameter_2: header.field_4_parameter_2,
            },
        ))
    }
}

impl CAMessage for RawMessage {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let header: MessageHeader = self.into();
        header.write(writer)?;

        writer.write_all(&self.payload)?;
        let extra_bytes = header.payload_size as usize - self.payload.len();
        if extra_bytes > 0 {
            writer.write_all(&vec![0; extra_bytes])?;
        }

        Ok(())
    }
}

/// Parsing message headers, without attempting to read the payload
pub struct MessageHeader {
    pub command: u16,
    pub payload_size: u32,
    pub field_1_data_type: u16,
    pub field_2_data_count: u32,
    pub field_3_parameter_1: u32,
    pub field_4_parameter_2: u32,
}

impl MessageHeader {
    pub fn header_size(&self) -> usize {
        if self.payload_size < 0xFFFF && self.field_2_data_count <= 0xFFFF {
            16
        } else {
            32
        }
    }

    pub fn parse(input: &[u8]) -> IResult<&[u8], Self>
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

            Ok((
                input,
                Self {
                    command,
                    payload_size,
                    field_1_data_type: field_1,
                    field_2_data_count: field_2,
                    field_3_parameter_1: field_3,
                    field_4_parameter_2: field_4,
                },
            ))
        } else {
            let (input, field_2) = be_u16(input)?;
            let (input, field_3) = be_u32(input)?;
            let (input, field_4) = be_u32(input)?;
            Ok((
                input,
                Self {
                    command,
                    field_1_data_type: field_1,
                    field_2_data_count: field_2 as u32,
                    field_3_parameter_1: field_3,
                    field_4_parameter_2: field_4,
                    payload_size: payload_size as u32,
                },
            ))
        }
    }
}

impl From<&RawMessage> for MessageHeader {
    fn from(value: &RawMessage) -> Self {
        Self {
            command: value.command,
            field_1_data_type: value.field_1_data_type,
            field_2_data_count: value.field_2_data_count,
            field_3_parameter_1: value.field_3_parameter_1,
            field_4_parameter_2: value.field_4_parameter_2,
            payload_size: value.payload_size() as u32,
        }
    }
}
impl From<RawMessage> for MessageHeader {
    fn from(value: RawMessage) -> Self {
        Self {
            command: value.command,
            field_1_data_type: value.field_1_data_type,
            field_2_data_count: value.field_2_data_count,
            field_3_parameter_1: value.field_3_parameter_1,
            field_4_parameter_2: value.field_4_parameter_2,
            payload_size: value.payload_size() as u32,
        }
    }
}

impl CAMessage for MessageHeader {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let payload_size = self.payload_size.div_ceil(8) * 8;

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
        Ok(())
    }
}
#[derive(Debug)]
pub enum Message {
    AccessRights(AccessRights),
    ClearChannel(ClearChannel),
    ClientName(ClientName),
    CreateChannel(CreateChannel),
    CreateChannelFailure(CreateChannelFailure),
    CreateChannelResponse(CreateChannelResponse),
    Echo,
    EventAdd(EventAdd),
    EventsOff,
    EventsOn,
    HostName(HostName),
    ReadNotify(ReadNotify),
    RsrvIsUp(RsrvIsUp),
    Search(Search),
    SearchResponse(SearchResponse),
    ServerDisconnect(ServerDisconnect),
    Version(Version),
}

impl AsBytes for Message {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Message::AccessRights(msg) => msg.as_bytes(),
            Message::ClearChannel(msg) => msg.as_bytes(),
            Message::ClientName(msg) => msg.as_bytes(),
            Message::CreateChannel(msg) => msg.as_bytes(),
            Message::CreateChannelFailure(msg) => msg.as_bytes(),
            Message::CreateChannelResponse(msg) => msg.as_bytes(),
            Message::Echo => Echo.as_bytes(),
            Message::EventAdd(message) => message.as_bytes(),
            Message::EventsOff => EventsOff.as_bytes(),
            Message::EventsOn => EventsOn.as_bytes(),
            Message::HostName(msg) => msg.as_bytes(),
            Message::ReadNotify(msg) => msg.as_bytes(),
            Message::RsrvIsUp(msg) => msg.as_bytes(),
            Message::Search(msg) => msg.as_bytes(),
            Message::SearchResponse(msg) => msg.as_bytes(),
            Message::ServerDisconnect(msg) => msg.as_bytes(),
            Message::Version(msg) => msg.as_bytes(),
        }
    }
}
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("IO Error Occured: {0}")]
    IO(#[from] io::Error),
    #[error("An error occured parsing a message")]
    ParsingError(#[from] nom::Err<nom::error::Error<Vec<u8>>>),
    #[error("Unknown command ID: {0}")]
    UnknownCommandId(u16),
    #[error("Got a valid message but is not valid at this state: {0:?}")]
    UnexpectedMessage(Message),
    #[error("Message command ID ({0}) does not match expected ({1})")]
    IncorrectCommandId(u16, u16),
    #[error("Invalid message field: {0}")]
    InvalidField(String),
    #[error("Error: {0}")]
    ErrorResponse(ErrorCondition),
}

impl From<nom::Err<nom::error::Error<&[u8]>>> for MessageError {
    fn from(err: nom::Err<nom::error::Error<&[u8]>>) -> Self {
        MessageError::ParsingError(err.to_owned())
    }
}
impl From<nom::Err<MessageError>> for MessageError {
    fn from(value: nom::Err<MessageError>) -> Self {
        match value {
            Err::Error(err) => err,
            Err::Failure(err) => err,
            _ => panic!("Not sure how to handle"),
        }
    }
}

impl ParseError<&[u8]> for MessageError {
    fn from_error_kind(input: &[u8], kind: ErrorKind) -> Self {
        MessageError::ParsingError(nom::Err::Error(nom::error::Error::from_error_kind(
            input.to_vec(),
            kind,
        )))
    }
    fn append(input: &[u8], kind: ErrorKind, _other: Self) -> Self {
        Self::from_error_kind(input, kind)
    }
    fn from_char(input: &[u8], _: char) -> Self {
        Self::from_error_kind(input, ErrorKind::Char)
    }
    fn or(self, other: Self) -> Self {
        other
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
            1 => Self::EventAdd(message.try_into()?),
            6 => Self::Search(message.try_into()?),
            8 => Self::EventsOff,
            9 => Self::EventsOn,
            12 => Self::ClearChannel(message.try_into()?),
            15 => Self::ReadNotify(message.try_into()?),
            18 => Self::CreateChannel(message.try_into()?),
            23 => Self::Echo,
            20 => Self::ClientName(message.try_into()?),
            21 => Self::HostName(message.try_into()?),
            unknown => Err(MessageError::UnknownCommandId(unknown))?,
        })
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

pub fn parse_search_packet(input: &[u8]) -> Result<Vec<Search>, MessageError> {
    // Starts with a version packet
    let (input, _) = Version::parse(input)?;
    // Then a stream of multiple messages
    let (_, messages) = all_consuming(many0(Search::parse)).parse(input)?;

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

impl CreateChannel {
    pub fn respond_failure(&self) -> CreateChannelFailure {
        CreateChannelFailure {
            client_id: self.client_id,
        }
    }
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
    pub data_type: DBRType,
    pub data_count: u32,
    pub client_id: u32,
    pub server_id: u32,
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

#[derive(Debug)]
pub struct CreateChannelFailure {
    client_id: u32,
}

impl TryFrom<RawMessage> for CreateChannelFailure {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(26)?;
        Ok(CreateChannelFailure {
            client_id: value.field_3_parameter_1,
        })
    }
}
impl CAMessage for CreateChannelFailure {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 26,
            field_3_parameter_1: self.client_id,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum AccessRight {
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
            _ => Err(MessageError::InvalidField(format!(
                "Invalid AccessRight: {value}"
            ))),
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
    pub client_id: u32,
    pub access_rights: AccessRight,
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
    pub client_id: u32,
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
pub struct EventsOn;

impl TryFrom<RawMessage> for EventsOn {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(9)?;
        Ok(Self)
    }
}
impl CAMessage for EventsOn {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 9,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct EventsOff;

impl TryFrom<RawMessage> for EventsOff {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(8)?;
        Ok(Self)
    }
}
impl CAMessage for EventsOff {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 8,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct ClearChannel {
    pub server_id: u32,
    pub client_id: u32,
}
impl TryFrom<RawMessage> for ClearChannel {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(12)?;
        Ok(ClearChannel {
            server_id: value.field_3_parameter_1,
            client_id: value.field_4_parameter_2,
        })
    }
}
impl CAMessage for ClearChannel {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 12,
            field_3_parameter_1: self.server_id,
            field_4_parameter_2: self.client_id,
            ..Default::default()
        }
        .write(writer)
    }
}

#[derive(Debug)]
pub struct EventAdd {
    data_type: DBRType,
    data_count: u32,
    server_id: u32,
    subscription_id: u32,
    mask: u16,
}

impl TryFrom<RawMessage> for EventAdd {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(1)?;
        if value.payload_size() != 16 {
            return Err(MessageError::InvalidField(
                "Payload not 16 bytes".to_string(),
            ));
        }
        let (_, (_, _, _, mask)) = (be_f32::<&[u8], MessageError>, be_f32, be_f32, be_u16)
            .parse(value.payload.as_slice())?;
        Ok(EventAdd {
            data_type: DBRType::try_from(value.field_1_data_type).map_err(|_| {
                MessageError::InvalidField(format!(
                    "Invalid Data Type Value: {}",
                    value.field_1_data_type
                ))
            })?,
            data_count: value.field_2_data_count,
            server_id: value.field_3_parameter_1,
            subscription_id: value.field_4_parameter_2,
            mask,
        })
    }
}

impl CAMessage for EventAdd {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut payload = vec![0u8; 12];
        payload.extend_from_slice(&self.mask.to_be_bytes());
        RawMessage {
            command: 1,
            field_1_data_type: self.data_type.into(),
            field_2_data_count: self.data_count,
            field_3_parameter_1: self.server_id,
            field_4_parameter_2: self.subscription_id,
            payload,
        }
        .write(writer)
    }
}
#[derive(Debug)]
pub struct EventCancel {}

#[derive(Debug)]
pub struct ReadNotify {
    pub data_type: DBRType,
    pub data_count: u32,
    pub server_id: u32,
    pub client_ioid: u32,
}

impl ReadNotify {
    pub fn respond(&self, data_count: usize, data: Vec<u8>) -> ReadNotifyResponse {
        ReadNotifyResponse {
            data_type: self.data_type,
            data_count: data_count as u32,
            client_ioid: self.client_ioid,
            server_id: self.server_id,
            data,
        }
    }
}
impl TryFrom<RawMessage> for ReadNotify {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(15)?;
        Ok(ReadNotify {
            data_type: value.field_1_data_type.try_into().map_err(|_| {
                MessageError::InvalidField(format!(
                    "Invalid data type value: {}",
                    value.field_1_data_type
                ))
            })?,
            data_count: value.field_2_data_count,
            server_id: value.field_3_parameter_1,
            client_ioid: value.field_4_parameter_2,
        })
    }
}

impl From<&ReadNotify> for RawMessage {
    fn from(value: &ReadNotify) -> Self {
        RawMessage {
            command: 15,
            field_1_data_type: value.data_type.into(),
            field_2_data_count: value.data_count,
            field_3_parameter_1: value.server_id,
            field_4_parameter_2: value.client_ioid,
            ..Default::default()
        }
    }
}

impl CAMessage for ReadNotify {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage::from(self).write(writer)
    }
}

pub struct ReadNotifyResponse {
    data_type: DBRType,
    data_count: u32,
    server_id: u32,
    client_ioid: u32,
    data: Vec<u8>,
}

impl From<&ReadNotifyResponse> for RawMessage {
    fn from(value: &ReadNotifyResponse) -> Self {
        RawMessage {
            command: 15,
            field_1_data_type: value.data_type.into(),
            field_2_data_count: value.data_count,
            field_3_parameter_1: value.server_id,
            field_4_parameter_2: value.client_ioid,
            payload: value.data.clone(),
        }
    }
}

impl TryFrom<RawMessage> for ReadNotifyResponse {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(15)?;
        Ok(ReadNotifyResponse {
            data_type: DBRType::try_from(value.field_1_data_type)
                .map_err(|_| MessageError::ErrorResponse(ErrorCondition::BadType))?,
            data_count: value.field_2_data_count,
            server_id: value.field_3_parameter_1,
            client_ioid: value.field_4_parameter_2,
            data: value.payload,
        })
    }
}

impl CAMessage for ReadNotifyResponse {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage::from(self).write(writer)
    }
}
#[derive(Debug)]
pub struct WriteChannel {}

#[derive(Debug)]
pub struct WriteNotify {}

#[derive(Debug)]
pub struct EventAddResponse {}

enum ErrorSeverity {
    Warning = 0,
    Success = 1,
    Error = 2,
    Info = 3,
    Severe = 4,
}

#[derive(Debug, Copy, Clone)]
pub enum ErrorCondition {
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
    fn eca_code(&self) -> u32 {
        *self as u32
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

impl TryFrom<u32> for ErrorCondition {
    type Error = MessageError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::Normal as u32 => Ok(Self::Normal),
            x if x == Self::AllocMem as u32 => Ok(Self::AllocMem),
            x if x == Self::TooLarge as u32 => Ok(Self::TooLarge),
            x if x == Self::Timeout as u32 => Ok(Self::Timeout),
            x if x == Self::BadType as u32 => Ok(Self::BadType),
            x if x == Self::Internal as u32 => Ok(Self::Internal),
            x if x == Self::DblClFail as u32 => Ok(Self::DblClFail),
            x if x == Self::GetFail as u32 => Ok(Self::GetFail),
            x if x == Self::PutFail as u32 => Ok(Self::PutFail),
            x if x == Self::BadCount as u32 => Ok(Self::BadCount),
            x if x == Self::BadStr as u32 => Ok(Self::BadStr),
            x if x == Self::Disconn as u32 => Ok(Self::Disconn),
            x if x == Self::EvDisallow as u32 => Ok(Self::EvDisallow),
            x if x == Self::BadMonId as u32 => Ok(Self::BadMonId),
            x if x == Self::BadMask as u32 => Ok(Self::BadMask),
            x if x == Self::IoDone as u32 => Ok(Self::IoDone),
            x if x == Self::IoInProgress as u32 => Ok(Self::IoInProgress),
            x if x == Self::BadSyncGrp as u32 => Ok(Self::BadSyncGrp),
            x if x == Self::PutCbInProg as u32 => Ok(Self::PutCbInProg),
            x if x == Self::NoRdAccess as u32 => Ok(Self::NoRdAccess),
            x if x == Self::NoWtAccess as u32 => Ok(Self::NoWtAccess),
            x if x == Self::Anachronism as u32 => Ok(Self::Anachronism),
            x if x == Self::NoSearchAddr as u32 => Ok(Self::NoSearchAddr),
            x if x == Self::NoConvert as u32 => Ok(Self::NoConvert),
            x if x == Self::BadChId as u32 => Ok(Self::BadChId),
            x if x == Self::BadFuncPtr as u32 => Ok(Self::BadFuncPtr),
            x if x == Self::IsAttached as u32 => Ok(Self::IsAttached),
            x if x == Self::UnavailInServ as u32 => Ok(Self::UnavailInServ),
            x if x == Self::ChanDestroy as u32 => Ok(Self::ChanDestroy),
            x if x == Self::BadPriority as u32 => Ok(Self::BadPriority),
            x if x == Self::NotThreaded as u32 => Ok(Self::NotThreaded),
            x if x == Self::Array16kClient as u32 => Ok(Self::Array16kClient),
            x if x == Self::ConnSeqTmo as u32 => Ok(Self::ConnSeqTmo),
            x if x == Self::UnrespTmo as u32 => Ok(Self::UnrespTmo),
            _ => Err(MessageError::InvalidField(format!(
                "ErrorCondition {value} unrecognised"
            ))),
        }
    }
}

pub struct ECAError {
    pub error_message: String,
    pub client_id: u32,
    pub condition: ErrorCondition,
    pub original_request: MessageHeader,
}

impl ECAError {
    pub fn new(condition: ErrorCondition, client_id: u32, original_request: Message) -> ECAError {
        let header = MessageHeader::parse(original_request.as_bytes().as_slice())
            .unwrap()
            .1;

        ECAError {
            error_message: format!("{}", condition),
            client_id,
            condition,
            original_request: header,
        }
    }
}

impl TryFrom<&RawMessage> for ECAError {
    type Error = MessageError;
    fn try_from(value: &RawMessage) -> Result<Self, Self::Error> {
        value.expect_id(11)?;

        let (i, header) = MessageHeader::parse(value.payload.as_slice())?;

        Ok(ECAError {
            client_id: value.field_3_parameter_1,
            condition: ErrorCondition::try_from(value.field_4_parameter_2)?,
            original_request: header,
            error_message: padded_string(i.len())(i)?.1,
        })
    }
}
impl TryFrom<RawMessage> for ECAError {
    type Error = MessageError;
    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let m: Self = value.try_into()?;
        Ok(m)
    }
}

impl CAMessage for ECAError {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        RawMessage {
            command: 11,
            field_1_data_type: 0,
            field_2_data_count: 0,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.condition.eca_code(),
            payload: self.original_request.as_bytes().to_vec(),
        }
        .write(writer)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek};

    #[test]
    fn parse_beacon() {
        let raw_beacon = b"\x00\x0d\x00\x00\x00\x0d\x92\x32\x00\x06\xde\xde\xac\x17\x7c\xcf";
        // let mut reader = Cursor::new(raw_beacon);
        // let beacon: CA_PROTO_RSRV_IS_UP = reader.read_be().unwrap();
        let beacon: RsrvIsUp = RawMessage::parse(raw_beacon).unwrap().1.try_into().unwrap();
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
        let ver = all_consuming(Version::parse).parse(raw).unwrap().1;
        println!("Version: {:?}", ver);
        assert_eq!(ver.priority, 1);
        let bytes = ver.as_bytes();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes, raw);
    }

    #[test]
    fn parse_search() {
        let raw = b"\x00\x06\x00 \x00\x05\x00\r\x00\x00\x00\x01\x00\x00\x00\x01ME02P-MO-ALIGN-01:Z:TEMPAAAAAAA\x00";
        let search = all_consuming(Search::parse).parse(raw).unwrap().1;
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
