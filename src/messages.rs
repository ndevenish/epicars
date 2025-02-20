#![allow(dead_code)]

use std::{
    io::{self, Write},
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

const EPICS_VERSION: u16 = 13;

/// A basic trait to tie nom parseability to the struct without a
/// plethora of named functions.
/// Also adds common interface for writing a message struct to a writer.
pub trait CAMessage {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized;
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()>;
}

fn check_known_protocol<I>(version: u16, input: I) -> Result<(), Err<nom::error::Error<I>>> {
    if version != EPICS_VERSION {
        Err(Err::Failure(Error::new(input, ErrorKind::Tag)))
    } else {
        Ok(())
    }
}

fn padded_string(length: usize) -> impl FnMut(&[u8]) -> IResult<&[u8], String> {
    move |input| {
        let (input, raw_string) = take(length)(input)?;
        let strlen = raw_string.iter().position(|&c| c == 0x00).unwrap_or(length);
        Ok((
            input,
            String::from_utf8_lossy(&raw_string[0..strlen]).into_owned(),
        ))
    }
}

/// Write a null-terminated string, padded to the specified length
fn write_padded_string<W: Write>(writer: &mut W, length: usize, string: &str) -> io::Result<()> {
    assert!(string.len() < length);
    writer.write_all(string.as_bytes())?;
    for _ in 0..(length - string.len()) {
        writer.write_all(&[0x00])?;
    }
    Ok(())
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

impl CAMessage for RsrvIsUp {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x0D, input)?;
        Ok((
            input,
            RsrvIsUp {
                server_port: header.field_2_data_count as u16,
                beacon_id: header.field_3_parameter_1,
                server_ip: match header.field_4_parameter_2 {
                    0u32 => None,
                    _ => Some(Ipv4Addr::from(header.field_4_parameter_2)),
                },
                protocol_version: header.field_1_data_type,
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
struct Version {
    priority: u16,
    protocol_version: u16,
}
impl CAMessage for Version {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x00, input)?;
        Ok((
            input,
            Version {
                priority: header.field_1_data_type,
                protocol_version: header.field_2_data_count as u16,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        Header {
            command: 0,
            payload_size: 0,
            field_1_data_type: self.priority,
            field_2_data_count: EPICS_VERSION as u32,
            field_3_parameter_1: 0,
            field_4_parameter_2: 0,
        }
        .write(writer)
    }
}

#[derive(Debug, Default)]
struct Header {
    command: u16,
    payload_size: u32,
    field_1_data_type: u16,
    field_2_data_count: u32,
    field_3_parameter_1: u32,
    #[allow(dead_code)]
    field_4_parameter_2: u32,
}

impl Header {
    /// Parse a Header, but check that it matches the expected tag
    fn parse_id(command_id: u16, input: &[u8]) -> IResult<&[u8], Header> {
        let (input, result) = Header::parse(input)?;
        if result.command != command_id {
            return Err(Err::Error(Error::new(input, ErrorKind::Tag)));
        }
        Ok((input, result))
    }
}

impl CAMessage for Header {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.command.to_be_bytes())?;
        if self.payload_size < 0xFFFF && self.field_2_data_count <= 0xFFFF {
            writer.write_all(&(self.payload_size as u16).to_be_bytes())?;
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
            writer.write_all(&self.payload_size.to_be_bytes())?;
            writer.write_all(&self.field_2_data_count.to_be_bytes())?;
        }
        Ok(())
    }
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, command) = be_u16(input)?;
        let (input, payload) = be_u16(input)?;
        // "Data Type" is always here, even in large packet headers
        let (input, field_1) = be_u16(input)?;

        // Handle packets that could be large
        if payload == 0xFFFF {
            let (input, _) = take(2usize)(input)?;
            let (input, field_3) = be_u32(input)?;
            let (input, field_4) = be_u32(input)?;
            let (input, payload) = be_u32(input)?;
            let (input, field_2) = be_u32(input)?;
            Ok((
                input,
                Header {
                    command,
                    payload_size: payload,
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
                Header {
                    command,
                    payload_size: payload as u32,
                    field_1_data_type: field_1,
                    field_2_data_count: field_2 as u32,
                    field_3_parameter_1: field_3,
                    field_4_parameter_2: field_4,
                },
            ))
        }
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

impl CAMessage for Search {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let padded_len = (self.channel_name.len() + 1).div_ceil(8);

        Header {
            command: 6,
            payload_size: padded_len as u32,
            field_1_data_type: if self.should_reply { 10 } else { 5 },
            field_2_data_count: EPICS_VERSION as u32,
            field_3_parameter_1: self.search_id,
            field_4_parameter_2: self.search_id,
        }
        .write(writer)?;
        write_padded_string(writer, padded_len, &self.channel_name)?;
        Ok(())
    }
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x06, input)?;

        let should_reply = header.field_1_data_type == 10;
        let protocol_version = header.field_2_data_count as u16;
        let search_id = header.field_3_parameter_1;
        let (_, channel_name) = padded_string(header.payload_size as usize)(input)?;

        Ok((
            input,
            Search {
                should_reply,
                search_id,
                channel_name,
                protocol_version,
            },
        ))
    }
}

#[derive(Debug)]
pub struct SearchResponse {
    port_number: u16,
    search_id: u32,
    /// Server to connect to, if different from the message sender
    server_ip: Option<Ipv4Addr>,
    /// Protocol version only present if this is being sent as UDP
    protocol_version: Option<u16>,
}

impl CAMessage for SearchResponse {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x06, input)?;

        let mut response = SearchResponse {
            port_number: header.field_1_data_type,
            server_ip: match header.field_3_parameter_1 {
                0xFFFFFFFFu32 => None,
                i => Some(Ipv4Addr::from(i)),
            },
            search_id: header.field_4_parameter_2,
            protocol_version: None,
        };
        assert!(header.payload_size == 0 || header.payload_size == 8);
        if header.payload_size == 8 {
            let (input, version) = be_u16(input)?;
            let (input, _) = take(6usize)(input)?;
            response.protocol_version = Some(version);
            Ok((input, response))
        } else {
            Ok((input, response))
        }
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        Header {
            command: 0x06,
            payload_size: if self.protocol_version.is_some() {
                8
            } else {
                0
            },
            field_1_data_type: self.port_number,
            field_2_data_count: 0,
            field_3_parameter_1: match self.server_ip {
                None => 0xFFFFFFFFu32,
                Some(ip) => ip.to_bits(),
            },
            field_4_parameter_2: self.search_id,
        }
        .write(writer)?;
        // If we set a protocol version, that goes in the payload
        if let Some(version) = self.protocol_version {
            writer.write_all(&version.to_be_bytes())?;
            writer.write_all(&[0, 0, 0, 0, 0, 0])?;
        }
        Ok(())
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
struct CreateChan {
    client_id: u32,
    protocol_version: u32,
    channel_name: String,
}

impl CAMessage for CreateChan {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(18, input)?;
        let (input, channel_name) = padded_string(header.payload_size as usize)(input)?;
        Ok((
            input,
            CreateChan {
                client_id: header.field_3_parameter_1,
                protocol_version: header.field_4_parameter_2,
                channel_name,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let payload_size = (self.channel_name.len() + 1).div_ceil(8) as u32;
        Header {
            command: 18,
            payload_size,
            field_1_data_type: 0,
            field_2_data_count: 0,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.protocol_version,
        }
        .write(writer)?;
        write_padded_string(writer, payload_size as usize, &self.channel_name)?;
        Ok(())
    }
}

#[derive(Debug)]
struct CreateChanResponse {
    data_type: u16,
    data_count: u32,
    client_id: u32,
    server_id: u32,
}

impl CAMessage for CreateChanResponse {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(18, input)?;
        Ok((
            input,
            CreateChanResponse {
                data_type: header.field_1_data_type,
                data_count: header.field_2_data_count,
                client_id: header.field_3_parameter_1,
                server_id: header.field_4_parameter_2,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        Header {
            command: 18,
            payload_size: 0,
            field_1_data_type: self.data_type,
            field_2_data_count: self.data_count,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.server_id,
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
    type Error = ();
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AccessRight::None),
            1 => Ok(AccessRight::Read),
            2 => Ok(AccessRight::Write),
            3 => Ok(AccessRight::ReadWrite),
            _ => Err(()),
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
struct AccessRights {
    client_id: u32,
    access_rights: AccessRight,
}

impl CAMessage for AccessRights {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(22, input)?;
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
        Header {
            command: 22,
            field_3_parameter_1: self.client_id,
            field_4_parameter_2: self.access_rights as u32,
            ..Default::default()
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
