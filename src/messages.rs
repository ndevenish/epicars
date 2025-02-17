#![allow(dead_code)]

use std::{
    io::{self, Write},
    net::Ipv4Addr,
};

use nom::{
    bytes::complete::take,
    error::{Error, ErrorKind},
    number::complete::{be_u16, be_u32},
    Err, IResult,
};

const EPICS_VERSION: u16 = 13;

/// A basic trait to tie nom parseability to the struct without a
/// plethora of named functions.
/// Also adds common interface for writing a message struct to a writer.
trait CAMessage {
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
/// Message CA_PROTO_RSRV_IS_UP.
///
/// Beacon sent by a server when it becomes available. Beacons are also
/// sent out periodically to announce the server is still alive. Another
/// function of beacons is to allow detection of changes in network
/// topology. Sent over UDP.
#[derive(Debug)]
struct RsrvIsUp {
    server_port: u16,
    beacon_id: u32,
    server_ip: Ipv4Addr,
}

impl CAMessage for RsrvIsUp {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x0D, input)?;
        check_known_protocol(header.field_1_data_type, input)?;
        Ok((
            input,
            RsrvIsUp {
                server_port: header.field_2_data_count as u16,
                beacon_id: header.field_3_parameter_1,
                server_ip: Ipv4Addr::from(header.field_4_parameter_2),
            },
        ))
    }

    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&13_u16.to_be_bytes())?;
        writer.write_all(&0_u16.to_be_bytes())?;
        writer.write_all(&EPICS_VERSION.to_be_bytes())?;
        writer.write_all(&self.server_port.to_be_bytes())?;
        writer.write_all(&self.beacon_id.to_be_bytes())?;
        writer.write_all(&self.server_ip.octets())?;
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
}
impl CAMessage for Version {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x00, input)?;
        check_known_protocol(header.field_2_data_count as u16, input)?;
        Ok((
            input,
            Version {
                priority: header.field_1_data_type,
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
    reply_flag: u16,
    search_id: u32,
    channel_name: String,
}

#[derive(Debug)]
pub struct SearchResponse {
    port_number: u16,
    search_id: u32,
    server_ip: Ipv4Addr,
}

impl CAMessage for Search {
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let padded_len = (self.channel_name.len() + 1).div_ceil(8);

        Header {
            command: 6,
            payload_size: padded_len as u32,
            field_1_data_type: self.reply_flag,
            field_2_data_count: EPICS_VERSION as u32,
            field_3_parameter_1: self.search_id,
            field_4_parameter_2: self.search_id,
        }
        .write(writer)?;
        writer.write_all(self.channel_name.as_bytes())?;
        for _ in 0..(padded_len - self.channel_name.len()) {
            writer.write_all(&[0x00])?;
        }
        Ok(())
    }
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x06, input)?;

        let reply = header.field_1_data_type;
        let version = header.field_2_data_count;
        check_known_protocol(version as u16, input)?;
        let search_id = header.field_3_parameter_1;

        let (input, raw_string) = take(header.payload_size)(input)?;
        let strlen = raw_string
            .iter()
            .position(|&c| c == 0x00)
            .unwrap_or(header.payload_size as usize);
        let channel_name = String::from_utf8_lossy(&raw_string[0..strlen]).into_owned();

        Ok((
            input,
            Search {
                reply_flag: reply,
                search_id,
                channel_name,
            },
        ))
    }
}

impl CAMessage for SearchResponse {
    fn parse(input: &[u8]) -> IResult<&[u8], Self>
    where
        Self: Sized,
    {
        let (input, header) = Header::parse_id(0x06, input)?;

        assert!(header.payload_size == 0 || header.payload_size == 8);
        if header.payload_size == 8 {
            let (input, version) = be_u16(input)?;
            let (input, _) = take(6usize)(input)?;
            check_known_protocol(version, input)?;
        }
        Ok((
            input,
            SearchResponse {
                port_number: header.field_1_data_type,
                server_ip: Ipv4Addr::from(header.field_3_parameter_1),
                search_id: header.field_4_parameter_2,
            },
        ))
    }
    fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        Header {
            command: 0x06,
            payload_size: 0,
            field_1_data_type: self.port_number,
            field_2_data_count: 0,
            field_3_parameter_1: self.server_ip.to_bits(),
            field_4_parameter_2: self.search_id,
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
            "172.23.124.207".parse::<Ipv4Addr>().unwrap()
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
        assert_eq!(search.reply_flag, 5);
        assert_eq!(search.search_id, 1);
        // Check parsing something that isn't a search
        let raw = b"\x00\x00\x00 \x00\x05\x00\r\x00\x00\x00\x01\x00";
        assert!(Search::parse(raw).is_err());
    }
}
