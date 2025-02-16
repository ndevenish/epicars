use std::net::Ipv4Addr;

use binrw::binrw;

// #[binrw]
// struct Header {
//     command: u16,
//     payload_size: u16,
//     data_type: u16,
//     data_count: u16,
//     parameter_1: u32,
//     parameter_2: u32,
// }

#[allow(non_camel_case_types)]
#[binrw]
#[derive(Debug)]
#[brw(magic = b"\x00\x0d")]
struct CA_PROTO_RSRV_IS_UP {
    #[bw(calc = 0)]
    #[br(assert(reserved == 0, "Invalid Command Number"))]
    #[br(temp)]
    reserved: u16,

    #[br(assert(version == 13, "Unrecognised version"))]
    #[bw(assert(*version == 13, "Unrecognised version"))]
    version: u16,

    server_port: u16,
    beacon_number: u32,

    #[br(map = |x: [u8; 4]| Ipv4Addr::from(x))]
    #[bw(map = |addr: &Ipv4Addr| addr.octets())]
    server_ip: Ipv4Addr,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use binrw::{BinReaderExt, BinWrite};

    use super::*;

    #[test]
    fn parse_beacon() {
        let raw_beacon = b"\x00\x0d\x00\x00\x00\x0d\x92\x32\x00\x06\xde\xde\xac\x17\x7c\xcf";
        let mut reader = Cursor::new(raw_beacon);
        let beacon: CA_PROTO_RSRV_IS_UP = reader.read_be().unwrap();
        assert_eq!(beacon.version, 13);
        assert_eq!(beacon.server_port, 37426);
        assert_eq!(beacon.beacon_number, 450270);
        assert_eq!(
            beacon.server_ip,
            "172.23.124.207".parse::<Ipv4Addr>().unwrap()
        );
        println!("Beacon: {:?}", beacon);

        // Now try converting it back
        let mut writer = Cursor::new(Vec::new());
        beacon.write_be(&mut writer).unwrap();
        assert_eq!(writer.into_inner(), raw_beacon);
    }
}
