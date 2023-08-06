use showbiz_core::{
  bytes::{BufMut, BytesMut},
  Name, NodeId,
};

pub(crate) trait NodeIdCodec {
  fn encoded_len(&self) -> usize;
  fn encode(&self, buf: &mut BytesMut);
  fn decode(src: &[u8]) -> std::io::Result<Self>
  where
    Self: Sized;
}

impl NodeIdCodec for NodeId {
  fn encoded_len(&self) -> usize {
    let name = self.name();
    let addr = self.addr();
    let addr_len = match addr {
      std::net::SocketAddr::V4(_) => 1 + 4 + 2,
      std::net::SocketAddr::V6(_) => 1 + 16 + 2,
    };
    2 + name.len() + addr_len
  }

  fn encode(&self, buf: &mut BytesMut) {
    let name = self.name();
    buf.put_u16(name.len() as u16);
    buf.put_slice(name.as_bytes());
    let addr = self.addr();
    match addr {
      std::net::SocketAddr::V4(addr) => {
        buf.put_u8(4);
        buf.put_slice(&addr.ip().octets());
        buf.put_u16(addr.port());
      }
      std::net::SocketAddr::V6(addr) => {
        buf.put_u8(6);
        buf.put_slice(&addr.ip().octets());
        buf.put_u16(addr.port());
      }
    }
  }

  fn decode(src: &[u8]) -> std::io::Result<Self> {
    let mut cur = 0;
    let name_len = u16::from_be_bytes([src[0], src[1]]) as usize;
    cur += 2;
    let name = Name::from_slice(&src[cur..cur + name_len])
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    cur += name_len;
    let addr_ty = src[cur];
    cur += 1;
    match addr_ty {
      4 => {
        let ip = std::net::Ipv4Addr::from([src[cur], src[cur + 1], src[cur + 2], src[cur + 3]]);
        cur += 4;
        let port = u16::from_be_bytes([src[cur], src[cur + 1]]);
        let addr = std::net::SocketAddr::V4(std::net::SocketAddrV4::new(ip, port));
        Ok(NodeId::new(name, addr))
      }
      6 => {
        let ip = std::net::Ipv6Addr::from([
          src[cur],
          src[cur + 1],
          src[cur + 2],
          src[cur + 3],
          src[cur + 4],
          src[cur + 5],
          src[cur + 6],
          src[cur + 7],
          src[cur + 8],
          src[cur + 9],
          src[cur + 10],
          src[cur + 11],
          src[cur + 12],
          src[cur + 13],
          src[cur + 14],
          src[cur + 15],
        ]);
        cur += 16;
        let port = u16::from_be_bytes([src[cur], src[cur + 1]]);
        let addr = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(ip, port, 0, 0));
        Ok(NodeId::new(name, addr))
      }
      _ => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid address type",
      )),
    }
  }
}
