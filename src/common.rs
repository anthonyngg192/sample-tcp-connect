// common.rs
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// 4 loại header
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderType { Handshake = 1, Send = 2, Reconnect = 3, Close = 4 }

/// Frame struct
#[derive(Debug)]
pub struct Frame {
    pub stream_id: u32,
    pub header: HeaderType,
    pub payload: Bytes,
}

/// Codec để đóng/mở khung: [4B id|1B header|3B len|payload]
pub struct SimpleCodec;

impl Decoder for SimpleCodec {
    type Item = Frame;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, io::Error> {
        if src.len() < 8 { return Ok(None); }
        let stream_id = (&src[..4]).get_u32();
        let header = match src[4] {
            1 => HeaderType::Handshake,
            2 => HeaderType::Send,
            3 => HeaderType::Reconnect,
            4 => HeaderType::Close,
            other => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("bad header {}", other))),
        };
        let len = ((src[5] as usize) << 16)
                | ((src[6] as usize) <<  8)
                |  (src[7] as usize);
        if src.len() < 8 + len { return Ok(None); }
        src.advance(8);
        let payload = src.split_to(len).freeze();
        Ok(Some(Frame { stream_id, header, payload }))
    }
}

impl Encoder<Frame> for SimpleCodec {
    type Error = io::Error;
    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        let len = item.payload.len() as u32;
        dst.reserve(8 + item.payload.len());
        dst.put_u32(item.stream_id);
        dst.put_u8(item.header as u8);
        dst.put_u8(((len >> 16) & 0xFF) as u8);
        dst.put_u8(((len >>  8) & 0xFF) as u8);
        dst.put_u8(( len        & 0xFF) as u8);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}
