use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::io;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Eq, Hash, PartialEq)]
#[repr(u32)]
enum FrameType {
    Handshake = 0,
    Data = 1,
    NewRoom = 2,
    Metric = 3,
    Log = 4,
    Ping = 5,
    Ack = 6,
    Nack = 7,
    ServiceName = 8,

    Unknown = 999,
}

impl From<u32> for FrameType {
    fn from(value: u32) -> Self {
        match value {
            0 => FrameType::Handshake,
            1 => FrameType::Data,
            2 => FrameType::NewRoom,
            3 => FrameType::Metric,
            4 => FrameType::Log,
            5 => FrameType::Ping,
            6 => FrameType::Ack,
            7 => FrameType::Nack,
            _ => FrameType::Unknown,
        }
    }
}

#[derive(Debug)]
struct Frame {
    r#type: FrameType,
    payload: Bytes,
}

struct MuxCodec;

impl Decoder for MuxCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, io::Error> {
        if src.len() < 8 {
            return Ok(None);
        }
        let mut header = &src[..8];
        let stream_id = header.get_u32();
        let len = header.get_u32() as usize;

        if src.len() < 8 + len {
            return Ok(None);
        }

        src.advance(8);
        let payload = src.split_to(len).freeze();
        Ok(Some(Frame {
            r#type: stream_id.into(),
            payload,
        }))
    }
}

impl Encoder<Frame> for MuxCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        let Frame {
            r#type: stream_id,
            payload,
        } = item;
        dst.reserve(8 + payload.len());
        dst.put_u32(stream_id as u32);
        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:9000").await?;
    let mut framed = Framed::new(stream, MuxCodec);

    framed
        .send(Frame {
            r#type: FrameType::Handshake,
            payload: Bytes::from("token"),
        })
        .await?;

    println!("[edge] handshake sent");

    // Chờ phản hồi ACK/NACK
    if let Some(Ok(frame)) = framed.next().await {
        match frame.r#type {
            FrameType::Ack => println!("[edge] Connected OK."),
            FrameType::Nack => return Err(anyhow::anyhow!("handshake rejected")),
            _ => return Err(anyhow::anyhow!("unexpected response: {:?}", frame)),
        }
    } else {
        return Err(anyhow::anyhow!("no response from server"));
    }

    framed
        .send(Frame {
            r#type: FrameType::ServiceName,
            payload: Bytes::from("edge-a"),
        })
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    framed
        .send(Frame {
            r#type: FrameType::Metric,
            payload: Bytes::from("cpu=82%;mem=55%"),
        })
        .await?;

    framed
        .send(Frame {
            r#type: FrameType::Data,
            payload: Bytes::from("this is data."),
        })
        .await?;

    while let Some(Ok(frame)) = framed.next().await {
        tracing::info!(
            "[edge] received: stream={:?}, {:?}",
            frame.r#type,
            frame.payload
        );
    }

    Ok(())
}
