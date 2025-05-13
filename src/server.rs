use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::{io, sync::Arc};
use tokio::time::{timeout, Duration};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::codec::Framed;
use tokio_util::codec::{Decoder, Encoder};

type ServiceId = String;
type Tx = mpsc::UnboundedSender<Frame>;
type Registry = Arc<DashMap<ServiceId, Tx>>;

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
            8 => FrameType::ServiceName,
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
        let Frame { r#type, payload } = item;
        dst.reserve(8 + payload.len());
        dst.put_u32(r#type as u32);
        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    let registry: Registry = Arc::new(DashMap::new());

    loop {
        let (socket, addr) = listener.accept().await?;
        let registry = registry.clone();

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, MuxCodec);

            let handshake_result = timeout(Duration::from_secs(1), framed.next()).await;
            let (mut writer, mut reader) = framed.split();

            let Some(Ok(Frame {
                r#type: FrameType::Handshake,
                payload,
            })) = handshake_result.ok().flatten()
            else {
                eprintln!("[-] Handshake failed or timed out");
                return;
            };

            let token = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(_) => {
                    eprintln!("[-] Handshake payload invalid UTF-8");
                    return;
                }
            };

            if token == "valid-token".to_string() {
                let _ = writer
                    .send(Frame {
                        r#type: FrameType::Ack,
                        payload: Bytes::from("ok"),
                    })
                    .await;
            } else {
                let _ = writer
                    .send(Frame {
                        r#type: FrameType::Nack,
                        payload: Bytes::from("invalid"),
                    })
                    .await;
            }
            println!("New connect {:?}", addr.ip());

            let incoming = reader.next().await;

            let payload = match incoming {
                Some(Ok(frame)) if frame.r#type == FrameType::ServiceName => frame.payload,
                _ => return,
            };

            let service_id = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(_) => {
                    eprintln!("[-] Handshake payload invalid UTF-8");
                    return;
                }
            };
            println!("[+] Edge {} connected", service_id);
            let (tx, mut rx) = mpsc::unbounded_channel::<Frame>();
            registry.insert(service_id.clone(), tx);

            let service_id_clone: String = service_id.clone();
            tokio::spawn(async move {
                while let Some(frame) = rx.recv().await {
                    if writer.send(frame).await.is_err() {
                        println!("[-] Failed to send to edge {}", service_id_clone);
                        break;
                    }
                }
            });

            while let Some(Ok(frame)) = reader.next().await {
                match frame.r#type {
                    FrameType::Metric => {
                        println!("[{}] METRIC: {:?}", service_id, frame.payload);
                    }
                    FrameType::Data => {
                        println!("[{}] data: {:?}", service_id, frame.payload);
                    }
                    _ => {
                        println!(
                            "[{}] UNKNOWN STREAM({:?}): {:?}",
                            service_id, frame.r#type, frame.payload
                        );
                    }
                }
            }

            println!("[-] Edge {} disconnected", service_id);
            registry.remove(&service_id);
        });
    }
}

type FrameHandler = fn(Frame, &str);
struct FrameDispatcher {
    handlers: HashMap<FrameType, FrameHandler>,
}

impl FrameDispatcher {
    fn new() -> Self {
        let mut handlers: HashMap<FrameType, FrameHandler> = HashMap::new();

        // Frame registry
        handlers.insert(FrameType::Handshake, handle_handshake);
        handlers.insert(FrameType::Metric, handle_metric);
        handlers.insert(FrameType::Log, handle_log);
        handlers.insert(FrameType::Ping, handle_ping);

        Self { handlers }
    }

    fn dispatch(&self, frame: Frame, service_id: &str) {
        match self.handlers.get(&frame.r#type) {
            Some(handler) => handler(frame, service_id),
            None => println!("[{}] No handler for {:?}", service_id, frame.r#type),
        }
    }
}

fn handle_handshake(frame: Frame, service_id: &str) {
    println!("[{}] HANDSHAKE with {:?}", service_id, frame.payload);
}

fn handle_metric(frame: Frame, service_id: &str) {
    println!("[{}] METRIC received: {:?}", service_id, frame.payload);
}

fn handle_log(frame: Frame, service_id: &str) {
    println!("[{}] LOG: {:?}", service_id, frame.payload);
}

fn handle_ping(_frame: Frame, service_id: &str) {
    println!("[{}] PONG", service_id);
}
