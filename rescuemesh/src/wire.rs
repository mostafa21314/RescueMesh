//! Framing: TCP stream -> 4-byte big-endian length prefix -> JSON `Msg`.

use crate::proto::Msg;
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub type Wire = Framed<TcpStream, LengthDelimitedCodec>;

pub fn framed(stream: TcpStream) -> Wire {
    let _ = stream.set_nodelay(true);
    Framed::new(stream, LengthDelimitedCodec::builder().max_frame_length(16 << 20).new_codec())
}

pub async fn send<S>(sink: &mut S, msg: &Msg) -> anyhow::Result<()>
where
    S: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    sink.send(Bytes::from(serde_json::to_vec(msg)?)).await?;
    Ok(())
}

pub async fn recv<S>(stream: &mut S) -> anyhow::Result<Option<Msg>>
where
    S: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
{
    match stream.next().await {
        None => Ok(None),
        Some(frame) => Ok(Some(serde_json::from_slice(&frame?)?)),
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
