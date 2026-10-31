//! Length-delimited framing over TCP: a 4-byte big-endian body length, then
//! the body, which is one JSON-encoded [`Message`].

use crate::msg::Message;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest accepted frame body. A peer announcing more is treated as broken.
pub const MAX_FRAME: usize = 16 << 20;

/// Writes one frame and returns the number of bytes put on the wire.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, msg: &Message) -> io::Result<usize> {
    let body = serde_json::to_vec(msg)?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
    }
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    w.write_all(&buf).await?;
    Ok(buf.len())
}

/// Reads one frame. `Ok(None)` means the peer closed the connection cleanly
/// between frames. Returns the message and the bytes it took on the wire.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<(Message, usize)>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    let msg = serde_json::from_slice(&body)?;
    Ok(Some((msg, 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RequestId;

    #[tokio::test]
    async fn round_trip() {
        let msg = Message::Busy { request_id: RequestId { client_id: uuid::Uuid::nil(), seq: 7 } };
        let mut buf = Vec::new();
        let n = write_frame(&mut buf, &msg).await.unwrap();
        assert_eq!(n, buf.len());
        let mut rd = &buf[..];
        let (back, m) = read_frame(&mut rd).await.unwrap().unwrap();
        assert_eq!(back, msg);
        assert_eq!(m, n);
        assert!(read_frame(&mut rd).await.unwrap().is_none());
    }
}
