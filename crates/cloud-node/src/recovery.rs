//! Pull a bounded, fixed snapshot over a dedicated connection (not the peer backlog).
use common::wire::{read_frame, write_frame};
use common::*;
use std::time::Duration;
use tokio::net::TcpStream;

pub(crate) struct Snapshot {
    pub records: Vec<RecoveryRecord>,
    pub epoch: Epoch,
    pub server: Option<NodeId>,
    pub electing: bool,
}

pub(crate) async fn fetch(addr: &str, me: NodeId, expected: NodeId) -> anyhow::Result<Snapshot> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(addr).await?;
        write_frame(&mut stream, &Message::RecoveryRequest { node_id: me }).await?;
        let mut received = Vec::new();
        loop {
            match read_frame(&mut stream).await? {
                Some((Message::RecoveryChunk { records }, _)) => {
                    anyhow::ensure!(
                        received.len() + records.len() <= 1_000_000,
                        "snapshot too large"
                    );
                    received.extend(records);
                }
                Some((
                    Message::RecoveryComplete {
                        node_id,
                        epoch,
                        server,
                        electing,
                        records,
                    },
                    _,
                )) => {
                    anyhow::ensure!(
                        node_id == expected && records == received.len(),
                        "invalid snapshot trailer"
                    );
                    return Ok(Snapshot {
                        records: received,
                        epoch,
                        server,
                        electing,
                    });
                }
                _ => anyhow::bail!("peer is not ready or snapshot was interrupted"),
            }
        }
    })
    .await?
}
