//! Pull a bounded, fixed snapshot over a dedicated connection (not the peer backlog).
use common::wire::{read_frame, write_frame};
use common::*;
use std::time::Duration;
use tokio::net::TcpStream;

/// Per-transfer limits: a snapshot that takes longer or is larger is rejected.
pub(crate) const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECORDS: usize = 1_000_000;

pub(crate) struct Snapshot {
    pub peer: NodeId,
    pub records: Vec<RecoveryRecord>,
    pub epoch: Epoch,
    pub server: Option<NodeId>,
    pub electing: bool,
    /// Frame bytes received, trailer included.
    pub bytes: u64,
}

pub(crate) enum Fetched {
    Snapshot(Snapshot),
    /// The peer answered but is not Ready. `has_state` is false for a
    /// process that just started empty.
    Unavailable { has_state: bool },
}

pub(crate) async fn fetch(
    addr: &str,
    me: NodeId,
    expected: NodeId,
    since_ms: Option<u64>,
) -> anyhow::Result<Fetched> {
    tokio::time::timeout(FETCH_TIMEOUT, async {
        let mut stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        write_frame(
            &mut stream,
            &Message::RecoveryRequest {
                node_id: me,
                since_ms,
            },
        )
        .await?;
        let mut received = Vec::new();
        let mut bytes = 0u64;
        loop {
            match read_frame(&mut stream).await? {
                Some((Message::RecoveryChunk { records }, n)) => {
                    anyhow::ensure!(
                        received.len() + records.len() <= MAX_RECORDS,
                        "snapshot too large"
                    );
                    bytes += n as u64;
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
                    n,
                )) => {
                    anyhow::ensure!(
                        node_id == expected && records == received.len(),
                        "invalid snapshot trailer"
                    );
                    return Ok(Fetched::Snapshot(Snapshot {
                        peer: expected,
                        records: received,
                        epoch,
                        server,
                        electing,
                        bytes: bytes + n as u64,
                    }));
                }
                Some((Message::RecoveryUnavailable { has_state, .. }, _)) => {
                    return Ok(Fetched::Unavailable { has_state });
                }
                _ => anyhow::bail!("snapshot was interrupted"),
            }
        }
    })
    .await?
}
