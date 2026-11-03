//! Pull a bounded, fixed snapshot over a dedicated connection (not the peer backlog).
use common::wire::{read_frame, write_frame};
use common::*;
use std::time::Duration;
use tokio::net::TcpStream;

/// How long a starting node waits for each peer's `Probe`.
pub(crate) const STARTUP_PROBE: Duration = Duration::from_millis(300);

pub(crate) struct Snapshot {
    pub records: Vec<RecoveryRecord>,
    pub epoch: Epoch,
    pub server: Option<NodeId>,
    pub electing: bool,
}

/// What one peer gave a recovering node.
pub(crate) enum Fetched {
    Snapshot(Snapshot),
    /// The peer is starting or recovering itself: nothing authoritative to give.
    NotReady,
}

pub(crate) async fn fetch(addr: &str, me: NodeId, expected: NodeId) -> anyhow::Result<Fetched> {
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
                    return Ok(Fetched::Snapshot(Snapshot {
                        records: received,
                        epoch,
                        server,
                        electing,
                    }));
                }
                Some((Message::RecoveryUnavailable { node_id, .. }, _)) if received.is_empty() => {
                    anyhow::ensure!(node_id == expected, "unexpected node in reply");
                    return Ok(Fetched::NotReady);
                }
                _ => anyhow::bail!("peer is not reachable or snapshot was interrupted"),
            }
        }
    })
    .await?
}

/// One peer's `Probe`, for the startup decision.
pub(crate) async fn probe(addr: &str) -> anyhow::Result<NodeProbe> {
    tokio::time::timeout(STARTUP_PROBE, async {
        let mut stream = TcpStream::connect(addr).await?;
        write_frame(&mut stream, &Message::ProbeRequest).await?;
        match read_frame(&mut stream).await? {
            Some((Message::Probe(p), _)) => Ok(p),
            _ => anyhow::bail!("no probe answer"),
        }
    })
    .await?
}

/// Whether a peer's probe shows a cluster that has been running: a ready peer
/// that has seen an election or a request, or a peer recovering from one.
pub(crate) fn has_history(p: &NodeProbe) -> bool {
    match p.phase {
        NodePhase::Ready => p.epoch > 0 || p.requests > 0 || p.results > 0,
        NodePhase::Recovering => true,
        NodePhase::Muted | NodePhase::Starting => false,
    }
}
