//! Pull a bounded, fixed snapshot over a dedicated connection (not the peer backlog).
use common::rr::Stream;
use common::rr::{read_frame, write_frame};
use common::*;
use std::time::Duration;

/// Per-transfer limits: a snapshot that takes longer or is larger is rejected.
pub(crate) const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECORDS: usize = 1_000_000;
/// How long a starting node waits for each peer's `Probe`.
pub(crate) const STARTUP_PROBE: Duration = Duration::from_millis(300);

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
    /// The peer is starting or recovering itself: nothing authoritative to
    /// give. `has_state` is false for a process that just started empty.
    Unavailable {
        has_state: bool,
    },
}

pub(crate) async fn fetch(
    addr: &str,
    me: NodeId,
    expected: NodeId,
    since_ms: Option<u64>,
) -> anyhow::Result<Fetched> {
    tokio::time::timeout(FETCH_TIMEOUT, async {
        let mut stream = Stream::connect(addr).await?;
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
                Some((
                    Message::RecoveryUnavailable {
                        node_id, has_state, ..
                    },
                    _,
                )) if received.is_empty() => {
                    anyhow::ensure!(node_id == expected, "unexpected node in reply");
                    return Ok(Fetched::Unavailable { has_state });
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
        let mut stream = Stream::connect(addr).await?;
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
