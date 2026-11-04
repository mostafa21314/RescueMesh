//! Incremental repair uses an incarnation plus a monotonic result revision.
//! Cursors advance only after a page is installed, including during recovery.
use super::*;
use common::config::PeerConfig;

#[derive(Default)]
pub(super) struct Progress {
    pub(super) incarnation: Option<uuid::Uuid>,
    pub(super) stats: SyncProgress,
}

async fn page(
    shared: &Arc<Shared>,
    peer: &PeerConfig,
    stream: &mut Stream,
    recovery: bool,
) -> anyhow::Result<SyncReply> {
    let (incarnation, after_revision) = {
        let progress = shared.sync.lock().unwrap();
        progress
            .get(&peer.id)
            .map_or((None, 0), |p| (p.incarnation, p.stats.cursor))
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        let request = Message::SyncRequest {
            node_id: shared.cfg.node_id,
            incarnation,
            after_revision,
            recovery,
        };
        let bytes = write_frame(stream, &request).await?;
        shared.stats.sent.add("SyncRequest", bytes);
        match read_frame(stream).await? {
            Some((Message::SyncReply(reply), bytes)) if reply.node_id == peer.id => {
                shared.stats.received.add("SyncReply", bytes);
                Ok(*reply)
            }
            _ => anyhow::bail!("invalid sync reply from peer {}", peer.id),
        }
    })
    .await?
}

fn install(shared: &Arc<Shared>, peer: NodeId, reply: &SyncReply) -> bool {
    // The node may have become muted while the network operation was pending.
    if shared.state.lock().unwrap().phase == NodePhase::Muted {
        return false;
    }
    shared.peers.heard_from(peer);
    let mut repaired = 0;
    for record in &reply.records {
        if let Some(result) = &record.result {
            repaired += u64::from(on_replicate(
                shared,
                record.request_id,
                record.incident.clone(),
                result.clone().into(),
            ));
        }
    }
    let mut st = shared.state.lock().unwrap();
    if st.phase == NodePhase::Muted {
        return false;
    }
    if st.phase == NodePhase::Ready
        && !reply.electing
        && reply.server.is_none_or(|id| {
            id == shared.cfg.node_id || shared.cfg.peers.iter().any(|p| p.id == id)
        })
    {
        let actions = st
            .core
            .on_peer_view(Instant::now(), reply.epoch, reply.server);
        run_actions(shared, &mut st, actions);
    }
    let mut progress = shared.sync.lock().unwrap();
    let entry = progress.entry(peer).or_default();
    entry.incarnation = Some(reply.incarnation);
    entry.stats.node_id = peer;
    entry.stats.cursor = reply.next_revision;
    entry.stats.latest = reply.latest_revision;
    entry.stats.repaired_results += repaired;
    true
}

pub(super) async fn run(shared: Arc<Shared>, peer: PeerConfig) {
    let mut stream = None;
    loop {
        if shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        if shared.state.lock().unwrap().phase != NodePhase::Ready {
            stream = None;
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        let result = async {
            if stream.is_none() {
                stream = Some(Stream::connect(&peer.addr).await?);
            }
            page(&shared, &peer, stream.as_mut().unwrap(), false).await
        }
        .await;
        match result {
            Ok(reply) if reply.phase == NodePhase::Ready => {
                if install(&shared, peer.id, &reply) && reply.next_revision < reply.latest_revision
                {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            Ok(_) => {}
            Err(error) => {
                stream = None;
                let mut progress = shared.sync.lock().unwrap();
                let entry = progress.entry(peer.id).or_default();
                entry.stats.node_id = peer.id;
                entry.stats.failures += 1;
                tracing::debug!(node = shared.cfg.node_id, peer = peer.id, %error, "incremental repair retry");
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A delta is validated by the source incarnation and an installed revision,
/// rather than a time window. Unknown/restarted sources restart at revision 0.
/// Partial progress survives a timeout so large repairs do not start over.
pub(super) async fn fetch(
    shared: &Arc<Shared>,
    peer: &PeerConfig,
) -> anyhow::Result<crate::recovery::Fetched> {
    use crate::recovery::{Fetched, Snapshot};
    tokio::time::timeout(crate::recovery::FETCH_TIMEOUT, async {
        let mut stream = Stream::connect(&peer.addr).await?;
        let mut records = Vec::new();
        let mut bytes = 0;
        let mut first = true;
        loop {
            let reply = page(shared, peer, &mut stream, first).await?;
            first = false;
            if reply.phase != NodePhase::Ready {
                return Ok(Fetched::Unavailable {
                    has_state: reply.has_authoritative_state,
                });
            }
            anyhow::ensure!(install(shared, peer.id, &reply), "node muted during repair");
            bytes += serde_json::to_vec(&reply)?.len() as u64;
            records.extend(reply.records);
            if reply.next_revision == reply.latest_revision {
                return Ok(Fetched::Snapshot(Snapshot {
                    peer: peer.id,
                    records,
                    epoch: reply.epoch,
                    server: reply.server,
                    electing: reply.electing,
                    bytes,
                }));
            }
        }
    })
    .await?
}
