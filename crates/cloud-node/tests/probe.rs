//! Bullet 6: the stress harness samples nodes with `Probe` and reads election
//! time, CPU and memory from `Status`. The probe must agree with `Status` on
//! the fields they share.

mod support;

use client::{query_probe, query_status, synth_incident, Client};
use common::config::CampusParams;
use common::msg::NodePhase;
use common::rng::Rng;
use std::time::Duration;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_matches_status_and_status_times_the_election() {
    limit(async {
        const N: usize = 20;
        let (nodes, addrs) = start_cluster().await;
        let client = Client::new(client_cfg(&addrs, 2_000, 5));
        let mut rng = Rng::new(11);
        for _ in 0..N {
            let incident = synth_incident(&mut rng, CampusParams::default().zones(), "it");
            assert!(client.submit(incident, 2).await.answered());
        }
        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, N, Duration::from_secs(20)).await;
        // Past the CPU sampler's first 250 ms tick, which also reads memory.
        tokio::time::sleep(Duration::from_millis(600)).await;

        let timeout = Duration::from_secs(2);
        let mut recorded = 0;
        for addr in &addrs {
            let p = query_probe(addr, timeout).await.unwrap();
            let s = query_status(addr, timeout).await.unwrap();
            assert_eq!(
                (p.node_id, p.phase, p.epoch, p.server),
                (s.node_id, s.phase, s.epoch, s.server)
            );
            assert_eq!((p.requests, p.results), (s.requests, s.results));
            assert_eq!(
                (p.phase, p.results, p.queue_len, p.running),
                (NodePhase::Ready, N, 0, 0)
            );
            assert!(p.load_score <= 1000 && (0.0..=1.0).contains(&p.cpu_load));
            assert!(p.rss_bytes > 0 && s.rss_bytes > 0, "memory not sampled");
            // One election (the first request's); each node that decided it
            // logged it, taking about one bid round.
            assert_eq!(s.epoch, 1);
            assert_eq!(s.elections_recorded > 0, s.election_time_us > 0);
            recorded += s.elections_recorded;
        }
        assert!(recorded >= 2, "at least the two deciding nodes record it");
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await;
}
