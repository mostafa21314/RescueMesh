//! Exercise remote load controls over the actual UDP protocol.
mod support;

use client::{set_load, synth_incident, Client};
use common::rng::Rng;
use std::time::Duration;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_can_be_replaced_stopped_and_rejected_without_stopping_service() {
    limit(async {
        let (nodes, addrs) = start_cluster().await;
        let timeout = Duration::from_secs(2);
        assert_eq!(set_load(&addrs[0], 1, 2000, timeout).await.unwrap(), 1);
        assert!(set_load(&addrs[0], 1, 0, timeout).await.is_err());
        assert!(set_load(&addrs[0], usize::MAX, 100, timeout).await.is_err());
        assert!(set_load(&addrs[0], 1, 3_600_001, timeout).await.is_err());
        assert_eq!(set_load(&addrs[0], 1, 100, timeout).await.unwrap(), 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(set_load(&addrs[0], 0, 0, timeout).await.unwrap(), 1);
        let client = Client::new(client_cfg(&addrs, 1000, 5));
        let mut rng = Rng::new(19);
        assert!(client
            .submit(synth_incident(&mut rng, 256, "load-control"), 1)
            .await
            .answered());
        wait_converged(&nodes.iter().collect::<Vec<_>>(), 1, Duration::from_secs(5)).await;
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}
