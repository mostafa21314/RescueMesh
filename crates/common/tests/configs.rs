//! Every config file shipped in config/ parses and validates.

use common::config::{ClientConfig, NodeConfig};
use std::path::PathBuf;

fn config_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config")
}

#[test]
fn shipped_configs_load() {
    for dir in ["", "local"] {
        let d = config_dir().join(dir);
        for i in 1..=3 {
            let cfg = NodeConfig::load(d.join(format!("node{i}.toml"))).unwrap();
            assert_eq!(cfg.node_id, i);
            assert_eq!(cfg.peers.len(), 2);
            assert!(cfg.peers.iter().all(|p| p.id != i));
        }
        let c = ClientConfig::load(d.join("client.toml")).unwrap();
        assert_eq!(c.nodes.len(), 3);
    }
}
