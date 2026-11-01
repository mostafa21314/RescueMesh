//! Message and byte counters, keyed by message type.

use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default, Debug)]
pub struct Counters(Mutex<BTreeMap<String, (u64, u64)>>);

impl Counters {
    pub fn add(&self, kind: &str, bytes: usize) {
        let mut m = self.0.lock().unwrap();
        let e = m.entry(kind.to_string()).or_default();
        e.0 += 1;
        e.1 += bytes as u64;
    }

    pub fn snapshot(&self) -> BTreeMap<String, (u64, u64)> {
        self.0.lock().unwrap().clone()
    }
}
