//! Per-run metrics for the stress harness (`src/bin/stress.rs`): parsing the
//! client processes' CSVs and turning one run's requests into latency,
//! throughput, service-gap and failover figures. Kept in the library so the
//! definitions are unit-tested on synthetic requests.

use crate::stats::{mean, percentile};
use common::{Epoch, NodeId};
use std::collections::BTreeMap;

/// One request, as a `client --out` CSV records it.
#[derive(Clone, Debug, PartialEq)]
pub struct Req {
    pub client_id: Option<uuid::Uuid>,
    pub elapsed_ms: Option<f64>,
    /// Index of the client process within the run.
    pub client: u32,
    pub seq: u64,
    /// First send, Unix ms.
    pub sent_ms: u64,
    /// Arrival of the accepted PlanResponse, Unix ms; `None` = not answered.
    pub response_ms: Option<u64>,
    pub latency_ms: Option<f64>,
    pub served_by: Option<NodeId>,
    pub epoch: Option<Epoch>,
    pub attempts: u32,
    pub busy: u32,
}

impl Req {
    pub fn answered(&self) -> bool {
        self.response_ms.is_some()
    }
}

/// Parses one client process's CSV, locating columns by header name.
pub fn parse_client_csv(text: &str, client: u32) -> anyhow::Result<Vec<Req>> {
    let mut lines = text.lines();
    let header: Vec<&str> = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty CSV"))?
        .split(',')
        .collect();
    let col = |name: &str| {
        header
            .iter()
            .position(|h| *h == name)
            .ok_or_else(|| anyhow::anyhow!("CSV has no column {name}"))
    };
    let (seq, sent, resp, lat, served, epoch, attempts, busy) = (
        col("seq")?,
        col("sent_at_ms")?,
        col("response_at_ms")?,
        col("latency_ms")?,
        col("served_by")?,
        col("epoch")?,
        col("attempts")?,
        col("busy_replies")?,
    );
    fn opt<T: std::str::FromStr>(s: &str) -> anyhow::Result<Option<T>>
    where
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        Ok(if s.is_empty() { None } else { Some(s.parse()?) })
    }
    let mut out = Vec::new();
    let client_id = header.iter().position(|h| *h == "client_id");
    let elapsed = header.iter().position(|h| *h == "elapsed_ms");
    for (i, line) in lines.enumerate().filter(|(_, l)| !l.is_empty()) {
        let f: Vec<&str> = line.split(',').collect();
        anyhow::ensure!(f.len() == header.len(), "line {}: wrong field count", i + 2);
        out.push(Req {
            client_id: client_id.and_then(|i| f[i].parse().ok()),
            elapsed_ms: elapsed.and_then(|i| f[i].parse().ok()),
            client,
            seq: f[seq].parse()?,
            sent_ms: f[sent].parse()?,
            response_ms: opt(f[resp])?,
            latency_ms: opt(f[lat])?,
            served_by: opt(f[served])?,
            epoch: opt(f[epoch])?,
            attempts: f[attempts].parse()?,
            busy: f[busy].parse()?,
        });
    }
    Ok(out)
}

/// Latency of the answered requests in a set, ms. NaN fields when n = 0.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Latency {
    pub n: usize,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

pub fn latency<'a>(reqs: impl IntoIterator<Item = &'a Req>) -> Latency {
    let mut v: Vec<f64> = reqs.into_iter().filter_map(|r| r.latency_ms).collect();
    v.sort_by(|a, b| a.total_cmp(b));
    Latency {
        n: v.len(),
        mean: mean(&v),
        p50: percentile(&v, 0.50),
        p95: percentile(&v, 0.95),
        p99: percentile(&v, 0.99),
        max: percentile(&v, 1.0),
    }
}

/// An outage the harness injected during the run.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fault {
    /// The server elected when the failure was injected.
    pub victim: NodeId,
    /// When the victim acknowledged `InjectFailure`, Unix ms.
    pub inject_ms: u64,
    /// When the harness first saw the victim `Ready` again, Unix ms.
    pub ready_ms: Option<u64>,
}

/// Fault-run figures. Windows split requests by first-send time:
/// before = sent before the injection; during = from the injection until the
/// victim was ready again; after = from then on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FaultMetrics {
    /// Injection → first answer from a node other than the victim. NaN if none.
    pub failover_ms: f64,
    pub before: Latency,
    pub during: Latency,
    pub after: Latency,
    /// Requests sent during the window that were never answered.
    pub failed_during: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunMetrics {
    pub requests: usize,
    pub answered: usize,
    /// Requests that needed more than one attempt.
    pub retried: usize,
    pub busy: u64,
    /// First send to last answer (or last send if nothing was answered), ms.
    pub span_ms: f64,
    /// Answer rate, (n − 1) / (last answer − first answer) per second: the
    /// rate estimate from n events, without the n / (n − 1) bias of n / span.
    /// NaN with fewer than 2 answers.
    pub throughput: f64,
    pub latency: Latency,
    /// Answered requests per serving node.
    pub served: BTreeMap<NodeId, usize>,
    /// Longest stretch without any answer between the first and last answer, ms.
    pub max_gap_ms: f64,
    pub fault: Option<FaultMetrics>,
}

pub fn run_metrics(reqs: &[Req], fault: Option<&Fault>) -> RunMetrics {
    let start = reqs.iter().map(|r| r.sent_ms).min().unwrap_or(0);
    let mut answers: Vec<u64> = reqs.iter().filter_map(|r| r.response_ms).collect();
    answers.sort_unstable();
    let end = answers
        .last()
        .copied()
        .or_else(|| reqs.iter().map(|r| r.sent_ms).max())
        .unwrap_or(start);
    let span_ms = end.saturating_sub(start) as f64;
    let answered = answers.len();
    let mut served = BTreeMap::new();
    for r in reqs {
        if let (true, Some(n)) = (r.answered(), r.served_by) {
            *served.entry(n).or_default() += 1;
        }
    }
    let max_gap_ms = answers
        .windows(2)
        .map(|w| (w[1] - w[0]) as f64)
        .fold(0.0, f64::max);
    let fault = fault.map(|f| {
        let ready = f.ready_ms.unwrap_or(u64::MAX);
        let failover_ms = reqs
            .iter()
            .filter(|r| r.served_by.is_some_and(|n| n != f.victim))
            .filter_map(|r| r.response_ms)
            .filter(|&t| t >= f.inject_ms)
            .min()
            .map_or(f64::NAN, |t| (t - f.inject_ms) as f64);
        let during = || {
            reqs.iter()
                .filter(move |r| r.sent_ms >= f.inject_ms && r.sent_ms < ready)
        };
        FaultMetrics {
            failover_ms,
            before: latency(reqs.iter().filter(|r| r.sent_ms < f.inject_ms)),
            during: latency(during()),
            after: latency(reqs.iter().filter(|r| r.sent_ms >= ready)),
            failed_during: during().filter(|r| !r.answered()).count(),
        }
    });
    RunMetrics {
        requests: reqs.len(),
        answered,
        retried: reqs.iter().filter(|r| r.attempts > 1).count(),
        busy: reqs.iter().map(|r| u64::from(r.busy)).sum(),
        span_ms,
        throughput: match (answers.first(), answers.last()) {
            (Some(&a), Some(&b)) if b > a => (answered - 1) as f64 / ((b - a) as f64 / 1e3),
            _ => f64::NAN,
        },
        latency: latency(reqs),
        served,
        max_gap_ms,
        fault,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(seq: u64, sent: u64, latency: Option<u64>, node: NodeId) -> Req {
        Req {
            client_id: None,
            elapsed_ms: latency.map(|l| l as f64),
            client: 0,
            seq,
            sent_ms: sent,
            response_ms: latency.map(|l| sent + l),
            latency_ms: latency.map(|l| l as f64),
            served_by: latency.map(|_| node),
            epoch: latency.map(|_| 1),
            attempts: if latency.unwrap_or(1000) > 500 { 2 } else { 1 },
            busy: 0,
        }
    }

    #[test]
    fn parses_the_client_csv_by_header_name() {
        let text = "client_id,seq,sent_at_ms,response_at_ms,latency_ms,served_by,epoch,degraded,attempts,busy_replies,answered\n\
                    abc,1,1000,1004,4.250,2,3,false,1,0,true\n\
                    abc,2,1100,,,,,false,5,2,false\n";
        let v = parse_client_csv(text, 7).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].client, 7);
        assert_eq!(v[0].response_ms, Some(1004));
        assert_eq!(v[0].latency_ms, Some(4.25));
        assert_eq!((v[0].served_by, v[0].epoch), (Some(2), Some(3)));
        assert!(!v[1].answered());
        assert_eq!((v[1].attempts, v[1].busy), (5, 2));
        assert!(parse_client_csv("seq\n1\n", 0).is_err());
    }

    #[test]
    fn preserves_client_uuid_and_failed_request_elapsed_time() {
        let text = "client_id,seq,sent_at_ms,response_at_ms,latency_ms,served_by,epoch,degraded,attempts,busy_replies,answered,elapsed_ms\n00000000-0000-0000-0000-000000000001,2,1100,,,,,false,8,2,false,8025.5\n";
        let rows = parse_client_csv(text, 0).unwrap();
        assert_eq!(rows[0].client_id, Some(uuid::Uuid::from_u128(1)));
        assert_eq!(rows[0].elapsed_ms, Some(8025.5));
        assert!(!rows[0].answered());
        assert_eq!(rows[0].latency_ms, None);
    }

    #[test]
    fn throughput_span_and_gap() {
        // 10 requests 100 ms apart, 5 ms each; one unanswered.
        let mut v: Vec<Req> = (0..10)
            .map(|i| req(i, 1000 + i * 100, Some(5), 1))
            .collect();
        v[9] = req(9, 1900, None, 1);
        let m = run_metrics(&v, None);
        assert_eq!((m.requests, m.answered, m.retried), (10, 9, 1));
        assert_eq!(m.span_ms, 805.0); // first send 1000 → last answer 1805
                                      // 9 answers, 1005 → 1805: 8 intervals in 0.8 s.
        assert!((m.throughput - 10.0).abs() < 1e-9);
        assert_eq!(m.max_gap_ms, 100.0);
        assert_eq!(m.served.get(&1), Some(&9));
        assert_eq!(m.latency.p50, 5.0);
        assert!(m.fault.is_none());
    }

    #[test]
    fn fault_windows_and_failover() {
        // Node 1 serves until it is muted at t = 2000; requests sent in
        // (1000, 2000] wait for a retry and node 2 answers from t = 3100.
        // Node 1 is ready again at t = 5000.
        let mut v = Vec::new();
        for i in 0..60u64 {
            let sent = i * 100;
            let (lat, node) = match sent {
                s if s < 1000 => (5, 1),
                s if s <= 2000 => (3100 - s, 2),
                s if s < 3100 => (3100 - s + 5, 2),
                _ => (5, 2),
            };
            v.push(req(i, sent, Some(lat), node));
        }
        let fault = Fault {
            victim: 1,
            inject_ms: 2000,
            ready_ms: Some(5000),
        };
        let m = run_metrics(&v, Some(&fault));
        let f = m.fault.unwrap();
        assert_eq!(f.failover_ms, 1100.0);
        assert_eq!(f.before.n, 20); // sent 0..1900
        assert_eq!(f.during.n, 30); // sent 2000..4900
        assert_eq!(f.after.n, 10); // sent 5000..5900
        assert_eq!(f.failed_during, 0);
        assert!(
            f.before.max > 1000.0,
            "in-flight requests at the injection wait for a retry"
        );
        assert_eq!(f.after.max, 5.0);
        // No answer between node 1's last (sent 900, at t = 905) and t = 3100.
        assert_eq!(m.max_gap_ms, 2195.0);
    }

    #[test]
    fn failover_is_nan_when_no_survivor_answers() {
        let v = vec![req(0, 0, Some(5), 1), req(1, 3000, Some(5), 1)];
        let fault = Fault {
            victim: 1,
            inject_ms: 1000,
            ready_ms: None,
        };
        let f = run_metrics(&v, Some(&fault)).fault.unwrap();
        assert!(f.failover_ms.is_nan());
        assert_eq!(f.during.n, 1); // never ready again: everything after is "during"
        assert_eq!(f.after.n, 0);
    }
}
