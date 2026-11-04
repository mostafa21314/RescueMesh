//! Ordered, bounded message delivery for Phase 1 request/reply over UDP.
//! See docs/udp-phase1.md for the wire format, limits and failure semantics.
use crate::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub const DATAGRAM: usize = 1200;
pub const HEADER: usize = 37;
pub const PAYLOAD: usize = DATAGRAM - HEADER;
pub const MAX_MESSAGE: usize = 1 << 20;
const WINDOW: usize = 32;
const MAX_SESSIONS: usize = 1024;
const IDLE: Duration = Duration::from_secs(60);
const DATA: u8 = 0;
const ACK: u8 = 1;
const HELLO: u8 = 2;
const WELCOME: u8 = 3;
const RESET: u8 = 4;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TransportStats {
    pub datagrams_sent: u64,
    pub bytes_sent: u64,
    pub datagrams_received: u64,
    pub retransmissions: u64,
    pub duplicate_fragments: u64,
    pub invalid_datagrams: u64,
    pub delivery_timeouts: u64,
}
static SENT: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static RECEIVED: AtomicU64 = AtomicU64::new(0);
static RETRIES: AtomicU64 = AtomicU64::new(0);
static DUPLICATES: AtomicU64 = AtomicU64::new(0);
static INVALID: AtomicU64 = AtomicU64::new(0);
static TIMEOUTS: AtomicU64 = AtomicU64::new(0);

/// Process-wide counters (each deployed cloud node is a separate process).
pub fn stats() -> TransportStats {
    let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
    TransportStats {
        datagrams_sent: get(&SENT),
        bytes_sent: get(&BYTES),
        datagrams_received: get(&RECEIVED),
        retransmissions: get(&RETRIES),
        duplicate_fragments: get(&DUPLICATES),
        invalid_datagrams: get(&INVALID),
        delivery_timeouts: get(&TIMEOUTS),
    }
}

fn packet(kind: u8, id: Uuid, seq: u64, index: u32, total: u32, body: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(HEADER + body.len());
    p.extend_from_slice(b"RMR1");
    p.push(kind);
    p.extend_from_slice(id.as_bytes());
    p.extend_from_slice(&seq.to_be_bytes());
    p.extend_from_slice(&index.to_be_bytes());
    p.extend_from_slice(&total.to_be_bytes());
    p.extend_from_slice(body);
    p
}

async fn send(socket: &UdpSocket, peer: SocketAddr, p: &[u8]) -> io::Result<()> {
    socket.send_to(p, peer).await?;
    SENT.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(p.len() as u64, Ordering::Relaxed);
    Ok(())
}

#[derive(Default)]
struct Acks {
    seq: u64,
    fragments: HashSet<u32>,
    welcome: bool,
    reset: bool,
}

#[derive(Default)]
struct Assembly {
    expected: u64,
    total: usize,
    fragments: BTreeMap<u32, Vec<u8>>,
}

struct State {
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
    id: Uuid,
    incoming: mpsc::Sender<(Message, usize)>,
    assembly: Mutex<Assembly>,
    acks: Mutex<Acks>,
    changed: Notify,
}

struct Task(JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct ReadHalf {
    rx: mpsc::Receiver<(Message, usize)>,
    state: Arc<State>,
    _task: Option<Arc<Task>>,
}
pub struct WriteHalf {
    state: Arc<State>,
    seq: u64,
    _task: Option<Arc<Task>>,
}
pub struct Stream {
    reader: ReadHalf,
    writer: WriteHalf,
}

fn session(socket: Arc<UdpSocket>, peer: SocketAddr, id: Uuid) -> Stream {
    let (tx, rx) = mpsc::channel(64);
    let state = Arc::new(State {
        socket,
        peer,
        id,
        incoming: tx,
        assembly: Mutex::new(Assembly::default()),
        acks: Mutex::new(Acks::default()),
        changed: Notify::new(),
    });
    Stream {
        reader: ReadHalf {
            rx,
            state: state.clone(),
            _task: None,
        },
        writer: WriteHalf {
            state,
            seq: 0,
            _task: None,
        },
    }
}

type Sessions = HashMap<(SocketAddr, Uuid), Weak<State>>;

pub struct Listener {
    addr: SocketAddr,
    accepted: tokio::sync::Mutex<mpsc::Receiver<(Stream, SocketAddr)>>,
    _task: Task,
}

impl Listener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let addr = socket.local_addr()?;
        let (tx, rx) = mpsc::channel(128);
        Ok(Self {
            addr,
            accepted: tokio::sync::Mutex::new(rx),
            _task: Task(tokio::spawn(receive(socket, HashMap::new(), Some(tx)))),
        })
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }
    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        self.accepted
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "UDP listener stopped"))
    }
}

impl Stream {
    pub async fn connect(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let peer = lookup_host(addr)
            .await?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no UDP address"))?;
        let bind = if peer.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = Arc::new(UdpSocket::bind(bind).await?);
        let id = Uuid::new_v4();
        let mut stream = session(socket.clone(), peer, id);
        let state = stream.writer.state.clone();
        let entries = HashMap::from([((peer, id), Arc::downgrade(&state))]);
        let task = Arc::new(Task(tokio::spawn(receive(socket, entries, None))));
        stream.reader._task = Some(task.clone());
        stream.writer._task = Some(task);
        let hello = packet(HELLO, id, 0, 0, 0, &[]);
        for attempt in 0..5 {
            send(&state.socket, peer, &hello).await?;
            let until = tokio::time::Instant::now() + Duration::from_millis(30 << attempt);
            loop {
                let changed = state.changed.notified();
                if state.acks.lock().unwrap().welcome {
                    return Ok(stream);
                }
                if tokio::time::timeout_at(until, changed).await.is_err() {
                    break;
                }
            }
        }
        TIMEOUTS.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "UDP peer did not answer handshake",
        ))
    }
    pub fn into_split(self) -> (ReadHalf, WriteHalf) {
        (self.reader, self.writer)
    }
}

async fn receive(
    socket: Arc<UdpSocket>,
    mut sessions: Sessions,
    accepted: Option<mpsc::Sender<(Stream, SocketAddr)>>,
) {
    let mut buf = [0u8; DATAGRAM + 1];
    loop {
        let (n, peer) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            // Windows can report ICMP port-unreachable on an unconnected UDP socket.
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        RECEIVED.fetch_add(1, Ordering::Relaxed);
        if !(HEADER..=DATAGRAM).contains(&n) || &buf[..4] != b"RMR1" {
            INVALID.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let kind = buf[4];
        let id = Uuid::from_slice(&buf[5..21]).expect("fixed UUID length");
        let seq = u64::from_be_bytes(buf[21..29].try_into().unwrap());
        let index = u32::from_be_bytes(buf[29..33].try_into().unwrap());
        let total = u32::from_be_bytes(buf[33..37].try_into().unwrap()) as usize;
        if kind > RESET || (kind != DATA && (n != HEADER || total != 0)) {
            INVALID.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let key = (peer, id);
        let mut state = sessions.get(&key).and_then(Weak::upgrade);
        if kind == HELLO && seq == 0 && index == 0 && state.is_none() {
            if let Some(tx) = &accepted {
                sessions.retain(|_, s| s.strong_count() > 0);
                if sessions.len() >= MAX_SESSIONS {
                    continue;
                }
                let stream = session(socket.clone(), peer, id);
                let s = stream.writer.state.clone();
                if tx.try_send((stream, peer)).is_err() {
                    continue;
                }
                sessions.insert(key, Arc::downgrade(&s));
                state = Some(s);
            }
        }
        let Some(state) = state else {
            // No implicit recreation from DATA: delayed datagrams cannot execute
            // an old operation in a fresh session after its cache was discarded.
            if kind == DATA && accepted.is_some() {
                let _ = send(&socket, peer, &packet(RESET, id, seq, 0, 0, &[])).await;
            }
            continue;
        };
        match kind {
            HELLO => {
                let _ = send(&socket, peer, &packet(WELCOME, id, 0, 0, 0, &[])).await;
            }
            ACK | WELCOME | RESET => {
                let mut a = state.acks.lock().unwrap();
                if kind == ACK && a.seq == seq {
                    a.fragments.insert(index);
                }
                if kind == WELCOME {
                    a.welcome = true;
                }
                if kind == RESET {
                    a.reset = true;
                }
                drop(a);
                state.changed.notify_one();
            }
            DATA if accept_fragment(&state, seq, index, total, &buf[HEADER..n]) => {
                let _ = send(&socket, peer, &packet(ACK, id, seq, index, 0, &[])).await;
            }
            _ => {}
        }
    }
}

fn accept_fragment(state: &State, seq: u64, index: u32, total: usize, payload: &[u8]) -> bool {
    let count = total.div_ceil(PAYLOAD);
    let offset = index as usize * PAYLOAD;
    if total == 0
        || total > MAX_MESSAGE
        || index as usize >= count
        || payload.len() != (total - offset).min(PAYLOAD)
    {
        INVALID.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    let mut a = state.assembly.lock().unwrap();
    if seq < a.expected {
        DUPLICATES.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    if seq != a.expected || (a.total != 0 && a.total != total) {
        return false;
    }
    a.total = total;
    if a.fragments.insert(index, payload.to_vec()).is_some() {
        DUPLICATES.fetch_add(1, Ordering::Relaxed);
    }
    if a.fragments.len() == count {
        let body: Vec<u8> = a.fragments.values().flatten().copied().collect();
        let Ok(message) = serde_json::from_slice(&body) else {
            INVALID.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        // Withhold the final ACK until the bounded application queue accepts
        // the message; retrying the last fragment then retries this enqueue.
        if state
            .incoming
            .try_send((message, total + count * HEADER))
            .is_err()
        {
            return false;
        }
        a.expected += 1;
        a.total = 0;
        a.fragments.clear();
    }
    true
}

pub trait ReceiveMessage {
    fn reader(&mut self) -> &mut ReadHalf;
}
pub trait SendMessage {
    fn writer(&mut self) -> &mut WriteHalf;
}
impl ReceiveMessage for Stream {
    fn reader(&mut self) -> &mut ReadHalf {
        &mut self.reader
    }
}
impl ReceiveMessage for ReadHalf {
    fn reader(&mut self) -> &mut ReadHalf {
        self
    }
}
impl SendMessage for Stream {
    fn writer(&mut self) -> &mut WriteHalf {
        &mut self.writer
    }
}
impl SendMessage for WriteHalf {
    fn writer(&mut self) -> &mut WriteHalf {
        self
    }
}

pub async fn read_frame(r: &mut impl ReceiveMessage) -> io::Result<Option<(Message, usize)>> {
    let r = r.reader();
    // Also bounds abandoned server sessions and partial-message retention.
    match tokio::time::timeout(IDLE, r.rx.recv()).await {
        Ok(m) => Ok(m),
        Err(_) => {
            r.state.acks.lock().unwrap().reset = true;
            Ok(None)
        }
    }
}

pub async fn write_frame(w: &mut impl SendMessage, msg: &Message) -> io::Result<usize> {
    let body = serde_json::to_vec(msg)?;
    if body.len() > MAX_MESSAGE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "UDP message exceeds 1 MiB",
        ));
    }
    let w = w.writer();
    let s = &w.state;
    let count = body.len().div_ceil(PAYLOAD);
    let seq = w.seq;
    {
        let mut a = s.acks.lock().unwrap();
        a.seq = seq;
        a.fragments.clear();
    }
    let mut sent = 0;
    let mut base = 0;
    while base < count {
        let end = (base + WINDOW).min(count);
        let mut complete = false;
        for attempt in 0..6 {
            for i in base..end {
                if s.acks.lock().unwrap().fragments.contains(&(i as u32)) {
                    continue;
                }
                let p = packet(
                    DATA,
                    s.id,
                    seq,
                    i as u32,
                    body.len() as u32,
                    &body[i * PAYLOAD..((i + 1) * PAYLOAD).min(body.len())],
                );
                send(&s.socket, s.peer, &p).await?;
                sent += p.len();
                if attempt > 0 {
                    RETRIES.fetch_add(1, Ordering::Relaxed);
                }
            }
            let jitter = (s.id.as_u64_pair().0 ^ seq) % 11;
            let until = tokio::time::Instant::now()
                + Duration::from_millis((30u64 << attempt).min(500) + jitter);
            loop {
                let changed = s.changed.notified();
                {
                    let a = s.acks.lock().unwrap();
                    if a.reset {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "UDP session expired",
                        ));
                    }
                    complete = (base..end).all(|i| a.fragments.contains(&(i as u32)));
                }
                if complete || tokio::time::timeout_at(until, changed).await.is_err() {
                    break;
                }
            }
            if complete {
                break;
            }
        }
        if !complete {
            s.acks.lock().unwrap().reset = true;
            TIMEOUTS.fetch_add(1, Ordering::Relaxed);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "UDP acknowledgement retries exhausted",
            ));
        }
        base = end;
    }
    w.seq += 1;
    Ok(sent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fragments_survive_lost_data_lost_acks_duplicates_and_reordering() {
        tokio::time::timeout(Duration::from_secs(20), async {
            let listener = Listener::bind("127.0.0.1:0").await.unwrap();
            let server = listener.local_addr().unwrap();
            let proxy = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let proxy_addr = proxy.local_addr().unwrap();
            let relay = tokio::spawn(async move {
                let mut client = None;
                let mut n = 0usize;
                let mut buf = [0; DATAGRAM + 1];
                let mut delayed: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
                let mut tick = tokio::time::interval(Duration::from_millis(5));
                loop {
                    tokio::select! {
                        r = proxy.recv_from(&mut buf) => {
                            let (len, from) = r.unwrap();
                            let to = if from == server { client.unwrap() }
                                else { client = Some(from); server };
                            n += 1;
                            if n.is_multiple_of(7) { continue; } // data and ACK loss
                            if n.is_multiple_of(5) { delayed.push((to, buf[..len].to_vec())); continue; }
                            proxy.send_to(&buf[..len], to).await.unwrap();
                            if n.is_multiple_of(11) { proxy.send_to(&buf[..len], to).await.unwrap(); }
                        }
                        _ = tick.tick() => {
                            for (to, p) in delayed.drain(..).rev() { proxy.send_to(&p, to).await.unwrap(); }
                        }
                    }
                }
            });
            let relay = Task(relay);
            let message = Message::FailureRejected { reason: "fragmented payload ".repeat(7000) };
            let expected = message.clone();
            let serve = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (got, _) = read_frame(&mut stream).await.unwrap().unwrap();
                assert_eq!(got, expected);
                write_frame(&mut stream, &got).await.unwrap();
                let (next, _) = read_frame(&mut stream).await.unwrap().unwrap();
                assert_eq!(next, Message::ProbeRequest, "duplicates must not be delivered twice");
                // Keep the socket alive until the final request ACK has left.
                tokio::time::sleep(Duration::from_millis(200)).await;
            });
            let mut stream = Stream::connect(proxy_addr).await.unwrap();
            write_frame(&mut stream, &message).await.unwrap();
            assert_eq!(read_frame(&mut stream).await.unwrap().unwrap().0, message);
            write_frame(&mut stream, &Message::ProbeRequest).await.unwrap();
            serve.await.unwrap();
            drop(relay);
            assert!(stats().retransmissions > 0);
        }).await.expect("loss test exceeded deadline");
    }

    #[tokio::test]
    async fn malformed_packets_do_not_stop_listener_and_delivery_is_bounded() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.send_to(b"garbage", addr).await.unwrap();
        socket.send_to(&vec![1; DATAGRAM + 1], addr).await.unwrap();
        let mut client = Stream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        write_frame(&mut client, &Message::StatusRequest)
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut server).await.unwrap().unwrap().0,
            Message::StatusRequest
        );
        assert!(write_frame(
            &mut client,
            &Message::FailureRejected {
                reason: "x".repeat(MAX_MESSAGE)
            }
        )
        .await
        .is_err());
        drop(listener);
        drop(server);
        assert!(tokio::time::timeout(
            Duration::from_secs(3),
            write_frame(&mut client, &Message::ProbeRequest)
        )
        .await
        .unwrap()
        .is_err());
    }
}
