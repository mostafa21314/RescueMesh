"""Small Python client for the RMR1 UDP control protocol (standard library).

The Rust implementation is in crates/common/src/rr.rs. This client handles a
single request/reply, including fragmentation, ACK loss and duplicate packets.
"""
import json
import socket
import struct
import time
import uuid

HEADER = struct.Struct('!4sB16sQII')
PAYLOAD = 1200 - HEADER.size
MAX_MESSAGE = 1 << 20


def call(addr, message, timeout=2.0):
    family, kind, proto, _, peer = socket.getaddrinfo(*addr, type=socket.SOCK_DGRAM)[0]
    body = json.dumps(message, separators=(',', ':')).encode()
    if len(body) > MAX_MESSAGE:
        raise ValueError('UDP message exceeds 1 MiB')
    session = uuid.uuid4().bytes
    packet = lambda kind, seq=0, index=0, total=0, data=b'': HEADER.pack(
        b'RMR1', kind, session, seq, index, total) + data
    chunks = [body[i:i + PAYLOAD] for i in range(0, len(body), PAYLOAD)]
    acked, fragments = set(), {}
    welcome, response_size = False, None
    deadline = time.monotonic() + timeout
    next_send, backoff = 0.0, .03
    with socket.socket(family, kind, proto) as sock:
        sock.connect(peer)
        while time.monotonic() < deadline:
            now = time.monotonic()
            if now >= next_send:
                if not welcome:
                    sock.send(packet(2))
                else:
                    first = next((i for i in range(len(chunks)) if i not in acked), len(chunks))
                    for i in range(first, min(first + 32, len(chunks))):
                        if i not in acked:
                            sock.send(packet(0, index=i, total=len(body), data=chunks[i]))
                next_send = now + backoff
                backoff = min(.5, backoff * 2)
            sock.settimeout(max(.001, min(next_send, deadline) - time.monotonic()))
            try:
                data = sock.recv(1201)
            except socket.timeout:
                continue
            if not HEADER.size <= len(data) <= 1200:
                continue
            magic, typ, sid, seq, index, total = HEADER.unpack_from(data)
            if magic != b'RMR1' or sid != session:
                continue
            if typ == 3 and len(data) == HEADER.size:
                welcome, next_send, backoff = True, 0, .03
            elif typ in (4, 5):
                raise ConnectionError('UDP session expired; server may have restarted')
            elif typ == 1 and seq == 0:
                acked.add(index)
                # Advance the fragment window immediately after its ACKs.
                if len(acked) % 32 == 0:
                    next_send, backoff = 0, .03
            elif typ == 0 and seq == 0 and 0 < total <= MAX_MESSAGE:
                count = (total + PAYLOAD - 1) // PAYLOAD
                if index >= count or len(data) - HEADER.size != min(PAYLOAD, total - index * PAYLOAD):
                    continue
                if response_size is not None and response_size != total:
                    continue
                response_size = total
                fragments[index] = data[HEADER.size:]
                sock.send(packet(1, index=index))
                if len(fragments) == count:
                    reply = json.loads(b''.join(fragments[i] for i in range(count)))
                    sock.send(packet(5))
                    return reply
    raise TimeoutError(f'No UDP reply from {addr} within {timeout}s')
