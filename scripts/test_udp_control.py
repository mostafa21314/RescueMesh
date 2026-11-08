"""Python/Rust UDP interoperability check. Build release binaries before running.

    python -m unittest discover -s scripts -p test_udp_control.py -v
"""
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from recovery_bench import client_toml, node_toml
from udp_rr import PAYLOAD, call

ROOT = Path(__file__).resolve().parent.parent
BIN = Path(os.environ.get('RESCUEMESH_BIN_DIR', str(ROOT / 'target' / 'release'))).resolve()
EXE = '.exe' if os.name == 'nt' else ''


class Interoperability(unittest.TestCase):
    def test_python_controls_rust_cluster_and_receives_fragmented_status(self):
        self.assertTrue((BIN / f'cloud-node{EXE}').is_file(), 'Run cargo build --release first')
        children = []
        reserved = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in range(3)]
        try:
            for sock in reserved:
                sock.bind(('127.0.0.1', 0))
            addrs = [sock.getsockname() for sock in reserved]
            with tempfile.TemporaryDirectory(prefix='rescuemesh-udp-') as directory:
                directory = Path(directory)
                try:
                    for i, sock in enumerate(reserved):
                        cfg = directory / f'node{i+1}.toml'
                        cfg.write_text(node_toml(i, {'on_start': False}, addrs), encoding='utf-8')
                        sock.close()
                        children.append(subprocess.Popen(
                            [str(BIN / f'cloud-node{EXE}'), '--config', str(cfg), '--log-file', str(directory / f'node{i+1}.log')],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            env={**os.environ, 'RUST_LOG': 'info'}))
                    for addr in addrs:
                        deadline = time.monotonic() + 10
                        while True:
                            try:
                                self.assertEqual(call(addr, {'type': 'ProbeRequest'}, .5)['type'], 'Probe')
                                break
                            except OSError:
                                if time.monotonic() >= deadline:
                                    raise
                    cfg = directory / 'client.toml'
                    cfg.write_text(client_toml(addrs), encoding='utf-8')
                    subprocess.run([str(BIN / f'client{EXE}'), '--config', str(cfg),
                                    '--count', '20', '--rate', '50', '--complexity', '1',
                                    '--out', str(directory / 'requests.csv')],
                                   check=True, timeout=20, stdout=subprocess.DEVNULL)
                    status = call(addrs[0], {'type': 'StatusRequest'}, 3)
                    self.assertEqual(status['type'], 'Status')
                    self.assertEqual(status['protocol_version'], 3)
                    self.assertIn('protocol_version=3', (directory / 'node1.log').read_text(encoding='utf-8'))
                    self.assertEqual(call(addrs[0], {'type': 'SetLoad', 'threads': 1,
                                                   'duration_ms': 100}, 3)['type'], 'LoadAccepted')
                    self.assertEqual(call(addrs[0], {'type': 'SetLoad', 'threads': 0,
                                                   'duration_ms': 0}, 3)['type'], 'LoadAccepted')
                    self.assertEqual(call(addrs[0], {'type': 'InjectFailure',
                                                   'duration_ms': 20_001}, 3)['type'], 'FailureRejected')
                    self.assertEqual(call(addrs[0], {'type': 'InjectFailure',
                                                   'duration_ms': 100}, 3)['type'], 'FailureAccepted')
                    time.sleep(.2)
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            recovered = call(addrs[0], {'type': 'StatusRequest'}, .5)
                            if recovered['phase'] == 'ready':
                                self.assertEqual(recovered['results'], 20)
                                # Recovery telemetry makes this response span
                                # multiple UDP datagrams.
                                self.assertGreater(len(json.dumps(recovered, separators=(',', ':')).encode()), PAYLOAD)
                                break
                        except OSError:
                            pass
                        if time.monotonic() >= deadline:
                            self.fail('muted node did not recover')
                        time.sleep(.05)
                finally:
                    for process in children:
                        if process.poll() is None:
                            process.kill()
                        process.wait(timeout=5)
        finally:
            for sock in reserved:
                sock.close()


if __name__ == '__main__':
    unittest.main()
