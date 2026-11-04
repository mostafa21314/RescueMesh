"""Unit checks and a six-agent real-binary rehearsal. Build release binaries first."""
import csv
import contextlib
import gzip
import json
import math
import os
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path

from phase1_study import arguments, api, case_id, idle_agreed, make_cases, preflight
from study_agent import Agent, Handler, Server
from study_metrics import analyze_requests, ci95, distribution

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get('RESCUEMESH_BIN_DIR', ROOT / 'target/release'))


class Statistics(unittest.TestCase):
    def test_sample_sd_and_independent_run_ci(self):
        self.assertAlmostEqual(distribution([2, 4, 4, 4, 5, 5, 7, 9])['sd_ms'], 2.138089935, places=8)
        self.assertIsNone(distribution([4])['sd_ms'])
        self.assertIsNone(distribution([])['p99_ms'])
        self.assertAlmostEqual(ci95([8, 9, 10, 11, 12])['ci95_half'], 1.963, places=3)

    def test_default_grid_and_matched_pairs(self):
        args = arguments(['plan'])
        cases = make_cases(args, 'normal')
        self.assertEqual(len(cases), 300)
        self.assertEqual(sum(3*c['processes']*math.ceil(c['duration']*c['rate']) for c in cases), 33_759_000)
        for a, b in zip(cases[::2], cases[1::2]):
            self.assertEqual(a['seed'], b['seed'])
            self.assertEqual({a['mode'], b['mode']}, {'on_demand', 'per_request'})
        fault = make_cases(args, 'resilience', [(1, 30)])
        self.assertEqual(len(fault), 40)
        self.assertEqual(len({c['id'] for c in fault}), 40)

    def test_missing_and_failed_requests_stay_in_denominator(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            path = directory / 'native.csv.gz'
            text = ('client_id,seq,sent_at_ms,response_at_ms,latency_ms,served_by,epoch,degraded,attempts,busy_replies,answered,elapsed_ms,scheduled_at_ms,dispatch_lag_ms\n'
                    '00000000-0000-0000-0000-000000000001,0,1000,1010,10,1,0,false,1,0,true,10,1000,0\n'
                    '00000000-0000-0000-0000-000000000001,1,2000,,,,0,false,8,1,false,8000,2000,0\n')
            with gzip.open(path, 'wt') as stream:
                stream.write(text)
            row, ids = analyze_requests(directory, {'rate': 1, 'duration': 2, 'processes': 1},
                                        [(1, 0, path, 0)], 1000, {})
            self.assertEqual(row['requests'], 6)
            self.assertEqual(row['failed'], 5)
            self.assertEqual(row['missing_records'], 4)
            self.assertEqual(row['failed_elapsed_p95_ms'], 8000)
            self.assertFalse(row['load_generation_valid'])
            self.assertFalse(row['uuid_valid'])
            self.assertEqual(len(ids), 1)


class SixAgentRehearsal(unittest.TestCase):
    def test_both_modes_all_interventions_and_resume(self):
        self.assertTrue((BIN / ('cloud-node.exe' if os.name == 'nt' else 'cloud-node')).is_file(), 'Build release binaries first')
        previous = os.environ.get('RESCUEMESH_LAB_TOKEN')
        os.environ['RESCUEMESH_LAB_TOKEN'] = 'isolated-integration-test-token'
        servers, reservations = [], []
        try:
            with contextlib.ExitStack() as cleanup:
                tmp = cleanup.enter_context(tempfile.TemporaryDirectory(prefix='rescuemesh-six-'))
                tmp = Path(tmp)
                for i in range(6):
                    agent = Agent('server' if i < 3 else 'client', tmp / str(i), BIN, os.environ['RESCUEMESH_LAB_TOKEN'])
                    server = Server(('127.0.0.1', 0), Handler)
                    server.agent = agent
                    threading.Thread(target=server.serve_forever, daemon=True).start()
                    servers.append(server)
                    # LIFO cleanup closes child log handles before temp files.
                    cleanup.callback(agent.close)
                for _ in range(3):
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.bind(('127.0.0.1', 0))
                    reservations.append(sock)
                addresses = [f'127.0.0.1:{s.server_port}' for s in servers]
                lab = {'nodes': [{'host': '127.0.0.1', 'port': reservations[i].getsockname()[1], 'agent': addresses[i]} for i in range(3)],
                       'clients': addresses[3:]}
                self.assertEqual(len(preflight(lab, True)['agents']), 6)
                os.environ['RESCUEMESH_LAB_TOKEN'] = 'wrong-token'
                with self.assertRaises(RuntimeError):
                    api(addresses[0], '/health')
                os.environ['RESCUEMESH_LAB_TOKEN'] = 'isolated-integration-test-token'
                # A separately owned UDP listener must not be adopted or killed.
                port = reservations[0].getsockname()[1]
                with self.assertRaises(RuntimeError):
                    api(addresses[0], '/node/start', {'config': f'listen="0.0.0.0:{port}"'})
                for sock in reservations:
                    sock.close()
                cfg = tmp / 'lab.json'
                cfg.write_text(json.dumps(lab), encoding='utf-8')
                out = ROOT / 'results/phase1/six-pc-local-smoke'
                if out.exists():
                    import uuid
                    out = out.with_name(out.name + '-' + uuid.uuid4().hex[:6])
                command = [sys.executable, str(ROOT / 'scripts/phase1_study.py'), 'run', '--lab', str(cfg),
                           '--out', str(out), '--profile', 'smoke', '--allow-local', '--lead-seconds', '1',
                           '--settle-seconds', '15', '--sample-ms', '500', '--load-fraction', '.05']
                out.mkdir(parents=True)
                # Log outside the output; the coordinator requires an empty destination.
                log_path = ROOT / '.tools/six-pc-rehearsal.log'
                log_path.parent.mkdir(exist_ok=True)
                with log_path.open('w', encoding='utf-8') as log:
                    completed = subprocess.run(command, env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, timeout=480)
                self.assertEqual(completed.returncode, 0, log_path.read_text(encoding='utf-8')[-6000:])
                results = [json.loads(p.read_text()) for p in (out / 'cases').glob('*/result.json')]
                self.assertEqual(len(results), 12)
                self.assertEqual({r['mode'] for r in results}, {'on_demand', 'per_request'})
                self.assertTrue(all(r['baseline_agreed'] and r['converged'] and r['intervention_valid'] for r in results), results)
                for result in results:
                    self.assertEqual(result['answered'], result['requests'], result)
                    self.assertIsNotNone(result['lat_sd_ms'])
                    self.assertTrue(result['measurement_valid'], result)
                self.assertTrue((out / 'report.html').is_file())
                # Resume must skip completed cases without starting any workload.
                resumed = subprocess.run(command + ['--resume', '--no-report'], capture_output=True, text=True, encoding='utf-8', timeout=20)
                self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
                self.assertNotIn('Starting ', resumed.stdout)
                self.assertTrue(all(s.agent.node is None for s in servers[:3]))
                self.assertTrue(all(not s.agent.jobs for s in servers[3:]))
                print(f'\nSix-agent validation report: {out / "report.html"}', flush=True)
                multiple = out.with_name(out.name + '-multiple')
                command = [sys.executable, str(ROOT / 'scripts/phase1_study.py'), 'run', '--lab', str(cfg),
                           '--out', str(multiple), '--allow-local', '--stage', 'normal', '--processes', '2',
                           '--rates', '30', '--reps', '2', '--duration', '4', '--complexity', '1',
                           '--at-seconds', '1', '--lead-seconds', '1', '--settle-seconds', '15']
                with (log_path.with_name('six-pc-multiple.log')).open('w', encoding='utf-8') as log:
                    completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
                self.assertEqual(completed.returncode, 0, log_path.with_name('six-pc-multiple.log').read_text()[-6000:])
                more = [json.loads(p.read_text()) for p in (multiple / 'cases').glob('*/result.json')]
                self.assertEqual(len(more), 4)
                self.assertTrue(all(r['measurement_valid'] and r['converged'] and r['requests'] == 720 and r['answered'] == 720 for r in more), more)
                with (multiple / 'summary.csv').open(newline='', encoding='utf-8') as stream:
                    summary = list(csv.DictReader(stream))
                self.assertTrue(all(r['goodput_rps_sd'] != '' and r['lat_p99_ms_ci95_half'] != '' for r in summary))
                print(f'Multiple-process / repeated-run validation: {multiple / "report.html"}', flush=True)
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
                server.agent.close()
            for sock in reservations:
                sock.close()
            if previous is None:
                os.environ.pop('RESCUEMESH_LAB_TOKEN', None)
            else:
                os.environ['RESCUEMESH_LAB_TOKEN'] = previous


if __name__ == '__main__':
    unittest.main()
