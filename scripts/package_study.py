#!/usr/bin/env python3
"""Build a source-only six-PC study bundle, excluding results, binaries and secrets."""
import hashlib
import json
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    destination = ROOT / 'dist/six-pc-study-v3-source.zip'
    destination.parent.mkdir(exist_ok=True)
    files = [ROOT / name for name in ['Cargo.toml', 'Cargo.lock', 'README.md', 'LICENSE', '.gitignore', '.gitattributes']]
    for directory, extensions in {'crates': {'.rs', '.toml'}, 'scripts': {'.py', '.sh', '.ps1', '.txt'},
                                  'config': {'.toml', '.json'}, 'docs': {'.md', '.png'}}.items():
        files.extend(p for p in (ROOT / directory).rglob('*') if p.is_file() and p.suffix in extensions
                     and '__pycache__' not in p.parts)
    with zipfile.ZipFile(destination, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(files):
            content = path.read_bytes()
            if path.suffix != '.png':
                content = content.replace(b'\r\n', b'\n')
            entry = zipfile.ZipInfo('RescueMesh-six-pc-study/' + path.relative_to(ROOT).as_posix())
            entry.create_system = 3
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = (0o100755 if path.suffix == '.sh' else 0o100644) << 16
            archive.writestr(entry, content)
        archive.writestr('RescueMesh-six-pc-study/START-HERE.txt', '''Protocol 3 update: read docs/allocation-v3.md and docs/six-pc-study.md.

Copy this same source to ALL SIX PCs and run cargo build --release on each.
Stop old manual servers. Python 3.11+ is required.
Server PCs: 10.26.210.62, .63, .64; clients: .65, .66, .67.
Start study_agent.py --role server/client --bind THIS_PC_IP on each PC.
All agents and controller need the same RESCUEMESH_LAB_TOKEN environment value.
The coordinator starts/restarts servers automatically; do not separately start them.
Run the smoke profile before the long study. Exact commands are in the guide.
Use NEW output directories (e.g. six-pc-smoke-v3). Old studies cannot be resumed across versions.
Resource availability is partitioned; shortfalls are reported and correctness failures stop the study.
The bundle contains source, not Linux/Windows executables or old results.
''')
    with zipfile.ZipFile(destination) as archive:
        assert archive.testzip() is None
        assert len(archive.namelist()) == len(set(archive.namelist()))
        assert all('/target/' not in n and '/.git/' not in n and '/results/' not in n and '/.tools/' not in n
                   for n in archive.namelist())
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    destination.with_suffix('.zip.sha256').write_text(f'{digest}  {destination.name}\n', encoding='ascii')
    print(json.dumps({'path': str(destination), 'files': len(files) + 1, 'bytes': destination.stat().st_size,
                      'sha256': digest}, indent=2))


if __name__ == '__main__':
    main()
