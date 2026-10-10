from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re

from model.nfl_joint_contracts import digest
from research.nfl_longest_touchdown import read, write


def write_bundle(root, study_id, run_id, payloads):
    for key in (study_id, run_id):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', key) or key in ('.', '..'):
            raise ValueError('Invalid artifact identity')
    if not payloads or 'manifest.json' in payloads:
        raise ValueError('Payloads required; manifest is generated')
    for name in payloads:
        if Path(name).name != name or not name.endswith(('.json', '.json.gz')):
            raise ValueError('Artifact names must be local JSON filenames')
    directory = Path(root) / study_id / run_id
    directory.mkdir(parents=True, exist_ok=False)
    entries = {}
    for name, payload in payloads.items():
        path = directory / name
        write(path, payload)
        entries[name] = {'sha256': sha256(path.read_bytes()).hexdigest(), 'content_sha256': digest(payload), 'bytes': path.stat().st_size}
    manifest = {'schema_version': 1, 'study_id': study_id, 'run_id': run_id, 'artifacts': entries, 'complete': True}
    write(directory / 'manifest.json', manifest)
    return manifest


def verify_bundle(directory):
    directory = Path(directory)
    manifest = read(directory / 'manifest.json')
    if not manifest.get('complete') or manifest.get('schema_version') != 1:
        raise ValueError('Incomplete artifact bundle')
    for name, entry in manifest['artifacts'].items():
        if Path(name).name != name:
            raise ValueError('Unsafe artifact reference')
        path = directory / name
        if sha256(path.read_bytes()).hexdigest() != entry['sha256'] or digest(read(path)) != entry['content_sha256']:
            raise ValueError('Artifact digest mismatch')
    return manifest
