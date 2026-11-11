"""Durable, append-only transport for unchanged prospective NBA research bytes.

This is storage, not a forecast/sidecar registration mechanism. Indexes may be
relocated on a new runner; the original artifact bytes are never rewritten.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath

from research.nba_points_forward import aware, digest, write_new

MODEL_SHA = '651441d2dcfbe40aa1bd2c320ef8327056e02bc2c06257995c1cb3c40254bec4'
EXPERIMENT_SHA = '324bd657f5918f9f1772cc2d38faea2f1f523872a9ed64a02bbf7f2494c8748f'
ROLE_SHA = '8de0e02d49dcdccf8fb7e1fe3638a4eac554e78ad60088938830671013a402f7'
MATH_SHA = '3ef587a3fc1283a4d75dcc73cd46a725411aa5a7abd772a6c75bdadf4dee7d5a'
MAX_BYTES = 256 * 1024 * 1024
SIDECARS = {
    'experiments-index': ('experiment.json', 'experiment_path', 'experiment_sha256', EXPERIMENT_SHA),
    'role-index': ('role-challenger.json', 'record_path', 'sha256', ROLE_SHA),
    'trio-math-index': ('trio-math.json', 'record_path', 'record_sha256', MATH_SHA),
}
DDL = """CREATE TABLE IF NOT EXISTS nba_research_archives (
 forecast_sha256 TEXT PRIMARY KEY, game_date DATE NOT NULL, model_sha256 TEXT NOT NULL,
 lock_at TIMESTAMPTZ NOT NULL, forecast_created_at TIMESTAMPTZ NOT NULL,
 archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), origin TEXT NOT NULL,
 manifest JSONB NOT NULL, archive BYTEA NOT NULL, UNIQUE(game_date,model_sha256))"""


def checked(value):
    if value.get('sha256') != digest({k: v for k, v in value.items() if k != 'sha256'}):
        raise ValueError('artifact digest mismatch')
    return value


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def checked_relative(name):
    p = PurePosixPath(name)
    if not name or '\\' in name or ':' in name or p.is_absolute() or any(x in ('..', '.') for x in name.split('/')):
        raise ValueError('unsafe archive path')
    return p


def bundle(root, marker):
    """Require an existing genuine index, preserve all original evidence bytes."""
    root, marker = Path(root), Path(marker)
    index = read(marker)
    directory = Path(index['forecast_path']).parent
    forecast = checked(read(directory / 'forecast.json'))
    source = checked(read(directory / 'source-snapshot.json'))
    if (directory / 'model.json').exists() and checked(read(directory / 'model.json'))['sha256'] != MODEL_SHA:
        raise ValueError('archived model differs from original pin')
    if (index['forecast_sha256'] != forecast['sha256'] or index['model_sha256'] != MODEL_SHA
            or forecast['model_sha256'] != MODEL_SHA or index['date'] != forecast['game_date']
            or marker.name != forecast['game_date'] + '-' + MODEL_SHA + '.json'
            or source['sha256'] != forecast['source_snapshot_sha256']
            or source['game_date'] != forecast['game_date']
            or source['mode'] != 'prospective' or forecast['mode'] != 'prospective'):
        raise ValueError('invalid indexed original')
    starts = [aware(g['start_time']) for g in source['slate_schedule']]
    if (not starts or aware(source['lock_at']) != aware(forecast['lock_at'])
            or aware(source['lock_at']) > min(starts)
            or not aware(source['as_of']) <= aware(source['created_at']) <= aware(forecast['created_at']) < min(starts)):
        raise ValueError('invalid original cutoff')
    base = 'originals/' + forecast['game_date'] + '-' + forecast['sha256']
    files = {}

    def add(name, path):
        checked_relative(name)
        if path.is_symlink():
            raise ValueError('symlink in research evidence')
        files[name] = path.read_bytes()

    for path in directory.rglob('*'):
        if path.is_file():
            add(base + '/' + path.relative_to(directory).as_posix(), path)
    indexes = {'prospective-index/' + marker.name: index}
    attachments = {}
    for folder, (filename, _, identity, protocol) in SIDECARS.items():
        path = root / folder / (forecast['game_date'] + '-' + protocol + '.json')
        if not path.exists():
            attachments[folder] = 'missing_or_failed'
            continue
        idx, record = read(path), checked(read(directory / filename))
        created = record.get('completed_at', record['created_at'])
        if (record['mode'] != 'prospective' or record['forecast_sha256'] != forecast['sha256']
                or record['source_snapshot_sha256'] != source['sha256']
                or record['protocol_sha256'] != protocol or idx[identity] != record['sha256']
                or record['game_date'] != forecast['game_date']
                or not aware(forecast['created_at']) <= aware(record['created_at']) <= aware(created) < min(starts)):
            raise ValueError('invalid registered attachment')
        indexes[folder + '/' + path.name] = idx
        attachments[folder] = 'indexed'
    packet = {
        'version': 'nba-research-archive-v1', 'game_date': forecast['game_date'],
        'model_sha256': MODEL_SHA, 'forecast_sha256': forecast['sha256'],
        'forecast_created_at': forecast['created_at'], 'lock_at': forecast['lock_at'],
        'record_directory': base, 'indexes': indexes, 'attachments': attachments,
        'files': {name: {'sha256': hashlib.sha256(data).hexdigest(),
                         'data': base64.b64encode(data).decode('ascii')} for name, data in files.items()},
    }
    encoded = json.dumps(packet, separators=(',', ':'), ensure_ascii=True).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError('research archive exceeds size limit')
    packed = gzip.compress(encoded, mtime=0)
    manifest = {k: v for k, v in packet.items() if k != 'files'}
    manifest['archive_sha256'] = hashlib.sha256(packed).hexdigest()
    manifest['files'] = {name: item['sha256'] for name, item in packet['files'].items()}
    return manifest, packed


def unpack(manifest, packed):
    if hashlib.sha256(packed).hexdigest() != manifest['archive_sha256']:
        raise ValueError('archive digest mismatch')
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError('archive exceeds expanded size limit')
    packet = json.loads(data)
    original = packet['version'] == 'nba-research-archive-v1' and packet.get('model_sha256') == MODEL_SHA
    review = packet['version'] == 'nba-research-review-archive-v1' and packet.get('source_version') == 'nba-points-ghost-v2' and not packet.get('indexes')
    if not (original or review):
        raise ValueError('unsupported archive identity')
    if any(packet[k] != manifest[k] for k in packet if k != 'files'):
        raise ValueError('archive metadata mismatch')
    if set(packet['files']) != set(manifest['files']):
        raise ValueError('archive file manifest mismatch')
    checked_relative(packet['record_directory'])
    for name in packet['indexes']:
        path = checked_relative(name)
        if len(path.parts) != 2 or path.parts[0] not in {'prospective-index', *SIDECARS}:
            raise ValueError('unexpected index path')
    files = {}
    for name, item in packet['files'].items():
        checked_relative(name)
        value = base64.b64decode(item['data'], validate=True)
        if hashlib.sha256(value).hexdigest() != item['sha256'] or item['sha256'] != manifest['files'][name]:
            raise ValueError('evidence bytes differ from original')
        files[name] = value
    if packet['indexes']:
        prefix = packet['record_directory'] + '/'
        forecast = checked(json.loads(files[prefix + 'forecast.json']))
        source = checked(json.loads(files[prefix + 'source-snapshot.json']))
        if (forecast['sha256'] != packet['forecast_sha256'] or forecast['model_sha256'] != MODEL_SHA
                or forecast['game_date'] != packet['game_date'] or forecast['lock_at'] != packet['lock_at']
                or forecast['created_at'] != packet['forecast_created_at']
                or forecast['source_snapshot_sha256'] != source['sha256']
                or forecast['mode'] != 'prospective' or source['mode'] != 'prospective'):
            raise ValueError('archived original identity mismatch')
    return packet, files


def restore(root, manifest, packed):
    root = Path(root)
    packet, files = unpack(manifest, packed)
    # Validate every byte/path before performing any write. Existing originals
    # are compared rather than replaced, including after a partial restoration.
    for name, data in files.items():
        path = root.joinpath(*checked_relative(name).parts)
        if path.exists() and (path.is_symlink() or path.read_bytes() != data):
            raise ValueError('existing evidence conflicts with archived original')
        if root.resolve() not in path.resolve().parents:
            raise ValueError('archive path escapes root')
    for name, data in files.items():
        path = root.joinpath(*PurePosixPath(name).parts)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('xb') as stream:
                stream.write(data)
    directory = root / packet['record_directory']
    for name, original in packet['indexes'].items():
        checked_relative(name)
        folder = PurePosixPath(name).parts[0]
        if folder == 'prospective-index':
            field, filename, identity = 'forecast_path', 'forecast.json', 'forecast_sha256'
        elif folder in SIDECARS:
            filename, field, identity, _ = SIDECARS[folder]
        else:
            raise ValueError('unexpected index folder')
        evidence = root / 'archive-index-evidence' / packet['forecast_sha256'] / name
        if not evidence.exists():
            evidence.parent.mkdir(parents=True, exist_ok=True)
            write_new(evidence, original)
        path = root / name
        if path.exists():
            old = read(path)
            if old[identity] != original[identity]:
                raise ValueError('existing index belongs to another original')
            if not Path(old[field]).exists():
                raise ValueError('existing index path is unavailable; explicit repair required')
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        write_new(path, {**original, field: str((directory / filename).resolve())})
    return directory


def initialize(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def store(conn, manifest, packed, *, migration=False):
    unpack(manifest, packed)
    from psycopg2.extras import Json
    with conn.cursor() as cur:
        cur.execute('SELECT forecast_sha256 FROM nba_research_archives WHERE game_date=%s AND model_sha256=%s',
                    (manifest['game_date'], MODEL_SHA))
        old = cur.fetchone()
        if old:
            if old[0] != manifest['forecast_sha256']:
                raise ValueError('another original already archived for date/model')
            return False
        cur.execute('''INSERT INTO nba_research_archives
            (forecast_sha256,game_date,model_sha256,lock_at,forecast_created_at,origin,manifest,archive)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s
            WHERE (%s OR clock_timestamp()<%s::timestamptz)
              AND %s::timestamptz < %s::timestamptz
              AND %s::timestamptz <= clock_timestamp()
            ON CONFLICT DO NOTHING RETURNING forecast_sha256''',
            (manifest['forecast_sha256'], manifest['game_date'], MODEL_SHA, manifest['lock_at'],
             manifest['forecast_created_at'], 'verified_existing_local_original' if migration else 'cloud_before_tip',
             Json(manifest), packed, migration, manifest['lock_at'], manifest['forecast_created_at'], manifest['lock_at'], manifest['forecast_created_at']))
        if cur.fetchone() is None:
            raise ValueError('archive cutoff passed or original already exists')
    conn.commit()
    return True


# Where each restored archive's bytes came from in this process, reported in the
# run's result so a cold cache (everything from the database) is visible.
TRANSPORT = {'cache': 0, 'database': 0}


def packed_archive(cache, archive_sha256, fetch):
    """The packed archive with this sha256: a verified local copy, else ``fetch()``.

    Every run used to download every archive (about 180 MB by 2026-11-10, growing
    daily). The cache changes only where the bytes come from: a copy is used only
    when its own sha256 is the manifest's, anything else is fetched from the
    database and replaces it, and ``unpack`` still checks every file inside. With
    no cache directory this is exactly the database read.
    """
    path = None if cache is None else Path(cache) / (archive_sha256 + '.gz')
    if path is not None and path.is_file():
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() == archive_sha256:
            TRANSPORT['cache'] += 1
            return data
    data = bytes(fetch())
    if hashlib.sha256(data).hexdigest() != archive_sha256:
        raise ValueError('archive digest mismatch')
    TRANSPORT['database'] += 1
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + '.partial')
        partial.write_bytes(data)
        partial.replace(path)
    return data


def fetch_archive(conn, table, key_column, key):
    """One row's archive bytes. Table and column are fixed by the caller, never input."""
    def fetch():
        with conn.cursor() as cur:
            cur.execute('SELECT archive FROM ' + table + ' WHERE ' + key_column + '=%s', (key,))
            row = cur.fetchone()
        if row is None:
            raise ValueError('archive row disappeared between manifest and bytes')
        return row[0]
    return fetch


def restore_all(conn, root, cache=None):
    with conn.cursor() as cur:
        cur.execute('SELECT forecast_sha256,manifest FROM nba_research_archives WHERE model_sha256=%s ORDER BY game_date', (MODEL_SHA,))
        rows = cur.fetchall()
    for key, manifest in rows:
        restore(root, manifest, packed_archive(cache, manifest['archive_sha256'],
                                               fetch_archive(conn, 'nba_research_archives', 'forecast_sha256', key)))
    # The fixed sequential P90 rule needs the actual previously observed grades,
    # not just historical seed residuals, on every ephemeral runner.
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('nba_trio_math_grades')")
        if cur.fetchone()[0] is not None:
            cur.execute('''SELECT DISTINCT ON (record_sha256) grade FROM nba_trio_math_grades
                WHERE grade->>'protocol_sha256'=%s ORDER BY record_sha256,graded_at DESC,id DESC''', (MATH_SHA,))
            for (grade,) in cur.fetchall():
                checked(grade)
                path = Path(root) / 'trio-math-grades' / (grade['sha256'] + '.json')
                if not path.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    write_new(path, grade)
    return len(rows)
