"""Cloud operations around the unchanged, registered NBA research commands."""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from research import nba_research_archive as storage
from research.nba_points_forward import aware, digest, write_new
from research.nba_ghost_runner import fetch_scoreboard

ROOT = Path(__file__).resolve().parent
RUN_DDL = """CREATE TABLE IF NOT EXISTS nba_research_runs (
 id BIGSERIAL PRIMARY KEY, game_date DATE NOT NULL, action TEXT NOT NULL,
 observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), result JSONB NOT NULL)"""
REVIEW_DDL = """CREATE TABLE IF NOT EXISTS nba_research_reviews (
 review_sha256 TEXT PRIMARY KEY, forecast_sha256 TEXT NOT NULL REFERENCES nba_research_archives(forecast_sha256),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), manifest JSONB NOT NULL, archive BYTEA NOT NULL)"""
V2_REVIEW_DDL = """CREATE TABLE IF NOT EXISTS nba_research_v2_reviews (
 review_sha256 TEXT PRIMARY KEY, snapshot_sha256 TEXT NOT NULL REFERENCES nba_ghost_snapshots(snapshot_sha256),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), manifest JSONB NOT NULL, archive BYTEA NOT NULL)"""


def now():
    return datetime.now(timezone.utc)


def session_database_url(url):
    """Neon transaction pooling cannot protect a session-scoped advisory lock.

    Same role/database/password; only the Neon pooler hostname is changed.
    Other installations can supply their existing DATABASE_URL_UNPOOLED.
    """
    from urllib.parse import urlsplit, urlunsplit
    if os.environ.get('DATABASE_URL_UNPOOLED'):
        return os.environ['DATABASE_URL_UNPOOLED']
    parts = urlsplit(url)
    host = parts.hostname or ''
    if host.endswith('.neon.tech') and '-pooler.' in host:
        account, sep, address = parts.netloc.rpartition('@')
        address = address.replace(host, host.replace('-pooler.', '.', 1), 1)
        return urlunsplit(parts._replace(netloc=account + sep + address))
    return url


def initialize(conn):
    storage.initialize(conn)
    with conn.cursor() as cur:
        cur.execute(RUN_DDL)
        cur.execute(REVIEW_DDL)
        cur.execute(V2_REVIEW_DDL)
    conn.commit()


def pinned_model(output):
    artifact = ROOT / 'artifacts' / 'nba_ghost_v3_model_v1.json.gz'
    data = gzip.decompress(artifact.read_bytes())
    model = storage.checked(json.loads(data))
    if model['sha256'] != storage.MODEL_SHA:
        raise ValueError('cloud model differs from original pin')
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        if output.read_bytes() != data:
            raise ValueError('existing model differs from pinned original')
    else:
        with output.open('xb') as stream:
            stream.write(data)
    return output


def command(label, args, evidence, timeout=600):
    try:
        result = subprocess.run([sys.executable, '-m', *map(str, args)], capture_output=True,
                                text=True, timeout=timeout)
        receipt = {'exit': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
    except subprocess.TimeoutExpired:
        write_new(evidence / (label + '-receipt.json'), {'status': 'timed_out', 'timeout_seconds': timeout})
        raise
    write_new(evidence / (label + '-receipt.json'), receipt)
    if result.returncode:
        raise RuntimeError(label + ' failed; inspect its retained receipt')


def restore_reviews(conn, root):
    with conn.cursor() as cur:
        cur.execute('SELECT manifest,archive FROM nba_research_reviews ORDER BY created_at')
        rows = cur.fetchall()
        cur.execute('SELECT manifest,archive FROM nba_research_v2_reviews ORDER BY created_at')
        rows += cur.fetchall()
    for manifest, packed in rows:
        storage.restore(root, manifest, bytes(packed))


def missing_attachments(manifest):
    enrollment = {'experiments-index': '2026-10-28', 'role-index': '2026-11-03', 'trio-math-index': '2026-11-04'}
    return [folder for folder, start in enrollment.items()
            if manifest['game_date'] >= start and manifest['attachments'].get(folder) != 'indexed']


def record(conn, root, day, evidence):
    payload, captured = fetch_scoreboard(day)
    write_new(evidence / 'scoreboard.json', {'captured_at': captured.isoformat(), 'payload': payload})
    starts = [aware(e['date']) for e in payload.get('events', [])]
    if not starts:
        return {'status': 'no_slate', 'new_originals': 0}
    with conn.cursor() as cur:
        cur.execute('SELECT manifest FROM nba_research_archives WHERE game_date=%s AND model_sha256=%s', (day, storage.MODEL_SHA))
        original = cur.fetchone()
    if original:
        return {'status': 'partial_attachments' if missing_attachments(original[0]) else 'original_preserved',
                'new_originals': 0, 'forecast_sha256': original[0]['forecast_sha256'],
                'attachments': original[0]['attachments']}
    if now() >= min(starts):
        return {'status': 'missed_cutoff', 'first_tip': min(starts).isoformat(), 'new_originals': 0}
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM nba_research_runs WHERE game_date=%s AND action='forecast_started'
            AND NOT EXISTS (SELECT 1 FROM nba_research_runs done WHERE done.action='no_forecast_created'
              AND done.result->>'attempt'=nba_research_runs.result->>'attempt')""", (day,))
        if cur.fetchone():
            raise ValueError('earlier forecast attempt may have an original; inspect retained artifacts, never refreeze blindly')
    # A global session advisory lock spans collection, original recording and
    # durable publication. Retries never attach missing sidecars to an original.
    storage.restore_all(conn, root)
    model = pinned_model(root / 'pinned-model.json')
    inputs, directory = evidence / 'inputs', root / ('record-' + day.isoformat() + '-' + evidence.name)
    command('collect', ['research.nba_points_v3', 'collect', '--date', day, '--output-dir', inputs], evidence)
    from psycopg2.extras import Json
    with conn.cursor() as cur:
        cur.execute('INSERT INTO nba_research_runs(game_date,action,result) VALUES(%s,%s,%s)',
                    (day, 'forecast_started', Json({'attempt': evidence.name, 'model_sha256': storage.MODEL_SHA})))
    conn.commit()
    command('record', ['research.nba_points_v3', 'record', '--date', day, '--input', inputs / 'inputs.json',
                      '--model', model, '--injury-archive', root / 'injury', '--draws', 5000,
                      '--seed', 20261028, '--output-dir', directory], evidence)
    receipt = storage.read(directory / 'receipt.json')
    marker = root / 'prospective-index' / (str(day) + '-' + storage.MODEL_SHA + '.json')
    if not marker.exists():
        if not (directory / 'forecast.json').exists():
            with conn.cursor() as cur:
                cur.execute('INSERT INTO nba_research_runs(game_date,action,result) VALUES(%s,%s,%s)',
                            (day, 'no_forecast_created', Json({'attempt': evidence.name, 'receipt': receipt})))
            conn.commit()
        return {'status': 'failed_recording', 'new_originals': 0, 'receipt': receipt}
    # Retain the exact collected bundle and official PDF observations alongside
    # the immutable forecast, including failures and source completion receipts.
    shutil.copyfile(inputs / 'inputs.json', directory / 'original-inputs.json')
    if (root / 'injury').exists():
        shutil.copytree(root / 'injury', directory / 'injury-evidence')
    manifest, packed = storage.bundle(root, marker)
    inserted = storage.store(conn, manifest, packed)
    return {'status': 'partial_attachments' if missing_attachments(manifest) else 'recorded',
            'new_originals': int(inserted), 'forecast_sha256': manifest['forecast_sha256'],
            'first_tip': manifest['lock_at'], 'attachments': manifest['attachments'], 'receipt': receipt}


def health(conn, day, payload, at):
    starts = [aware(e['date']) for e in payload.get('events', [])]
    if not starts:
        return {'status': 'no_slate', 'problems': []}
    eastern = at.astimezone(ZoneInfo('America/New_York'))
    problems = []
    with conn.cursor() as cur:
        cur.execute("SELECT snapshot_sha256 FROM nba_ghost_snapshots WHERE game_date=%s AND version='nba-points-ghost-v2' AND mode='prospective'", (day,))
        if cur.fetchone() is None:
            problems.append('Original v2 snapshot is absent')
        cur.execute('SELECT manifest FROM nba_research_archives WHERE game_date=%s AND model_sha256=%s', (day, storage.MODEL_SHA))
        row = cur.fetchone()
    if row is None:
        problems.append('Durable original v3 recording is absent')
    else:
        problems.extend(folder + ': original attachment absent or failed' for folder in missing_attachments(row[0]))
    if problems and at < min(starts) and (eastern.hour, eastern.minute) < (11, 52):
        return {'status': 'awaiting_scheduled_freeze', 'first_tip': min(starts).isoformat(),
                'observed_missing': problems, 'problems': []}
    return {'status': 'healthy' if not problems else 'missing_before_tip' if at < min(starts) else 'missed_or_incomplete',
            'first_tip': min(starts).isoformat(), 'minutes_until_tip': (min(starts) - at).total_seconds() / 60,
            'problems': problems, 'can_record_new_original': at < min(starts) and row is None}


def verified_game_copy(source, diagnostics, summaries):
    """Fill game identity only for unlisted players, using reconciled finals."""
    from copy import deepcopy
    from model.nba_points_ladder import team_key
    from research.nba_points_diagnostics import parse_summary
    games = [parse_summary(storage.read(p), p.stem.removeprefix('summary-')) for p in summaries.glob('summary-*.json')]
    candidates = {(r['event_id'], r['player_key']): r for r in source['candidates']}
    result, joins = deepcopy(diagnostics), []
    for row in result['rows']:
        if row.get('espn_event_id') or row['status'] != 'not_listed_in_final_box':
            continue
        c = candidates[(row['event_id'], row['player_key'])]
        hits = [g for g in games if g['game_date'] == source['game_date']
                and team_key(g['home_team']) == team_key(c['home_team'])
                and team_key(g['away_team']) == team_key(c['away_team'])]
        frozen = [g for g in source['slate_schedule'] if team_key(g['home_team']) == team_key(c['home_team'])
                  and team_key(g['away_team']) == team_key(c['away_team'])]
        if len(hits) != 1 or len(frozen) != 1 or str(hits[0]['espn_event_id']) != str(frozen[0]['event_id']):
            raise ValueError('unlisted player game identity is ambiguous or conflicting')
        row['espn_event_id'] = hits[0]['espn_event_id']
        joins.append({'event_id': row['event_id'], 'player_key': row['player_key'],
                      'espn_event_id': row['espn_event_id'], 'status_unchanged': row['status']})
    result['game_identity_verification'] = {'source_snapshot_sha256': source['sha256'], 'joins': joins,
        'limitation': 'Game identity only; unlisted outcomes remain unresolved.'}
    return result


def save_review(conn, root, review, manifest):
    files = {p.relative_to(root).as_posix(): p.read_bytes() for p in review.rglob('*') if p.is_file()}
    packet = {k: v for k, v in manifest.items() if k not in ('files', 'archive_sha256')}
    packet['indexes'] = {}
    packet['files'] = {k: {'sha256': hashlib.sha256(v).hexdigest(), 'data': base64.b64encode(v).decode('ascii')} for k, v in files.items()}
    encoded = json.dumps(packet, separators=(',', ':'), ensure_ascii=True).encode()
    if len(encoded) > storage.MAX_BYTES:
        raise ValueError('review archive too large')
    packed = gzip.compress(encoded, mtime=0)
    meta = {k: v for k, v in packet.items() if k != 'files'}
    meta['files'] = {k: v['sha256'] for k, v in packet['files'].items()}
    meta['archive_sha256'] = hashlib.sha256(packed).hexdigest()
    storage.unpack(meta, packed)
    from psycopg2.extras import Json
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO nba_research_reviews(review_sha256,forecast_sha256,manifest,archive)
            VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
            (meta['archive_sha256'], manifest['forecast_sha256'], Json(meta), packed))
    conn.commit()


def save_v2_review(conn, root, directory):
    """Retain v2-only days without inventing a corresponding v3 original."""
    source = storage.checked(storage.read(directory / 'v2-review' / 'snapshot.json'))
    if source['version'] != 'nba-points-ghost-v2' or source['mode'] != 'prospective':
        raise ValueError('v2 archive requires its genuine original snapshot')
    packet = {'version': 'nba-research-review-archive-v1', 'source_version': source['version'],
        'source_snapshot_sha256': source['sha256'], 'game_date': source['game_date'],
        'record_directory': 'operations', 'indexes': {}, 'files': {}}
    for path in directory.rglob('*'):
        if path.is_file():
            data = path.read_bytes()
            packet['files'][path.relative_to(root).as_posix()] = {
                'sha256': hashlib.sha256(data).hexdigest(), 'data': base64.b64encode(data).decode('ascii')}
    encoded = json.dumps(packet, separators=(',', ':'), ensure_ascii=True).encode()
    if len(encoded) > storage.MAX_BYTES:
        raise ValueError('v2 review evidence exceeds size limit')
    packed = gzip.compress(encoded, mtime=0)
    meta = {k: v for k, v in packet.items() if k != 'files'}
    meta['files'] = {k: v['sha256'] for k, v in packet['files'].items()}
    meta['archive_sha256'] = hashlib.sha256(packed).hexdigest()
    storage.unpack(meta, packed)
    from psycopg2.extras import Json
    with conn.cursor() as cur:
        cur.execute('''INSERT INTO nba_research_v2_reviews(review_sha256,snapshot_sha256,manifest,archive)
            VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
            (meta['archive_sha256'], source['sha256'], Json(meta), packed))
    conn.commit()


def import_history(conn, root, manifest):
    """Transport genuine prior grades/evidence, retaining their observation times.

    This creates no enrollment or grade. It prevents a fresh runner from silently
    reporting only the latest three days of the original fixed experiments.
    """
    forecast_sha = manifest['forecast_sha256']
    marker = root / 'prospective-index' / (manifest['game_date'] + '-' + storage.MODEL_SHA + '.json')
    directory = Path(storage.read(marker)['forecast_path']).parent
    forecast = storage.checked(storage.read(directory / 'forecast.json'))
    source_sha = forecast['source_snapshot_sha256']
    output = root / 'operations' / ('legacy-history-' + manifest['game_date'] + '-' + forecast_sha[:12])
    if output.exists():
        raise ValueError('history staging directory exists; preserve it and use its receipt')
    output.mkdir(parents=True)
    latest = {}
    # Discovery is by exact artifact identity, never by an approximate date/name.
    for path in root.rglob('*.json'):
        if 'operations' in path.relative_to(root).parts or 'archive-index-evidence' in path.relative_to(root).parts:
            continue
        if path.name not in ('diagnostics.json', 'experiment-grade.json', 'grade.json') and path.parent.name not in ('role-grades', 'trio-math-grades'):
            continue
        value = storage.read(path)
        kind = None
        if path.name == 'diagnostics.json' and value.get('snapshot_sha256') == source_sha:
            kind = 'source-review'
        elif path.name == 'experiment-grade.json' and value.get('forecast_sha256') == forecast_sha and value.get('mode') == 'prospective':
            storage.checked(value); kind = 'experiment-grade'
        elif path.name == 'grade.json' and value.get('snapshot_sha256') == forecast_sha and value.get('source_snapshot_sha256') == source_sha:
            kind = 'v3-grade'
        else:
            for folder, filename in [('role-grades', 'role-challenger.json'), ('trio-math-grades', 'trio-math.json')]:
                if path.parent.name == folder and (directory / filename).exists():
                    original = storage.checked(storage.read(directory / filename))
                    if value.get('record_sha256') == original['sha256'] and value.get('protocol_sha256') == original['protocol_sha256']:
                        storage.checked(value); kind = 'role-grade' if folder == 'role-grades' else 'math-grade'
        if kind:
            observed = value.get('graded_at', value.get('captured_at', ''))
            if kind not in latest or observed > latest[kind][0]:
                latest[kind] = (observed, path)
    for kind, (_, path) in latest.items():
        if kind == 'source-review':
            shutil.copytree(path.parent, output / 'source-review')
        elif kind in ('role-grade', 'math-grade'):
            shutil.copyfile(path, output / (kind + '.json'))
        else:
            target = output / kind; target.mkdir()
            shutil.copyfile(path, target / path.name)
    if 'math-grade' in latest:
        grade_sha = storage.read(latest['math-grade'][1])['sha256']
        for path in (root / 'trio-math-grades' / 'pool-audits').glob('*.json'):
            audit = storage.checked(storage.read(path))
            if audit['math_grade_sha256'] == grade_sha:
                write_new(output / 'pool-audits' / 'audit.json', audit)
                break
    for path in root.glob('*inputs*/inputs.json'):
        if hashlib.sha256(path.read_bytes()).hexdigest() == forecast.get('input_bundle_sha256'):
            shutil.copyfile(path, output / 'original-inputs.json')
            break
    write_new(output / 'transport-receipt.json', {'forecast_sha256': forecast_sha, 'new_forecasts': 0,
              'new_grades': 0, 'transported': {k: {'original_path': str(p), 'observed_at': at} for k, (at, p) in latest.items()}})
    save_review(conn, root, output, manifest)
    return sorted(latest)


def review(conn, root, last, evidence):
    storage.restore_all(conn, root)
    restore_reviews(conn, root)
    results = []
    for day in [last - timedelta(days=i) for i in range(3)]:
        output = evidence / str(day)
        output.mkdir()
        try:
            readiness, captured = fetch_scoreboard(day)
            write_new(output / 'scoreboard.json', {'captured_at': captured.isoformat(), 'payload': readiness})
            if readiness.get('events') and not all(e['status']['type'].get('completed') for e in readiness['events']):
                results.append({'date': str(day), 'status': 'pending_games'})
                continue
            command('hydrate', ['research.nba_injury_store', '--hydrate-date', day, '--archive-root', root / 'injury'], output)
            hydrated = json.loads(storage.read(output / 'hydrate-receipt.json')['stdout'])
            warnings = hydrated['failures']
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM nba_ghost_snapshots WHERE game_date=%s AND version='nba-points-ghost-v2' AND mode='prospective'", (day,))
                has_v2 = cur.fetchone() is not None
                cur.execute('SELECT manifest FROM nba_research_archives WHERE game_date=%s AND model_sha256=%s', (day, storage.MODEL_SHA))
                row = cur.fetchone()
            if has_v2:
                command('v2-daily', ['research.nba_points_daily', '--date', day, '--output-dir', output / 'v2-review',
                                    '--injury-archive', root / 'injury', '--apply'], output)
            if row is None:
                if has_v2:
                    save_v2_review(conn, root, output)
                results.append({'date': str(day), 'v3': 'no_original', 'v2': has_v2, 'evidence_failures': warnings})
                continue
            manifest = row[0]
            directory = root / manifest['record_directory']
            source_review = output / 'source-review'
            command('source-review', ['research.nba_points_daily', '--snapshot', directory / 'source-snapshot.json',
                                     '--output-dir', source_review, '--injury-archive', root / 'injury'], output)
            diagnostics = source_review / 'diagnostics.json'
            command('v3-grade', ['research.nba_points_v3', 'grade', '--forecast', directory / 'forecast.json',
                                '--diagnostics', diagnostics, '--output-dir', output / 'v3-grade'], output)
            if manifest['attachments']['experiments-index'] == 'indexed':
                command('experiment-grade', ['research.nba_points_experiments', 'grade', '--experiment', directory / 'experiment.json',
                    '--v3-grade', output / 'v3-grade' / 'grade.json', '--diagnostics', diagnostics,
                    '--output-dir', output / 'experiment-grade'], output)
            if manifest['attachments']['role-index'] == 'indexed':
                from research.nba_points_role_challenger import grade as role_grade
                board, captured = fetch_scoreboard(day)
                write_new(output / 'role-readiness' / 'scoreboard.json', {'captured_at': captured.isoformat(), 'payload': board})
                write_new(output / 'role-grade.json', role_grade(storage.read(directory / 'role-challenger.json'), storage.read(diagnostics), board))
            if manifest['attachments']['trio-math-index'] == 'indexed':
                checked_diagnostics = source_review / 'diagnostics-game-identity-verified.json'
                write_new(checked_diagnostics, verified_game_copy(storage.read(directory / 'source-snapshot.json'),
                    storage.read(diagnostics), source_review / 'summaries'))
                math_output = output / 'math-grade.json'
                command('math-grade', ['research.nba_trio_math_runner', 'grade', '--record', directory / 'trio-math.json',
                    '--diagnostics', checked_diagnostics, '--output', math_output, '--apply'], output)
                command('pool-audit', ['research.nba_trio_pool_audit', '--record', directory / 'trio-math.json',
                    '--forecast', directory / 'forecast.json', '--diagnostics', checked_diagnostics,
                    '--math-grade', math_output, '--output', output / 'pool-audits' / 'audit.json', '--apply'], output)
            save_review(conn, root, output, manifest)
            results.append({'date': str(day), 'status': 'reviewed', 'evidence_failures': warnings})
        except Exception as exc:
            write_new(output / 'failure.json', {'type': type(exc).__name__, 'reason': str(exc)})
            results.append({'date': str(day), 'status': 'failed', 'reason': str(exc)})
    # Materialize latest role/math outputs in the exact directories scanned by
    # the unchanged report readers; experiment-grade.json is discovered recursively.
    materialize_grades(root)
    reports(root, evidence)
    return {'status': 'failed' if any(r.get('status') == 'failed' or r.get('evidence_failures') for r in results) else 'reviewed', 'dates': results}


def reports(root, evidence):
    command('v2-report', ['research.nba_ghost_runner', 'report', '--since', '2026-10-25',
                          '--output', evidence / 'v2-report.json'], evidence)
    command('experiments-report', ['research.nba_points_experiments', 'report', '--research-root', root,
                                   '--output-dir', evidence / 'experiments-report'], evidence)
    command('role-report', ['research.nba_points_role_challenger', 'report', '--research-root', root,
                            '--output', evidence / 'role-report.json'], evidence)
    command('math-report', ['research.nba_trio_math_runner', 'report', '--root', root,
                            '--output', evidence / 'math-report.json'], evidence)
    command('pool-report', ['research.nba_trio_pool_report', '--root', root,
                            '--output', evidence / 'pool-summary.json'], evidence)


def materialize_grades(root):
    for filename, folder in [('math-grade.json', 'trio-math-grades'), ('role-grade.json', 'role-grades')]:
        for path in (root / 'operations').rglob(filename):
            grade = storage.checked(storage.read(path))
            target = root / folder / (grade['sha256'] + '.json')
            if not target.exists():
                write_new(target, grade)
    for path in (root / 'operations').rglob('audit.json'):
        if path.parent.name != 'pool-audits':
            continue
        audit = storage.checked(storage.read(path))
        target = root / 'trio-math-grades' / 'pool-audits' / (audit['sha256'] + '.json')
        if not target.exists():
            write_new(target, audit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['record', 'check', 'review', 'restore', 'import-local'])
    parser.add_argument('--date', type=date.fromisoformat)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--env-file', type=Path, help='Optional existing local credentials file; read into process memory only')
    args = parser.parse_args()
    if args.env_file:
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    day = args.date or (now().astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=args.action == 'review'))
    root = args.root.resolve()
    evidence = root / 'operations' / (now().strftime('%Y%m%d-%H%M%S-%fZ') + '-' + args.action)
    evidence.mkdir(parents=True, exist_ok=False)
    import psycopg2
    from psycopg2.extras import Json
    from config import load_config
    result = {'status': 'failed', 'date': str(day)}
    from contextlib import closing
    with closing(psycopg2.connect(session_database_url(load_config().database_url), connect_timeout=30)) as conn:
        initialize(conn)
        locked = True
        if args.action in {'record', 'review', 'import-local'}:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(hashtextextended('nba-research-cloud-v1',0))")
                locked = cur.fetchone()[0]
        if not locked:
            write_new(evidence / 'result.json', {'status': 'already_running', 'action': args.action, 'date': str(day)})
            print(json.dumps({'status': 'already_running'}))
            return
        try:
            if args.action == 'record':
                result = record(conn, root, day, evidence)
            elif args.action == 'check':
                payload, captured = fetch_scoreboard(day)
                write_new(evidence / 'scoreboard.json', {'captured_at': captured.isoformat(), 'payload': payload})
                result = health(conn, day, payload, now())
            elif args.action == 'review':
                result = review(conn, root, day, evidence)
            elif args.action == 'import-local':
                marker = root / 'prospective-index' / (str(day) + '-' + storage.MODEL_SHA + '.json')
                manifest, packed = storage.bundle(root, marker)
                inserted = storage.store(conn, manifest, packed, migration=True)
                result = {'status': 'original_archived', 'inserted': inserted, 'new_forecasts': 0,
                          'forecast_sha256': manifest['forecast_sha256'], 'history': import_history(conn, root, manifest)}
            else:
                result = {'status': 'restored', 'originals': storage.restore_all(conn, root)}
                restore_reviews(conn, root)
                materialize_grades(root)
                reports(root, evidence)
                with conn.cursor() as cur:
                    cur.execute("SELECT observed_at,result FROM nba_research_runs WHERE action='review' ORDER BY observed_at DESC,id DESC LIMIT 1")
                    last = cur.fetchone()
                result['latest_review'] = {'observed_at': last[0].isoformat(), 'result': last[1]} if last else None
        except Exception as exc:
            conn.rollback()
            result = {'status': 'failed', 'type': type(exc).__name__, 'reason': str(exc)}
        result.update(date=str(day), action=args.action, observed_at=now().isoformat())
        write_new(evidence / 'result.json', result)
        with conn.cursor() as cur:
            cur.execute('INSERT INTO nba_research_runs(game_date,action,result) VALUES(%s,%s,%s)', (day, args.action, Json(result)))
        conn.commit()
    print(json.dumps(result))
    if result['status'] in ('failed', 'failed_recording', 'partial_attachments', 'missed_cutoff', 'missing_before_tip', 'missed_or_incomplete'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
