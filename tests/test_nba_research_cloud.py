import gzip
import hashlib
import json
from copy import deepcopy
from datetime import date, datetime, timezone

import pytest

from research import nba_research_archive as archive
from research.nba_points_forward import digest, write_new
from research.nba_research_alert import alert, TITLE
from research.nba_research_cloud import health, pinned_model, verified_game_copy, session_database_url, record, save_v2_review


def signed(value):
    return {**value, 'sha256': digest(value)}


def original(tmp_path):
    root = tmp_path / 'original-root'
    directory = root / 'first-original'
    source = signed({'mode': 'prospective', 'game_date': '2026-11-06', 'as_of': '2026-11-06T12:00:00Z',
        'created_at': '2026-11-06T12:00:01Z', 'lock_at': '2026-11-06T20:00:00Z',
        'slate_schedule': [{'event_id': 'espn', 'start_time': '2026-11-06T20:00:00Z'}]})
    forecast = signed({'mode': 'prospective', 'game_date': source['game_date'], 'model_sha256': archive.MODEL_SHA,
        'source_snapshot_sha256': source['sha256'], 'created_at': '2026-11-06T12:00:02Z', 'lock_at': source['lock_at']})
    write_new(directory / 'source-snapshot.json', source)
    write_new(directory / 'forecast.json', forecast)
    write_new(directory / 'receipt.json', {'status': 'recorded'})
    marker = root / 'prospective-index' / (source['game_date'] + '-' + archive.MODEL_SHA + '.json')
    write_new(marker, {'date': source['game_date'], 'model_sha256': archive.MODEL_SHA,
        'forecast_sha256': forecast['sha256'], 'forecast_path': str(directory / 'forecast.json')})
    return root, directory, marker


def test_ephemeral_runner_restores_exact_bytes_and_index_once(tmp_path):
    root, directory, marker = original(tmp_path)
    manifest, packed = archive.bundle(root, marker)
    restored = tmp_path / 'ephemeral'
    target = archive.restore(restored, manifest, packed)
    assert (target / 'forecast.json').read_bytes() == (directory / 'forecast.json').read_bytes()
    assert archive.restore(restored, manifest, packed) == target
    restored_index = archive.read(restored / 'prospective-index' / marker.name)
    assert restored_index['forecast_path'] == str((target / 'forecast.json').resolve())
    # The original path-bearing marker is retained as separate transport evidence.
    assert archive.read(next((restored / 'archive-index-evidence').rglob(marker.name))) == archive.read(marker)
    (target / 'forecast.json').write_text('{}')
    with pytest.raises(ValueError, match='conflicts'):
        archive.restore(restored, manifest, packed)


def test_late_or_reconstructed_source_cannot_be_archived_as_original(tmp_path):
    root, directory, marker = original(tmp_path)
    forecast = archive.read(directory / 'forecast.json')
    forecast['created_at'] = forecast['lock_at']
    forecast = signed({k: v for k, v in forecast.items() if k != 'sha256'})
    (directory / 'forecast.json').write_text(json.dumps(forecast))
    idx = archive.read(marker); idx['forecast_sha256'] = forecast['sha256']; marker.write_text(json.dumps(idx))
    with pytest.raises(ValueError, match='cutoff'):
        archive.bundle(root, marker)


@pytest.mark.parametrize('name', ['../escape.json', '/absolute.json', 'C:/escape.json', 'a\\escape.json', 'a/../escape.json'])
def test_archive_rejects_path_traversal_before_writing(tmp_path, name):
    root, _, marker = original(tmp_path)
    manifest, packed = archive.bundle(root, marker)
    packet = json.loads(gzip.decompress(packed))
    old = next(iter(packet['files']))
    packet['files'][name] = packet['files'].pop(old)
    manifest['files'][name] = manifest['files'].pop(old)
    packed = gzip.compress(json.dumps(packet).encode())
    manifest['archive_sha256'] = hashlib.sha256(packed).hexdigest()
    dest = tmp_path / 'not-written'
    with pytest.raises(ValueError, match='unsafe'):
        archive.restore(dest, manifest, packed)
    assert not dest.exists()


def test_archive_checks_bytes_and_metadata(tmp_path):
    root, _, marker = original(tmp_path)
    manifest, packed = archive.bundle(root, marker)
    with pytest.raises(ValueError, match='digest'):
        archive.unpack(manifest, packed[:-1] + b'x')
    bad = deepcopy(manifest); bad['forecast_sha256'] = 'wrong'
    with pytest.raises(ValueError, match='metadata'):
        archive.unpack(bad, packed)


def test_cloud_unpacks_the_original_pinned_model(tmp_path):
    p = pinned_model(tmp_path / 'model.json')
    assert archive.checked(archive.read(p))['sha256'] == archive.MODEL_SHA
    assert pinned_model(p) == p


def test_session_lock_uses_direct_neon_endpoint_without_changing_credentials(monkeypatch):
    monkeypatch.delenv('DATABASE_URL_UNPOOLED', raising=False)
    pooled='postgresql://role:encoded%40password@ep-example-pooler.us-east-2.aws.neon.tech/db?sslmode=require'
    assert session_database_url(pooled)==pooled.replace('ep-example-pooler.','ep-example.')
    assert session_database_url('postgresql://localhost/db')=='postgresql://localhost/db'
    monkeypatch.setenv('DATABASE_URL_UNPOOLED','postgresql://direct/db')
    assert session_database_url(pooled)=='postgresql://direct/db'


class Cursor:
    def __init__(self, values): self.values = iter(values)
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, *args): pass
    def fetchone(self): return next(self.values)


class Connection:
    def __init__(self, values): self.values = iter(values)
    def cursor(self): return Cursor(self.values)


def test_prelock_health_distinguishes_missing_partial_and_no_slate():
    payload = {'events': [{'date': '2026-11-06T20:00:00Z'}]}
    before = datetime(2026, 11, 6, 17, tzinfo=timezone.utc)
    missing = health(Connection([None, None]), date(2026,11,6), payload, before)
    assert missing['status'] == 'missing_before_tip' and missing['can_record_new_original']
    partial = health(Connection([('v2',), ({'game_date':'2026-11-06','attachments': {'trio-math-index': 'missing_or_failed'}},)]), date(2026,11,6), payload, before)
    assert not partial['can_record_new_original'] and any('trio-math-index' in p for p in partial['problems'])
    assert health(Connection([]), date(2026,11,6), {'events': []}, before)['status'] == 'no_slate'
    early = datetime(2026,11,6,14,tzinfo=timezone.utc)
    assert health(Connection([None,None]), date(2026,11,6), payload, early)['status']=='awaiting_scheduled_freeze'


def test_uncertain_previous_attempt_and_late_run_never_forecast(monkeypatch, tmp_path):
    import research.nba_research_cloud as cloud
    at=datetime(2026,11,6,17,tzinfo=timezone.utc)
    monkeypatch.setattr(cloud,'now',lambda:at)
    monkeypatch.setattr(cloud,'fetch_scoreboard',lambda day:({'events':[{'date':'2026-11-06T20:00:00Z'}]},at))
    monkeypatch.setattr(cloud,'command',lambda *args,**kwargs:pytest.fail('must not generate another forecast'))
    with pytest.raises(ValueError,match='earlier forecast attempt'):
        record(Connection([None,(1,)]),tmp_path,date(2026,11,6),tmp_path/'uncertain')
    at=datetime(2026,11,6,21,tzinfo=timezone.utc)
    result=record(Connection([None]),tmp_path,date(2026,11,6),tmp_path/'late')
    assert result['status']=='missed_cutoff' and result['new_originals']==0


def test_v2_only_review_is_durable_without_creating_v3_index(tmp_path):
    root=tmp_path/'worker'
    review=root/'operations'/'review-1'/'2026-11-08'
    source=signed({'version':'nba-points-ghost-v2','mode':'prospective','game_date':'2026-11-08'})
    write_new(review/'v2-review'/'snapshot.json',source)
    write_new(review/'v2-review'/'summaries'/'summary-game.json',{'original':'raw ESPN payload'})
    captured=[]
    class Writer:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,parameters):captured.append(parameters)
    class Database:
        def cursor(self):return Writer()
        def commit(self):pass
    save_v2_review(Database(),root,review)
    meta=captured[0][2].adapted
    restored=tmp_path/'reader'
    archive.restore(restored,meta,captured[0][3])
    assert (restored/'operations'/'review-1'/'2026-11-08'/'v2-review'/'summaries'/'summary-game.json').exists()
    assert not (restored/'prospective-index').exists()


def test_notifications_are_deduplicated_and_recovery_is_scoped():
    calls, issues, comments = [], [], []
    def api(method, path, data=None):
        calls.append((method, path, data))
        if method == 'GET': return comments if 'comments?' in path else issues
        if method == 'POST' and path == '/issues':
            issues.append({'title': data['title'], 'number': 1, 'state': 'open', 'body': data['body']})
        if method == 'POST' and path.endswith('/comments'): comments.append(data)
        if method == 'PATCH': issues[0]['state'] = data['state']
    failure = [{'date': '2026-11-06', 'status': 'missing_before_tip', 'problems': ['missing v3'], 'action': 'check'}]
    assert alert(failure, api, 'https://example.test/run')['notification']
    assert not alert(failure, api, 'https://example.test/later-run')['notification']
    assert not alert([{'status': 'reviewed', 'action': 'review'}], api, 'url')['notification']
    assert issues[0]['state'] == 'open'
    assert not alert([{'status':'already_running','action':'check'}],api,'url')['notification']
    assert issues[0]['state'] == 'open'
    assert alert([{'status': 'healthy', 'action': 'check'}], api, 'url')['notification']
    assert issues[0]['state'] == 'closed'


def test_failure_notifier_starts_without_installed_dependencies():
    import subprocess, sys
    from pathlib import Path
    result=subprocess.run([sys.executable,'-S','-m','research.nba_research_alert','--help'],
        cwd=Path(__file__).resolve().parents[1],capture_output=True,text=True,timeout=20)
    assert result.returncode==0, result.stderr
    assert '--action' in result.stdout


def test_ambiguous_unlisted_game_is_not_silently_filled(tmp_path):
    source = {'sha256':'source','game_date':'2026-11-06','candidates':[
        {'event_id':'book','player_key':'p','home_team':'Boston Celtics','away_team':'Miami Heat'}], 'slate_schedule':[]}
    diagnostics = {'rows':[{'event_id':'book','player_key':'p','status':'not_listed_in_final_box'}]}
    with pytest.raises(ValueError, match='ambiguous'):
        verified_game_copy(source, diagnostics, tmp_path)
    assert 'espn_event_id' not in diagnostics['rows'][0]


class ArchiveDatabase:
    """Answers the restore queries from memory and counts how many archives were downloaded."""
    def __init__(self, originals=(), reviews=(), v2_reviews=()):
        self.tables = {'nba_research_archives': [(m['forecast_sha256'], m, p) for m, p in originals],
                       'nba_research_reviews': [(m['archive_sha256'], m, p) for m, p in reviews],
                       'nba_research_v2_reviews': [(m['archive_sha256'], m, p) for m, p in v2_reviews]}
        self.downloads = []
    def cursor(self):
        database = self
        class Reader:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, parameters=()):
                table = next((t for t in sorted(database.tables, key=len, reverse=True) if ' ' + t + ' ' in sql + ' '), None)
                if 'to_regclass' in sql:
                    self.rows = [(None,)]
                elif sql.startswith('SELECT archive FROM '):
                    database.downloads.append((table, parameters[0]))
                    self.rows = [(p,) for key, _, p in database.tables[table] if key == parameters[0]]
                elif 'manifest' in sql and ',archive' not in sql:
                    self.rows = [(key, m) for key, m, _ in database.tables[table]]
                else:
                    raise AssertionError('unexpected query: ' + sql)
            def fetchall(self): return self.rows
            def fetchone(self): return self.rows[0] if self.rows else None
        return Reader()


def tree(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob('*')) if p.is_file()}


def test_archive_cache_changes_only_where_the_bytes_come_from(tmp_path):
    archive.TRANSPORT.update(cache=0, database=0)
    packed, name = b'packed archive bytes', hashlib.sha256(b'packed archive bytes').hexdigest()
    calls = []
    def fetch():
        calls.append(1)
        return memoryview(packed)                      # psycopg2 hands BYTEA back as a memoryview
    assert archive.packed_archive(None, name, fetch) == packed and len(calls) == 1
    cache = tmp_path / 'cache'
    assert archive.packed_archive(cache, name, fetch) == packed and len(calls) == 2      # cold: downloaded and kept
    assert (cache / (name + '.gz')).read_bytes() == packed
    assert archive.packed_archive(cache, name, fetch) == packed and len(calls) == 2      # warm: no download
    assert archive.TRANSPORT == {'cache': 1, 'database': 2}
    (cache / (name + '.gz')).write_bytes(b'damaged copy')
    assert archive.packed_archive(cache, name, fetch) == packed and len(calls) == 3      # a wrong copy is never used
    assert (cache / (name + '.gz')).read_bytes() == packed                              # and is replaced
    with pytest.raises(ValueError, match='digest mismatch'):
        archive.packed_archive(cache, hashlib.sha256(b'other').hexdigest(), fetch)       # the database row must match too
    assert not (cache / (hashlib.sha256(b'other').hexdigest() + '.gz')).exists()
    assert not list(cache.glob('*.partial'))


def test_restore_reads_manifests_always_and_archives_only_when_not_cached(tmp_path):
    from research.nba_research_cloud import restore_reviews
    root, _, marker = original(tmp_path)
    manifest, packed = archive.bundle(root, marker)
    # A v2-only review, built by the real writer so its manifest and bytes are genuine.
    worker = tmp_path / 'worker'
    review = worker / 'operations' / 'review-1' / '2026-11-08'
    write_new(review / 'v2-review' / 'snapshot.json', signed({'version': 'nba-points-ghost-v2', 'mode': 'prospective', 'game_date': '2026-11-08'}))
    captured = []
    class Writer:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, parameters): captured.append(parameters)
    class Sink:
        def cursor(self): return Writer()
        def commit(self): pass
    save_v2_review(Sink(), worker, review)
    v2 = (captured[0][2].adapted, captured[0][3])

    def run(target, cache):
        database = ArchiveDatabase(originals=[(manifest, packed)], v2_reviews=[v2])
        assert archive.restore_all(database, target, cache) == 1
        restore_reviews(database, target, cache)
        return database.downloads

    plain = tmp_path / 'no-cache'
    assert len(run(plain, None)) == 2                                    # no cache: every archive downloaded, as before
    cache, cold, warm = tmp_path / 'cache', tmp_path / 'cold', tmp_path / 'warm'
    assert len(run(cold, cache)) == 2                                    # first run fills the cache
    assert run(warm, cache) == []                                        # second run downloads nothing
    index = 'prospective-index/' + marker.name                           # the only path-bearing file: it names its own root
    files = {name: tree(folder) for name, folder in (('plain', plain), ('cold', cold), ('warm', warm))}
    for name in ('cold', 'warm'):
        assert set(files[name]) == set(files['plain'])
        assert all(files[name][f] == data for f, data in files['plain'].items() if f != index)
    assert (plain / 'operations' / 'review-1' / '2026-11-08' / 'v2-review' / 'snapshot.json').exists()

    # An archive the cache does not hold (a new night's review) is the only download.
    (cache / (v2[0]['archive_sha256'] + '.gz')).unlink()
    assert run(tmp_path / 'later', cache) == [('nba_research_v2_reviews', v2[0]['archive_sha256'])]
