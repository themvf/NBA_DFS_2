from datetime import datetime, timezone

import pytest

from research import nba_percentile_trial as trial, nba_research_archive as archive
from research.nba_points_forward import write_new


def original(tmp_path):
    date = trial.FIRST_SLATE
    at = lambda hour: f'{date}T{hour}:00:00+00:00'
    directory = tmp_path / 'original'
    source = trial.signed({'game_date': date, 'mode': 'prospective', 'as_of': at('15'),
        'created_at': at('15'), 'lock_at': at('23'),
        'slate_schedule': [{'event_id': 'espn1', 'start_time': at('23')},
                           {'event_id': 'espn2', 'start_time': at('23')} ]})
    # Players 1 and 2 have opposite draw ranks, exposing an implementation
    # that adds individual P75s instead of summing within the same draw.
    samples = [[30, 0, 0, 30], [0, 30, 30, 0]] + [[20+n]*4 for n in range(4)]
    rows = [{'event_id': 'market1', 'player_key': str(i), 'player': f'Player {i}',
             'samples': values, 'distribution': {'mean': sum(values)/len(values),
             'median': 15 if i<2 else values[0], 'p90': max(values)},
             'market_line': 15.5 if i<2 else 20.5, 'v2_blend': 16.0 if i<2 else 21.0}
            for i, values in enumerate(samples)]
    forecast = trial.signed({'version': 'nba-points-ghost-v3', 'game_date': date,
        'mode': 'prospective', 'source_snapshot_sha256': source['sha256'], 'model_sha256': archive.MODEL_SHA,
        'lock_at': at('23'), 'created_at': at('16'), 'rows': rows})
    write_new(directory / 'forecast.json', forecast)
    write_new(directory / 'source-snapshot.json', source)
    marker = tmp_path / 'prospective-index' / (date + '-' + archive.MODEL_SHA + '.json')
    write_new(marker, {'date': date, 'forecast_path': str(directory / 'forecast.json'),
                       'forecast_sha256': forecast['sha256'], 'model_sha256': archive.MODEL_SHA})
    return directory, marker, forecast, source


def test_percentile_end_to_end_restore_and_pairing(tmp_path, monkeypatch):
    root = tmp_path / 'source'
    directory, marker, forecast, source = original(root)
    date = source['game_date']
    monkeypatch.setattr(trial, 'now', lambda: datetime.fromisoformat(date + 'T16:01:00+00:00'))
    receipt = trial.attach(forecast, source, directory, root)
    assert receipt['status'] == 'indexed'
    manifest, bundle = archive.bundle(root, marker)
    assert manifest['attachments'][trial.INDEX] == 'indexed'
    restored = tmp_path / 'restored'
    target = archive.restore(restored, manifest, bundle)
    assert (target / trial.FILENAME).read_bytes() == (directory / trial.FILENAME).read_bytes()
    assert trial.report(restored)['pending_dates'] == [date]
    record = trial.read(target / trial.FILENAME)
    assert record['cards']['mean_baseline'][0]['mean'] == 66
    assert record['cards']['joint_p75'][0]['p75'] == 75
    assert record['cards']['joint_p75'][0]['players'] == [['market1', '0'], ['market1', '4'], ['market1', '5']]
    diag = {'game_date': date, 'snapshot_sha256': source['sha256'],
            'rows': [{'event_id': 'market1', 'player_key': str(i), 'actual': {'points': i+20}, 'status': 'verified'}
                     for i in range(6)]}
    board = {'payload': {'events': [{'id': event, 'status': {'type': {'completed': True}}}
                                     for event in ('espn1', 'espn2')]}}
    graded_at = datetime.fromisoformat('2026-11-17T13:00:00+00:00')
    pending_board = {'payload': {'events': board['payload']['events'][:1]}}
    assert trial.grade(record, forecast, source, diag, pending_board, graded_at)['cards']['joint_p75']['best'] is None
    pending = {**diag, 'rows': [{**r, 'status': 'dnp'} if r['player_key'] == '0' else r for r in diag['rows']]}
    assert trial.grade(record, forecast, source, pending, board, graded_at)['cards']['joint_p75']['best'] is None
    result = trial.grade(record, forecast, source, diag, board, graded_at)
    for attempt in ('first', 'repeat'):
        write_new(restored / 'operations' / attempt / date / trial.GRADE, result)
    stats = trial.report(restored)
    assert stats['recordings'] == stats['graded_dates'] == stats['paired_dates'] == 1
    assert stats['research']['best']['n'] == 1 and not stats['pending_dates']
    assert stats['cohorts']['all']['75']['experimental']['n'] == 6
    assert stats['cohorts']['market_line_at_least_20']['75']['experimental']['n'] == 4


def test_no_late_or_duplicate_enrollment(tmp_path, monkeypatch):
    directory, _, forecast, source = original(tmp_path)
    date = source['game_date']
    monkeypatch.setattr(trial, 'now', lambda: datetime.fromisoformat(date + 'T23:00:00+00:00'))
    with pytest.raises(ValueError, match='cutoff'):
        trial.attach(forecast, source, directory, tmp_path)
    assert not (tmp_path / trial.INDEX).exists()
    monkeypatch.setattr(trial, 'now', lambda: datetime.fromisoformat(date + 'T16:01:00+00:00'))
    trial.attach(forecast, source, directory, tmp_path)
    saved = (directory / trial.FILENAME).read_bytes()
    with pytest.raises(ValueError, match='already exists'):
        trial.attach(forecast, source, directory, tmp_path)
    assert (directory / trial.FILENAME).read_bytes() == saved
    assert trial.attach({'game_date': '2026-11-14'}, {}, directory, tmp_path)['status'] == 'before_trial'
