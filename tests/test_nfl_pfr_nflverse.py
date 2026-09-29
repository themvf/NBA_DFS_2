from datetime import datetime, timezone

import pandas as pd
import pytest

from ingest.nfl_pfr_nflverse import KINDS, build_supplement

GAME = dict(game_id="2026_01_DAL_PHI", pfr="202609130phi", season=2026, week=1,
            home_team="PHI", away_team="DAL")


def frames():
    row = dict(game_id=GAME["game_id"], pfr_game_id=GAME["pfr"], season=2026, week=1,
               game_type="REG", team="DAL", opponent="PHI", pfr_player_name="Test Player",
               pfr_player_id="TestPl00", times_pressured=10, times_pressured_pct=.185,
               passing_drops=float("nan"), times_sacked=0)
    return {k: pd.DataFrame([row]) for k in KINDS}


def test_fraction_conversion_and_missing_sections():
    result = build_supplement(GAME, frames(), {}, "2026-09-27T00:00:00Z")
    assert result["status"] == "partial"
    assert result["coverage"]["home_starters"]["status"] == "missing"
    assert result["rows"][0]["stats"]["times_pressured_pct"] == 18.5
    assert result["rows"][0]["raw"]["times_pressured_pct"] == .185
    assert result["rows"][0]["stats"]["passing_drops"] is None
    assert result["rows"][0]["stats"]["times_sacked"] == 0


@pytest.mark.parametrize("field,value", [("pfr_game_id", "202609140phi"), ("team", "NYG"),
                                         ("week", 2), ("times_pressured_pct", 18.5), ("pfr_player_id", None)])
def test_fail_closed_on_mismatches(field, value):
    data = frames()
    data["pass"].loc[0, field] = value
    with pytest.raises(ValueError):
        build_supplement(GAME, data, {}, "2026-09-27T00:00:00Z")


def test_duplicate_player_rejected():
    data = frames()
    data["pass"] = pd.concat([data["pass"], data["pass"]])
    with pytest.raises(ValueError, match="Duplicate"):
        build_supplement(GAME, data, {}, "2026-09-27T00:00:00Z")


def test_publication_lag_does_not_block_other_games(tmp_path, monkeypatch):
    import json
    from ingest import nfl_pfr_nflverse as module
    schedule = pd.DataFrame([dict(GAME,home_score=10,away_score=7),
                             dict(GAME,game_id='2026_02_DAL_PHI',pfr='202609200phi',week=2,home_score=20,away_score=14)])
    monkeypatch.setattr(module, 'fetch_schedule', lambda: schedule)
    for kind, frame in frames().items():
        frame.to_csv(tmp_path / f'advstats_week_{kind}_2026.csv', index=False)
    result = module.main(['--season','2026','--input-dir',str(tmp_path),'--output-dir',str(tmp_path/'out')])
    # This schedule carries no kickoff, so the uncharted game cannot be dated
    # and is reported as degraded rather than excused as a normal delay.
    assert result == 2
    report = json.loads((tmp_path/'out'/'run-report.json').read_text())
    assert {r['game_id']:r['status'] for r in report} == {
        GAME['game_id']:'partial','2026_02_DAL_PHI':'awaiting_publication'}
    assert (tmp_path/'out'/f"{GAME['game_id']}.json").exists()


# -- Run status: a normal charting delay is not a degraded input ------------

def _run(tmp_path, monkeypatch, schedule, now):
    import json
    from ingest import nfl_pfr_nflverse as module
    monkeypatch.setattr(module, 'fetch_schedule', lambda: schedule)
    for kind, frame in frames().items():
        frame.to_csv(tmp_path / f'advstats_week_{kind}_2026.csv', index=False)
    code = module.main(['--season', '2026', '--input-dir', str(tmp_path), '--output-dir', str(tmp_path / 'out')],
                       now=now)
    return code, json.loads((tmp_path / 'out' / 'run-status.json').read_text())


def _schedule(week2_day):
    return pd.DataFrame([
        dict(GAME, home_score=10, away_score=7, gameday='2026-09-13', gametime='13:00'),
        dict(GAME, game_id='2026_02_DAL_PHI', pfr='202609200phi', week=2, home_score=20, away_score=14,
             gameday=week2_day, gametime='13:00')])


def test_a_game_finished_within_the_grace_window_is_awaiting_not_degraded(tmp_path, monkeypatch):
    # Week 2 kicked off Sunday 17:00 UTC; Tuesday 12:00 UTC is ~39h after it ended.
    code, status = _run(tmp_path, monkeypatch, _schedule('2026-09-20'),
                        datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc))
    assert code == 0
    assert status['status'] == 'awaiting_publication'
    assert status['awaiting_publication'] == ['2026_02_DAL_PHI'] and status['overdue'] == []


def test_a_game_still_uncharted_after_the_grace_window_is_degraded(tmp_path, monkeypatch):
    code, status = _run(tmp_path, monkeypatch, _schedule('2026-09-20'),
                        datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc))
    assert code == 2
    assert status['status'] == 'degraded' and status['overdue'] == ['2026_02_DAL_PHI']
    assert '2026_02_DAL_PHI' in status['reasons'][0]


def test_an_undatable_game_is_reported_not_excused():
    from ingest.nfl_pfr_nflverse import publication_lag
    now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
    assert publication_lag(dict(GAME), now) == 'overdue'
    assert publication_lag(dict(GAME, gameday='not a date', gametime='13:00'), now) == 'overdue'
    assert publication_lag(dict(GAME, gameday='2026-09-20', gametime='13:00'), now) == 'recent'


def test_identity_refresh_failure_is_degraded_even_when_everything_is_charted():
    from ingest.nfl_pfr_nflverse import run_status
    now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
    ok = run_status([dict(GAME)], [], {"status": "captured"}, now)
    assert ok['status'] == 'refreshed' and ok['reasons'] == []
    failed = run_status([dict(GAME)], [], {"status": "refresh_unavailable", "reason": "HTTPError"}, now)
    assert failed['status'] == 'degraded' and 'identity' in failed['reasons'][0]


def test_a_download_failure_raises_and_leaves_no_stale_status(tmp_path, monkeypatch):
    from ingest import nfl_pfr_nflverse as module
    out = tmp_path / 'out'
    out.mkdir()
    (out / 'run-status.json').write_text('{"status": "refreshed"}')
    monkeypatch.setattr(module, 'fetch_schedule', lambda: _schedule('2026-09-20'))

    def unavailable(*args, **kwargs):
        raise module.requests.ConnectionError("release unavailable")
    monkeypatch.setattr(module.requests, 'get', unavailable)
    with pytest.raises(module.requests.ConnectionError):
        module.main(['--season', '2026', '--output-dir', str(out)],
                    now=datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc))
    assert not (out / 'run-status.json').exists()
