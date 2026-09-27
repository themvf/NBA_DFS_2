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
    assert result == 2
    report = json.loads((tmp_path/'out'/'run-report.json').read_text())
    assert {r['game_id']:r['status'] for r in report} == {
        GAME['game_id']:'partial','2026_02_DAL_PHI':'awaiting_publication'}
    assert (tmp_path/'out'/f"{GAME['game_id']}.json").exists()
