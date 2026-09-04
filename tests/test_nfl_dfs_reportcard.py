from datetime import datetime, timezone, timedelta
from model.nfl_dfs_reportcard import build_report

KICK = datetime(2026,9,13,17,tzinfo=timezone.utc)
GAME = dict(id=1, kickoff=KICK, completed=True, home_team="BUF", away_team="MIA")
PLAYER = dict(player_id=1, name="Example", position="WR", team="BUF")


def forecast(**kw):
    return dict(**{**PLAYER, "forecast_id": "1", "variant": "production", "mean": 10.,
                "p10": 2., "p90": 20., "captured_at": KICK-timedelta(hours=1), "stat_means": {"receptions": 5}, **kw})


def result(**kw):
    return dict(**{"id": 1, "player_id": 1, "game_id": 1, "computed_at": KICK+timedelta(hours=8),
        "actual_dk_fpts": 12., "scoring_status": "exact", "scoring_version": "v1", "input_digest": "r1",
        "scoring_evidence": {"scoring_input": {"receptions": 6}}, **kw})


def report(forecasts=None, results=None, **kw):
    return build_report(**{"season": 2026, "week": 1, "games": [GAME], "players": [PLAYER],
        "forecasts": forecasts if forecasts is not None else [forecast()],
        "results": results if results is not None else [result()], "now": KICK+timedelta(days=3), **kw})


def test_latest_valid_snapshot_once_and_variants_separate():
    r = report([forecast(), forecast(forecast_id="2",mean=11.,captured_at=KICK-timedelta(minutes=2)),
                forecast(forecast_id="3",mean=100.,captured_at=KICK)])
    assert r["summary"]["production"]["scored"] == 1
    assert r["rows"][0]["error"] == 1
    assert r["summary"]["opportunity"]["forecasted"] == 0
    assert r["rejected_non_pregame_snapshots"] == 1


def test_corrected_outcome_never_changes_forecast_and_latest_exclusion_wins():
    original = report()["rows"][0]
    corrected = report(results=[result(),result(id=2, actual_dk_fpts=0,computed_at=KICK+timedelta(days=2))])["rows"][0]
    assert corrected["forecast"] == original["forecast"]
    assert corrected["status"] == "corrected" and corrected["actual"] == 0
    excluded = report(results=[result(),result(id=2,actual_dk_fpts=None,scoring_status="excluded",computed_at=KICK+timedelta(days=2))])["rows"][0]
    assert excluded["actual"] is None and excluded["status"] == "excluded_scoring"


def test_missing_stats_is_not_zero_and_is_overdue_after_grace():
    row = report(results=[])["rows"][0]
    assert row["actual"] is None and row["error"] is None
    assert row["status"] == "awaiting_source" and row["overdue"]
    pending = report(results=[],games=[{**GAME,"completed":False}],now=KICK-timedelta(hours=1))["rows"][0]
    assert pending["status"] == "pending_game" and not pending["overdue"]


def test_forecasted_inactive_player_retained_components_not_recreated():
    row = report(players=[])["rows"][0]
    assert row["player_id"] == 1 and row["components"][0]["error"] == 1
    assert report([forecast(stat_means={})])["rows"][0]["components"] == []


def test_no_forecast_is_in_denominator_not_graded_as_zero():
    r = report(forecasts=[])
    assert r["summary"]["production"]["players"] == 1
    assert r["summary"]["production"]["scored"] == 0
    assert r["rows"][0]["status"] == "forecast_unavailable"


def test_future_correction_cannot_change_an_earlier_evaluation():
    r = report(results=[result(), result(id=2, actual_dk_fpts=99,computed_at=KICK+timedelta(days=4))])
    assert r["rows"][0]["actual"] == 12
