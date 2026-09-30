"""A stats refresh that writes nothing must not end green.

refresh_mlb_stats.yml succeeded every day from 2026-07-13 to 2026-09-29 while
printing "Batter stats: 0 players upserted" and "Pitcher stats: 0 pitchers
upserted" (run 36611897851). pybaseball's `batting_stats_range` /
`pitching_stats_range` scrape Baseball-Reference, whose frames carry `mlbID`
and no FanGraphs `playerid`/`IDfg`, so `_get_player_id()` returned None for
every row and every row was skipped. The batter splits UPDATE then stamped
`fetched_at = NOW()` on 396 rows, so the table read as refreshed today over a
maximum games-played of 30 (late April).
"""

from __future__ import annotations

import sys
import types
from contextlib import contextmanager

import pandas as pd
import pytest
import requests

from ingest import mlb_stats
from ingest.mlb_stats import MlbStatsRefreshError


def _br_batting_frame(n: int = 60) -> pd.DataFrame:
    """The Baseball-Reference shape batting_stats_range actually returns."""
    return pd.DataFrame({
        "Name": [f"Player {i}" for i in range(n)], "Age": 28, "#days": 0, "Lev": "MLB", "Tm": "NYY",
        "G": 20, "PA": 60, "AB": 55, "R": 8, "H": 15, "2B": 3, "3B": 0, "HR": 2, "RBI": 7, "BB": 5,
        "IBB": 0, "SO": 12, "HBP": 1, "SH": 0, "SF": 0, "GDP": 1, "SB": 1, "CS": 0,
        "BA": 0.273, "OBP": 0.35, "SLG": 0.45, "OPS": 0.8, "mlbID": [600000 + i for i in range(n)],
    })


def _fg_batting_frame(n: int = 60, *, games: int = 20, ids: bool = True) -> pd.DataFrame:
    frame = pd.DataFrame({
        "Name": [f"Player {i}" for i in range(n)], "Team": "NYY", "G": games, "PA": 60, "H": 15,
        "2B": 3, "3B": 0, "HR": 2, "RBI": 7, "R": 8, "BB": 5, "SB": 1, "HBP": 1,
        "AVG": 0.273, "OBP": 0.35, "SLG": 0.45, "ISO": 0.18, "BABIP": 0.3, "wRC+": 110.0,
        "K%": 0.2, "BB%": 0.08,
    })
    frame["IDfg"] = [10000 + i for i in range(n)] if ids else [float("nan")] * n
    return frame


def _forbidden(*_args, **_kwargs):
    raise requests.HTTPError("Error accessing 'https://www.fangraphs.com/leaders-legacy.aspx'. Received status code 403")


def _install_fake_pybaseball(monkeypatch, **functions) -> None:
    monkeypatch.setitem(sys.modules, "pybaseball", types.SimpleNamespace(**functions))


def test_a_baseball_reference_frame_is_unusable_and_falls_through(caplog) -> None:
    """60 rows with mlbID only: not a usable window, so the season/MLB fallbacks run."""
    df, _label = mlb_stats._fetch_batting(
        lambda _s, _e: _br_batting_frame(), _forbidden, "2026-08-15", "2026-09-29", "2026",
    )
    assert df is None
    assert "without a FanGraphs player id column" in caplog.text


def test_a_fangraphs_frame_is_used_as_is() -> None:
    df, label = mlb_stats._fetch_batting(
        lambda _s, _e: _fg_batting_frame(), _forbidden, "2026-08-15", "2026-09-29", "2026",
    )
    assert df is not None and len(df) == 60
    assert label == "2026-08-15 to 2026-09-29"


def test_batters_fall_back_to_the_mlb_api_when_fangraphs_ids_are_absent(monkeypatch) -> None:
    _install_fake_pybaseball(
        monkeypatch, batting_stats_range=lambda _s, _e: _br_batting_frame(), batting_stats=_forbidden,
    )
    calls: list[str] = []
    monkeypatch.setattr(mlb_stats, "fetch_batter_stats_from_mlb_api", lambda db, season: calls.append(season) or 455)
    monkeypatch.setattr(mlb_stats, "upsert_mlb_batter_stats", lambda *a, **k: pytest.fail("must not write BR rows"))

    assert mlb_stats.fetch_batter_stats(object(), "2026") == 455
    assert calls == ["2026"]


def test_a_non_empty_frame_that_writes_zero_rows_raises_with_the_skip_counts(monkeypatch) -> None:
    """The exact silent shape: rows fetched, every row skipped, nothing written."""
    _install_fake_pybaseball(
        monkeypatch, batting_stats_range=lambda _s, _e: _fg_batting_frame(games=0), batting_stats=_forbidden,
    )
    monkeypatch.setattr(mlb_stats, "build_mlb_team_abbrev_cache", lambda db: {})
    monkeypatch.setattr(mlb_stats, "upsert_mlb_batter_stats", lambda *a, **k: pytest.fail("no row is writable"))

    with pytest.raises(MlbStatsRefreshError) as info:
        mlb_stats.fetch_batter_stats(object(), "2026")
    assert "60 FanGraphs rows fetched" in str(info.value)
    assert "no_games=60" in str(info.value)


def test_pitchers_with_no_player_ids_raise_instead_of_printing_zero(monkeypatch) -> None:
    frame = pd.DataFrame({
        "Name": [f"P {i}" for i in range(40)], "Team": "BOS", "G": 10, "GS": 5, "IP": "30.1",
        "W": 3, "ERA": 3.5, "SO": 30, "ER": 12, "H": 25, "BB": 8, "IDfg": [float("nan")] * 40,
    })
    _install_fake_pybaseball(
        monkeypatch, pitching_stats_range=lambda _s, _e: frame, pitching_stats=_forbidden,
    )
    monkeypatch.setattr(mlb_stats, "build_mlb_team_abbrev_cache", lambda db: {})
    monkeypatch.setattr(mlb_stats, "upsert_mlb_pitcher_stats", lambda *a, **k: pytest.fail("no row is writable"))

    with pytest.raises(MlbStatsRefreshError) as info:
        mlb_stats.fetch_pitcher_stats(object(), "2026")
    assert "no_player_id=40" in str(info.value)


def test_the_mlb_api_fallback_returning_nothing_is_a_failure(monkeypatch) -> None:
    monkeypatch.setattr(mlb_stats, "_fetch_mlb_api_player_stats", lambda season, group: [])
    with pytest.raises(MlbStatsRefreshError, match="0 hitting rows"):
        mlb_stats.fetch_batter_stats_from_mlb_api(object(), "2026")
    with pytest.raises(MlbStatsRefreshError, match="0 pitching rows"):
        mlb_stats.fetch_pitcher_stats_from_mlb_api(object(), "2026")


def test_the_mlb_api_transport_failure_carries_its_status(monkeypatch) -> None:
    response = requests.Response()
    response.status_code = 503

    def fail(*_a, **_k):
        raise requests.HTTPError("503 Server Error", response=response)

    monkeypatch.setattr(mlb_stats.requests, "get", fail)
    with pytest.raises(MlbStatsRefreshError, match="HTTP 503"):
        mlb_stats._fetch_mlb_api_player_stats("2026", "pitching")


def test_team_stats_fallback_says_how_old_the_current_state_table_is(monkeypatch, capsys) -> None:
    class Db:
        def execute(self, sql, params=None):
            return [{"team_id": 1, "mlb_id": 147, "abbreviation": "NYY"}]

        def execute_one(self, sql, params=None):
            assert "mlb_team_stats" in sql
            from datetime import datetime, timezone
            return {"latest": datetime(2026, 4, 6, 13, 31, tzinfo=timezone.utc)}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"stats": [{"splits": [{"stat": {"plateAppearances": 6000, "avg": ".250", "slg": ".400",
                                                     "strikeOuts": 1300, "baseOnBalls": 500, "ops": ".720",
                                                     "gamesPlayed": 160, "battersFaced": 6100, "era": "3.90"}}]}]}

    monkeypatch.setattr(mlb_stats.requests, "get", lambda *a, **k: Response())
    monkeypatch.setattr(mlb_stats, "insert_mlb_team_stats_snapshot", lambda *a, **k: None)
    assert mlb_stats.fetch_team_stats_from_mlb_api(Db(), "2026") == 1
    out = capsys.readouterr().out
    assert "current-state rows last refreshed 2026-04-06 13:31 UTC" in out


class _Cursor:
    def __init__(self, fail_on_update: bool) -> None:
        self.sql: list[str] = []
        self.fail_on_update = fail_on_update

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if "UPDATE mlb_batter_stats" in sql and self.fail_on_update:
            raise RuntimeError("connection reset")

    def fetchall(self):
        return [{"id": 1, "player_id": 10001}]


class _SplitsDb:
    def __init__(self, fail_on_update: bool = False) -> None:
        self.cur = _Cursor(fail_on_update)

    @contextmanager
    def connect(self):
        yield self

    def cursor(self):
        return self.cur

    def execute(self, sql, params=None):
        return []


def _splits_response(*_a, **_k):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"data": [{"playerid": 10001, "wRC+": 120.0}]}

    return Response()


def test_the_splits_update_no_longer_stamps_fetched_at(monkeypatch) -> None:
    db = _SplitsDb()
    monkeypatch.setattr(mlb_stats.requests, "get", _splits_response)
    monkeypatch.setattr(mlb_stats, "capture_team_offense_split_snapshots", lambda *a, **k: 0)
    assert mlb_stats.fetch_batter_splits(db, "2026") == 1  # type: ignore[arg-type]
    update = next(s for s in db.cur.sql if "UPDATE mlb_batter_stats" in s)
    assert "fetched_at" not in update


def test_a_database_failure_in_the_splits_update_is_not_swallowed(monkeypatch) -> None:
    db = _SplitsDb(fail_on_update=True)
    monkeypatch.setattr(mlb_stats.requests, "get", _splits_response)
    with pytest.raises(RuntimeError, match="connection reset"):
        mlb_stats.fetch_batter_splits(db, "2026")  # type: ignore[arg-type]


def test_run_refresh_runs_every_stage_then_fails_naming_the_ones_that_wrote_nothing(monkeypatch, capsys) -> None:
    ran: list[str] = []

    def stage(name: str, result):
        def _run(*_a, **_k):
            ran.append(name)
            if isinstance(result, Exception):
                raise result
            return result
        return _run

    monkeypatch.setattr(mlb_stats, "fetch_team_stats", stage("team", 30))
    monkeypatch.setattr(mlb_stats, "fetch_batter_stats", stage("batter", MlbStatsRefreshError("batter stats: 0 written")))
    monkeypatch.setattr(mlb_stats, "fetch_pitcher_stats", stage("pitcher", MlbStatsRefreshError("pitcher stats: 0 written")))
    monkeypatch.setattr(mlb_stats, "fetch_batter_splits", stage("splits", 396))

    with pytest.raises(MlbStatsRefreshError) as info:
        mlb_stats.run_refresh(object(), "2026")  # type: ignore[arg-type]
    assert ran == ["team", "batter", "pitcher", "splits"]
    assert "2 of 4 MLB stats stages refreshed nothing" in str(info.value)
    assert "batter_stats: MlbStatsRefreshError: batter stats: 0 written" in str(info.value)
    out = capsys.readouterr().out
    assert "MLB stats refresh summary: team_stats=30, batter_stats=FAILED, pitcher_stats=FAILED, batter_splits=396" in out


def test_run_refresh_returns_counts_when_every_stage_wrote(monkeypatch) -> None:
    monkeypatch.setattr(mlb_stats, "fetch_team_stats", lambda *a, **k: 30)
    monkeypatch.setattr(mlb_stats, "fetch_batter_stats", lambda *a, **k: 455)
    monkeypatch.setattr(mlb_stats, "fetch_pitcher_stats", lambda *a, **k: 740)
    monkeypatch.setattr(mlb_stats, "fetch_batter_splits", lambda *a, **k: 396)
    assert mlb_stats.run_refresh(object(), "2026") == {  # type: ignore[arg-type]
        "team_stats": 30, "batter_stats": 455, "pitcher_stats": 740, "batter_splits": 396,
    }
