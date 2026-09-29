from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from model import line_alerts

ROOT = Path(__file__).resolve().parents[1]
NHL_DETECTORS = {"pinnacle_divergence", "dk_value", "steam", "walking"}


def test_nhl_registers_only_the_generic_moneyline_detectors() -> None:
    assert "nhl" in line_alerts._ALERT_SPORTS
    registered = {row["alert_type"] for row in line_alerts.DETECTOR_REGISTRY if row["sport"] == "nhl"}
    # No Polymarket book is captured for NHL, so Pin/Poly delta could never fire.
    assert registered == NHL_DETECTORS


def _ts_registry() -> set[tuple[str, str, str]]:
    source = (ROOT / "web/src/db/queries.ts").read_text(encoding="utf-8")
    block = source[source.index("const DETECTOR_REGISTRY"):]
    block = block[: block.index("];")]
    return set(re.findall(r'sport: "(\w+)", alertType: "(\w+)", deployedAt: "([\d-]+)"', block))


def test_web_detector_registry_mirrors_python_for_nhl() -> None:
    python = {(row["sport"], row["alert_type"], row["deployed_at"].isoformat())
              for row in line_alerts.DETECTOR_REGISTRY if row["sport"] == "nhl"}
    web = {row for row in _ts_registry() if row[0] == "nhl"}
    assert web == python


def _book(home: int, away: int) -> dict:
    return {"ml_home": home, "ml_away": away, "last_update": "2026-09-29T18:00:00Z"}


def test_scan_runs_only_moneyline_detectors_and_stamps_steam_interval(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    moved = {key: _book(-150, 130) for key in ("draftkings", "fanduel", "betmgm")}
    opening = {key: _book(-110, -110) for key in moved}
    current = {"history_id": 7, "matchup_id": 1, "game_date": "2026-09-29",
               "home_team_name": "Carolina Hurricanes", "away_team_name": "Florida Panthers",
               "captured_at": now, "capture_key": "k", "books": moved,
               "commence_time": now + timedelta(hours=2), "tour": None, "tournament": None, "surface": None}

    class Db:
        def execute(self, sql, params=None):
            assert "JOIN nhl_matchups m" in sql
            return [dict(current)]

        def execute_one(self, sql, params=None):
            captured = now - timedelta(minutes=45)
            return {"history_id": 6, "captured_at": captured, "capture_key": "p", "books": opening}

    inserted: list[dict] = []
    monkeypatch.setattr(line_alerts, "_insert", lambda _db, **kw: inserted.append(kw) or [])
    for football_or_tennis_only in ("_moneyline_structure_signals", "_cfb_market_signals", "_nfl_market_signals"):
        monkeypatch.setattr(line_alerts, football_or_tennis_only,
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("not an NHL detector")))
    line_alerts.scan(Db(), "nhl")
    kinds = {(row["alert_type"], row["side"]) for row in inserted}
    assert kinds == {("steam", "home"), ("walking", "home")}
    steam = next(row for row in inserted if row["alert_type"] == "steam")
    assert steam["details"]["interval_minutes"] == 45.0
    assert steam["details"]["exec_decimal"] > 1  # price frozen at trigger


class _Cursor:
    def __init__(self):
        self.writes: list = []

    def execute(self, sql, params=None):
        self.writes.append((sql, params))


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self.cursor_instance


def _settle(monkeypatch, matchup: dict):
    class Db:
        database_url = None

        def __init__(self):
            self.connection = _Connection()

        def execute(self, sql, params=None):
            return [{"id": 5, "matchup_id": 1, "side": "home", "sport": "nhl", "alert_type": "steam",
                     "alert_prob": 0.55, "commence_time": datetime(2026, 9, 29, 21, tzinfo=timezone.utc),
                     "details_json": {"market": "moneyline", "exec_decimal": 1.8}}]

        def execute_one(self, sql, params=None):
            assert "FROM nhl_matchups" in sql and "completed" in sql
            return matchup

        def connect(self):
            return self.connection

    monkeypatch.setattr(line_alerts, "_verified_close", lambda *_a, **_k: {"books": {"fanduel": _book(-160, 140)}})
    monkeypatch.setattr(line_alerts, "_grade_alert_prices", lambda *_: {
        "dk_close_decimal": None, "dk_clv_pct": None, "pin_close_prob": None, "convergence": None,
        "dk_survival_min": None, "grading_json": {}, "comparison_status": "NO_REFERENCE",
        "grading_version": "convergence_v2"})
    monkeypatch.setattr(line_alerts, "_append_grade_history_cur", lambda *_a, **_k: None)
    db = Db()
    assert line_alerts.settle(db, "nhl") == 1
    return db.connection.cursor_instance.writes[0][1]


def test_an_unfinished_game_is_never_graded_on_a_running_score(monkeypatch) -> None:
    params = _settle(monkeypatch, {"home_score": 2, "away_score": 1, "completed": False})
    assert params[2] is None  # outcome
    assert params[1] is not None  # CLV against the frozen close still recorded
    assert params[12] is None  # no units without an outcome


def test_a_final_settles_with_units_at_the_frozen_price(monkeypatch) -> None:
    params = _settle(monkeypatch, {"home_score": 3, "away_score": 2, "completed": True})
    assert params[2] == "won"
    assert params[12] == pytest.approx(0.8)  # 1.8 decimal, 1 unit staked
    assert json.loads(params[8])["pnl_units"] == 0.8


def test_live_schedule_payloads_do_not_store_running_scores() -> None:
    from ingest.nhl_schedule import parse_schedule_games

    team = lambda i, a: {"id": i, "abbrev": a, "placeName": {"default": a}, "commonName": {"default": "X"}}
    game = {"id": 1, "season": 20262027, "gameType": 2, "startTimeUTC": "2026-09-29T23:00:00Z",
            "gameState": "LIVE", "awayTeam": {**team(1, "MTL"), "score": 2},
            "homeTeam": {**team(2, "TOR"), "score": 1}}
    row = parse_schedule_games({"gameWeek": [{"games": [game]}]})[0]
    assert (row["home_score"], row["away_score"], row["completed"]) == (None, None, False)
