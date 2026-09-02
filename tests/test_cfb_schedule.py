from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest import cfb_schedule
from ingest.game_odds_market import extract_game_markets, lower_median


class _Response:
    def __init__(self, payload, *, last: str = "3"):
        self._payload = payload
        self.headers = {
            "x-requests-remaining": "8000",
            "x-requests-used": "12000",
            "x-requests-last": last,
        }

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _odds_event(**overrides):
    event = {
        "id": "cfb-odds-1",
        "commence_time": "2099-09-05T19:30:00Z",
        "home_team": "Oregon",
        "away_team": "Ohio State",
        "bookmakers": [{
            "key": "draftkings",
            "title": "DraftKings",
            "last_update": "2099-09-05T18:59:00Z",
            "markets": [
                {"key": "h2h", "outcomes": [
                    {"name": "Oregon", "price": -185},
                    {"name": "Ohio State", "price": 155},
                ]},
                {"key": "spreads", "outcomes": [
                    {"name": "Oregon", "point": -4.5, "price": -108},
                    {"name": "Ohio State", "point": 4.5, "price": -112},
                ]},
                {"key": "totals", "outcomes": [
                    {"name": "Over", "point": 52.0, "price": -105},
                    {"name": "Under", "point": 52.0, "price": -115},
                ]},
            ],
        }],
    }
    event.update(overrides)
    return event


def test_lower_median_is_observed_and_deterministic() -> None:
    assert lower_median([-3.0, -3.0, -3.5, -4.0]) == -3.5
    assert lower_median([45.0, 45.5]) == 45.0
    assert lower_median([44.5, 45.0, 45.5]) == 45.0
    assert lower_median([]) is None


def test_shared_market_parser_preserves_cfb_line_and_price() -> None:
    parsed = extract_game_markets(_odds_event())
    dk = parsed["books"]["draftkings"]
    assert dk["spread_home"] == -4.5
    assert dk["spread_home_price"] == -108
    assert dk["spread_away"] == 4.5
    assert dk["spread_away_price"] == -112
    assert dk["total_line"] == 52.0
    assert dk["over"] == -105
    assert dk["under"] == -115
    assert dk["last_update"] == "2099-09-05T18:59:00Z"


def test_fetch_schedule_uses_cfbd_identity_and_overtime_fields(monkeypatch) -> None:
    game = {
        "id": 401000001,
        "season": 2099,
        "week": 1,
        "seasonType": "regular",
        "startDate": "2099-09-05T19:30:00Z",
        "startTimeTBD": False,
        "completed": True,
        "neutralSite": False,
        "conferenceGame": False,
        "venueId": 99,
        "venue": "Autzen Stadium",
        "homeId": 1,
        "homeTeam": "Oregon",
        "homeConference": "Big Ten",
        "homeClassification": "fbs",
        "homePoints": 35,
        "homeLineScores": [7, 7, 7, 7, 7],
        "awayId": 2,
        "awayTeam": "Ohio State",
        "awayConference": "Big Ten",
        "awayClassification": "fbs",
        "awayPoints": 28,
        "awayLineScores": [7, 7, 7, 7, 0],
    }
    monkeypatch.setattr(
        cfb_schedule.requests,
        "get",
        lambda url, **kwargs: _Response(
            [{"id": game["id"], "mediaType": "tv", "outlet": "NBC"}]
            if url.endswith("/games/media") else [game]
        ),
    )
    teams = {}
    monkeypatch.setattr(
        cfb_schedule,
        "upsert_cfb_team",
        lambda _db, **kwargs: teams.setdefault(kwargs["name"], len(teams) + 1),
    )
    monkeypatch.setattr(cfb_schedule, "upsert_cfb_team_alias", lambda *args, **kwargs: None)
    monkeypatch.setattr(cfb_schedule, "upsert_cfb_venue", lambda *args, **kwargs: 9)
    captured = {}
    monkeypatch.setattr(
        cfb_schedule,
        "upsert_cfb_matchup",
        lambda _db, **kwargs: captured.update(kwargs) or 77,
    )

    assert cfb_schedule.fetch_schedule(object(), "cfbd-key", year=2099, week=1) == 1
    assert captured["cfbd_game_id"] == 401000001
    assert captured["network"] == "NBC"
    assert captured["home_line_scores"][-1] == 7
    assert captured["completed"] is True


class _OddsDb:
    def __init__(self):
        self.updated = None

    def execute_one(self, sql, params=None):
        if "FROM cfb_matchups m" in sql:
            return {
                "id": 77,
                "odds_event_id": "cfb-odds-1",
                "game_date": "2099-09-05",
                "commence_time": datetime(2099, 9, 5, 19, 30, tzinfo=timezone.utc),
                "home_team_id": 1,
                "away_team_id": 2,
                "home_name": "Oregon",
                "away_name": "Ohio State",
            }
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        if "FROM cfb_teams t" in sql:
            return [
                {"name": "Oregon", "team_id": 1},
                {"name": "Ohio State", "team_id": 2},
                {"name": "OSU Buckeyes", "team_id": 2},
            ]
        if "UPDATE cfb_matchups SET" in sql:
            self.updated = params
        return []


def test_fetch_odds_writes_one_exact_book_row_per_game(monkeypatch) -> None:
    db = _OddsDb()
    monkeypatch.setattr(
        cfb_schedule.requests,
        "get",
        lambda *args, **kwargs: _Response([_odds_event()]),
    )
    rows = []
    monkeypatch.setattr(
        cfb_schedule,
        "insert_game_odds_history_rows",
        lambda _db, payload: rows.extend(payload) or len(payload),
    )
    audit = {}
    assert cfb_schedule.fetch_odds(
        db,
        "odds-key",
        event_ids={"cfb-odds-1"},
        refresh_events=False,
        request_audit=audit,
    ) == 1
    assert len(rows) == 1
    assert rows[0]["sport"] == "cfb"
    assert rows[0]["home_spread"] == -4.5
    assert set(rows[0]["books"]) == {"draftkings"}
    assert audit["requests_last"] == "3"
    assert db.updated is not None


def test_fetch_odds_accepts_reviewed_provider_alias(monkeypatch) -> None:
    db = _OddsDb()
    monkeypatch.setattr(
        cfb_schedule.requests,
        "get",
        lambda *args, **kwargs: _Response([_odds_event(away_team="OSU Buckeyes")]),
    )
    rows = []
    monkeypatch.setattr(
        cfb_schedule,
        "insert_game_odds_history_rows",
        lambda _db, payload: rows.extend(payload) or len(payload),
    )

    assert cfb_schedule.fetch_odds(
        db,
        "odds-key",
        event_ids={"cfb-odds-1"},
        refresh_events=False,
    ) == 1
    assert len(rows) == 1


def test_capture_due_makes_one_bulk_request_for_many_games(monkeypatch) -> None:
    due = [
        {"id": 1, "odds_event_id": "a", "checkpoint": "t_minus_90m"},
        {"id": 2, "odds_event_id": "b", "checkpoint": "t_minus_90m"},
        {"id": 3, "odds_event_id": "c", "checkpoint": "t_minus_6h"},
    ]
    monkeypatch.setattr(cfb_schedule, "due_checkpoints", lambda _db: due)
    calls = []

    def fake_fetch(_db, _key, **kwargs):
        calls.append(kwargs["event_ids"])
        kwargs["request_audit"].update({"requests_last": "3"})
        return 3

    monkeypatch.setattr(cfb_schedule, "fetch_odds", fake_fetch)
    result = cfb_schedule.capture_due_checkpoints(object(), "key")
    assert calls == [{"a", "b", "c"}]
    assert result["paid_request"] is True
    assert result["captured_events"] == 3


def test_unmapped_provider_team_is_quarantined(monkeypatch) -> None:
    class Db:
        def execute_one(self, sql, params=None):
            return None

    quarantined = []
    monkeypatch.setattr(
        cfb_schedule,
        "quarantine_cfb_event",
        lambda _db, **kwargs: quarantined.append(kwargs),
    )
    event = _odds_event(home_team="Unknown College")
    assert cfb_schedule._resolve_event_matchup(Db(), event, {}) is None
    assert quarantined[0]["reason"] == "unknown team alias"


def test_checkpoint_windows_are_hittable_by_fifteen_minute_scheduler() -> None:
    assert ("t_minus_15m", 5, 25) in cfb_schedule.CHECKPOINTS
    assert all(name != "t_minus_2m" for name, _, _ in cfb_schedule.CHECKPOINTS)
