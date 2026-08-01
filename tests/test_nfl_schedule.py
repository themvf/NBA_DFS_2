from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest import nfl_schedule
from ingest.nfl_teams import NFL_TEAMS
from model.line_alerts import (
    _game_side_outcome,
    _nfl_line_clv,
    _nfl_line_outcome,
    _nfl_market_signals,
    _pinnacle_polymarket_signals,
    _retail_fair_side,
)


class _Response:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {
            "x-requests-remaining": "100",
            "x-requests-used": "2",
            "x-requests-last": "1",
        }

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _EventDatabase:
    def __init__(self) -> None:
        self.matchup_params = None

    def execute(self, sql, params=None):
        if "FROM nfl_teams" in sql:
            return [
                {"team_id": index + 1, "odds_api_name": team["name"]}
                for index, team in enumerate(NFL_TEAMS)
            ]
        return []

    def execute_one(self, sql, params=None):
        if "INSERT INTO nfl_matchups" in sql:
            self.matchup_params = params
            return {"id": 77}
        raise AssertionError(sql)


class _OddsDatabase(_EventDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.current_update = None

    def execute_one(self, sql, params=None):
        if "INSERT INTO nfl_matchups" in sql:
            self.matchup_params = params
            return {"id": 77}
        if "FROM nfl_matchups m" in sql:
            return {
                "id": 77,
                "event_id": "nfl-event-1",
                "commence_time": datetime(2099, 9, 14, 0, 20, tzinfo=timezone.utc),
                "home_team_id": 9,
                "away_team_id": 26,
                "home_name": "Dallas Cowboys",
                "away_name": "Philadelphia Eagles",
            }
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        if "FROM nfl_teams" in sql:
            return super().execute(sql, params)
        if "SELECT event_id, season_type FROM nfl_matchups" in sql:
            return [{"event_id": "nfl-event-1", "season_type": "regular"}]
        if "UPDATE nfl_matchups SET" in sql:
            self.current_update = params
            return []
        return []


def _event(**overrides):
    value = {
        "id": "nfl-event-1",
        "commence_time": "2026-09-14T00:20:00Z",
        "home_team": "Dallas Cowboys",
        "away_team": "Philadelphia Eagles",
    }
    value.update(overrides)
    return value


def test_all_32_provider_team_names_are_unique() -> None:
    assert len(NFL_TEAMS) == 32
    assert len({team["name"] for team in NFL_TEAMS}) == 32
    assert len({team["abbreviation"] for team in NFL_TEAMS}) == 32


def test_live_nfl_and_mlb_regions_include_polymarket() -> None:
    from ingest.mlb_schedule import MLB_ODDS_REGIONS

    assert set(nfl_schedule.NFL_ODDS_REGIONS.split(",")) == {"us", "eu", "us_ex"}
    assert set(MLB_ODDS_REGIONS.split(",")) == {"us", "eu", "us_ex"}


def test_eastern_game_date_handles_sunday_night_utc_rollover() -> None:
    kickoff = datetime(2026, 9, 14, 0, 20, tzinfo=timezone.utc)
    assert nfl_schedule._eastern_date(kickoff) == "2026-09-13"
    assert nfl_schedule._season_for_kickoff(kickoff) == 2026


def test_postseason_january_belongs_to_prior_season() -> None:
    kickoff = datetime(2027, 1, 18, 1, 20, tzinfo=timezone.utc)
    assert nfl_schedule._season_for_kickoff(kickoff) == 2026


def test_fetch_events_uses_provider_id_and_eastern_date(monkeypatch) -> None:
    db = _EventDatabase()
    def fake_get(url, **kwargs):
        payload = [_event()] if "/americanfootball_nfl/events" in url else []
        return _Response(payload)

    monkeypatch.setattr(nfl_schedule.requests, "get", fake_get)

    assert nfl_schedule.fetch_events(db, "key", "2026-09-13") == 1
    assert db.matchup_params is not None
    assert db.matchup_params[0] == "nfl-event-1"
    assert db.matchup_params[2] == "regular"
    assert db.matchup_params[4] == "2026-09-13"
    assert db.matchup_params[6] != db.matchup_params[7]


def test_fetch_events_labels_preseason_matchups(monkeypatch) -> None:
    db = _EventDatabase()
    preseason_event = _event(
        id="nfl-preseason-1",
        commence_time="2026-08-07T00:00:00Z",
    )

    def fake_get(url, **kwargs):
        payload = [preseason_event] if "/americanfootball_nfl_preseason/events" in url else []
        return _Response(payload)

    monkeypatch.setattr(nfl_schedule.requests, "get", fake_get)

    assert nfl_schedule.fetch_events(db, "key", "2026-08-06") == 1
    assert db.matchup_params is not None
    assert db.matchup_params[0] == "nfl-preseason-1"
    assert db.matchup_params[2] == "preseason"
    assert db.matchup_params[4] == "2026-08-06"


def test_fetch_events_rejects_unknown_team(monkeypatch) -> None:
    db = _EventDatabase()
    def fake_get(url, **kwargs):
        payload = [_event(home_team="Unknown Expansion Team")] if "/americanfootball_nfl/events" in url else []
        return _Response(payload)

    monkeypatch.setattr(
        nfl_schedule.requests,
        "get",
        fake_get,
    )
    with pytest.raises(ValueError, match="unmapped NFL provider team"):
        nfl_schedule.fetch_events(db, "key", "2026-09-13")


def test_market_parser_preserves_both_spread_sides_and_prices() -> None:
    event = _event(bookmakers=[{
        "key": "draftkings",
        "last_update": "2026-09-13T12:00:00Z",
        "markets": [
            {"key": "h2h", "outcomes": [
                {"name": "Dallas Cowboys", "price": -145},
                {"name": "Philadelphia Eagles", "price": 125},
            ]},
            {"key": "spreads", "outcomes": [
                {"name": "Dallas Cowboys", "price": -110, "point": -3.0},
                {"name": "Philadelphia Eagles", "price": -110, "point": 3.0},
            ]},
            {"key": "totals", "outcomes": [
                {"name": "Over", "price": -108, "point": 47.5},
                {"name": "Under", "price": -112, "point": 47.5},
            ]},
        ],
    }])

    parsed = nfl_schedule._extract_markets(event)

    assert parsed["home_spread"] == -3.0
    assert parsed["vegas_total"] == 47.5
    assert parsed["books"]["draftkings"]["spread_home_price"] == -110
    assert parsed["books"]["draftkings"]["spread_away"] == 3.0
    assert parsed["books"]["draftkings"]["under"] == -112


def test_odds_ingestion_writes_append_only_fixture(monkeypatch) -> None:
    db = _OddsDatabase()
    event = _event(
        commence_time="2099-09-14T00:20:00Z",
        bookmakers=[{
            "key": key,
            "last_update": "2099-09-13T12:00:00Z",
            "markets": [
                {"key": "h2h", "outcomes": [
                    {"name": "Dallas Cowboys", "price": -145},
                    {"name": "Philadelphia Eagles", "price": 125},
                ]},
                {"key": "spreads", "outcomes": [
                    {"name": "Dallas Cowboys", "price": -110, "point": -3.0},
                    {"name": "Philadelphia Eagles", "price": -110, "point": 3.0},
                ]},
                {"key": "totals", "outcomes": [
                    {"name": "Over", "price": -108, "point": 47.5},
                    {"name": "Under", "price": -112, "point": 47.5},
                ]},
            ],
        } for key in ("draftkings", "fanduel", "pinnacle")],
    )
    responses = iter([_Response([event]), _Response([]), _Response([event])])
    monkeypatch.setattr(nfl_schedule.requests, "get", lambda *args, **kwargs: next(responses))
    captured = []
    monkeypatch.setattr(
        nfl_schedule,
        "insert_game_odds_history_rows",
        lambda _db, rows: captured.extend(rows) or len(rows),
    )

    assert nfl_schedule.fetch_odds(db, "key", "2099-09-13") == 1
    assert len(captured) == 1
    assert captured[0]["sport"] == "nfl"
    assert captured[0]["event_id"] == "nfl-event-1"
    assert captured[0]["home_spread"] == -3.0
    assert captured[0]["vegas_total"] == 47.5
    assert captured[0]["home_implied"] == 25.25
    assert set(captured[0]["books"]) == {"draftkings", "fanduel", "pinnacle"}
    assert db.current_update is not None


def test_capture_must_precede_provider_and_stored_kickoff() -> None:
    captured = datetime(2026, 9, 13, 17, 5, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="stored kickoff"):
        nfl_schedule._require_pregame_capture(
            event_commence=datetime(2026, 9, 13, 17, 30, tzinfo=timezone.utc),
            stored_commence=datetime(2026, 9, 13, 17, 0, tzinfo=timezone.utc),
            captured_at=captured,
        )
    with pytest.raises(ValueError, match="provider kickoff"):
        nfl_schedule._require_pregame_capture(
            event_commence=datetime(2026, 9, 13, 17, 0, tzinfo=timezone.utc),
            stored_commence=datetime(2026, 9, 13, 17, 30, tzinfo=timezone.utc),
            captured_at=captured,
        )


class _FreshnessDatabase:
    def __init__(self, row):
        self.row = row
        self.params = None

    def execute_one(self, sql, params=None):
        self.params = params
        return self.row


def test_freshness_fails_on_silent_zero_write() -> None:
    db = _FreshnessDatabase({"upcoming_games": 3, "fresh_games": 2, "latest_capture": "now"})
    assert nfl_schedule.verify_fresh_upcoming_odds(db, "2026-09-13") is False
    assert db.params == (35, "2026-09-13")


def test_freshness_passes_when_no_upcoming_games() -> None:
    db = _FreshnessDatabase({"upcoming_games": 0, "fresh_games": 0, "latest_capture": None})
    assert nfl_schedule.verify_fresh_upcoming_odds(db, "2026-09-13") is True


def test_health_fails_on_post_kickoff_capture() -> None:
    db = _FreshnessDatabase({
        "missing_capture": 0,
        "stale_capture": 0,
        "missing_score": 0,
        "post_kickoff": 1,
        "unsettled_alerts": 0,
    })
    result = nfl_schedule.collect_nfl_data_health(db, "2026-09-13")
    assert result["status"] == "fail"
    assert result["post_kickoff"] == 1


def test_scores_skip_paid_request_when_no_recent_matchups(monkeypatch) -> None:
    db = _FreshnessDatabase({"n": 0})
    monkeypatch.setattr(
        nfl_schedule.requests,
        "get",
        lambda *args, **kwargs: pytest.fail("scores API should not be called"),
    )
    assert nfl_schedule.fetch_scores(db, "key", 3) == 0


def test_nfl_two_way_moneyline_tie_is_void() -> None:
    assert _game_side_outcome("nfl", 20, 20, "home") == "void"
    assert _game_side_outcome("nfl", 20, 20, "away") == "void"
    assert _game_side_outcome("nfl", 24, 20, "home") == "won"


def test_nfl_spread_and_total_pushes_are_void() -> None:
    assert _nfl_line_outcome("spread", "home", -3.0, 24, 21) == "void"
    assert _nfl_line_outcome("spread", "away", -3.0, 24, 21) == "void"
    assert _nfl_line_outcome("total", "over", 45.0, 24, 21) == "void"
    assert _nfl_line_outcome("total", "under", 45.0, 24, 21) == "void"


def test_nfl_line_clv_is_positive_when_alert_beats_close() -> None:
    assert _nfl_line_clv("spread", "home", -3.0, -4.0) == 1.0
    assert _nfl_line_clv("spread", "away", -3.0, -2.0) == 1.0
    assert _nfl_line_clv("total", "over", 45.0, 46.5) == 1.5
    assert _nfl_line_clv("total", "under", 45.0, 43.5) == 1.5


def test_nfl_market_signals_detect_spread_and_total_steam_and_walk() -> None:
    opening = {
        key: {"spread_home": -2.5, "total_line": 44.0}
        for key in ("draftkings", "fanduel", "pinnacle")
    }
    previous = {
        key: {"spread_home": -3.0, "total_line": 44.5}
        for key in ("draftkings", "fanduel", "pinnacle")
    }
    current = {
        key: {"spread_home": -3.5, "total_line": 45.0}
        for key in ("draftkings", "fanduel", "pinnacle")
    }

    signals = _nfl_market_signals(current, previous, opening)
    keyed = {(signal["alert_type"], signal["side"]) for signal in signals}

    assert ("spread_steam", "home") in keyed
    assert ("spread_walking", "home") in keyed
    assert ("total_steam", "over") in keyed
    assert ("total_walking", "over") in keyed


def test_pinnacle_polymarket_delta_flags_the_higher_pinnacle_side() -> None:
    books = {
        "pinnacle": {"ml_home": -150, "ml_away": 130},
        "polymarket": {"ml_home": -120, "ml_away": 100},
        "draftkings": {"ml_home": -140, "ml_away": 120},
    }

    signals = _pinnacle_polymarket_signals(books)

    assert len(signals) == 1
    assert signals[0]["side"] == "home"
    assert signals[0]["details"]["gap_pp"] >= 2.0


def test_polymarket_is_not_folded_into_retail_consensus() -> None:
    books = {
        "draftkings": {"ml_home": -110, "ml_away": -110},
        "pinnacle": {"ml_home": -150, "ml_away": 130},
        "polymarket": {"ml_home": 150, "ml_away": -170},
    }

    assert _retail_fair_side(books, "home") == pytest.approx(0.5)
