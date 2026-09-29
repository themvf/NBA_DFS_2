from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from db.schema import CLOSE_CAPTURE_CONSTRAINT_DDLS, CLOSE_CAPTURE_SPORTS, INDEXES, NHL_INDEXES, NHL_TABLES
from ingest import event_closing_lines as closes
from ingest import nhl_schedule as nhl

ROOT = Path(__file__).resolve().parents[1]


def _team(team_id: int, abbrev: str, place: str, common: str, **extra) -> dict:
    return {"id": team_id, "abbrev": abbrev, "placeName": {"default": place, "fr": place},
            "commonName": {"default": common}, "logo": f"https://x/{abbrev}.svg", **extra}


FLA = _team(13, "FLA", "Florida", "Panthers")
CAR = _team(12, "CAR", "Carolina", "Hurricanes")
MTL = _team(8, "MTL", "Montréal", "Canadiens")
TOR = _team(10, "TOR", "Toronto", "Maple Leafs")


def _game(game_id: int, start: str, away: dict, home: dict, **extra) -> dict:
    return {"id": game_id, "season": 20262027, "gameType": 2, "startTimeUTC": start,
            "venue": {"default": "Arena"}, "neutralSite": False, "gameState": "FUT",
            "gameScheduleState": "OK", "awayTeam": dict(away), "homeTeam": dict(home), **extra}


def test_full_names_match_odds_api_spelling_including_accents_and_periods() -> None:
    assert nhl.team_full_name(MTL) == "Montréal Canadiens"
    # The Odds API spells these "Montréal Canadiens" / "St Louis Blues"; both normalize equal.
    assert nhl._normal_name("Montréal Canadiens") == nhl._normal_name("Montreal Canadiens")
    assert nhl._normal_name(nhl.team_full_name(_team(19, "STL", "St. Louis", "Blues"))) == nhl._normal_name("St Louis Blues")
    with pytest.raises(ValueError):
        nhl.team_full_name({"abbrev": "XXX", "placeName": {"default": "Somewhere"}})


def test_schedule_parse_keeps_regular_season_and_settlement_fields() -> None:
    payload = {"gameWeek": [
        {"date": "2026-09-29", "games": [
            _game(2026010099, "2026-09-29T18:00:00Z", FLA, CAR, gameType=1),  # preseason: dropped
            _game(2026020001, "2026-09-29T21:00:00Z", FLA, CAR, tvBroadcasts=[
                {"market": "H", "network": "FDSNSO", "sequenceNumber": 1},
                {"market": "N", "network": "TNT", "sequenceNumber": 9},
                {"market": "A", "network": "FDSNFL", "sequenceNumber": 2},
            ]),
            # 02:00 UTC on the 30th is still the 29th in Eastern time.
            _game(2026020004, "2026-09-30T02:00:00Z", MTL, TOR, gameState="OFF",
                  awayTeam={**MTL, "score": 3}, homeTeam={**TOR, "score": 2},
                  gameOutcome={"lastPeriodType": "SO"}),
            _game(2026020005, "2026-09-29T23:00:00Z", MTL, TOR, gameState="LIVE",
                  awayTeam={**MTL, "score": 1}, homeTeam={**TOR, "score": 0},
                  gameOutcome={"lastPeriodType": "REG"}),
        ]},
    ]}
    games = {row["nhl_game_id"]: row for row in nhl.parse_schedule_games(payload)}
    assert set(games) == {2026020001, 2026020004, 2026020005}
    opener = games[2026020001]
    assert opener["game_date"] == "2026-09-29"
    assert opener["networks"] == "TNT, FDSNSO, FDSNFL"  # national first
    assert opener["completed"] is False and opener["home_score"] is None
    final = games[2026020004]
    assert final["game_date"] == "2026-09-29"
    assert final["completed"] is True
    # The shootout winner is credited one goal, which is how books settle.
    assert (final["away_score"], final["home_score"], final["last_period_type"]) == (3, 2, "SO")
    live = games[2026020005]
    assert live["completed"] is False and live["last_period_type"] is None


def test_fetch_schedule_steps_by_week_and_trims_to_the_window(monkeypatch) -> None:
    calls: list[str] = []
    week_one = {"gameWeek": [{"games": [_game(1, "2026-10-01T23:00:00Z", FLA, CAR)]}]}
    week_two = {"gameWeek": [{"games": [
        _game(2, "2026-10-08T23:00:00Z", MTL, TOR),
        _game(3, "2026-10-20T23:00:00Z", CAR, FLA),  # beyond start + 14 days
    ]}]}

    class Response:
        def __init__(self, payload): self.payload = payload
        def raise_for_status(self): return None
        def json(self): return self.payload

    def get(url, **_kwargs):
        calls.append(url.rsplit("/", 1)[-1])
        return Response(week_one if len(calls) == 1 else week_two)

    stored: list[list[int]] = []
    monkeypatch.setattr(nhl.requests, "get", get)
    monkeypatch.setattr(nhl, "_store_games", lambda _db, games: stored.append([g["nhl_game_id"] for g in games]) or len(games))
    assert nhl.fetch_schedule(object(), start=date(2026, 9, 29), days=14) == 2
    assert calls == ["2026-09-29", "2026-10-06"]
    assert stored == [[1, 2]]


def test_recent_score_refresh_is_one_call_covering_the_last_two_days(monkeypatch) -> None:
    seen = {}
    monkeypatch.setattr(nhl, "fetch_schedule", lambda _db, *, start, days: seen.update(start=start, days=days) or 5)
    assert nhl.refresh_recent_scores(object(), today=date(2026, 10, 5)) == 5
    assert seen == {"start": date(2026, 10, 3), "days": 7}


class _ResolveDb:
    def __init__(self, candidates, existing=None, mapped_to=None):
        self.candidates, self.existing, self.mapped_to = candidates, existing, mapped_to
        self.mapped: list = []

    def execute_one(self, sql, params=None):
        if "WHERE odds_event_id=%s" in sql:
            return self.existing
        if "SELECT odds_event_id FROM nhl_matchups WHERE id=%s" in sql:
            return {"odds_event_id": self.mapped_to}
        raise AssertionError(sql)

    def execute(self, sql, params=None):
        if "FROM nhl_matchups" in sql and "home_team_id=%s" in sql:
            return self.candidates
        if "UPDATE nhl_matchups SET odds_event_id" in sql:
            self.mapped.append(params)
        return []


def _event(**overrides) -> dict:
    event = {"id": "evt-1", "home_team": "Carolina Hurricanes", "away_team": "Florida Panthers",
             "commence_time": "2026-09-29T21:10:47Z"}
    event.update(overrides)
    return event


CACHE = {nhl._normal_name("Carolina Hurricanes"): 1, nhl._normal_name("Florida Panthers"): 2}


def test_event_ten_minutes_after_scheduled_start_maps(monkeypatch) -> None:
    db = _ResolveDb([{"id": 77, "odds_event_id": None, "game_date": "2026-09-29",
                      "commence_time": datetime(2026, 9, 29, 21, tzinfo=timezone.utc)}])
    result = nhl._resolve_event_matchup(db, _event(), CACHE)
    assert result["id"] == 77 and result["odds_event_id"] == "evt-1"
    assert db.mapped == [("evt-1", 77)]


@pytest.mark.parametrize("event, candidates, reason", [
    (_event(home_team="Hartford Whalers"), [], "unknown team name"),
    (_event(), [], "no unique canonical matchup within start-time tolerance"),
    (_event(), [{"id": 7, "commence_time": datetime(2026, 9, 30, 21, tzinfo=timezone.utc)}],
     "no unique canonical matchup within start-time tolerance"),
])
def test_unresolvable_events_are_quarantined_not_guessed(monkeypatch, event, candidates, reason) -> None:
    quarantined: list[dict] = []
    monkeypatch.setattr(nhl, "quarantine_nhl_event", lambda _db, **kwargs: quarantined.append(kwargs))
    assert nhl._resolve_event_matchup(_ResolveDb(candidates), event, CACHE) is None
    assert quarantined[0]["reason"] == reason


def test_game_already_mapped_to_another_event_quarantines_instead_of_crashing(monkeypatch) -> None:
    quarantined: list[dict] = []
    monkeypatch.setattr(nhl, "quarantine_nhl_event", lambda _db, **kwargs: quarantined.append(kwargs))
    db = _ResolveDb([{"id": 77, "commence_time": datetime(2026, 9, 29, 21, tzinfo=timezone.utc)}],
                    mapped_to="evt-older")
    assert nhl._resolve_event_matchup(db, _event(), CACHE) is None
    assert "already maps to odds event evt-older" in quarantined[0]["reason"]


class _Response:
    status_code = 200
    headers = {"x-requests-remaining": "71000", "x-requests-used": "29000", "x-requests-last": "3"}

    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


def _odds_event(event_id: str, home: str, away: str, commence: str) -> dict:
    return {"id": event_id, "home_team": home, "away_team": away, "commence_time": commence,
            "bookmakers": [{
                "key": "draftkings", "title": "DraftKings", "last_update": "2099-01-01T00:00:00Z",
                "markets": [
                    {"key": "h2h", "outcomes": [{"name": home, "price": -140}, {"name": away, "price": 120}]},
                    {"key": "spreads", "outcomes": [{"name": home, "point": -1.5, "price": 165},
                                                     {"name": away, "point": 1.5, "price": -200}]},
                    {"key": "totals", "outcomes": [{"name": "Over", "point": 6.5, "price": -105},
                                                    {"name": "Under", "point": 6.5, "price": -115}]},
                ]}, {"key": "bovada", "title": "Bovada", "markets": []}]}


def _mapped(event_id: str, matchup_id: int, home_id: int, away_id: int, home: str, away: str, start: datetime) -> dict:
    return {"id": matchup_id, "odds_event_id": event_id, "game_date": start.date().isoformat(),
            "commence_time": start, "home_team_id": home_id, "away_team_id": away_id,
            "home_name": home, "away_name": away}


def test_fetch_odds_records_every_mapped_game_in_one_paid_call(monkeypatch) -> None:
    future = datetime.now(timezone.utc) + timedelta(hours=5)
    later = future + timedelta(days=1)
    mapped = {
        "a": _mapped("a", 1, 1, 2, "Carolina Hurricanes", "Florida Panthers", future),
        "b": _mapped("b", 2, 3, 4, "Toronto Maple Leafs", "Montréal Canadiens", later),
    }
    monkeypatch.setattr(nhl, "_mapped_upcoming", lambda _db, _h: mapped)
    monkeypatch.setattr(nhl, "_team_cache", lambda _db: {
        nhl._normal_name(n): i for n, i in [("Carolina Hurricanes", 1), ("Florida Panthers", 2),
                                           ("Toronto Maple Leafs", 3), ("Montreal Canadiens", 4)]})
    calls: list[dict] = []
    payload = [
        _odds_event("a", "Carolina Hurricanes", "Florida Panthers", future.isoformat()),
        _odds_event("b", "Toronto Maple Leafs", "Montréal Canadiens", later.isoformat()),
        _odds_event("zzz", "Boston Bruins", "New York Rangers", later.isoformat()),  # unmapped
    ]
    monkeypatch.setattr(nhl.requests, "get", lambda url, **kwargs: calls.append(kwargs["params"]) or _Response(payload))
    rows: list[dict] = []
    monkeypatch.setattr(nhl, "insert_game_odds_history_rows", lambda _db, r: rows.extend(r) or len(r))

    class Db:
        def execute(self, sql, params=None): return []
    audit: dict = {}
    # Only "a" is due, yet the same billed call also records "b".
    assert nhl.fetch_odds(Db(), "key", event_ids={"a"}, request_audit=audit) == 2
    assert len(calls) == 1
    assert calls[0]["markets"] == "h2h,spreads,totals"
    assert calls[0]["bookmakers"] == ",".join(nhl.NHL_BOOKMAKERS)
    assert {row["event_id"] for row in rows} == {"a", "b"}
    first = next(row for row in rows if row["event_id"] == "a")
    assert first["sport"] == "nhl"
    assert set(first["books"]) == {"draftkings"}  # unselected books are dropped
    dk = first["books"]["draftkings"]
    assert (dk["spread_home"], dk["spread_home_price"], dk["total_line"], dk["over"]) == (-1.5, 165, 6.5, -105)
    assert audit["requests_last"] == "3" and audit["status"] == 200 and audit["endpoint"].endswith("/icehockey_nhl/odds")


def test_no_paid_call_when_no_due_game_is_still_upcoming(monkeypatch) -> None:
    future = datetime.now(timezone.utc) + timedelta(hours=5)
    monkeypatch.setattr(nhl, "_mapped_upcoming", lambda _db, _h: {"a": _mapped("a", 1, 1, 2, "X", "Y", future)})
    monkeypatch.setattr(nhl.requests, "get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not buy")))
    audit: dict = {}
    assert nhl.fetch_odds(object(), "key", event_ids={"postponed"}, request_audit=audit) == 0
    assert audit == {}
    monkeypatch.setattr(nhl, "_mapped_upcoming", lambda _db, _h: {})
    assert nhl.fetch_odds(object(), "key") == 0


def test_capture_after_scheduled_start_is_rejected(monkeypatch) -> None:
    started = datetime.now(timezone.utc) - timedelta(minutes=1)  # provider still lists it
    monkeypatch.setattr(nhl, "_mapped_upcoming", lambda _db, _h: {
        "a": _mapped("a", 1, 1, 2, "Carolina Hurricanes", "Florida Panthers", started)})
    monkeypatch.setattr(nhl, "_team_cache", lambda _db: CACHE)
    later = (datetime.now(timezone.utc) + timedelta(minutes=9)).isoformat()
    monkeypatch.setattr(nhl.requests, "get", lambda *a, **k: _Response(
        [_odds_event("a", "Carolina Hurricanes", "Florida Panthers", later)]))
    rows: list = []
    monkeypatch.setattr(nhl, "insert_game_odds_history_rows", lambda _db, r: rows.extend(r) or len(r))
    assert nhl.fetch_odds(object(), "key") == 0
    assert rows == []


def test_ensure_schema_is_a_single_catalog_read_once_applied() -> None:
    class Db:
        def __init__(self, state): self.state, self.ddl = state, []
        def execute_one(self, sql, params=None): return self.state
        def connect(self):
            db = self
            class Ctx:
                def __enter__(self):
                    class Conn:
                        def cursor(self):
                            class Cur:
                                def execute(self, sql, params=None): db.ddl.append(sql)
                            return Cur()
                    return Conn()
                def __exit__(self, *exc): return False
            return Ctx()

    ready = Db({"tables_present": True, "nhl_constraints": 3})
    assert nhl.ensure_nhl_schema(ready) == {"applied": []}
    assert ready.ddl == []
    fresh = Db({"tables_present": False, "nhl_constraints": 0})
    assert nhl.ensure_nhl_schema(fresh) == {"applied": ["tables", "close_capture_constraints"]}
    assert fresh.ddl[: len(NHL_TABLES)] == NHL_TABLES
    assert all(ddl in fresh.ddl for ddl in (*NHL_INDEXES, *CLOSE_CAPTURE_CONSTRAINT_DDLS))


def test_health_separates_integrity_failures_from_coverage_gaps() -> None:
    class Db:
        def __init__(self, row): self.row = row
        def execute_one(self, sql, params=None): return self.row
    assert nhl.collect_data_health(Db({"upcoming": 9}))["status"] == "pass"
    assert nhl.collect_data_health(Db({"unmapped_upcoming": 1}))["status"] == "warn"
    assert nhl.collect_data_health(Db({"post_start": 1, "quarantined": 2}))["status"] == "fail"


def test_close_constraints_admit_nhl_and_every_nhl_checkpoint() -> None:
    assert "nhl" in CLOSE_CAPTURE_SPORTS
    assert all(ddl in INDEXES for ddl in CLOSE_CAPTURE_CONSTRAINT_DDLS)
    checkpoint_ddl = next(sql for sql in CLOSE_CAPTURE_CONSTRAINT_DDLS if "checkpoint_check\n" in sql)
    patterns = re.findall(r"checkpoint ~ '([^']+)'", checkpoint_ddl)
    for name, _target, _due in closes.CHECKPOINTS_BY_SPORT["nhl"]:
        assert f"'{name}'" in checkpoint_ddl or any(re.fullmatch(p, name) for p in patterns), name


def test_web_constraint_mirror_admits_every_close_capture_sport() -> None:
    # web/src/db/ensure-schema.ts drops and re-adds this CHECK on cold start. A
    # sport missing there makes the re-add fail once one of its closes exists.
    ts = (ROOT / "web/src/db/ensure-schema.ts").read_text(encoding="utf-8")
    match = re.search(r"event_closing_lines_sport_check CHECK \(sport IN \(([^)]*)\)\)", ts)
    assert match, "web event_closing_lines CHECK not found"
    assert set(re.findall(r"'([a-z]+)'", match.group(1))) == set(CLOSE_CAPTURE_SPORTS)


def test_vercel_dispatcher_windows_cover_every_nhl_checkpoint() -> None:
    route = (ROOT / "web/src/app/api/cron/event-closing-lines/route.ts").read_text(encoding="utf-8")
    windows = {(int(a), int(b)) for sport, a, b in re.findall(r"\('(nhl|all)', (\d+), (\d+)\)", route)}
    assert "FROM nhl_matchups" in route
    for name, target, due in closes.CHECKPOINTS_BY_SPORT["nhl"]:
        assert (target, due) in windows, name
