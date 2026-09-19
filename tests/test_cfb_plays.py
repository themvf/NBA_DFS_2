"""Unit tests for the CFBD drive/play backfill.

These exercise the pure parsing/linking layer plus the season loop with the
database and HTTP boundaries replaced.  They deliberately do NOT claim the
backfill has run against real data — that requires CFBD_API_KEY and a live
database, and is reported separately.
"""

import contextlib
import gzip
import json

import pytest

from ingest import cfb_plays
from ingest.cfb_plays import (
    audit_rows,
    clock_seconds,
    drive_rows,
    game_seconds_remaining,
    ingest_season,
    play_rows,
    schedule_weeks,
)


def _game(game_id=401001, week=1, season_type="regular"):
    return {
        "id": game_id, "season": 2024, "seasonType": season_type, "week": week,
        "startDate": "2024-09-07T16:00:00Z", "completed": True,
        "neutralSite": False, "conferenceGame": True,
        "homeId": 11, "homeTeam": "Home U", "homeConference": "Big", "homeClassification": "fbs",
        "awayId": 22, "awayTeam": "Away U", "awayConference": "Big", "awayClassification": "fbs",
        "homePoints": 27, "awayPoints": 24, "venue": "Stadium", "venueId": 7,
    }


def _drive(drive_id=1, game_id=401001):
    return {
        "id": drive_id, "gameId": game_id, "driveNumber": 3,
        "offense": "Home U", "defense": "Away U", "isHomeOffense": True,
        "scoring": True, "driveResult": "TD",
        "startPeriod": 2, "startYardsToGoal": 75, "startTime": {"minutes": 8, "seconds": 12},
        "endPeriod": 2, "endYardsToGoal": 0, "endTime": {"minutes": 4, "seconds": 2},
        "plays": 9, "yards": 75,
        "startOffenseScore": 7, "startDefenseScore": 10,
        "endOffenseScore": 14, "endDefenseScore": 10,
    }


def _play(play_id=9001, game_id=401001, drive_id=1, period=2):
    return {
        "id": play_id, "gameId": game_id, "driveId": drive_id,
        "driveNumber": 3, "playNumber": 5,
        "offense": "Home U", "defense": "Away U",
        "offenseScore": 7, "defenseScore": 10,
        "period": period, "clock": {"minutes": 5, "seconds": 30},
        "offenseTimeouts": 3, "defenseTimeouts": 2,
        "yardline": 40, "yardsToGoal": 40, "down": 2, "distance": 7,
        "yardsGained": 12, "scoring": False, "playType": "Rush",
        "playText": "Back run for 12 yards", "ppa": 0.41,
        "wallclock": "2024-09-07T17:12:03.000Z",
    }


# ── clock derivation ─────────────────────────────────────────────

def test_clock_seconds_reads_the_feeds_minute_second_pair():
    assert clock_seconds({"minutes": 5, "seconds": 30}) == 330
    assert clock_seconds({"minutes": 0, "seconds": 9}) == 9
    assert clock_seconds({}) is None
    assert clock_seconds(None) is None


def test_game_seconds_remaining_counts_down_through_regulation():
    assert game_seconds_remaining(1, 900) == 3600
    assert game_seconds_remaining(2, 330) == 2130
    assert game_seconds_remaining(4, 480) == 480


def test_overtime_has_zero_regulation_time_left_never_a_negative():
    """A negative value would silently corrupt any endgame threshold filter."""
    assert game_seconds_remaining(5, 900) == 0
    assert game_seconds_remaining(7, 120) == 0


def test_game_seconds_remaining_is_none_when_either_input_is_missing():
    assert game_seconds_remaining(None, 300) is None
    assert game_seconds_remaining(3, None) is None


# ── week discovery ───────────────────────────────────────────────

def test_weeks_come_from_the_schedule_not_a_blind_probe():
    games = [
        _game(1, week=1), _game(2, week=1), _game(3, week=14),
        _game(4, week=1, season_type="postseason"),
    ]
    assert schedule_weeks(games, 2024) == {"regular": [1, 14], "postseason": [1]}


def test_weeks_ignore_games_from_another_season():
    other = _game(9, week=3)
    other["season"] = 2023
    assert schedule_weeks([_game(1, week=1), other], 2024)["regular"] == [1]


# ── row building ─────────────────────────────────────────────────

_MATCHUPS = {401001: 55}
_TEAMS = {401001: {"Home U": 11, "Away U": 22}}


def test_drive_row_carries_identity_teams_and_derived_clocks():
    rows, skipped = drive_rows(
        [_drive()], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular",
    )
    assert skipped == {}
    row = rows[0]
    assert row[0] == 1 and row[1] == 55 and row[2] == 401001
    assert row[8] == 11 and row[9] == 22           # offense/defense team ids
    assert row[15] == 8 * 60 + 12                  # start clock seconds
    assert row[18] == 4 * 60 + 2                   # end clock seconds


def test_play_row_derives_regulation_clock_from_period_and_clock():
    rows, _ = play_rows(
        [_play()], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=2,
    )
    row = rows[0]
    assert row[16] == 330                          # clock_seconds_remaining
    assert row[17] == 2130                         # game_seconds_remaining


def test_rows_for_a_game_outside_the_schedule_are_skipped_not_orphaned():
    """The FK would reject them; counting them keeps the loss visible."""
    plays = [_play(9002, game_id=999999)]
    rows, skipped = play_rows(
        plays, matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=1,
    )
    assert rows == []
    assert skipped["game_not_in_schedule"] == 1


def test_an_unmapped_team_name_is_counted_but_the_play_is_still_kept():
    play = _play()
    play["offense"] = "Home University"
    rows, skipped = play_rows(
        [play], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=1,
    )
    assert len(rows) == 1
    assert rows[0][11] is None                     # offense_team_id
    assert rows[0][9] == "Home University"         # raw name preserved
    assert skipped["team_name_unmapped"] == 1


def test_a_play_with_no_id_is_dropped_rather_than_written_with_a_null_key():
    play = _play()
    play["id"] = None
    rows, skipped = play_rows(
        [play], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=1,
    )
    assert rows == [] and skipped["missing_identity"] == 1


# ── audit ────────────────────────────────────────────────────────

def test_audit_reports_drive_linkage_and_field_coverage():
    orphan = _play(9002, drive_id=404)
    orphan["ppa"] = None
    report = audit_rows([_drive()], [_play(), orphan])
    assert report["plays"] == 2
    assert report["plays_linked_to_a_drive"] == 1
    assert report["play_drive_link_rate"] == 0.5
    assert report["field_coverage"]["ppa"] == 0.5
    assert report["duplicate_play_ids"] == 0


def test_audit_counts_duplicate_play_ids():
    assert audit_rows([], [_play(), _play()])["duplicate_play_ids"] == 1


# ── caching ──────────────────────────────────────────────────────

def test_cached_weeks_replay_without_an_api_key(tmp_path):
    path = tmp_path / "plays-2024-regular-w03.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump([_play()], handle)
    payload = cfb_plays.fetch_cfbd_week(
        "plays", api_key="", season=2024, season_type="regular",
        week=3, cache_dir=tmp_path,
    )
    assert payload[0]["id"] == 9001


def test_a_missing_cache_without_a_key_fails_loudly(tmp_path):
    with pytest.raises(ValueError, match="CFBD_API_KEY"):
        cfb_plays.fetch_cfbd_week(
            "drives", api_key="", season=2024, season_type="regular",
            week=3, cache_dir=tmp_path,
        )


# ── season loop ──────────────────────────────────────────────────

class _FakeDb:
    def __init__(self):
        self.statements = []

    @contextlib.contextmanager
    def connect(self):
        db = self

        class _Cursor:
            def execute(self, sql, params=None):
                db.statements.append(sql)

        class _Connection:
            def cursor(self):
                return _Cursor()

        yield _Connection()


def test_season_loop_walks_every_scheduled_week_and_upserts_each_game_once(monkeypatch):
    import psycopg2.extras

    written = {"drives": 0, "plays": 0}

    def fake_execute_values(cursor, sql, values, page_size=None):
        written["drives" if "cfb_drives" in sql else "plays"] += len(values)

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)

    upserted = []

    def fake_upsert(db, game, *, team_cache, venue_cache, **_):
        upserted.append(int(game["id"]))
        team_cache[int(game["homeId"])] = 11
        team_cache[int(game["awayId"])] = 22
        return 500 + len(upserted)

    monkeypatch.setattr(cfb_plays, "_upsert_game", fake_upsert)

    games = [_game(401001, week=1), _game(401002, week=2)]
    calls = []

    def fetch_week(endpoint, season_type, week):
        calls.append((endpoint, season_type, week))
        game_id = 401001 if week == 1 else 401002
        if endpoint == "drives":
            return [_drive(week, game_id=game_id)]
        return [_play(9000 + week, game_id=game_id, drive_id=week)]

    report = ingest_season(_FakeDb(), season=2024, games=games, fetch_week=fetch_week)

    assert [c for c in calls if c[0] == "plays"] == [("plays", "regular", 1), ("plays", "regular", 2)]
    assert upserted == [401001, 401002]            # each game upserted exactly once
    assert report["games_upserted"] == 2
    assert report["drive_rows"] == 2 and report["play_rows"] == 2
    assert written == {"drives": 2, "plays": 2}
    assert report["per_week"] == {
        "regular-w01": {"drives": 1, "plays": 1},
        "regular-w02": {"drives": 1, "plays": 1},
    }


def test_season_loop_records_games_it_could_not_place_in_the_schedule(monkeypatch):
    import psycopg2.extras

    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda *a, **k: None)
    monkeypatch.setattr(
        cfb_plays, "_upsert_game",
        lambda db, game, *, team_cache, venue_cache, **_: (
            team_cache.update({int(game["homeId"]): 11, int(game["awayId"]): 22}) or 501
        ),
    )

    def fetch_week(endpoint, season_type, week):
        return [] if endpoint == "drives" else [_play(1, game_id=777777)]

    report = ingest_season(
        _FakeDb(), season=2024, games=[_game(401001, week=1)], fetch_week=fetch_week,
    )
    assert report["play_rows"] == 0
    assert report["skipped"]["game_not_in_schedule"] >= 1


def test_a_missing_wallclock_becomes_null_rather_than_failing_the_week():
    """parse_iso raises on None for its odds callers; a play may simply lack one."""
    play = _play()
    play["wallclock"] = None
    rows, _ = play_rows(
        [play], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=1,
    )
    assert rows[0][29] is None


def test_an_unparseable_wallclock_is_dropped_not_raised():
    play = _play()
    play["wallclock"] = "not a timestamp"
    rows, _ = play_rows(
        [play], matchup_ids=_MATCHUPS, team_ids=_TEAMS,
        season=2024, season_type="regular", week=1,
    )
    assert rows[0][29] is None
