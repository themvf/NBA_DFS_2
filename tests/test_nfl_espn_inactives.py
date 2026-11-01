from datetime import datetime, timedelta, timezone

from ingest.ff_injuries import normalize_injury_observation
from ingest.nfl_espn_inactives import capture, parse_inactives, select_for_games
from ingest.nfl_official_inactives import prepare_import

KICKOFF = datetime(2026, 11, 1, 18, tzinfo=timezone.utc)
NOW = KICKOFF - timedelta(minutes=80)


def entry(name, espn_id, team="NYJ", position="RB", fantasy="INACTIVE", date="2026-11-01T16:32Z"):
    return {
        "status": "Out", "date": date,
        "athlete": {
            "displayName": name,
            "links": [{"href": f"https://www.espn.com/nfl/player/_/id/{espn_id}/x"}],
            "position": {"abbreviation": position},
            "team": {"abbreviation": team},
        },
        "details": {"fantasyStatus": {"abbreviation": fantasy}, "type": "Quadriceps"},
    }


def payload(*entries):
    return {"injuries": [{"displayName": "team", "injuries": list(entries)}]}


GAME = {"season": 2026, "week": 8, "game_id": "2026_08_LV_NYJ", "kickoff": KICKOFF,
        "home_team": "NYJ", "away_team": "LV"}


def test_athlete_id_comes_from_the_player_card_link():
    rows = parse_inactives(payload(entry("Breece Hall", "4427366")))
    assert rows[0]["espn_id"] == "4427366"
    assert rows[0]["team"] == "NYJ" and rows[0]["position"] == "RB"


def test_only_inactive_designations_are_read():
    rows = parse_inactives(payload(entry("A", "1", fantasy="QUESTIONABLE"), entry("B", "2", fantasy="IR")))
    assert rows == []


def test_espn_team_codes_map_to_ours():
    assert parse_inactives(payload(entry("A", "1", team="WAS")))[0]["team"] == "WSH"


def test_rows_outside_three_hours_before_kickoff_are_skipped():
    rows = parse_inactives(payload(
        entry("Fresh", "1"),
        entry("Friday", "2", date="2026-10-30T20:00Z"),
        entry("After kickoff", "3", date="2026-11-01T18:05Z"),
    ))
    grouped, skipped = select_for_games(rows, [GAME], NOW)
    assert [r["name"] for r in grouped["2026_08_LV_NYJ"]] == ["Fresh"]
    assert {r["name"] for r in skipped} == {"Friday", "After kickoff"}


def test_a_game_already_started_takes_nothing():
    rows = parse_inactives(payload(entry("Breece Hall", "4427366")))
    grouped, _ = select_for_games(rows, [GAME], KICKOFF + timedelta(minutes=1))
    assert grouped == {}


class FakeDb:
    def __init__(self, recorded=()):
        self.recorded = set(recorded)

    def execute(self, sql, params=None):
        if "nfl_season_games" in sql:
            return [GAME]
        if "espn_id" in sql:
            return [{"id": 456, "gsis_id": "00-0038120"}] if params[1] == "4427366" else []
        if "canonical_name" in sql:
            return []
        if "ff_player_injury_observations" in sql:
            return [{"player_id": p} for p in self.recorded]
        raise AssertionError(sql)

    def execute_one(self, sql, params=None):
        if "nfl_season_games" in sql:
            return {**GAME, "kickoff": KICKOFF}
        if "ff_players" in sql:
            return {"id": 456, "team_abbrev": "NYJ"}
        raise AssertionError(sql)


def test_unmatched_skill_player_is_a_gap_but_a_lineman_is_not():
    report = capture(FakeDb(), payload(
        entry("Breece Hall", "4427366"),
        entry("Unknown Back", "999", position="RB"),
        entry("Max Mitchell", "998", position="OT"),
    ), NOW, apply=False)
    assert report["games"][0]["players"] == ["Breece Hall"]
    assert [u["name"] for u in report["unmatched"]] == ["Unknown Back"]
    assert report["untracked"] == 1


def test_a_player_already_recorded_for_the_game_is_not_written_again():
    report = capture(FakeDb(recorded={456}), payload(entry("Breece Hall", "4427366")), NOW, apply=False)
    assert report["games"][0]["new"] == 0


def test_stored_inactive_rows_normalize_to_out():
    prepared = prepare_import(FakeDb(), {
        "season": 2026, "week": 8, "source_label": "test",
        "source_published_at": (NOW - timedelta(minutes=5)).isoformat(),
        "records": [{"game_id": "2026_08_LV_NYJ", "player_id": 456, "status": "INACTIVE"}],
    }, reviewed_by="automated:test", reviewed_at=NOW)
    record = prepared["records"][0]
    assert record["status"] == "INACTIVE"
    assert normalize_injury_observation("nfl_official", record).normalized_status == "OUT"
