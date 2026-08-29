import json

import pandas as pd

from model.ff_v2_historical_bestball_cohort import (
    LINEUP_POLICY,
    ROSTER_POLICY,
    WeeklyPlayer,
    _weekly_outcomes,
    build_focal_seat_roster,
    draftkings_points,
    maximum_legal_lineup,
    normalize_name,
    roster_match_report,
    score_roster,
)


def test_exact_draftkings_scoring_with_bonuses_and_turnover() -> None:
    row = {
        "passing_yards": 300, "passing_tds": 2, "passing_interceptions": 1,
        "rushing_yards": 100, "rushing_tds": 1,
        "receiving_yards": 100, "receiving_tds": 1, "receptions": 5,
        "passing_2pt_conversions": 1, "fumbles_lost_total": 1,
    }
    assert draftkings_points(row) == 66.0


def test_maximum_legal_lineup_uses_one_flex_and_required_slots() -> None:
    players = []
    for position, count in (("QB", 2), ("RB", 4), ("WR", 5), ("TE", 3)):
        players.extend(WeeklyPlayer(f"{position}{i}", f"{position}{i}", position, 30 - i) for i in range(count))
    points, selected = maximum_legal_lineup(players)
    positions = [key[:2] if key.startswith(("QB", "RB", "WR", "TE")) else key for key in selected]
    assert len(selected) == 8
    assert positions.count("QB") == LINEUP_POLICY["QB"]
    assert positions.count("RB") >= LINEUP_POLICY["RB"]
    assert positions.count("WR") >= LINEUP_POLICY["WR"]
    assert positions.count("TE") >= LINEUP_POLICY["TE"]
    assert points > 0


def test_week_18_is_excluded() -> None:
    frame = pd.DataFrame([
        {"player_display_name": "Test Player", "player_name": "T.Player", "player_id": "1", "position": "WR", "season_type": "REG", "week": 17, "receiving_yards": 10},
        {"player_display_name": "Test Player", "player_name": "T.Player", "player_id": "1", "position": "WR", "season_type": "REG", "week": 18, "receiving_yards": 200},
    ])
    weekly, _ = _weekly_outcomes(frame)
    assert weekly["testplayer:WR"] == {17: 1.0}


def test_roster_policy_and_replay_are_deterministic() -> None:
    players = []
    index = 1
    for position, count in (("QB", 8), ("RB", 20), ("WR", 24), ("TE", 10)):
        for offset in range(count):
            players.append({"name": f"{position} Player {offset}", "position": position, "adp": index, "times_drafted": 10})
            index += 1
    payload = {"players": players}
    first = build_focal_seat_roster(payload, 7)
    second = build_focal_seat_roster(json.loads(json.dumps(payload)), 7)
    assert first == second
    assert {position: sum(row["position"] == position for row in first) for position in ROSTER_POLICY} == ROSTER_POLICY
    assert len({row["key"] for row in first}) == 20


def test_missing_names_normalize_conservatively() -> None:
    assert normalize_name("Odell Beckham Jr.") == normalize_name("Odell Beckham")
    assert normalize_name("D'Andre Swift") == "dandreswift"
    assert normalize_name(None) == ""
    roster = [
        {"key": "knownplayer:WR", "name": "Known Player", "position": "WR"},
        {"key": "missingplayer:WR", "name": "Missing Player", "position": "WR"},
    ]
    weekly = {"knownplayer:WR": {1: 10.0}}
    assert roster_match_report(roster, weekly) == {
        "matched_roster_players": 1,
        "missing_roster_players": ["Missing Player"],
    }
    # Unmatched means unavailable outcome, not a falsely matched zero row.
    assert score_roster(roster, weekly)["counted_points"] == 10.0
