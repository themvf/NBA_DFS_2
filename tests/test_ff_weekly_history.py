"""Weekly stat history: what a refresh may rewrite, and whose stats land where."""

from __future__ import annotations

import re

import pandas as pd

from ingest.ff_independent import save_dst_weekly_history, save_weekly_history


class Recorder:
    """Records upserts instead of executing them."""

    def __init__(self):
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        return []


UNIVERSE = [
    {"player_id": 1, "gsis_id": "00-0000001", "name": "Alpha Back", "position": "RB", "team": "ATL"},
    {"player_id": 2, "gsis_id": "00-0000002", "name": "Beta Wide", "position": "WR", "team": "ATL"},
    {"player_id": 50, "gsis_id": None, "name": "Atlanta Falcons", "position": "DST", "team": "ATL"},
]


def player_row(gsis, name, position, week=1, **kw):
    return {"season_type": "REG", "season": 2026, "week": week, "player_id": gsis, "player_display_name": name,
            "position": position, "team": "ATL", "opponent_team": "CAR",
            "fantasy_points": 10.0, "fantasy_points_ppr": 12.0, **kw}


def _set_and_guard_columns(statement):
    set_part = statement.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0]
    updated = {c.strip().split("=")[0] for c in set_part.split(",") if "=" in c}
    guard = set(re.findall(r"ff_player_week_stats\.(\w+)", statement.split("WHERE", 1)[1]))
    return updated - {"fetched_at"}, guard


def test_an_unchanged_week_keeps_its_fetched_at():
    """Point-in-time readers select fetched_at <= as_of, so an upsert may only
    move fetched_at when the content changes -- for every column it updates."""
    db = Recorder()
    save_weekly_history(db, UNIVERSE, 2026, pd.DataFrame([player_row("00-0000001", "Alpha Back", "RB")]))
    teams = pd.DataFrame([{"season_type": "REG", "season": 2026, "week": 1, "team": "ATL", "opponent_team": "CAR",
                           "def_sacks": 2, "def_interceptions": 1}])
    schedule = pd.DataFrame([{"season": 2026, "game_type": "REG", "week": 1, "home_team": "ATL", "away_team": "CAR",
                              "home_score": 20, "away_score": 10}])
    save_dst_weekly_history(db, UNIVERSE, 2026, teams, schedule)
    assert len(db.calls) == 2
    for statement, _ in db.calls:
        assert "IS DISTINCT FROM" in statement
        updated, guard = _set_and_guard_columns(statement)
        assert updated == guard, f"columns updated without a change guard: {updated ^ guard}"
