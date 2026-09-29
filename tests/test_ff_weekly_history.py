"""Weekly stat history: what a refresh may rewrite, and whose stats land where."""

from __future__ import annotations

import re

import pandas as pd
import pytest

from ingest.ff_independent import (
    PRIOR_SEASON_UNMATCHED_MAX_SHARE, WeeklyIdentityError, save_dst_weekly_history, save_weekly_history,
)


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


def test_an_unchanged_week_keeps_its_fetched_at(tmp_path):
    """Point-in-time readers select fetched_at <= as_of, so an upsert may only
    move fetched_at when the content changes -- for every column it updates."""
    db = Recorder()
    save_weekly_history(db, UNIVERSE, 2026, pd.DataFrame([player_row("00-0000001", "Alpha Back", "RB")]),
                        report_dir=tmp_path)
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


# -- Identity: report what could not be matched, never guess ----------------

def _rows(n_matched, n_unmatched):
    rows = [player_row("00-0000001", "Alpha Back", "RB", week=w) for w in range(1, n_matched + 1)]
    rows += [player_row(f"00-99{i:05d}", f"Gone Player {i}", "WR", week=1, fantasy_points_ppr=float(i))
             for i in range(n_unmatched)]
    return pd.DataFrame(rows)


def test_unmatched_rows_are_reported_with_names_and_points(tmp_path):
    import json
    db = Recorder()
    written = save_weekly_history(db, UNIVERSE, 2026, _rows(17, 3), report_dir=tmp_path)
    assert written == 17
    report = json.loads((tmp_path / "unmatched-2026.json").read_text())
    assert report["eligible_rows"] == 20 and report["unmatched_rows"] == 3
    assert [r["name"] for r in report["unmatched"]] == ["Gone Player 2", "Gone Player 1", "Gone Player 0"]
    assert report["unmatched"][0] == {"gsis_id": "00-9900002", "name": "Gone Player 2", "position": "WR",
                                      "team": "ATL", "week": 1, "fantasy_points_ppr": 2.0}


def test_too_many_unmatched_rows_fail_before_anything_is_written(tmp_path):
    db = Recorder()
    # 12 of 30 unmatched: 40%, above both the 5% share and the 10-row floor.
    with pytest.raises(WeeklyIdentityError, match="12 of 30"):
        save_weekly_history(db, UNIVERSE, 2026, _rows(18, 12), report_dir=tmp_path)
    assert db.calls == []
    assert (tmp_path / "unmatched-2026.json").exists()


def test_a_few_unmatched_rows_under_the_floor_do_not_fail(tmp_path):
    # 10 unmatched is at the floor, however small the week.
    written = save_weekly_history(Recorder(), UNIVERSE, 2026, _rows(5, 10), report_dir=tmp_path)
    assert written == 5


def test_prior_season_matching_allows_structural_retirements(tmp_path):
    # 12 of 100 unmatched (12%): a past season against the current roster.
    frame = _rows(88, 12)
    with pytest.raises(WeeklyIdentityError):
        save_weekly_history(Recorder(), UNIVERSE, 2025, frame, report_dir=tmp_path)
    written = save_weekly_history(Recorder(), UNIVERSE, 2025, frame, report_dir=tmp_path,
                                  max_unmatched_share=PRIOR_SEASON_UNMATCHED_MAX_SHARE)
    assert written == 88


def test_two_feed_rows_for_one_player_week_is_an_error_not_an_overwrite(tmp_path):
    # The gsis row and a name-only row both resolve to Alpha Back, week 1.
    frame = pd.DataFrame([player_row("00-0000001", "Alpha Back", "RB"),
                          player_row(None, "Alpha Back", "RB", fantasy_points_ppr=30.0)])
    db = Recorder()
    with pytest.raises(WeeklyIdentityError, match="overwrite"):
        save_weekly_history(db, UNIVERSE, 2026, frame, report_dir=tmp_path)
    assert db.calls == []


def test_an_ambiguous_name_match_is_an_error(tmp_path):
    universe = UNIVERSE + [{"player_id": 3, "gsis_id": None, "name": "Alpha Back", "position": "RB", "team": "NO"}]
    frame = pd.DataFrame([player_row(None, "Alpha Back", "RB")])
    with pytest.raises(WeeklyIdentityError, match="matches 2 players"):
        save_weekly_history(Recorder(), universe, 2026, frame, report_dir=tmp_path)


def test_the_name_fallback_never_crosses_two_gsis_ids(tmp_path):
    # A different person who shares Beta Wide's name and position: his stats
    # must not land on player 2.
    frame = pd.DataFrame([player_row("00-0000777", "Beta Wide", "WR")])
    db = Recorder()
    written = save_weekly_history(db, UNIVERSE, 2026, frame, report_dir=tmp_path)
    assert written == 0 and db.calls == []


def test_the_name_fallback_still_serves_rows_without_a_gsis(tmp_path):
    db = Recorder()
    written = save_weekly_history(db, UNIVERSE, 2026, pd.DataFrame([player_row(None, "Beta Wide", "WR")]),
                                  report_dir=tmp_path)
    assert written == 1 and db.calls[0][1][0] == 2
