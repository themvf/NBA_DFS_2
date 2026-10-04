"""The availability loader bounds Sleeper observations to the two weeks before the week's first kickoff.

Before 2026-10-04 it read every Sleeper observation of the season with its JSON
payload on every run (311k rows, 930 MB of pages). The bound is keyed to the
week's schedule, not to now(), so replays of a past week stay reproducible.
"""
import ingest.nfl_dfs_projections as proj


class RecordingDB:
    def __init__(self):
        self.calls = []
    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))
        return [{"player_id": 1, "observation_id": 5}, {"player_id": 1, "observation_id": 4}, {"player_id": 2, "observation_id": 9}]


def test_sleeper_rows_are_bounded_by_the_weeks_schedule_not_by_now():
    db = RecordingDB()
    observed = proj._availability(db, 2026, 5)
    sql, params = db.calls[0]
    assert "o.source='sleeper' AND o.observed_at >= COALESCE( (SELECT min(g.kickoff) - interval '14 days' FROM nfl_season_games g WHERE g.season = %s AND g.week = %s AND g.game_type = 'REG'), '-infinity'::timestamptz)" in sql
    assert "now()" not in sql.lower()
    assert params == (2026, 2026, 5, 5, "5", "game-week-injuries-v2-2026-5%")
    assert "o.source IN ('fantasypros','nfl_official')" in sql, "the week-keyed sources keep their own filter"
    assert {k: [r["observation_id"] for r in v] for k, v in observed.items()} == {1: [5, 4], 2: [9]}


def test_no_week_means_no_query():
    db = RecordingDB()
    assert proj._availability(db, 2026, None) == {}
    assert db.calls == []
