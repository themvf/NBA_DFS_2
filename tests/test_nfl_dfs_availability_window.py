"""The availability loader bounds Sleeper observations to the two weeks before the week's first kickoff.

Before 2026-10-04 it read every Sleeper observation of the season with its JSON
payload on every run (311k rows, 930 MB of pages). The bound is keyed to the
week's schedule, not to now(), so replays of a past week stay reproducible.
"""
from datetime import datetime, timedelta, timezone

import ingest.nfl_dfs_projections as proj

AS_OF = datetime(2026, 10, 4, 15, 0, tzinfo=timezone.utc)
EARLY = datetime(2026, 10, 4, 17, 0, tzinfo=timezone.utc)
LATE = datetime(2026, 10, 5, 0, 20, tzinfo=timezone.utc)


class RecordingDB:
    def __init__(self):
        self.calls = []
    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))
        return [{"player_id": 1, "observation_id": 5}, {"player_id": 1, "observation_id": 4}, {"player_id": 2, "observation_id": 9}]


def test_sleeper_rows_are_bounded_by_the_weeks_schedule_not_by_now():
    db = RecordingDB()
    observed = proj._availability(db, 2026, 5, as_of_at=AS_OF, kickoffs=[LATE, EARLY, EARLY, None])
    sql, params = db.calls[0]
    assert "o.season = %s AND o.source = 'sleeper' AND o.observed_at >= COALESCE( (SELECT min(g.kickoff) - interval '14 days' FROM nfl_season_games g WHERE g.season = %s AND g.week = %s AND g.game_type = 'REG'), '-infinity'::timestamptz)" in sql
    assert "now()" not in sql.lower()
    assert params[2:] == (2026, 5, "5", "game-week-injuries-v2-2026-5%", 2026, 2026, 5)
    assert "o.source IN ('fantasypros','nfl_official')" in sql, "the week-keyed sources keep their own filter"
    assert {k: [r["observation_id"] for r in v] for k, v in observed.items()} == {1: [5, 4], 2: [9]}


def test_sleeper_returns_only_the_latest_eligible_row_per_decision_cutoff():
    """The resolver decides from the newest qualifying row per source at each
    player's decision time: kickoff - 1us for a started game, as_of_at
    otherwise. Those instants are the cutoffs, so the decision is unchanged
    while every repeat capture of the same status stays in the database
    (231k rows per run on 2026-10-11, about 1k after)."""
    db = RecordingDB()
    proj._availability(db, 2026, 5, as_of_at=AS_OF, kickoffs=[LATE, EARLY, EARLY, None, EARLY.replace(tzinfo=None)])
    sql, params = db.calls[0]
    assert params[0] == [EARLY, LATE, AS_OF], "each distinct aware kickoff, then the run time"
    assert params[1] == [False, False, True], "strictly before a kickoff, at or before as_of_at"
    # A row's bucket is the number of cutoffs it is not yet eligible for; eligibility only grows
    # with the cutoff, so the newest row per (player, bucket) covers every cutoff's newest row.
    assert ("WHERE NOT (sleeper.available_at < c.cutoff OR (c.inclusive AND sleeper.available_at = c.cutoff))) bucket"
            in sql)
    assert "DISTINCT ON (player_id, bucket)" in sql
    assert "WHERE bucket < (SELECT count(*) FROM cutoffs)" in sql, "rows available after every cutoff are not loaded"
    assert "s.model_eligible IS TRUE AND COALESCE(lower(s.status), 'success') = 'success'" in sql
    assert "ORDER BY player_id, bucket, available_at DESC, observation_id DESC" in sql
    assert "window_rows AS MATERIALIZED" in sql, "the Sleeper window is read from its covering index first"
    window = sql.split("window_rows AS MATERIALIZED")[1].split("), sleeper AS")[0]
    assert "raw_payload" not in window, "Sleeper rows never read the JSON payload"
    assert "o.source IN ('fantasypros','nfl_official')" in sql, "FantasyPros and official rows are returned whole"


def test_no_week_means_no_query():
    db = RecordingDB()
    assert proj._availability(db, 2026, None, as_of_at=AS_OF) == {}
    assert db.calls == []
