"""One definition of the current NFL week, shared by every availability writer."""
from datetime import datetime, timedelta, timezone
import inspect

import pytest

import ingest.nfl_availability_operations as operations
import ingest.nfl_dfs_availability as fantasypros
import ingest.nfl_dfs_projections as projections
from ingest.nfl_target_week import GAME_GRACE, SeasonComplete, select_target_week, target_week

UTC = timezone.utc
# The real 2026 shape: week 3 ends with Monday night 2026-09-29 00:15 UTC,
# week 4 opens Thursday 2026-10-02 00:15 UTC.
MNF = datetime(2026, 9, 29, 0, 15, tzinfo=UTC)
SCHEDULE = [
    {"week": 3, "kickoff": datetime(2026, 9, 25, 0, 15, tzinfo=UTC)},
    {"week": 3, "kickoff": datetime(2026, 9, 27, 17, 0, tzinfo=UTC)},
    {"week": 3, "kickoff": MNF},
    {"week": 4, "kickoff": datetime(2026, 10, 2, 0, 15, tzinfo=UTC)},
    {"week": 4, "kickoff": datetime(2026, 10, 4, 17, 0, tzinfo=UTC)},
]


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 9, 28, 23, 0, tzinfo=UTC), 3),    # before the last game
    (datetime(2026, 9, 29, 0, 30, tzinfo=UTC), 3),    # Monday night in progress
    (MNF + GAME_GRACE - timedelta(seconds=1), 3),
    (MNF + GAME_GRACE, 4),                            # grace is exclusive
    (datetime(2026, 9, 29, 6, 30, tzinfo=UTC), 4),
    (datetime(2026, 9, 29, 13, 0, tzinfo=UTC), 4),
])
def test_a_week_stays_current_until_its_last_game_is_six_hours_old(now, expected):
    assert select_target_week(SCHEDULE, now) == expected


def test_a_postponed_game_is_targeted_only_when_it_is_actually_next():
    postponed = SCHEDULE + [{"week": 3, "kickoff": datetime(2026, 10, 6, 23, 0, tzinfo=UTC)}]
    # Thursday of week 4: week 4's opener is the earliest open game.
    assert select_target_week(postponed, datetime(2026, 10, 1, 12, tzinfo=UTC)) == 4
    # After week 4's games, the makeup game is next.
    assert select_target_week(postponed, datetime(2026, 10, 6, 12, tzinfo=UTC)) == 3


def test_games_without_a_kickoff_cannot_hold_a_week_open():
    rows = [{"week": 2, "kickoff": None}, {"week": 5, "kickoff": datetime(2026, 10, 9, tzinfo=UTC)}]
    assert select_target_week(rows, datetime(2026, 10, 1, tzinfo=UTC)) == 5


def test_now_must_be_timezone_aware():
    with pytest.raises(ValueError):
        select_target_week(SCHEDULE, datetime(2026, 9, 29, 0, 30))


class ScheduleDb:
    def __init__(self, rows):
        self.rows = rows
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        assert "game_type='REG'" in sql
        return self.rows


def test_missing_schedule_is_a_failure_but_a_finished_season_is_not():
    with pytest.raises(ValueError) as missing:
        target_week(ScheduleDb([]), 2026, MNF)
    assert not isinstance(missing.value, SeasonComplete)
    with pytest.raises(SeasonComplete):
        target_week(ScheduleDb(SCHEDULE), 2026, datetime(2026, 10, 30, tzinfo=UTC))


def test_projection_publisher_uses_the_shared_rule():
    db = ScheduleDb(SCHEDULE)
    assert projections.infer_target_week(db, 2026, datetime(2026, 9, 29, 0, 30, tzinfo=UTC)) == 3
    assert projections.infer_target_week(db, 2026, datetime(2026, 9, 29, 6, 30, tzinfo=UTC)) == 4


@pytest.mark.parametrize("module", [operations, fantasypros, projections])
def test_no_writer_keeps_a_private_week_query(module):
    """The disagreement came from three private queries. Only the shared one may remain."""
    source = inspect.getsource(module).lower()
    assert "select min(week)" not in source
    assert "interval '6 hours'" not in source
    assert "target_week(" in source
