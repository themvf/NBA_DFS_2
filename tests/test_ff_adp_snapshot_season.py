from datetime import date

from ingest.ff_adp_snapshot import draft_season_open


def test_draft_season_runs_july_through_week_one():
    assert not draft_season_open(date(2026, 6, 30))
    assert draft_season_open(date(2026, 7, 1))
    assert draft_season_open(date(2026, 8, 31))
    # Week 1 always kicks off by September 10 (the Thursday after Labor Day).
    assert draft_season_open(date(2026, 9, 10))
    assert not draft_season_open(date(2026, 9, 11))
    assert not draft_season_open(date(2026, 9, 29))
    assert not draft_season_open(date(2027, 1, 15))
