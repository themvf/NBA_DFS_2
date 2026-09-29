"""The ADP snapshot stops after NFL week 1 instead of failing all season.

Refresh Fantasy Football ADP Snapshot failed every run from 2026-09-14 on
(e.g. 36526777281) because Fantasy Football Calculator's HALF feed shrank to
54 players once drafting ended. The row guard is kept for draft season: a
short feed then means a truncated one and must not be stored.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest import ff_adp_snapshot as snap

WEEK1_ENDS = datetime(2026, 9, 15, 0, 15, tzinfo=timezone.utc)


class FakeDb:
    def __init__(self, closes_at=WEEK1_ENDS, players=150):
        self.closes_at = closes_at
        self.players = players
        self.writes = []

    def execute_one(self, sql, params=None):
        assert "nfl_season_games" in sql
        return {"closes_at": self.closes_at}

    def execute(self, sql, params=None):
        if "FROM ff_players" in sql:
            return [{"id": i, "normalized_name": f"p{i}", "position": "WR", "team_abbrev": "KC"}
                    for i in range(self.players)]
        self.writes.append(sql)
        return []


def test_after_week_one_nothing_is_fetched_or_stored(monkeypatch):
    def no_fetch(*_):
        raise AssertionError("must not call the ADP feed after the draft market closed")

    monkeypatch.setattr(snap, "_fetch_json", no_fetch)
    db = FakeDb()
    result = snap._run(2026, db, now=datetime(2026, 9, 29, 5, 34, tzinfo=timezone.utc))
    assert result["skipped"] is True
    assert "draft market closed when NFL week 1 ended (2026-09-15 00:15 UTC)" in result["reason"]
    assert db.writes == []


def test_during_draft_season_a_thin_feed_still_fails(monkeypatch):
    monkeypatch.setattr(snap, "_fetch_json", lambda url: ({"players": [{}] * 54}, "digest"))
    with pytest.raises(RuntimeError, match="suspiciously few rows"):
        snap._run(2026, FakeDb(), now=datetime(2026, 9, 12, tzinfo=timezone.utc))


def test_unknown_schedule_keeps_the_guard(monkeypatch):
    monkeypatch.setattr(snap, "_fetch_json", lambda url: ({"players": [{}] * 54}, "digest"))
    with pytest.raises(RuntimeError, match="suspiciously few rows"):
        snap._run(2026, FakeDb(closes_at=None), now=datetime(2026, 9, 29, tzinfo=timezone.utc))
