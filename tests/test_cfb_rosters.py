import pytest

from ingest.cfb_rosters import _previous_player_ids, position_group, summarize_roster


def test_position_groups_cover_core_continuity_units() -> None:
    assert position_group("QB") == "QB"
    assert position_group("OT") == "OL"
    assert position_group("WR") == "PASS_CATCHER"
    assert position_group("CB") == "SECONDARY"
    assert position_group(None) is None


def test_roster_summary_tracks_ids_not_names() -> None:
    players = [
        {"id": "qb-1", "position": "QB"},
        {"id": "ol-1", "position": "OT"},
        {"id": "wr-1", "position": "WR"},
    ]
    summary = summarize_roster(
        players, {"qb-1", "ol-1"}, {"percentPPA": 0.7}, 820.5,
        [{"rating": 0.91}], [{"rating": 0.85}],
    )
    assert summary["returning_roster_count"] == 2
    assert summary["roster_continuity_pct"] == pytest.approx(2 / 3)
    assert summary["returning_quarterbacks"] == 1
    assert summary["returning_offensive_line"] == 1
    assert summary["transfer_rating_in"] == 0.91
    assert summary["talent_composite"] == 820.5
    assert summary["availability_source"] == "not_provided_by_cfbd_roster"


class _RecordingDb:
    """Captures the SQL and parameters a query helper issues."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, statement: str, params: tuple = ()) -> list[dict]:
        self.statements.append((statement, params))
        return self.rows


def test_returning_players_come_from_one_prior_season_snapshot() -> None:
    db = _RecordingDb([{"source_player_id": "qb-1"}, {"source_player_id": "ol-1"}])
    assert _previous_player_ids(db, team_id=7, season=2026) == {"qb-1", "ol-1"}
    statement, params = db.statements[0]
    assert params == (7, 2025)
    # A weekly capture cadence means a union across every prior-season snapshot
    # would count anyone who ever appeared as returning, so the lookup must
    # resolve to exactly one snapshot.
    assert "LIMIT 1" in statement
    assert statement.count("ORDER BY") == 1


def test_continuity_uses_only_the_latest_prior_roster() -> None:
    """A player cut before the prior season ended is not a returning player."""
    players = [{"id": "qb-1", "position": "QB"}, {"id": "rb-1", "position": "RB"}]
    latest_prior_roster = {"qb-1"}
    summary = summarize_roster(players, latest_prior_roster, None, None, [], [])
    assert summary["returning_roster_count"] == 1
    assert summary["roster_continuity_pct"] == pytest.approx(0.5)
