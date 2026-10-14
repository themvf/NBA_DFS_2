"""The matchup loader reads stat rows slim and fetches whole rows for participants only.

Each participant's manifest records a digest of its whole source row, so the
manifests must come out identical to building them from whole rows; a row that
changes between the two reads must fail loudly rather than mix two versions.
"""
from datetime import datetime, timedelta, timezone

import pytest

from ingest.nfl_matchup_context import with_participant_source_rows
from model.nfl_matchup_features import PARTICIPANT_FIELDS, participant_manifests

NOW = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
GAME_ID = "2026_04_MIN_TB"
GAMES = [{"game_id": GAME_ID, "home": "TB", "away": "MIN", "completed": True, "kickoff": NOW - timedelta(days=2)}]
FETCHED = NOW - timedelta(days=1)


def stat_row(row_id, player, position, attempts, carries, sacks=0, name=None):
    return {"id": row_id, "team": "TB", "source": "nflverse", "fetched_at": FETCHED,
            "source_row": {"game_id": GAME_ID, "team": "TB", "player_id": player, "player_name": name or player,
                           "position": position, "attempts": attempts, "carries": carries, "sacks_suffered": sacks,
                           "receiving_yards": 41.0, "targets": 6, "air_yards_share": 0.18, "season_type": "REG"}}


def slim(row):
    return {**row, "source_row": {k: v for k, v in row["source_row"].items() if k in PARTICIPANT_FIELDS}}


class TableDatabase:
    def __init__(self, tables):
        self.rows = {table: {row["id"]: row for row in rows} for table, rows in tables.items()}
        self.reads = []

    def execute(self, sql, params):
        table = sql.split(" FROM ")[1].split()[0]
        self.reads.append((table, sorted(params[0], key=str)))
        return [self.rows[table][row_id] for row_id in params[0] if row_id in self.rows[table]]


EVIDENCE = [stat_row("a" * 64, "qb1", "QB", 31, 3, sacks=2), stat_row("b" * 64, "wr1", "WR", 0, 0)]
WEEKLY = [stat_row(301032, "rb1", "RB", 0, 18), stat_row(301033, "te1", "TE", 0, 0)]


def test_participants_get_whole_rows_and_manifests_match_whole_row_reads():
    db = TableDatabase({"nfl_weekly_participant_evidence": EVIDENCE, "ff_player_week_stats": WEEKLY})
    tables = [("nfl_weekly_participant_evidence", [slim(r) for r in EVIDENCE]),
              ("ff_player_week_stats", [slim(r) for r in WEEKLY])]
    rows = with_participant_source_rows(db, tables, GAMES, NOW, [])
    assert participant_manifests(rows, GAMES, NOW, []) == participant_manifests(EVIDENCE + WEEKLY, GAMES, NOW, [])
    assert db.reads == [("nfl_weekly_participant_evidence", ["a" * 64]), ("ff_player_week_stats", [301032])]


def test_a_row_rewritten_between_the_two_reads_fails_loudly():
    rewritten = {**WEEKLY[0], "fetched_at": FETCHED + timedelta(hours=1)}
    db = TableDatabase({"ff_player_week_stats": [rewritten]})
    with pytest.raises(ValueError, match="changed between"):
        with_participant_source_rows(db, [("ff_player_week_stats", [slim(WEEKLY[0])])], GAMES, NOW, [])
