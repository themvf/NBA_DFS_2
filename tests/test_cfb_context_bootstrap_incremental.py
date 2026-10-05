"""bootstrap() sends only missing engine rows; a run with nothing new writes nothing to the four big sets.

2026-10-04: the full team, game, schedule-revision and schedule-source sets
(~9,000 rows each) were re-sent on every one of ~370 runs a day as no-op
inserts, 650-850 MB of pages touched per statement.
"""
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

import ingest.cfb_context_bootstrap as mod

KICKOFF = datetime(2026, 10, 10, 19, 30, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, teams, games, histories, present_everything):
        self.teams, self.games, self.histories = teams, games, histories
        self.present_everything = present_everything
        self.sql = []
        self.last = ""
        self.last_params = None

    def execute(self, sql, params=None):
        self.last = " ".join(sql.split())
        self.last_params = params
        self.sql.append((self.last, params))

    def fetchall(self):
        s = self.last
        if s.startswith("SELECT team_id FROM cfb_teams"):
            return [dict(r) for r in self.teams]
        if s.startswith("SELECT * FROM cfb_matchups"):
            return [dict(r) for r in self.games]
        if "FROM game_odds_history h WHERE h.sport='cfb'" in s:
            return [dict(r) for r in self.histories]
        if s.startswith("SELECT ") and " AS k FROM cfb_engine_" in s:
            # The existence probe: echo every asked-for key back as present (or none).
            return [{"k": key} for key in self.last_params[0]] if self.present_everything else []
        return []


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = None
        self.committed = False
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False
    def cursor(self):
        return self._cursor
    def commit(self):
        self.committed = True
    def rollback(self):
        pass


def run(monkeypatch, *, present_everything):
    teams = [{"team_id": 1}, {"team_id": 2}]
    games = [{"id": 10, "cfbd_game_id": 500, "commence_time": KICKOFF, "fetched_at": KICKOFF, "odds_event_id": "e1"}]
    cursor = FakeCursor(teams, games, [], present_everything)
    conn = FakeConnection(cursor)
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: conn)
    monkeypatch.setattr(psycopg2.extras, "register_uuid", lambda: None)
    calls = []
    monkeypatch.setattr(psycopg2.extras, "execute_values", lambda cur, sql, rows, **kw: calls.append((" ".join(sql.split()), list(rows))))
    result = mod.bootstrap("postgres://x", apply=True, new_origin="auto")
    return result, calls, conn


BIG_SETS = ("INSERT INTO cfb_engine_subjects", "INSERT INTO cfb_engine_teams", "INSERT INTO cfb_engine_events",
            "INSERT INTO cfb_engine_sources", "INSERT INTO cfb_engine_schedule_revisions")


def test_nothing_new_sends_none_of_the_big_sets(monkeypatch):
    result, calls, conn = run(monkeypatch, present_everything=True)
    # psycopg2's execute_values runs nothing for an empty row list, so only
    # statements that carry rows count as sent.
    sent = [text for text, rows in calls if text.startswith(BIG_SETS) and rows]
    assert sent == [], sent
    assert result["new_subjects"] == 0 and result["new_events"] == 0 and result["new_schedule_revisions"] == 0
    assert result["captures"] == 0 and conn.committed


def test_missing_rows_are_still_inserted(monkeypatch):
    result, calls, conn = run(monkeypatch, present_everything=False)
    by_table = {}
    for text, rows in calls:
        if text.startswith(BIG_SETS):
            by_table.setdefault(text.split("(")[0].strip(), []).extend(rows)
    assert len(by_table["INSERT INTO cfb_engine_subjects"]) == 3, "two teams and one game"
    assert len(by_table["INSERT INTO cfb_engine_teams"]) == 2
    assert len(by_table["INSERT INTO cfb_engine_events"]) == 1
    assert len(by_table["INSERT INTO cfb_engine_sources"]) == 1
    assert len(by_table["INSERT INTO cfb_engine_schedule_revisions"]) == 1
    assert result["new_subjects"] == 3 and result["new_events"] == 1 and result["new_schedule_revisions"] == 1
