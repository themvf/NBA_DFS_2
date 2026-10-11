from datetime import datetime, timezone
from ingest.cfb_capture_audit import quote_issues


def test_audit_accepts_unchanged_and_stale_quotes_without_inventing_movement():
    assert quote_issues({"book": {"spread_home": -3, "spread_away": 3,
        "last_update": "2026-09-03T10:00:00Z"}}, datetime(2026, 9, 4, tzinfo=timezone.utc)) == []


def test_audit_rejects_impossible_quotes_and_future_updates():
    result = quote_issues({"book": {"spread_home": -3, "spread_away": 4,
        "total_line": float("nan"), "last_update": "2026-09-05T00:00:00Z"}},
        datetime(2026, 9, 4, tzinfo=timezone.utc))
    assert "book:asymmetric_spread" in result
    assert "book:total_line:nonfinite" in result
    assert "book:future_quote_timestamp" in result


class AuditCursor:
    """Answers the audit's reads; every integrity check finds nothing."""

    def __init__(self, live_games, quotes):
        self.live_games, self.quotes, self.sql, self.last = live_games, quotes, [], ""

    def execute(self, sql, params=None):
        self.last = " ".join(sql.split())
        self.sql.append((self.last, params))

    def fetchall(self):
        if self.last.startswith("SELECT id FROM cfb_matchups"):
            return [{"id": game} for game in self.live_games]
        if self.last.startswith("SELECT h.id,h.books,h.captured_at FROM game_odds_history h"):
            return self.quotes
        return []


class AuditConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, cursor_factory=None):
        return self._cursor


def history_reads(cursor):
    return [(sql, params) for sql, params in cursor.sql
            if sql.startswith(("SELECT h.id,h.books", "SELECT h.id AS history_id"))]


def run_audit(full=False, quotes=()):
    from ingest.cfb_capture_audit import audit
    cursor = AuditCursor([7, 9], list(quotes))
    return audit(AuditConnection(cursor), full=full), cursor


def test_a_capture_run_rereads_only_games_that_can_still_gain_history():
    result, cursor = run_audit()
    reads = history_reads(cursor)
    assert len(reads) == 2
    assert all(" AND h.matchup_id=ANY(%s)" in sql and params == ([7, 9],) for sql, params in reads)
    assert result["history_scope"] == {"mode": "live", "games": 2}
    assert result["integrity_status"] == "pass"


def test_the_full_audit_rereads_every_game():
    result, cursor = run_audit(full=True)
    reads = history_reads(cursor)
    assert len(reads) == 2
    assert all("ANY" not in sql and params is None for sql, params in reads)
    assert not any(sql.startswith("SELECT id FROM cfb_matchups") for sql, _ in cursor.sql)
    assert result["history_scope"] == {"mode": "full"}


def test_a_bad_quote_in_a_live_game_still_fails_the_capture_run():
    captured = datetime(2026, 10, 9, tzinfo=timezone.utc)
    result, _ = run_audit(quotes=[{"id": 41, "books": {"dk": {"spread_home": -3, "spread_away": 4}}, "captured_at": captured}])
    assert result["integrity_status"] == "fail"
    assert result["errors"]["invalid_quotes"] == [{"history_id": 41, "issues": ["dk:asymmetric_spread"]}]
