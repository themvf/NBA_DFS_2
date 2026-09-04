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
