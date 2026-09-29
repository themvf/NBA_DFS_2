"""The settlement gate must name the fixtures it is failing on.

Tennis Settlement (frequent) failed 60 of its last 60 runs on
`stale_unresolved_matches: 7` with nothing but "status: unhealthy". The gate
is right to fail (retirements, walkovers and draw replacements are never
auto-settled), but the job now lists which fixtures and what to do.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from ingest import tennis_reconciliation as rec

ROWS = [
    {"id": 8678, "tour": "ATP", "tournament": "ATP US Open", "home_player": "Karen Khachanov",
     "away_player": "Alexander Blockx", "starts_at": datetime(2026, 9, 9, 20, 45, tzinfo=timezone.utc),
     "pending_bets": 2},
    {"id": 9467, "tour": "WTA", "tournament": "WTA Singapore Open", "home_player": "Alycia Parks",
     "away_player": "Janice Tjen", "starts_at": datetime(2026, 9, 22, 3, 0, tzinfo=timezone.utc),
     "pending_bets": 0},
]


def test_format_names_each_fixture_and_the_remedy():
    text = rec.format_stale_unresolved(ROWS, total=2, max_stale_hours=72)
    lines = text.splitlines()
    assert lines[0].startswith("::error title=Tennis settlement::2 fixture(s) started more than 72h ago")
    assert "(2 pending bet(s))" in lines[0]
    assert "ingest/repair_tennis_stale_results.py" in lines[0]
    assert lines[1] == ("  match 8678: ATP US Open | Karen Khachanov v Alexander Blockx | "
                        "2026-09-09 20:45 UTC | 2 pending bet(s)")
    assert len(lines) == 3


def test_format_reports_rows_beyond_the_limit():
    text = rec.format_stale_unresolved(ROWS[:1], total=30, max_stale_hours=72)
    assert text.splitlines()[-1] == "  ... and 29 more"


def _run_main(monkeypatch, capsys, stale: int):
    metrics = {key: 0 for key in (
        "repaired_moneyline_bets", "repaired_alert_outcomes", "pending_after_result",
        "settled_without_result", "projection_mismatch", "moneyline_outcome_mismatch",
        "alert_outcome_mismatch", "alert_grade_mismatch", "stale_running_provider_runs", "open_disputes")}
    metrics.update(stale_unresolved_matches=stale, fresh_healthy_provider_runs=1)
    monkeypatch.setattr(sys, "argv", ["tennis_reconciliation", "--fail-on-unhealthy"])
    monkeypatch.setattr(rec, "load_config", lambda: type("C", (), {"database_url": "postgres://x"})())
    monkeypatch.setattr(rec, "DatabaseManager", lambda url: object())
    monkeypatch.setattr(rec, "reconciliation_report", lambda db, **kw: (metrics, stale == 0))
    monkeypatch.setattr(rec, "stale_unresolved_detail", lambda db, **kw: ROWS)
    code = rec.main()
    return code, capsys.readouterr().out


def test_main_still_fails_and_lists_stale_fixtures(monkeypatch, capsys):
    code, out = _run_main(monkeypatch, capsys, stale=2)
    assert code == 1
    assert "status: unhealthy" in out
    assert "match 9467: WTA Singapore Open | Alycia Parks v Janice Tjen" in out


def test_main_is_quiet_when_nothing_is_stale(monkeypatch, capsys):
    code, out = _run_main(monkeypatch, capsys, stale=0)
    assert code == 0
    assert "::error" not in out
