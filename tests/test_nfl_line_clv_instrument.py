"""NFL line alerts must be CLV-graded at the line they were actually priced at.

Before 2026-09-22 the NFL insert path froze `exec_line` but never wrote
`entry_home_line`, so settlement fell back to the mean-consensus trigger line
-- a number no book posts. 271 NFL alerts, 0 with an entry line; 58 settled
rows graded at non-half-point lines; 0 pushes possible because a mean cannot
land on a key number. The CFB path had done it right from day one.

These tests pin: the shared helper's sign convention, that the NFL scan path
now calls it, that settlement derives the entry line from a frozen exec_line
on legacy rows and withholds line CLV when only a consensus mean exists, and
that regrading is append-only and version-gated.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from model import line_alerts
from tests.test_cfb_market_signals import SettlementDb, _books


def test_entry_home_line_is_home_referenced() -> None:
    # An away spread of +3.0 is the home team laying 3.
    assert line_alerts._entry_home_line("spread", "away", 3.0) == -3.0
    assert line_alerts._entry_home_line("spread", "home", -3.0) == -3.0
    # Totals are side-neutral.
    assert line_alerts._entry_home_line("total", "over", 44.5) == 44.5
    assert line_alerts._entry_home_line("total", "under", 44.5) == 44.5
    # Nothing priceable -> None, never a fabricated number.
    assert line_alerts._entry_home_line("spread", "away", None) is None


def test_nfl_scan_path_freezes_the_entry_line_like_cfb() -> None:
    """The regression that produced 271 entry-less alerts: guard it at source."""
    src = open(line_alerts.__file__, encoding="utf-8").read()
    nfl_block = src.split('if sport == "nfl":\n            previous_books')[1]
    nfl_block = nfl_block.split('if sport in ("cfb", "nfl"):')[0]
    assert "_entry_home_line(" in nfl_block, "NFL insert path must freeze entry_home_line"
    assert '"entry_home_line"' in nfl_block


def _legacy_row(details: dict, *, side: str = "away", settled: bool = True) -> dict:
    return {
        "id": 77, "matchup_id": 9, "side": side,
        "commence_time": datetime(2026, 9, 13, 17, tzinfo=timezone.utc),
        "settled_at": datetime(2026, 9, 14, tzinfo=timezone.utc) if settled else None,
        "home_score": 24, "away_score": 20,
        "details_json": details,
    }


class _RegradeDb(SettlementDb):
    def __init__(self, row: dict):
        super().__init__()
        self.row = row
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql, params=None):
        if "SELECT a.*, m.home_score" in sql:
            self.queries.append((sql, params))
            return [dict(self.row)]
        self.updates.append((sql, params))
        return []


def test_legacy_nfl_row_derives_entry_from_frozen_exec_line(monkeypatch) -> None:
    """spread_steam on the AWAY side at exec_line +3.0 -> entry_home_line -3.0.

    v1 graded this row at trigger_line -2.83 (the consensus mean). Under v2
    the outcome is recomputed at the real line and CLV is measured from it.
    """
    grades = []
    monkeypatch.setattr(line_alerts, "_append_grade_history",
                        lambda _db, alert_id, grade, outcome=None: grades.append((alert_id, grade, outcome)))
    db = _RegradeDb(_legacy_row({
        "market": "spread", "trigger_line": -2.83, "exec_line": 3.0,
        "exec_book": "draftkings", "exec_decimal": 1.91,
    }))
    assert line_alerts._settle_football_line_alerts(db, "nfl", regrade=True) == 1
    sql, params = db.queries[0]
    assert "grading_version IS DISTINCT FROM %s" in sql, "regrade must be version-gated"
    assert "NOT EXISTS (SELECT 1 FROM alert_grades" in sql, "regrade must self-heal a missing grade row"
    assert params == ("nfl", line_alerts._NFL_GRADING_VERSION, line_alerts._NFL_GRADING_VERSION)
    update_params = db.updates[-1][1]
    grading = json.loads(update_params[1])
    assert grading["entry_home_line"] == -3.0
    assert grading["entry_line_basis"] == "exec_derived"
    # Close from the fixture is -4.0 home; away side gained a point: CLV +1? No:
    # entry -3 (home) vs close -4 (home) means home moved further, so the AWAY
    # bettor's number got WORSE by one point.
    assert grading["line_clv"] == pytest.approx(-1.0)
    assert grading["line_clv_basis"] == "exec_line_vs_verified_close"
    # Home won by 4 at a home line of -3 -> away loses (home covers).
    assert update_params[0] == "lost"
    assert update_params[3] == line_alerts._NFL_GRADING_VERSION
    assert grades[0][1]["grading_version"] == line_alerts._NFL_GRADING_VERSION


def test_consensus_only_row_keeps_outcome_but_withholds_line_clv(monkeypatch) -> None:
    """No exec_line at all: the outcome stays graded at the mean (as v1 did)
    but line CLV is None with an explicit basis, never a number."""
    monkeypatch.setattr(line_alerts, "_append_grade_history", lambda *a, **k: None)
    db = _RegradeDb(_legacy_row({"market": "total", "trigger_line": 43.667}, side="over"))
    assert line_alerts._settle_football_line_alerts(db, "nfl", regrade=True) == 1
    grading = json.loads(db.updates[-1][1][1])
    assert grading["entry_line_basis"] == "consensus_mean"
    assert grading["line_clv"] is None
    assert grading["line_clv_basis"] == "withheld_consensus_entry"
    assert db.updates[-1][1][0] == "won"          # 44 > 43.667


def test_regrade_does_not_skip_a_settled_row_without_a_close(monkeypatch) -> None:
    """Normal settlement skips settled rows lacking a close (nothing to enrich).
    A regrade must still re-stamp them, with CLV honestly None."""
    monkeypatch.setattr(line_alerts, "_append_grade_history", lambda *a, **k: None)
    db = _RegradeDb(_legacy_row({"market": "spread", "trigger_line": -3.0, "exec_line": -3.0,
                                 "entry_home_line": -3.0}, side="home"))
    monkeypatch.setattr(db, "execute_one", lambda *_: None)
    assert line_alerts._settle_football_line_alerts(db, "nfl") == 0
    assert line_alerts._settle_football_line_alerts(db, "nfl", regrade=True) == 1
    grading = json.loads(db.updates[-1][1][1])
    assert grading["entry_line_basis"] == "frozen"
    assert grading["line_clv"] is None
    assert grading["line_clv_basis"] == "unavailable_no_close"
    assert grading["close_source"] == "unavailable"


def test_fresh_settlement_stamps_v2_and_cfb_is_untouched(monkeypatch) -> None:
    monkeypatch.setattr(line_alerts, "_append_grade_history", lambda *a, **k: None)
    db = SettlementDb()
    assert line_alerts._settle_football_line_alerts(db, "nfl") == 1
    assert db.updates[-1][1][3] == "nfl-lines-v2"
    db = SettlementDb()
    assert line_alerts._settle_football_line_alerts(db, "cfb") == 1
    assert db.updates[-1][1][3] == line_alerts._CFB_SIGNAL_VERSION


def test_fade_study_exclusion_targets_only_the_sealed_population() -> None:
    pred = line_alerts._FADE_STUDY_EXCLUSION
    assert "a.sport = 'nfl'" in pred and "'total_walking'" in pred
    assert "season_type = 'regular'" in pred and "2026-09-09" in pred
    # Preseason discovery rows and CFB rows are NOT excluded by construction.
    assert "cfb" not in pred and "preseason" not in pred
