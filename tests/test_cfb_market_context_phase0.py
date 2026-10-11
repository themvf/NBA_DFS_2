from datetime import datetime, timezone

from research.cfb_market_context_phase0 import (
    CONTRACT_REVISION,
    SELECTED_CFBD_DEFINITION,
    build_phase0_report,
)


class FakeDb:
    def execute(self, sql, params=None):
        if "FROM line_alerts" in sql and "SELECT id, alert_type" in sql:
            return [{
                "id": 11, "alert_type": "dk_value", "signal_version": "cfb-lines-v1",
                "origin": "prospective", "game_date": "2026-09-05", "matchup_id": 9,
                "created_at": datetime(2026, 9, 5, tzinfo=timezone.utc),
                "outcome": "lost", "pnl_units": None,
                "details_json": {"exec_decimal": 8.5, "dk_decimal": 8.5},
            }]
        if "FROM alert_grades" in sql:
            return [{
                "id": 21, "alert_id": 11, "outcome": "lost", "pnl_units": None,
                "grading_json": {}, "graded_at": datetime(2026, 9, 6, tzinfo=timezone.utc),
            }]
        return []

    def execute_one(self, sql, params=None):
        return {"n": 5}


def test_phase0_report_is_pinned_and_exposes_hidden_economics():
    report = build_phase0_report(
        FakeDb(), generated_at=datetime(2026, 9, 23, tzinfo=timezone.utc),
    )
    assert report["contract_revision"] == CONTRACT_REVISION == 3
    assert len(report["ledger_digest"]) == 64
    assert report["canonical_economics"][0]["pnl_units"] == "-1"
    assert report["canonical_economics"][0]["roi"] == "-1"
    assert report["selected_cfbd_feature"]["definition_id"] == SELECTED_CFBD_DEFINITION["definition_id"]
    assert report["selected_cfbd_feature"]["decision_permission"] == "denied"
