"""Produce the read-only revision-3 CFB market-context Phase 0 baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from config import PROJECT_DIR, load_config
from db.database import DatabaseManager
from model.cfb_context_economics import resolve_legacy_economics, summarize_resolutions


ARCHITECTURE_VERSION = "cfb-market-context-v2"
CONTRACT_REVISION = 3
SELECTED_CFBD_DEFINITION = {
    "definition_id": "cfb_offensive_drive_volume",
    "definition_version": 1,
    "display_name": "Trailing completed-game offensive drive volume",
    "source": "CollegeFootballData drives through cfb_drives",
    "grain": "team-game",
    "unit": "offensive_drives_per_game",
    "calculation": (
        "Mean count of distinct CFBD drive IDs for the subject offense over its latest four "
        "eligible completed FBS-v-FBS games whose stored drive observations were ingested "
        "no later than the consumer as-of boundary."
    ),
    "eligibility": {
        "games": "completed regular/postseason FBS-v-FBS; known canonical teams and kickoff",
        "drives": "non-null cfbd_drive_id and offense_team_id linked to the canonical game",
        "history_window_games": 4,
    },
    "missingness": "No imputation; publish missing when zero eligible games and partial below four.",
    "temporal_rule": (
        "Existing backfill supports current descriptive use and future prospective forecasts only; "
        "it cannot reconstruct historical availability before ingested_at."
    ),
    "initial_consumers": ["cfb-shadow-study", "cfb-terminal", "cfb-postgame-export"],
    "decision_permission": "denied",
}
PROVIDER_EVIDENCE_POLICIES = [
    {
        "provider": "collegefootballdata",
        "policy_version": 1,
        "retention_mode": "unknown",
        "terms_artifact": None,
        "approved_by": None,
        "scope": ["games", "drives", "plays", "rosters", "returning-production", "portal", "talent", "coaches"],
        "permitted_representations": [],
        "blocker": "Account-specific agreement and retention/redistribution terms require human review.",
    },
    {
        "provider": "the-odds-api",
        "policy_version": 1,
        "retention_mode": "unknown",
        "terms_artifact": None,
        "approved_by": None,
        "scope": ["events", "bookmaker-quotes", "scores"],
        "permitted_representations": [],
        "blocker": "Account-specific agreement and normalized/raw quote retention terms require human review.",
    },
]


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def _query_alert_inputs(db: DatabaseManager) -> list[tuple[dict, list[dict]]]:
    alerts = db.execute(
        """SELECT id, alert_type, signal_version, origin, game_date, matchup_id,
                  created_at, outcome, pnl_units, details_json
           FROM line_alerts WHERE sport='cfb' ORDER BY id"""
    )
    grades = db.execute(
        """SELECT id, alert_id, outcome, pnl_units, grading_json, graded_at
           FROM alert_grades
           WHERE is_current=TRUE AND alert_id IN
             (SELECT id FROM line_alerts WHERE sport='cfb')
           ORDER BY alert_id,id"""
    )
    by_alert: dict[int, list[dict]] = {}
    for grade in grades:
        by_alert.setdefault(int(grade["alert_id"]), []).append(dict(grade))
    return [(dict(alert), by_alert.get(int(alert["id"]), [])) for alert in alerts]


def _coverage(db: DatabaseManager) -> dict:
    return {
        "canonical_games": db.execute_one("SELECT COUNT(*) n FROM cfb_matchups")["n"],
        "drive_rows": db.execute_one("SELECT COUNT(*) n FROM cfb_drives")["n"],
        "play_rows": db.execute_one("SELECT COUNT(*) n FROM cfb_plays")["n"],
        "roster_snapshots": db.execute_one("SELECT COUNT(*) n FROM cfb_roster_snapshots")["n"],
        "odds_captures": db.execute_one(
            "SELECT COUNT(*) n FROM game_odds_history WHERE sport='cfb'"
        )["n"],
        "verified_closes": db.execute_one(
            "SELECT COUNT(*) n FROM verified_clv_closes WHERE sport='cfb'"
        )["n"],
        "drive_games": db.execute_one(
            "SELECT COUNT(DISTINCT game_id) n FROM cfb_drives"
        )["n"],
        "drive_rows_with_team_identity": db.execute_one(
            "SELECT COUNT(*) n FROM cfb_drives WHERE offense_team_id IS NOT NULL AND defense_team_id IS NOT NULL"
        )["n"],
    }


def build_phase0_report(db: DatabaseManager, *, generated_at: datetime | None = None) -> dict:
    generated_at = generated_at or datetime.now(timezone.utc)
    inputs = _query_alert_inputs(db)
    resolved = [(alert, resolve_legacy_economics(alert, grades)) for alert, grades in inputs]
    ledger_rows = [{
        "alert_id": int(alert["id"]),
        "alert_type": alert["alert_type"],
        "signal_version": alert.get("signal_version"),
        "origin": alert.get("origin"),
        "game_date": str(alert.get("game_date")),
        "resolution": resolution.to_dict(),
    } for alert, resolution in resolved]
    ledger_digest = hashlib.sha256(_canonical_json(ledger_rows)).hexdigest()
    feature = dict(SELECTED_CFBD_DEFINITION)
    feature["configuration_digest"] = hashlib.sha256(_canonical_json(feature)).hexdigest()
    return {
        "architecture_version": ARCHITECTURE_VERSION,
        "contract_revision": CONTRACT_REVISION,
        "generated_at": generated_at.isoformat(),
        "mode": "read_only_baseline",
        "ledger_digest": ledger_digest,
        "ledger_rows": len(ledger_rows),
        "canonical_economics": summarize_resolutions(resolved),
        "coverage": _coverage(db),
        "selected_cfbd_feature": feature,
        "provider_evidence_policies": PROVIDER_EVIDENCE_POLICIES,
        "source_inventory": [
            {"source": "cfb_matchups", "state": "used", "role": "canonical schedule/results identity"},
            {"source": "cfb_drives", "state": "selected_vertical_slice", "role": "offensive drive volume"},
            {"source": "cfb_plays", "state": "stored_unused", "role": "future pace/field-position definitions require separate freeze"},
            {"source": "cfb_roster_snapshots", "state": "stored_unused", "role": "membership only; no inferred availability/depth"},
            {"source": "CFBD returning-production captures", "state": "stored_unused", "role": "source-defined production context"},
            {"source": "CFBD transfer/portal captures", "state": "stored_unused", "role": "point-in-time transfer context"},
            {"source": "CFBD talent captures", "state": "stored_unused", "role": "talent context"},
            {"source": "CFBD coach captures", "state": "stored_unused", "role": "supported coaching continuity only"},
            {"source": "game_odds_history", "state": "used_legacy", "role": "prospective market captures; normalized quote migration pending"},
        ],
        "consumer_inventory": [
            {"consumer_id": "cfb-terminal", "path": "web/src/app/cfb", "permission": "descriptive"},
            {"consumer_id": "cfb-shadow-study", "path": "research", "permission": "shadow-predictive"},
            {"consumer_id": "cfb-postgame-export", "path": "not_yet_implemented", "permission": "descriptive"},
            {"consumer_id": "pickem", "path": "web/src/app/pickem", "permission": "decision-denied"},
            {"consumer_id": "survivor", "path": "web/src/app/survivor", "permission": "decision-denied"},
        ],
        "limitations": [
            "No engine migration has been applied.",
            "Legacy current-grade selection is a migration-time baseline, not reconstruction of older reports.",
            "Provider evidence entitlement remains unresolved until a reviewed Phase 0 policy artifact is recorded.",
            "Historical CFBD play/drive backfill is not assumed available at its event time.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path,
        default=PROJECT_DIR / "artifacts" / "cfb_market_context_phase0_rev3.json",
    )
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    report = build_phase0_report(db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "ledger_digest": report["ledger_digest"],
        "ledger_rows": report["ledger_rows"],
    }, indent=2))


if __name__ == "__main__":
    main()
