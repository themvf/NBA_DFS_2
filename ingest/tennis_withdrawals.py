"""Explicitly void a pre-match tennis scratch after manual verification.

This command is intentionally manual: no current provider supplies reliable
pre-match withdrawal data. It refuses to alter a started or settled match.
"""

from __future__ import annotations

import argparse
import os

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_result_settlement import ResultObservation, record_observation_and_settle


def void_pre_match_withdrawal(
    db: DatabaseManager, match_id: int, reason: str, *, actor: str, evidence_url: str,
) -> dict[str, int]:
    """Append a verified withdrawal and atomically void open ledger rows."""
    result = record_observation_and_settle(
        db,
        ResultObservation(
            match_id=match_id,
            provider="manual_withdrawal",
            winner_side=None,
            completion_status="walkover",
            status_evidence=True,
            parser_version="manual-withdrawal-v2",
            raw_payload={"reason": reason, "actor": actor, "evidence_url": evidence_url},
            match_method="manual_verified",
            match_confidence=1.0,
            actor=actor,
            reason=reason,
            evidence_url=evidence_url,
        ),
        require_prestart=True,
    )
    return {"bets_voided": int(result["bets"]), "alerts_voided": int(result["alerts"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Void a manually verified pre-match tennis withdrawal")
    parser.add_argument("--match-id", type=int, required=True, help="tennis_matches.id")
    parser.add_argument("--reason", required=True, help="Verified withdrawal detail")
    parser.add_argument("--evidence-url", required=True, help="Official source URL")
    parser.add_argument(
        "--actor", default=os.getenv("GITHUB_ACTOR") or os.getenv("USERNAME") or "unknown",
        help="Operator identity (defaults to GITHUB_ACTOR/USERNAME)",
    )
    args = parser.parse_args()

    result = void_pre_match_withdrawal(
        DatabaseManager(load_config().database_url), args.match_id, args.reason,
        actor=args.actor, evidence_url=args.evidence_url,
    )
    print(f"Tennis withdrawal voided: {result['bets_voided']} bets, {result['alerts_voided']} alerts")
