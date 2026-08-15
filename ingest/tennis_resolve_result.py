"""Audited manual Tennis result resolution and correction CLI."""
from __future__ import annotations

import argparse
import os
from datetime import date

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_result_settlement import ResultObservation, record_observation_and_settle


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish a verified Tennis result")
    parser.add_argument("--match-id", type=int, required=True)
    parser.add_argument(
        "--winner", choices=("home", "away"),
        help="Advancing player; omit only for a verified no-contest walkover/cancellation",
    )
    parser.add_argument(
        "--completion-status",
        choices=("completed", "retired", "walkover", "awarded", "cancelled"), required=True,
    )
    parser.add_argument("--home-sets", type=int)
    parser.add_argument("--away-sets", type=int)
    parser.add_argument("--home-games", type=int)
    parser.add_argument("--away-games", type=int)
    parser.add_argument("--result-date", type=date.fromisoformat)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--evidence-url", required=True)
    parser.add_argument(
        "--actor", default=os.getenv("GITHUB_ACTOR") or os.getenv("USERNAME"), required=False,
    )
    parser.add_argument(
        "--correct-published-result", action="store_true",
        help="Explicitly authorize a compensating correction of a published winner",
    )
    args = parser.parse_args()
    if not args.actor:
        parser.error("--actor is required when GITHUB_ACTOR/USERNAME is unavailable")
    if args.winner is None and args.completion_status not in {"walkover", "cancelled"}:
        parser.error("--winner is required unless resolving a walkover/cancellation as void")

    result = record_observation_and_settle(
        DatabaseManager(load_config().database_url),
        ResultObservation(
            match_id=args.match_id,
            provider="manual_resolution",
            winner_side=args.winner,
            completion_status=args.completion_status,
            status_evidence=True,
            observed_match_date=args.result_date,
            home_sets=args.home_sets,
            away_sets=args.away_sets,
            home_games=args.home_games,
            away_games=args.away_games,
            source_url=args.evidence_url,
            parser_version="manual-resolution-v1",
            raw_payload=vars(args),
            match_method="manual_verified",
            match_confidence=1.0,
            actor=args.actor,
            reason=args.reason,
            evidence_url=args.evidence_url,
        ),
        allow_correction=args.correct_published_result,
    )
    print(
        f"Tennis resolution {result['resolution_id']}: {result['state']}; "
        f"{result['bets']} bets and {result['alerts']} alerts changed"
    )
    return 0 if result["state"] in {"resolved", "void"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
