"""Explicitly void a pre-match tennis scratch after manual verification.

This command is intentionally manual: no current provider supplies reliable
pre-match withdrawal data. It refuses to alter a started or settled match.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager


def void_pre_match_withdrawal(db: DatabaseManager, match_id: int, reason: str) -> dict[str, int]:
    """Record a verified pre-start withdrawal and void open ledger rows."""
    match = db.execute_one(
        """SELECT id, commence_time, winner FROM tennis_matches WHERE id=%s""", (match_id,)
    )
    if not match:
        raise ValueError(f"Tennis match {match_id} was not found")
    if match["winner"] is not None:
        raise ValueError(f"Tennis match {match_id} already has a settled result")
    commence = match["commence_time"]
    if commence is not None and commence <= datetime.now(timezone.utc):
        raise ValueError("Refusing to void a match at or after first serve")

    db.execute(
        """UPDATE tennis_matches
           SET completion_status='walkover', retired=FALSE, walkover=TRUE,
               result_source='manual_withdrawal', result_comment=%s
           WHERE id=%s""",
        (reason, match_id),
    )
    bets = db.execute(
        """UPDATE tennis_bets SET status='void', result_detail=%s, settled_at=NOW()
           WHERE match_id=%s AND status='pending' RETURNING id""",
        (f"Pre-match withdrawal: {reason}", match_id),
    )
    alerts = db.execute(
        """UPDATE line_alerts
           SET outcome='void', settled_at=NOW(),
               details_json=details_json || jsonb_build_object('void_reason', %s)
           WHERE sport='tennis' AND matchup_id=%s AND settled_at IS NULL
           RETURNING id""",
        (f"Pre-match withdrawal: {reason}", match_id),
    )
    return {"bets_voided": len(bets), "alerts_voided": len(alerts)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Void a manually verified pre-match tennis withdrawal")
    parser.add_argument("--match-id", type=int, required=True, help="tennis_matches.id")
    parser.add_argument("--reason", required=True, help="Verified withdrawal source and detail")
    args = parser.parse_args()

    result = void_pre_match_withdrawal(
        DatabaseManager(load_config().database_url), args.match_id, args.reason
    )
    print(f"Tennis withdrawal voided: {result['bets_voided']} bets, {result['alerts_voided']} alerts")
