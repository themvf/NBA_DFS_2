"""Tennis settlement invariant repair and operational health gate."""
from __future__ import annotations

import argparse

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_result_settlement import (
    replay_pending_moneylines,
    replay_tennis_alert_outcomes,
)


def reconciliation_report(
    db: DatabaseManager, *, max_stale_hours: int = 72,
    provider_freshness_minutes: int = 45, repair: bool = True,
) -> tuple[dict[str, int], bool]:
    repaired = replay_pending_moneylines(db) if repair else 0
    repaired_alerts = replay_tennis_alert_outcomes(db) if repair else 0
    pending_after_result = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_bets tb
           JOIN tennis_matches tm ON tm.id=tb.match_id
           WHERE tb.bet_type='moneyline' AND tb.status='pending'
             AND tm.winner IN ('home','away')"""
    )["n"]
    settled_without_result = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_bets tb
           JOIN tennis_matches tm ON tm.id=tb.match_id
           WHERE tb.bet_type='moneyline' AND tb.status IN ('won','lost')
             AND tm.winner IS NULL"""
    )["n"]
    projection_mismatch = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_matches tm
           JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
           WHERE (tr.state='resolved' AND tm.winner IS DISTINCT FROM tr.winner_side)
              OR (tr.state='void' AND (tm.winner IS NOT NULL
                  OR tm.completion_status NOT IN ('walkover','cancelled')))"""
    )["n"]
    moneyline_outcome_mismatch = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_bets tb
           JOIN tennis_matches tm ON tm.id=tb.match_id
           JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
           WHERE tb.bet_type='moneyline' AND (
             (tr.state='resolved' AND tm.winner IN ('home','away')
               AND tb.status IN ('pending','won','lost')
               AND tb.status IS DISTINCT FROM
                   CASE WHEN tb.side=tm.winner THEN 'won' ELSE 'lost' END)
             OR (tr.state='void' AND tb.status IN ('pending','won','lost'))
           )"""
    )["n"]
    alert_outcome_mismatch = db.execute_one(
        """SELECT COUNT(*) AS n FROM line_alerts la
           JOIN tennis_matches tm ON tm.id=la.matchup_id
           JOIN tennis_result_resolutions tr ON tr.id=tm.result_resolution_id
           WHERE la.sport='tennis' AND (
             (la.alert_type IN ('pinnacle_divergence','pinnacle_polymarket_delta',
                                'steam','dk_value','walking')
               AND ((tr.state='resolved' AND tm.winner IN ('home','away')
                     AND la.outcome IS DISTINCT FROM
                         CASE WHEN la.side=tm.winner THEN 'won' ELSE 'lost' END)
                    OR (tr.state='void' AND la.outcome IS DISTINCT FROM 'void')))
             OR (la.alert_type IN ('dk_prop_value','prop_line_gap')
                 AND (tr.state='void'
                      OR tm.completion_status IN ('retired','walkover','awarded'))
                 AND la.outcome IS DISTINCT FROM 'void')
           )"""
    )["n"]
    alert_grade_mismatch = db.execute_one(
        """SELECT COUNT(*) AS n FROM line_alerts la
           WHERE la.sport='tennis' AND la.outcome IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM alert_grades ag
               WHERE ag.alert_id=la.id AND ag.is_current
                 AND ag.outcome IS NOT DISTINCT FROM la.outcome
             )"""
    )["n"]
    stale = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_matches tm
           WHERE tm.winner IS NULL AND tm.completion_status='scheduled'
             AND COALESCE(tm.commence_time, tm.match_date::timestamp) <
                 NOW() - (%s * INTERVAL '1 hour')""",
        (max_stale_hours,),
    )["n"]
    healthy_runs = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_provider_runs
           WHERE finished_at >= NOW() - (%s * INTERVAL '1 minute')
             AND status IN ('success','empty')""",
        (provider_freshness_minutes,),
    )["n"]
    stale_running_provider_runs = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_provider_runs
           WHERE status='running' AND finished_at IS NULL
             AND started_at < NOW() - (%s * INTERVAL '1 minute')""",
        (provider_freshness_minutes,),
    )["n"]
    open_disputes = db.execute_one(
        """SELECT COUNT(*) AS n FROM tennis_result_resolutions tr
           WHERE tr.state='disputed' AND NOT EXISTS (
             SELECT 1 FROM tennis_result_resolutions newer
             WHERE newer.match_id=tr.match_id AND newer.created_at > tr.created_at
               AND newer.state IN ('resolved','void'))"""
    )["n"]

    metrics = {
        "repaired_moneyline_bets": int(repaired),
        "repaired_alert_outcomes": int(repaired_alerts),
        "pending_after_result": int(pending_after_result),
        "settled_without_result": int(settled_without_result),
        "projection_mismatch": int(projection_mismatch),
        "moneyline_outcome_mismatch": int(moneyline_outcome_mismatch),
        "alert_outcome_mismatch": int(alert_outcome_mismatch),
        "alert_grade_mismatch": int(alert_grade_mismatch),
        "stale_unresolved_matches": int(stale),
        "fresh_healthy_provider_runs": int(healthy_runs),
        "stale_running_provider_runs": int(stale_running_provider_runs),
        "open_disputes": int(open_disputes),
    }
    healthy = (
        pending_after_result == 0
        and settled_without_result == 0
        and projection_mismatch == 0
        and moneyline_outcome_mismatch == 0
        and alert_outcome_mismatch == 0
        and alert_grade_mismatch == 0
        and stale == 0
        and healthy_runs > 0
        and stale_running_provider_runs == 0
        and open_disputes == 0
    )
    return metrics, healthy


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair and gate Tennis settlement health")
    parser.add_argument("--max-stale-hours", type=int, default=72)
    parser.add_argument("--provider-freshness-minutes", type=int, default=45)
    parser.add_argument("--no-repair", action="store_true")
    parser.add_argument("--fail-on-unhealthy", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url)
    metrics, healthy = reconciliation_report(
        db, max_stale_hours=args.max_stale_hours,
        provider_freshness_minutes=args.provider_freshness_minutes,
        repair=not args.no_repair,
    )
    print("Tennis settlement health:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    print(f"  status: {'healthy' if healthy else 'unhealthy'}")
    return 1 if args.fail_on_unhealthy and not healthy else 0


if __name__ == "__main__":
    raise SystemExit(main())
