"""Coordinate only routine MLB movement purchases; leave other collectors intact.

The existing schedule CLI opts in for its movement job. A session advisory
lock spans the freshness decision and purchase so bridge/fallback invocations
cannot race. Checkpoint and full-refresh observations can satisfy freshness.
"""
from __future__ import annotations

import json
import os

BOOKMAKERS = "draftkings,fanduel,betmgm,betrivers,pinnacle,fanatics,williamhill_us,bovada,betonlineag,polymarket"
POLICY = "mlb-movement-ten-books-v1"


def capture_movement(db, api_key, game_date):
    from ingest.mlb_schedule import fetch_odds

    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS acquired", ("mlb_routine_movement_capture",))
        if not cursor.fetchone()["acquired"]:
            return 0
        try:
            row = db.execute_one("""SELECT COUNT(*)::int AS upcoming,
                COUNT(*) FILTER (WHERE NOT EXISTS (
                  SELECT 1 FROM game_odds_history h WHERE h.sport='mlb' AND h.matchup_id=m.id
                    AND h.game_date=m.game_date AND h.captured_at>NOW()-INTERVAL '20 minutes'
                    AND h.captured_at<m.commence_time
                    AND EXISTS (SELECT 1 FROM jsonb_each(h.books) b
                      WHERE b.key IN ('draftkings','fanduel','betmgm','betrivers')
                        AND b.value ? 'ml_home' AND b.value ? 'ml_away'
                        AND b.value ? 'total_line' AND b.value ? 'spread_home')
                ))::int AS missing
                FROM mlb_matchups m WHERE m.game_date=%s AND m.commence_time>NOW()
                  AND COALESCE(m.game_status,'') NOT IN ('Postponed','Cancelled')""", (game_date,)) or {}
            if not row.get("upcoming") or not row.get("missing"):
                return 0
            # Global remaining quota is read only. This does not change another sport's allocation.
            budget = db.execute_one("""SELECT
              (SELECT requests_remaining FROM odds_api_usage WHERE requests_remaining IS NOT NULL
               ORDER BY requested_at DESC,id DESC LIMIT 1) AS remaining,
              COALESCE(SUM(requests_last),0)::int AS spent
              FROM odds_api_usage WHERE sport='mlb' AND purpose='routine_movement'
                AND requested_at>=date_trunc('day',NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'""") or {}
            if (budget.get("remaining") is not None and budget["remaining"] < 253) or budget.get("spent", 0) + 3 > int(os.getenv("MLB_MOVEMENT_DAILY_CREDIT_CAP", "120")):
                print("MLB routine capture skipped: quota reserved for closing checkpoints")
                return 0
            audit = {}
            try:
                return fetch_odds(db, api_key, game_date, bookmakers=BOOKMAKERS, request_audit=audit, capture_policy=POLICY)
            finally:
                if audit:
                    def integer(key):
                        try:
                            return int(audit.get(key))
                        except (ValueError, TypeError):
                            return None
                    db.execute("""INSERT INTO odds_api_usage
                        (sport,purpose,endpoint,event_count,markets,bookmakers,requests_last,requests_used,
                         requests_remaining,response_status,metadata)
                        VALUES ('mlb','routine_movement',%s,%s,'h2h,totals,spreads',%s,%s,%s,%s,%s,%s::jsonb)""",
                        (audit.get("endpoint", "unknown"), row["upcoming"], BOOKMAKERS,
                         integer("requests_last"), integer("requests_used"), integer("requests_remaining"),
                         integer("status"), json.dumps({"capture_policy": POLICY, "game_date": game_date})))
        finally:
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("mlb_routine_movement_capture",))
