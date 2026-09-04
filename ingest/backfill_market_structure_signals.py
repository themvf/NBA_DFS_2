"""Replay newly deployed market-structure detectors over completed CFB/Tennis events.

No API calls are made. Rows are explicitly ``origin=retrospective`` and are
kept separate from prospective detector evaluation.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor

from config import load_config
from model.line_alerts import (
    _book_fair_side,
    _freeze_game_price,
    _moneyline_structure_signals,
    _retail_fair_side,
)


ALLOWED = {
    "tennis": {"reversal", "reference_led", "price_pressure", "book_disagreement",
               "market_convergence", "late_move", "favorite_flip"},
    "cfb": {"book_disagreement", "market_convergence", "late_move"},
}
MATCHUPS = {"tennis": "tennis_matches", "cfb": "cfb_matchups"}


def replay(sport: str, apply: bool = False) -> dict:
    table = MATCHUPS[sport]
    with psycopg2.connect(load_config().database_url, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT m.id AS matchup_id, m.match_date AS game_date,
                       m.home_player AS home_name, m.away_player AS away_name,
                       m.commence_time, h.id AS history_id, h.captured_at,
                       h.capture_key, h.books
                FROM {table} m JOIN game_odds_history h
                  ON h.matchup_id=m.id AND h.sport=%s
                WHERE m.commence_time <= NOW() AND h.captured_at < m.commence_time
                  AND h.books IS NOT NULL
                  AND EXISTS (SELECT 1 FROM jsonb_object_keys(h.books) source(book_key)
                              WHERE source.book_key <> 'polymarket')
                ORDER BY m.id, h.captured_at, h.id
            """ if sport == "tennis" else f"""
                SELECT m.id AS matchup_id, m.game_date,
                       ht.name AS home_name, at.name AS away_name,
                       m.commence_time, h.id AS history_id, h.captured_at,
                       h.capture_key, h.books
                FROM {table} m
                JOIN cfb_teams ht ON ht.team_id=m.home_team_id
                JOIN cfb_teams at ON at.team_id=m.away_team_id
                JOIN game_odds_history h ON h.matchup_id=m.id AND h.sport=%s
                WHERE m.commence_time <= NOW() AND h.captured_at < m.commence_time
                  AND h.books IS NOT NULL
                  AND EXISTS (SELECT 1 FROM jsonb_object_keys(h.books) source(book_key)
                              WHERE source.book_key <> 'polymarket')
                ORDER BY m.id, h.captured_at, h.id
            """, (sport,))
            grouped = defaultdict(list)
            metadata = {}
            for row in cur.fetchall():
                grouped[row["matchup_id"]].append(row)
                metadata[row["matchup_id"]] = row

            candidates = []
            for matchup_id, rows in grouped.items():
                seen = set()
                for end in range(2, len(rows) + 1):
                    prefix = rows[:end]
                    for signal in _moneyline_structure_signals(
                            prefix, metadata[matchup_id]["commence_time"],
                            include_favorite_flip=sport == "tennis"):
                        key = (signal["alert_type"], signal["side"])
                        if signal["alert_type"] not in ALLOWED[sport] or key in seen:
                            continue
                        seen.add(key)
                        trigger = prefix[-1]
                        books = trigger["books"] or {}
                        side = signal["side"]
                        details = {
                            **signal["details"],
                            "origin": "retrospective",
                            "evaluation_arm": "retrospective_only",
                            **_freeze_game_price(sport, books, market="moneyline", side=side),
                        }
                        pin = books.get("pinnacle")
                        candidates.append({
                            "sport": sport, "matchup_id": matchup_id,
                            "game_date": metadata[matchup_id]["game_date"],
                            "matchup": f"{metadata[matchup_id]['away_name']} @ {metadata[matchup_id]['home_name']}",
                            "commence_time": metadata[matchup_id]["commence_time"],
                            "alert_type": signal["alert_type"], "side": side,
                            "capture_key": trigger["capture_key"],
                            "alert_prob": _retail_fair_side(books, side),
                            "sharp_prob": _book_fair_side(pin, side) if pin else None,
                            "details": details, "trigger_history_id": trigger["history_id"],
                            "previous_history_id": signal["details"].get("previous_history_id"),
                            "opening_history_id": signal["details"].get("opening_history_id"),
                        })

            inserted = 0
            if apply:
                for row in candidates:
                    cur.execute("""
                        INSERT INTO line_alerts
                          (created_at,sport,matchup_id,game_date,matchup,commence_time,
                           alert_type,side,capture_key,alert_prob,sharp_prob,details_json,
                           signal_version,origin,trigger_history_id,previous_history_id,
                           opening_history_id,dedupe_key)
                        VALUES (NOW(),%(sport)s,%(matchup_id)s,%(game_date)s,%(matchup)s,
                          %(commence_time)s,%(alert_type)s,%(side)s,%(capture_key)s,
                          %(alert_prob)s,%(sharp_prob)s,%(details_json)s,
                          'market-structure-v1','retrospective',%(trigger_history_id)s,
                          %(previous_history_id)s,%(opening_history_id)s,%(dedupe_key)s)
                        ON CONFLICT (sport,matchup_id,alert_type,side) DO NOTHING
                    """, {**row, "details_json": json.dumps(row["details"]),
                           "dedupe_key": f"retrospective:market-structure-v1:{row['alert_type']}:{row['side']}"})
                    inserted += cur.rowcount
            if not apply:
                conn.rollback()
            return {"sport": sport, "events": len(grouped),
                    "candidates": len(candidates), "by_type": dict(sorted(Counter(
                        row["alert_type"] for row in candidates).items())),
                    "inserted": inserted, "apply": apply}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", choices=("tennis", "cfb", "all"), default="all")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    sports = ("tennis", "cfb") if args.sport == "all" else (args.sport,)
    for sport in sports:
        print(json.dumps(replay(sport, apply=args.apply), sort_keys=True))


if __name__ == "__main__":
    main()
