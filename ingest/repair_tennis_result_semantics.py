"""Correct tennis-data retirement/walkover/awarded semantics immutably."""

from __future__ import annotations

import json

from config import load_config
from db.database import DatabaseManager

TRANSFORMATION_VERSION = "tennis-result-semantics-v1"


def _status(comment: str | None) -> tuple[str, bool, bool]:
    normalized = str(comment or "").strip().lower()
    if "walkover" in normalized:
        return "walkover", False, True
    if "retir" in normalized:
        return "retired", True, False
    if "awarded" in normalized:
        return "awarded", False, False
    return "completed", False, False


def run(db: DatabaseManager) -> dict:
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM tennis_historical_matches
            WHERE source='tennis_data' AND is_current
              AND COALESCE(raw_payload->'source_row'->>'Comment','Completed') <> 'Completed'
            ORDER BY id
            """
        )
        rows = cur.fetchall()
        counts = {"retired": 0, "walkover": 0, "awarded": 0}
        corrected = 0
        for row in rows:
            completion, retired, walkover = _status(row["raw_payload"]["source_row"].get("Comment"))
            if completion == row["completion_status"] and retired == row["retired"] and walkover == row["walkover"]:
                continue
            cur.execute(
                "UPDATE tennis_historical_matches SET is_current=FALSE, superseded_at=NOW() WHERE id=%s",
                (row["id"],),
            )
            cur.execute(
                """
                INSERT INTO tennis_historical_matches (
                    source, source_match_key, source_partition_id, tour, season,
                    match_date, start_time, tournament, round, best_of, surface, indoor,
                    winner_player_id, loser_player_id, score, completion_status,
                    retired, walkover, winner_rank, loser_rank, winner_rank_points,
                    loser_rank_points, winner_decimal_odds, loser_decimal_odds,
                    odds_source, odds_timing, source_available_at, stats_through_at,
                    captured_at, transformation_version, raw_checksum, raw_payload,
                    correction_of_id, is_current
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,TRUE
                )
                ON CONFLICT (source, source_match_key, raw_checksum, transformation_version)
                DO UPDATE SET is_current=TRUE, superseded_at=NULL
                RETURNING id
                """,
                (row["source"], row["source_match_key"], row["source_partition_id"], row["tour"],
                 row["season"], row["match_date"], row["start_time"], row["tournament"],
                 row["round"], row["best_of"], row["surface"], row["indoor"],
                 row["winner_player_id"], row["loser_player_id"], row["score"], completion,
                 retired, walkover, row["winner_rank"], row["loser_rank"],
                 row["winner_rank_points"], row["loser_rank_points"],
                 row["winner_decimal_odds"], row["loser_decimal_odds"], row["odds_source"],
                 row["odds_timing"], row["source_available_at"], row["stats_through_at"],
                 row["captured_at"], TRANSFORMATION_VERSION, row["raw_checksum"],
                 json.dumps(row["raw_payload"]), row["id"]),
            )
            new_match_id = cur.fetchone()["id"]
            cur.execute(
                """
                INSERT INTO tennis_player_match_stats (
                    historical_match_id,player_id,opponent_player_id,is_winner,
                    aces,double_faults,serve_points,first_serves_in,
                    first_serve_points_won,second_serve_points_won,service_games,
                    break_points_saved,break_points_faced,serve_points_won_pct,
                    return_points_won_pct,stats_available,missing_reason,
                    formula_version,sample_size
                )
                SELECT %s,player_id,opponent_player_id,is_winner,
                       aces,double_faults,serve_points,first_serves_in,
                       first_serve_points_won,second_serve_points_won,service_games,
                       break_points_saved,break_points_faced,serve_points_won_pct,
                       return_points_won_pct,stats_available,missing_reason,
                       formula_version,sample_size
                FROM tennis_player_match_stats WHERE historical_match_id=%s
                ON CONFLICT (historical_match_id,player_id) DO NOTHING
                """,
                (new_match_id, row["id"]),
            )
            counts[completion] += 1
            corrected += 1

        if corrected:
            cur.execute(
                """
                UPDATE tennis_elo_runs
                SET status='superseded',
                    error_message='Source result semantics corrected by tennis-result-semantics-v1'
                WHERE status='complete' AND algorithm_version='tennis-surface-elo-v1'
                """
            )
    report = {"transformation_version": TRANSFORMATION_VERSION,
              "corrected_matches": corrected, "counts": counts}
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    run(DatabaseManager(load_config().database_url))
