"""Apply reviewed Tennis identity corrections without mutating history in place.

Provider abbreviations can collapse two real players onto one surname/initial
key.  Resolutions here are explicit, reviewable source decisions.  A corrected
historical row supersedes the prior interpretation while retaining the raw
source checksum and a new transformation version.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_foundation import checksum, normalize_name

TRANSFORMATION_VERSION = "tennis-identity-resolution-v1"
RESOLUTIONS = (
    {
        "tour": "WTA",
        "provider": "tennis_data",
        "raw_name": "Wang Xiy.",
        "old_norm_name": "wangx",
        "new_norm_name": "wangxiy",
        "reason": (
            "tennis-data abbreviations distinguish Wang Xiy. from Wang Xin.; "
            "surname+first-initial normalization incorrectly merged them"
        ),
    },
)


def _resolve(db: DatabaseManager, resolution: dict) -> dict:
    now = datetime.now(timezone.utc)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM tennis_players WHERE tour=%s AND norm_name=%s",
            (resolution["tour"], resolution["old_norm_name"]),
        )
        old = cur.fetchone()
        if not old:
            raise RuntimeError(f"Old identity not found: {resolution}")
        old_player_id = old["id"]

        cur.execute(
            """
            INSERT INTO tennis_players (tour, canonical_name, norm_name)
            VALUES (%s,%s,%s)
            ON CONFLICT (tour, norm_name) DO UPDATE SET updated_at=NOW()
            RETURNING id
            """,
            (resolution["tour"], resolution["raw_name"], resolution["new_norm_name"]),
        )
        new_player_id = cur.fetchone()["id"]
        evidence_checksum = checksum(resolution)
        cur.execute(
            """
            INSERT INTO tennis_player_aliases (
                player_id, provider, tour, raw_name, norm_name, match_method,
                match_confidence, verified, source_available_at, captured_at, raw_checksum
            ) VALUES (%s,%s,%s,%s,%s,'manual_provider_disambiguation',1.0,TRUE,%s,%s,%s)
            ON CONFLICT (provider, tour, norm_name, player_id) DO UPDATE SET
                verified=TRUE, match_method='manual_provider_disambiguation',
                match_confidence=1.0, captured_at=GREATEST(tennis_player_aliases.captured_at, EXCLUDED.captured_at),
                raw_checksum=EXCLUDED.raw_checksum
            """,
            (new_player_id, resolution["provider"], resolution["tour"],
             resolution["raw_name"], resolution["new_norm_name"], now, now, evidence_checksum),
        )

        cur.execute(
            """
            SELECT * FROM tennis_historical_matches
            WHERE source=%s AND tour=%s AND is_current
              AND (
                (winner_player_id=%s AND raw_payload->'source_row'->>'Winner'=%s)
                OR
                (loser_player_id=%s AND raw_payload->'source_row'->>'Loser'=%s)
              )
            ORDER BY id
            """,
            (resolution["provider"], resolution["tour"], old_player_id,
             resolution["raw_name"], old_player_id, resolution["raw_name"]),
        )
        matches = cur.fetchall()
        corrected = 0
        for match in matches:
            winner_id = (
                new_player_id
                if match["winner_player_id"] == old_player_id
                and match["raw_payload"]["source_row"].get("Winner") == resolution["raw_name"]
                else match["winner_player_id"]
            )
            loser_id = (
                new_player_id
                if match["loser_player_id"] == old_player_id
                and match["raw_payload"]["source_row"].get("Loser") == resolution["raw_name"]
                else match["loser_player_id"]
            )
            if winner_id == loser_id:
                raise RuntimeError(f"Identity resolution still produces a self-match for {match['id']}")

            cur.execute(
                "UPDATE tennis_historical_matches SET is_current=FALSE, superseded_at=%s WHERE id=%s",
                (now, match["id"]),
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
                (match["source"], match["source_match_key"], match["source_partition_id"],
                 match["tour"], match["season"], match["match_date"], match["start_time"],
                 match["tournament"], match["round"], match["best_of"], match["surface"],
                 match["indoor"], winner_id, loser_id, match["score"],
                 match["completion_status"], match["retired"], match["walkover"],
                 match["winner_rank"], match["loser_rank"], match["winner_rank_points"],
                 match["loser_rank_points"], match["winner_decimal_odds"],
                 match["loser_decimal_odds"], match["odds_source"], match["odds_timing"],
                 match["source_available_at"], match["stats_through_at"], match["captured_at"],
                 TRANSFORMATION_VERSION, match["raw_checksum"], json.dumps(match["raw_payload"]),
                 match["id"]),
            )
            corrected_match_id = cur.fetchone()["id"]
            cur.execute(
                """
                INSERT INTO tennis_player_match_stats (
                    historical_match_id, player_id, opponent_player_id, is_winner,
                    stats_available, missing_reason
                ) VALUES
                    (%s,%s,%s,TRUE,FALSE,'source_unavailable'),
                    (%s,%s,%s,FALSE,FALSE,'source_unavailable')
                ON CONFLICT (historical_match_id, player_id) DO NOTHING
                """,
                (corrected_match_id, winner_id, loser_id,
                 corrected_match_id, loser_id, winner_id),
            )
            corrected += 1

        cur.execute(
            """
            INSERT INTO tennis_identity_reviews (
                provider, tour, raw_name, norm_name, context, candidates,
                reason, status, resolution_player_id, resolved_at
            )
            SELECT %s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,'resolved',%s,%s
            WHERE NOT EXISTS (
                SELECT 1 FROM tennis_identity_reviews
                WHERE provider=%s AND tour=%s AND raw_name=%s
                  AND reason=%s AND status='resolved'
            )
            """,
            (resolution["provider"], resolution["tour"], resolution["raw_name"],
             normalize_name(resolution["raw_name"]),
             json.dumps({"old_player_id": old_player_id, "corrected_matches": corrected}),
             json.dumps([{"player_id": new_player_id, "norm_name": resolution["new_norm_name"]}]),
             resolution["reason"], new_player_id, now,
             resolution["provider"], resolution["tour"], resolution["raw_name"],
             resolution["reason"]),
        )
        return {
            "raw_name": resolution["raw_name"],
            "old_player_id": old_player_id,
            "new_player_id": new_player_id,
            "corrected_matches": corrected,
        }


def run(db: DatabaseManager) -> dict:
    results = [_resolve(db, resolution) for resolution in RESOLUTIONS]
    report = {"transformation_version": TRANSFORMATION_VERSION, "resolutions": results}
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    run(DatabaseManager(load_config().database_url))
