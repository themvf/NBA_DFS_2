"""Append-only provenance for MLB game-line model predictions."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone

from db.database import DatabaseManager


PROSPECTIVE = "prospective"
RETROSPECTIVE_BACKFILL = "retrospective_backfill"


def create_prediction_run(
    db: DatabaseManager,
    *,
    model_version: str,
    trained_through: date | str | None,
    origin: str,
    source: str,
    config: dict,
) -> int:
    if origin not in {PROSPECTIVE, RETROSPECTIVE_BACKFILL}:
        raise ValueError(f"invalid prediction origin: {origin}")
    return db.execute_insert(
        """
        INSERT INTO mlb_prediction_runs
            (trained_through, model_version, git_sha, origin, source, config_json)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (
            trained_through,
            model_version,
            os.getenv("GITHUB_SHA") or os.getenv("VERCEL_GIT_COMMIT_SHA"),
            origin,
            source,
            json.dumps(config),
        ),
    )


def record_prediction_snapshot(
    db: DatabaseManager,
    *,
    run_id: int,
    matchup_id: int,
    market: str,
    feature_values: dict,
    raw_prediction: float,
    calibrated_probability: float | None = None,
    market_line: float | None = None,
    market_odds: int | None = None,
    market_prob: float | None = None,
    book: str | None = None,
    missingness: dict | None = None,
    feature_available_at: datetime | None = None,
) -> int:
    """Freeze one pregame prediction and link its latest eligible odds capture."""
    if market not in {"moneyline", "total"}:
        raise ValueError(f"invalid MLB prediction market: {market}")
    available_at = feature_available_at or datetime.now(timezone.utc)
    row = db.execute_one(
        """
        INSERT INTO mlb_game_prediction_snapshots (
            run_id, matchup_id, odds_snapshot_id, market, event_commence,
            feature_available_at, feature_values, missingness_json,
            market_line, market_odds, market_prob, book,
            raw_prediction, calibrated_probability
        )
        SELECT
            %s, m.id,
            (
                SELECT h.id FROM game_odds_history h
                WHERE h.sport = 'mlb' AND h.matchup_id = m.id
                  AND h.captured_at < m.commence_time
                ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
            ),
            %s, m.commence_time, %s, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s, %s, %s
        FROM mlb_matchups m
        WHERE m.id = %s
          AND m.commence_time IS NOT NULL
          AND %s < m.commence_time
        RETURNING id
        """,
        (
            run_id,
            market,
            available_at,
            json.dumps(feature_values),
            json.dumps(missingness or {}),
            market_line,
            market_odds,
            market_prob,
            book,
            raw_prediction,
            calibrated_probability,
            matchup_id,
            available_at,
        ),
    )
    if row is None:
        raise ValueError(
            f"matchup {matchup_id} is missing a future commence time; snapshot rejected"
        )
    return int(row["id"])


def latest_prediction_snapshot_id(
    db: DatabaseManager,
    *,
    matchup_id: int,
    market: str,
    origin: str,
) -> int | None:
    row = db.execute_one(
        """
        SELECT s.id
        FROM mlb_game_prediction_snapshots s
        JOIN mlb_prediction_runs r ON r.id = s.run_id
        WHERE s.matchup_id = %s AND s.market = %s AND r.origin = %s
          AND s.created_at < s.event_commence
        ORDER BY s.created_at DESC, s.id DESC
        LIMIT 1
        """,
        (matchup_id, market, origin),
    )
    return int(row["id"]) if row else None
