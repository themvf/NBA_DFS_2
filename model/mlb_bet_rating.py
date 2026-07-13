"""MLB game-line accountability ledger.

Every prospective recommendation must carry an immutable prediction snapshot
and the exact sportsbook quote used to evaluate it. Pending rows may be
refreshed before first pitch; each refresh appends a quote/selection snapshot.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from db.database import DatabaseManager
from model.soccer_bet_rating import (  # noqa: F401
    american_to_decimal,
    american_to_prob,
    new_capture_key,
    prob_to_american,
    rate_market,
    rate_no_market,
)

logger = logging.getLogger(__name__)


def record_bet(
    db: DatabaseManager,
    *,
    model_version: str,
    bet_type: str,
    scope: str,
    selection_label: str,
    our_prob: float,
    capture_key: str,
    market_odds: int | None = None,
    market_prob: float | None = None,
    book: str | None = None,
    odds_snapshot_id: int | None = None,
    market_line: float | None = None,
    baseline_prob: float | None = None,
    matchup_id: int | None = None,
    subject_team_id: int | None = None,
    event_commence: datetime | None = None,
    prediction_snapshot_id: int | None = None,
    origin: str = "legacy",
    inputs: dict | None = None,
    longshot_odds_cap: bool = False,
    max_stars: int | None = None,
    conn=None,
) -> int | None:
    """Rate and persist one MLB recommendation plus its exact-price snapshot."""
    if origin == "prospective":
        missing = []
        if prediction_snapshot_id is None:
            missing.append("prediction snapshot")
        if odds_snapshot_id is None:
            missing.append("odds snapshot")
        if not book:
            missing.append("sportsbook")
        if market_odds is None:
            missing.append("exact price")
        if missing:
            logger.warning(
                "Refusing prospective MLB bet without %s: %s %s",
                ", ".join(missing), bet_type, scope,
            )
            return None

    if market_odds is not None and market_prob is not None:
        decimal_odds = american_to_decimal(market_odds)
        stars, ev, edge = rate_market(
            our_prob, decimal_odds, market_prob, longshot_odds_cap,
        )
    else:
        decimal_odds = None
        ref = baseline_prob if baseline_prob is not None else 0.0
        stars, ev, edge = rate_no_market(our_prob, ref)
    if max_stars is not None:
        stars = min(stars, max_stars)

    now = datetime.now(timezone.utc)
    locked = event_commence is not None and now >= event_commence
    inputs_json = json.dumps(inputs or {})
    insert_params = (
        model_version, bet_type, scope, matchup_id, subject_team_id,
        selection_label, market_odds, decimal_odds, market_prob, book,
        our_prob, edge, ev, stars, inputs_json, event_commence, locked,
        prediction_snapshot_id, odds_snapshot_id, origin,
    )

    def _do(cur) -> int | None:
        # A game/market has one active row. If the model flips before first
        # pitch, update the unlocked row rather than creating two active bets.
        cur.execute(
            """
            SELECT id
            FROM mlb_bets
            WHERE model_version = %s AND bet_type = %s AND scope = %s
              AND status = 'pending' AND locked = FALSE
            ORDER BY updated_at DESC, id DESC
            FOR UPDATE
            """,
            (model_version, bet_type, scope),
        )
        active_rows = cur.fetchall()
        if active_rows:
            bet_id = active_rows[0]["id"]
            if len(active_rows) > 1:
                cur.execute(
                    """
                    UPDATE mlb_bets
                    SET status = 'superseded', locked = TRUE, updated_at = NOW(),
                        result_detail = 'Superseded by a newer pregame recommendation'
                    WHERE id = ANY(%s)
                    """,
                    ([row["id"] for row in active_rows[1:]],),
                )
            cur.execute(
                """
                UPDATE mlb_bets
                SET matchup_id = %s, subject_team_id = %s,
                    selection_label = %s, market_odds = %s,
                    market_decimal = %s, market_prob = %s, book = %s,
                    our_prob = %s, edge = %s, ev = %s, stars = %s,
                    inputs_json = %s, event_commence = %s, locked = %s,
                    prediction_snapshot_id = %s, odds_snapshot_id = %s,
                    origin = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (
                    matchup_id, subject_team_id, selection_label, market_odds,
                    decimal_odds, market_prob, book, our_prob, edge, ev, stars,
                    inputs_json, event_commence, locked, prediction_snapshot_id,
                    odds_snapshot_id, origin, bet_id,
                ),
            )
            row = cur.fetchone()
        else:
            cur.execute(
                """
                INSERT INTO mlb_bets (
                    model_version, bet_type, scope, matchup_id, subject_team_id,
                    selection_label, market_odds, market_decimal, market_prob, book,
                    our_prob, edge, ev, stars, inputs_json, event_commence, locked,
                    prediction_snapshot_id, odds_snapshot_id, origin, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (bet_type, scope, selection_label, model_version) DO UPDATE SET
                    matchup_id = EXCLUDED.matchup_id,
                    subject_team_id = EXCLUDED.subject_team_id,
                    market_odds = EXCLUDED.market_odds,
                    market_decimal = EXCLUDED.market_decimal,
                    market_prob = EXCLUDED.market_prob,
                    book = EXCLUDED.book,
                    our_prob = EXCLUDED.our_prob,
                    edge = EXCLUDED.edge,
                    ev = EXCLUDED.ev,
                    stars = EXCLUDED.stars,
                    inputs_json = EXCLUDED.inputs_json,
                    event_commence = EXCLUDED.event_commence,
                    locked = EXCLUDED.locked,
                    prediction_snapshot_id = EXCLUDED.prediction_snapshot_id,
                    odds_snapshot_id = EXCLUDED.odds_snapshot_id,
                    origin = EXCLUDED.origin,
                    updated_at = NOW()
                WHERE mlb_bets.locked = FALSE
                  AND mlb_bets.status = 'pending'
                RETURNING id
                """,
                insert_params,
            )
            row = cur.fetchone()

        if not row:
            return None
        bet_id = row["id"]
        cur.execute(
            """
            INSERT INTO mlb_bet_snapshots (
                bet_id, capture_key, our_prob, market_prob, market_odds,
                edge, ev, stars, prediction_snapshot_id, odds_snapshot_id,
                book, selection_label, market_line
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                bet_id, capture_key, our_prob, market_prob, market_odds,
                edge, ev, stars, prediction_snapshot_id, odds_snapshot_id,
                book, selection_label, market_line,
            ),
        )
        return bet_id

    if conn is not None:
        return _do(conn.cursor())
    with db.connect() as own_conn:
        return _do(own_conn.cursor())
