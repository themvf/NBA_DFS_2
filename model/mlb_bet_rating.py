"""MLB bet ledger — parity with the soccer accountability framework.

Every MLB bet recommendation (moneyline, total) flows through ``record_bet``,
which mirrors ``model/soccer_bet_rating.py`` exactly:
  1. market odds → decimal + vig-free implied probability,
  2. EV + edge vs our model probability,
  3. deterministic 1–5 star rating (shared rubric, imported from soccer),
  4. upsert into ``mlb_bets`` (one row per selection per model_version),
  5. append-only ``mlb_bet_snapshots`` row (audit trail),
  6. LOCK once first pitch has passed, so the backtest uses the closing number
     we actually committed to — never a post-hoc edit.

The rating math (``rate_market``, ``american_to_decimal`` …) is identical to
soccer's, so it is imported rather than duplicated; only the table the row lands
in differs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from db.database import DatabaseManager
# Reuse the exact, shared rating spine — same star rubric for every sport.
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
    baseline_prob: float | None = None,
    matchup_id: int | None = None,
    subject_team_id: int | None = None,
    event_commence: datetime | None = None,
    inputs: dict | None = None,
    longshot_odds_cap: bool = False,
    conn=None,
) -> int | None:
    """Rate + persist one MLB bet into ``mlb_bets`` (+ snapshot).  Returns the
    bet id, or None if the row is locked (first pitch passed) and left untouched.

    Pass ``conn`` to batch many bets over one connection (fewer Neon round-trips).
    """
    if market_odds is not None and market_prob is not None:
        decimal_odds = american_to_decimal(market_odds)
        stars, ev, edge = rate_market(our_prob, decimal_odds, market_prob, longshot_odds_cap)
    else:
        decimal_odds = None
        ref = baseline_prob if baseline_prob is not None else 0.0
        stars, ev, edge = rate_no_market(our_prob, ref)

    now = datetime.now(timezone.utc)
    locked = event_commence is not None and now >= event_commence
    params = (
        model_version, bet_type, scope, matchup_id, subject_team_id,
        selection_label, market_odds, decimal_odds, market_prob, book,
        our_prob, edge, ev, stars, json.dumps(inputs or {}), event_commence, locked,
    )

    def _do(cur) -> int | None:
        cur.execute(
            """
            INSERT INTO mlb_bets (
                model_version, bet_type, scope, matchup_id, subject_team_id,
                selection_label, market_odds, market_decimal, market_prob, book,
                our_prob, edge, ev, stars, inputs_json, event_commence, locked, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (bet_type, scope, selection_label, model_version) DO UPDATE SET
                matchup_id      = EXCLUDED.matchup_id,
                subject_team_id = EXCLUDED.subject_team_id,
                market_odds     = EXCLUDED.market_odds,
                market_decimal  = EXCLUDED.market_decimal,
                market_prob     = EXCLUDED.market_prob,
                book            = EXCLUDED.book,
                our_prob        = EXCLUDED.our_prob,
                edge            = EXCLUDED.edge,
                ev              = EXCLUDED.ev,
                stars           = EXCLUDED.stars,
                inputs_json     = EXCLUDED.inputs_json,
                event_commence  = EXCLUDED.event_commence,
                locked          = EXCLUDED.locked,
                updated_at      = NOW()
            WHERE mlb_bets.locked = FALSE
              AND mlb_bets.status = 'pending'
            RETURNING id
            """,
            params,
        )
        row = cur.fetchone()
        if not row:
            return None  # locked — frozen closing recommendation
        bet_id = row["id"]
        cur.execute(
            """
            INSERT INTO mlb_bet_snapshots
                (bet_id, capture_key, our_prob, market_prob, market_odds, edge, ev, stars)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (bet_id, capture_key, our_prob, market_prob, market_odds, edge, ev, stars),
        )
        return bet_id

    if conn is not None:
        return _do(conn.cursor())
    with db.connect() as own_conn:
        return _do(own_conn.cursor())
