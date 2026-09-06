"""Append-only movement lifecycle; never updates the first-breach/CLV ledger.

One observation per capture, market, detector, side and version. Rescanning a
capture is idempotent. This describes price paths, not predictive confidence.
"""
from __future__ import annotations

import json
import math
from statistics import median

from db.market_signal_schema import SCHEMA, INDEX

TYPES = {"steam", "walking", "spread_steam", "total_steam", "spread_walking",
         "total_walking", "reversal", "reference_led", "price_pressure",
         "key_cross", "late_move", "favorite_flip"}


def market_for(candidate):
    return candidate["details"].get("market") or (
        "spread" if candidate["alert_type"].startswith("spread_") or candidate["alert_type"] == "key_cross"
        else "total" if candidate["alert_type"].startswith("total_") else "moneyline")


def _number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _prob(value):
    n = _number(value)
    if n is None or abs(n) < 100:
        return None
    return 100 / (n + 100) if n > 0 else -n / (-n + 100)


def _value(book, market, side, pressure):
    if market == "moneyline" or pressure:
        fields = (("ml_home", "ml_away") if market == "moneyline" else
                  ("spread_home_price", "spread_away_price") if market == "spread" else
                  ("over", "under"))
        a, b = (_prob(book.get(key)) for key in fields)
        if a is None or b is None:
            return None
        return (a if side in ("home", "over") else b) / (a + b)
    n = _number(book.get("spread_home" if market == "spread" else "total_line"))
    return None if n is None else n * (-1 if side in ("home", "under") else 1)


def lifecycle(baseline, trigger, current, market, side, pressure=False):
    """Measure the SAME retail cohort at all three times; missing != faded."""
    pairs = []
    keys = set(baseline) & set(trigger) & set(current) - {"pinnacle", "polymarket"}
    for key in sorted(keys):
        quotes = [books[key] for books in (baseline, trigger, current)]
        if pressure and market != "moneyline":
            line_key = "spread_home" if market == "spread" else "total_line"
            lines = [_number(book.get(line_key)) for book in quotes]
            if None in lines or len(set(lines)) != 1:
                continue
        values = [_value(book, market, side, pressure) for book in quotes]
        if all(value is not None for value in values):
            pairs.append((key, values))
    evidence = {"comparable_books": len(pairs), "cohort": [key for key, _ in pairs]}
    if len(pairs) < 3:
        return "unavailable", evidence
    first = median(values[1] - values[0] for _, values in pairs)
    retained = median(values[2] - values[0] for _, values in pairs)
    evidence.update(initial_move=first, retained_move=retained,
                    units="probability" if market == "moneyline" or pressure else "points")
    if first <= 0:
        return "unavailable", evidence
    evidence["retention_fraction"] = retained / first
    if retained < -1e-9:
        return "reversed", evidence
    if retained <= 1e-9:
        return "faded", evidence
    if retained < first - 1e-9:
        return "weakened", evidence
    return ("strengthened" if retained > first + 1e-9 else "held"), evidence


def record_observations(db, sport, row):
    # Only current prospective captures enroll. Delayed scheduler runs must not
    # manufacture a fresh signal from an old snapshot.
    from datetime import datetime, timezone
    captured = row["captured_at"]
    if captured.tzinfo is None:
        captured = captured.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - captured).total_seconds()
    if age < 0 or age > 30 * 60 or captured >= row["commence_time"]:
        return
    existing = db.execute("""
        SELECT DISTINCT ON (o.market, o.alert_type, o.side, o.detector_version)
            o.*, t.books AS trigger_books, b.books AS baseline_books
        FROM market_signal_observations o
        JOIN game_odds_history t ON t.id=o.trigger_history_id
        LEFT JOIN game_odds_history b ON b.id=o.baseline_history_id
        WHERE o.sport=%s AND o.matchup_id=%s AND o.observed_at <= %s
        ORDER BY o.market, o.alert_type, o.side, o.detector_version, o.observed_at, o.id
    """, (sport, row["matchup_id"], captured))
    candidates = {}
    for candidate in row["movement_candidates"]:
        if candidate["alert_type"] not in TYPES:
            continue
        details = candidate["details"]
        version = details.get("detector_version") or details.get("signal_version") or f"{sport}-legacy-v1"
        key = (market_for(candidate), candidate["alert_type"], candidate["side"], version)
        candidates[key] = candidate
    origins = {(o["market"], o["alert_type"], o["side"], o["detector_version"]): o for o in existing}
    for key in origins.keys() | candidates.keys():
        market, alert_type, side, version = key
        origin = origins.get(key)
        candidate = candidates.get(key)
        if origin:
            if origin["history_id"] == row["history_id"]:
                continue
            state, evidence = lifecycle(origin["baseline_books"] or {}, origin["trigger_books"],
                                        row["books"], market, side, alert_type == "price_pressure")
            if state == "held" and candidate:
                state = "confirmed"
            details = {**origin["details_json"], **evidence, "detector_fired": candidate is not None}
            trigger_id, baseline_id = origin["trigger_history_id"], origin["baseline_history_id"]
        else:
            details = dict(candidate["details"])
            baseline_id = (details.get("opening_history_id") if alert_type.endswith("walking")
                           else details.get("pivot_history_id") if alert_type == "reversal"
                           else details.get("previous_history_id"))
            if baseline_id is None:
                baseline = db.execute_one(
                    "SELECT id FROM game_odds_history WHERE sport=%s AND matchup_id=%s "
                    "AND captured_at < %s AND books IS NOT NULL AND EXISTS (SELECT 1 FROM jsonb_object_keys(books) AS b(k) WHERE b.k NOT IN ('polymarket')) ORDER BY captured_at "
                    + ("ASC" if alert_type.endswith("walking") else "DESC") + ", id LIMIT 1",
                    (sport, row["matchup_id"], captured))
                baseline_id = baseline["id"] if baseline else None
            trigger_id, state = row["history_id"], "triggered"
            details["first_observed_at"] = captured.isoformat()
        details.setdefault("trigger_capture_at", details.get("first_observed_at", captured.isoformat()))
        details.update(market=market, lifecycle_state=state, observation_capture_at=captured.isoformat(),
                       lifecycle_version="movement-lifecycle-v1", origin="prospective")
        db.execute("""INSERT INTO market_signal_observations
            (sport, matchup_id, market, alert_type, side, detector_version, history_id,
             trigger_history_id, baseline_history_id, observed_at, state, details_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING""",
            (sport, row["matchup_id"], market, alert_type, side, version, row["history_id"],
             trigger_id, baseline_id, captured, state, json.dumps(details)))


if __name__ == "__main__":
    from config import load_config
    from db.database import DatabaseManager
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    db.execute(SCHEMA)
    db.execute(INDEX)
    # The worker uses --existing-schema; migrate the checkpoint allowlist too.
    # A single DO statement makes replacement atomic, and skips DDL once current.
    from db.schema import INDEXES
    checkpoint_check = next(sql for sql in INDEXES
                            if sql.startswith("ALTER TABLE odds_capture_checkpoints ADD CONSTRAINT odds_capture_checkpoints_checkpoint_check"))
    db.execute("""DO $migration$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint
            WHERE conrelid='odds_capture_checkpoints'::regclass
              AND conname='odds_capture_checkpoints_checkpoint_check'
              AND pg_get_constraintdef(oid) LIKE '%%nfl_first_observed%%') THEN
            ALTER TABLE odds_capture_checkpoints DROP CONSTRAINT IF EXISTS odds_capture_checkpoints_checkpoint_check;
    """ + checkpoint_check + "; END IF; END $migration$")
    print("Movement observation schema ready.")
