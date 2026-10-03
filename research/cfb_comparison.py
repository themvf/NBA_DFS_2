"""Freeze the market evidence available when each CFB forecast is published.

The comparison row is append-only. Later odds imports cannot move its market
reference, and an absent or thin market remains explicitly ineligible.
"""

from __future__ import annotations

from datetime import datetime
import math

from psycopg2.extras import Json

from ingest.sportsbook_policy import selected_books

MARKETS = ("spread", "total", "moneyline")


def _number(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _complete(quote: dict, market: str) -> bool:
    fields = {
        "spread": ("spread_home", "spread_away", "spread_home_price", "spread_away_price"),
        "total": ("total_line", "over", "under"),
        "moneyline": ("ml_home", "ml_away"),
    }[market]
    return all(_number(quote.get(field)) for field in fields)


def _fresh(quote: dict, captured_at: datetime) -> bool:
    try:
        updated = datetime.fromisoformat(str(quote["last_update"]).replace("Z", "+00:00"))
        age = (captured_at - updated).total_seconds()
        return -90 <= age <= 300
    except (KeyError, TypeError, ValueError):
        return False


def comparison(forecast: dict, market: dict | None, feature: dict | None) -> dict:
    freeze_at = forecast["generated_at"]
    kickoff = forecast["kickoff"]
    lead_hours = (kickoff - freeze_at).total_seconds() / 3600
    captured_at = market["captured_at"] if market else None
    age_minutes = (freeze_at - captured_at).total_seconds() / 60 if captured_at else None
    max_age = 360 if lead_hours > 12 else 90
    books = selected_books(market.get("books") or {}) if market else {}
    values = {
        "spread": -float(market["home_spread"]) if market and _number(market["home_spread"]) else None,
        "total": float(market["vegas_total"]) if market and _number(market["vegas_total"]) else None,
        "moneyline": None,
    }
    if market and _number(market["home_ml"]) and _number(market["away_ml"]):
        from research.cfb_forecast_v1 import american_probability
        home = american_probability(market["home_ml"])
        away = american_probability(market["away_ml"])
        if home is not None and away is not None and home + away > 0:
            values["moneyline"] = home / (home + away)
    by_market = {}
    for name in MARKETS:
        quoting = [quote for quote in books.values() if isinstance(quote, dict) and _complete(quote, name)]
        fresh = sum(_fresh(quote, captured_at) for quote in quoting) if captured_at else 0
        reasons = []
        if values[name] is None:
            reasons.append("missing_market_value")
        if len(quoting) < 3:
            reasons.append("fewer_than_3_books")
        if fresh < 3:
            reasons.append("fewer_than_3_fresh_books")
        if age_minutes is None or age_minutes < 0 or age_minutes > max_age:
            reasons.append("capture_too_old")
        if forecast.get("start_time_tbd"):
            reasons.append("kickoff_time_unconfirmed")
        by_market[name] = {"value": values[name], "books": len(quoting),
                           "fresh_books": fresh, "eligible": not reasons, "reasons": reasons}
    return {"definition": "cfb-comparison-v1", "forecast_at": freeze_at.isoformat(),
            "kickoff": kickoff.isoformat(), "market_captured_at": captured_at.isoformat() if captured_at else None,
            "market_age_minutes": round(age_minutes, 1) if age_minutes is not None else None,
            "max_market_age_minutes": max_age, "lead_hours": round(lead_hours, 2),
            "forecast": {key: forecast.get(key) for key in
                         ("home_points", "away_points", "home_win_probability")},
            "feature": feature or {}, "markets": by_market}


def freeze_comparisons(cursor, run_id: int, report: dict) -> None:
    """Run inside the forecast publication transaction."""
    cursor.execute("""
        SELECT f.id forecast_id, f.game_id, f.kickoff, r.generated_at,
               f.home_points,f.away_points,f.home_win_probability,
               m.start_time_tbd,
               h.id odds_history_id, h.captured_at, h.books,
               h.home_ml, h.away_ml, h.home_spread, h.vegas_total
        FROM cfb_game_forecasts f JOIN cfb_forecast_runs r ON r.id=f.run_id
        JOIN cfb_matchups m ON m.id=f.game_id
        LEFT JOIN LATERAL (
            SELECT id,captured_at,books,home_ml,away_ml,home_spread,vegas_total
            FROM game_odds_history WHERE sport='cfb' AND matchup_id=f.game_id
              AND captured_at<=r.generated_at AND captured_at<f.kickoff
            ORDER BY captured_at DESC,id DESC LIMIT 1
        ) h ON TRUE
        WHERE f.run_id=%s AND r.generated_at<f.kickoff
    """, (run_id,))
    for row in cursor.fetchall():
        row = dict(row)
        feature = report.get("explanations", {}).get(str(row["game_id"]))
        market = row if row["odds_history_id"] is not None else None
        evidence = comparison(row, market, feature)
        cursor.execute("""
            INSERT INTO cfb_market_comparisons
              (forecast_id,run_id,game_id,odds_history_id,forecast_at,evidence_json)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (forecast_id) DO NOTHING
        """, (row["forecast_id"], run_id, row["game_id"], row["odds_history_id"],
              row["generated_at"], Json(evidence)))
