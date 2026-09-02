"""Shared full-game sportsbook market normalization.

The append-only ledger stores one JSONB book map per event capture.  Consensus
fields are convenience caches; exact book lines and prices remain authoritative.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ingest.mlb_odds_policy import consensus_american
from model.soccer_bet_rating import american_to_prob

EASTERN = ZoneInfo("America/New_York")


def parse_iso(value: object) -> datetime:
    if not value:
        raise ValueError("missing commence_time")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def eastern_date(value: datetime) -> str:
    return value.astimezone(EASTERN).date().isoformat()


def lower_median(values: list[float]) -> float | None:
    """Return an observed lower-middle value; never invent a quarter point."""
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    return ordered[(len(ordered) - 1) // 2]


def vig_free_home_probability(home_ml: int | None, away_ml: int | None) -> float | None:
    if home_ml is None or away_ml is None:
        return None
    home = american_to_prob(home_ml)
    away = american_to_prob(away_ml)
    return home / (home + away) if home + away > 0 else None


def require_pregame_capture(
    *, event_commence: datetime, stored_commence: datetime, captured_at: datetime,
) -> None:
    stored = stored_commence if stored_commence.tzinfo else stored_commence.replace(tzinfo=timezone.utc)
    if captured_at >= event_commence.astimezone(timezone.utc):
        raise ValueError("capture is at or after provider kickoff")
    if captured_at >= stored.astimezone(timezone.utc):
        raise ValueError("capture is at or after stored kickoff")


def extract_game_markets(event: dict) -> dict:
    """Preserve both sides and prices for h2h, spreads, and totals.

    The scalar spread/total values intentionally retain the legacy mean cache
    used by existing consumers. CFB terminal consensus is derived as a lower
    median from ``books`` at read time.
    """
    home_name = str(event.get("home_team") or "")
    away_name = str(event.get("away_team") or "")
    home_prices: list[int] = []
    away_prices: list[int] = []
    home_spreads: list[float] = []
    total_lines: list[float] = []
    books: dict[str, dict] = {}

    for bookmaker in event.get("bookmakers") or []:
        key = str(bookmaker.get("key") or "?")
        book = books.setdefault(key, {
            "title": bookmaker.get("title"),
            "last_update": bookmaker.get("last_update"),
        })
        for market in bookmaker.get("markets") or []:
            outcomes = market.get("outcomes") or []
            if market.get("key") == "h2h":
                for outcome in outcomes:
                    if outcome.get("name") == home_name:
                        home_prices.append(int(outcome["price"]))
                        book["ml_home"] = int(outcome["price"])
                    elif outcome.get("name") == away_name:
                        away_prices.append(int(outcome["price"]))
                        book["ml_away"] = int(outcome["price"])
            elif market.get("key") == "spreads":
                for outcome in outcomes:
                    if outcome.get("point") is None:
                        continue
                    if outcome.get("name") == home_name:
                        point = float(outcome["point"])
                        home_spreads.append(point)
                        book["spread_home"] = point
                        book["spread_home_price"] = outcome.get("price")
                    elif outcome.get("name") == away_name:
                        book["spread_away"] = float(outcome["point"])
                        book["spread_away_price"] = outcome.get("price")
            elif market.get("key") == "totals":
                over = next((outcome for outcome in outcomes if outcome.get("name") == "Over"), None)
                under = next((outcome for outcome in outcomes if outcome.get("name") == "Under"), None)
                if over and over.get("point") is not None:
                    line = float(over["point"])
                    total_lines.append(line)
                    book["total_line"] = line
                    book["over"] = over.get("price")
                    book["under"] = under.get("price") if under else None

    spread_mean = sum(home_spreads) / len(home_spreads) if home_spreads else None
    total_mean = sum(total_lines) / len(total_lines) if total_lines else None
    return {
        "home_ml": consensus_american(home_prices),
        "away_ml": consensus_american(away_prices),
        "home_spread": round(spread_mean * 2) / 2 if spread_mean is not None else None,
        "vegas_total": round(total_mean * 2) / 2 if total_mean is not None else None,
        "vegas_total_raw": total_mean,
        "books": books,
        "bookmaker_count": len(books),
    }
