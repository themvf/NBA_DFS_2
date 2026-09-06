"""Add exact MLB outcome metadata without changing legacy consensus fields."""
from __future__ import annotations

from ingest.mlb_odds_policy import normalize_team_name


def enrich_terminal_books(event: dict, books: dict) -> None:
    home = normalize_team_name(event.get("home_team"))
    away = normalize_team_name(event.get("away_team"))
    for bookmaker in event.get("bookmakers") or []:
        book = books.setdefault(bookmaker.get("key", "?"), {})
        book["title"] = bookmaker.get("title")
        for market in bookmaker.get("markets") or []:
            key = market.get("key")
            if key not in ("h2h", "totals", "spreads"):
                continue
            book[f"{key}_last_update"] = market.get("last_update") or bookmaker.get("last_update")
            for outcome in market.get("outcomes") or []:
                if key == "spreads":
                    name = normalize_team_name(outcome.get("name"))
                    side = "home" if name == home else "away" if name == away else None
                    if side and outcome.get("point") is not None:
                        book[f"spread_{side}"] = outcome["point"]
                        book[f"spread_{side}_price"] = outcome.get("price")
                elif key == "totals" and outcome.get("name") in ("Over", "Under"):
                    book[f"{outcome['name'].lower()}_line"] = outcome.get("point")
