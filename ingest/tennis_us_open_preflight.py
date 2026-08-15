"""Verify provider coverage and stored fixture readiness before the 2026 US Open."""

from __future__ import annotations

import argparse
import json

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_schedule import discover_tournaments


def is_us_open(sport_key: str, title: str) -> bool:
    """Identify the ATP/WTA US Open provider keys without relying on one title form."""
    normalized = f"{sport_key} {title}".lower().replace("_", " ")
    return "us open" in normalized


def active_us_open_tournaments(tournaments: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    return [item for item in tournaments if is_us_open(item[1], item[2])]


def preflight(db: DatabaseManager, api_key: str) -> dict:
    active = active_us_open_tournaments(discover_tournaments(api_key))
    rows = db.execute(
        """SELECT tour, COUNT(*) AS fixtures, COUNT(*) FILTER (WHERE home_ml IS NOT NULL AND away_ml IS NOT NULL) AS priced
           FROM tennis_matches
           WHERE tournament ILIKE '%us open%' AND match_date >= CURRENT_DATE
           GROUP BY tour ORDER BY tour"""
    )
    coverage = [
        {"tour": row["tour"], "fixtures": int(row["fixtures"]), "priced": int(row["priced"])}
        for row in rows
    ]
    return {
        "provider_us_open_active": bool(active),
        "provider_tournaments": [
            {"tour": tour, "sport_key": key, "title": title} for tour, key, title in active
        ],
        "stored_coverage": coverage,
        "rating_policy": "moneyline calibration only; capped at 2 stars",
        "derivative_policy": "total-games alerts only; void on retired, walkover, or awarded result",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="US Open tennis readiness preflight")
    parser.parse_args()
    config = load_config()
    print(json.dumps(preflight(DatabaseManager(config.database_url), config.odds_api.api_key), indent=2))
