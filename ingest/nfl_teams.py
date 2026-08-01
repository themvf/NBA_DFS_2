"""Seed the 32 active NFL teams used by The Odds API.

The provider name is the ingestion identity. Automatic schedule ingestion uses
exact matching and fails closed when a provider name is not in this list.

Usage:
    python -m ingest.nfl_teams
"""

from __future__ import annotations

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_nfl_team


NFL_TEAMS: list[dict[str, str]] = [
    {"name": "Arizona Cardinals", "abbreviation": "ARI", "city": "Glendale, AZ", "conference": "NFC", "division": "West"},
    {"name": "Atlanta Falcons", "abbreviation": "ATL", "city": "Atlanta, GA", "conference": "NFC", "division": "South"},
    {"name": "Baltimore Ravens", "abbreviation": "BAL", "city": "Baltimore, MD", "conference": "AFC", "division": "North"},
    {"name": "Buffalo Bills", "abbreviation": "BUF", "city": "Orchard Park, NY", "conference": "AFC", "division": "East"},
    {"name": "Carolina Panthers", "abbreviation": "CAR", "city": "Charlotte, NC", "conference": "NFC", "division": "South"},
    {"name": "Chicago Bears", "abbreviation": "CHI", "city": "Chicago, IL", "conference": "NFC", "division": "North"},
    {"name": "Cincinnati Bengals", "abbreviation": "CIN", "city": "Cincinnati, OH", "conference": "AFC", "division": "North"},
    {"name": "Cleveland Browns", "abbreviation": "CLE", "city": "Cleveland, OH", "conference": "AFC", "division": "North"},
    {"name": "Dallas Cowboys", "abbreviation": "DAL", "city": "Arlington, TX", "conference": "NFC", "division": "East"},
    {"name": "Denver Broncos", "abbreviation": "DEN", "city": "Denver, CO", "conference": "AFC", "division": "West"},
    {"name": "Detroit Lions", "abbreviation": "DET", "city": "Detroit, MI", "conference": "NFC", "division": "North"},
    {"name": "Green Bay Packers", "abbreviation": "GB", "city": "Green Bay, WI", "conference": "NFC", "division": "North"},
    {"name": "Houston Texans", "abbreviation": "HOU", "city": "Houston, TX", "conference": "AFC", "division": "South"},
    {"name": "Indianapolis Colts", "abbreviation": "IND", "city": "Indianapolis, IN", "conference": "AFC", "division": "South"},
    {"name": "Jacksonville Jaguars", "abbreviation": "JAX", "city": "Jacksonville, FL", "conference": "AFC", "division": "South"},
    {"name": "Kansas City Chiefs", "abbreviation": "KC", "city": "Kansas City, MO", "conference": "AFC", "division": "West"},
    {"name": "Las Vegas Raiders", "abbreviation": "LV", "city": "Las Vegas, NV", "conference": "AFC", "division": "West"},
    {"name": "Los Angeles Chargers", "abbreviation": "LAC", "city": "Inglewood, CA", "conference": "AFC", "division": "West"},
    {"name": "Los Angeles Rams", "abbreviation": "LAR", "city": "Inglewood, CA", "conference": "NFC", "division": "West"},
    {"name": "Miami Dolphins", "abbreviation": "MIA", "city": "Miami Gardens, FL", "conference": "AFC", "division": "East"},
    {"name": "Minnesota Vikings", "abbreviation": "MIN", "city": "Minneapolis, MN", "conference": "NFC", "division": "North"},
    {"name": "New England Patriots", "abbreviation": "NE", "city": "Foxborough, MA", "conference": "AFC", "division": "East"},
    {"name": "New Orleans Saints", "abbreviation": "NO", "city": "New Orleans, LA", "conference": "NFC", "division": "South"},
    {"name": "New York Giants", "abbreviation": "NYG", "city": "East Rutherford, NJ", "conference": "NFC", "division": "East"},
    {"name": "New York Jets", "abbreviation": "NYJ", "city": "East Rutherford, NJ", "conference": "AFC", "division": "East"},
    {"name": "Philadelphia Eagles", "abbreviation": "PHI", "city": "Philadelphia, PA", "conference": "NFC", "division": "East"},
    {"name": "Pittsburgh Steelers", "abbreviation": "PIT", "city": "Pittsburgh, PA", "conference": "AFC", "division": "North"},
    {"name": "San Francisco 49ers", "abbreviation": "SF", "city": "Santa Clara, CA", "conference": "NFC", "division": "West"},
    {"name": "Seattle Seahawks", "abbreviation": "SEA", "city": "Seattle, WA", "conference": "NFC", "division": "West"},
    {"name": "Tampa Bay Buccaneers", "abbreviation": "TB", "city": "Tampa, FL", "conference": "NFC", "division": "South"},
    {"name": "Tennessee Titans", "abbreviation": "TEN", "city": "Nashville, TN", "conference": "AFC", "division": "South"},
    {"name": "Washington Commanders", "abbreviation": "WSH", "city": "Landover, MD", "conference": "NFC", "division": "East"},
]

NFL_PROVIDER_NAMES = frozenset(team["name"] for team in NFL_TEAMS)


def seed_teams(db: DatabaseManager) -> int:
    seeded = 0
    for team in NFL_TEAMS:
        team_id = upsert_nfl_team(
            db,
            name=team["name"],
            abbreviation=team["abbreviation"],
            odds_api_name=team["name"],
            city=team["city"],
            conference=team["conference"],
            division=team["division"],
        )
        seeded += int(bool(team_id))
    print(f"Seeded {seeded}/32 NFL teams")
    return seeded


if __name__ == "__main__":
    config = load_config()
    seed_teams(DatabaseManager(config.database_url))
