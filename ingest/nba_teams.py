"""Seed the 30 NBA teams into the teams table.

One-time idempotent operation. Uses ON CONFLICT DO UPDATE so it's safe to
re-run — useful when logo URLs or conference/division data changes.

Usage:
    python -m ingest.nba_teams
"""

from __future__ import annotations

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_nba_team

# NBA numeric team IDs used for CDN logo URLs.
# Format: https://cdn.nba.com/logos/nba/{nba_id}/global/L/logo.svg
NBA_TEAMS = [
    # ── Eastern Conference ─────────────────────────────────────
    # Atlantic
    {"name": "Boston Celtics",        "abbreviation": "BOS", "conference": "East", "division": "Atlantic",  "nba_id": 1610612738},
    {"name": "Brooklyn Nets",         "abbreviation": "BKN", "conference": "East", "division": "Atlantic",  "nba_id": 1610612751},
    {"name": "New York Knicks",       "abbreviation": "NYK", "conference": "East", "division": "Atlantic",  "nba_id": 1610612752},
    {"name": "Philadelphia 76ers",    "abbreviation": "PHI", "conference": "East", "division": "Atlantic",  "nba_id": 1610612755},
    {"name": "Toronto Raptors",       "abbreviation": "TOR", "conference": "East", "division": "Atlantic",  "nba_id": 1610612761},
    # Central
    {"name": "Chicago Bulls",         "abbreviation": "CHI", "conference": "East", "division": "Central",   "nba_id": 1610612741},
    {"name": "Cleveland Cavaliers",   "abbreviation": "CLE", "conference": "East", "division": "Central",   "nba_id": 1610612739},
    {"name": "Detroit Pistons",       "abbreviation": "DET", "conference": "East", "division": "Central",   "nba_id": 1610612765},
    {"name": "Indiana Pacers",        "abbreviation": "IND", "conference": "East", "division": "Central",   "nba_id": 1610612754},
    {"name": "Milwaukee Bucks",       "abbreviation": "MIL", "conference": "East", "division": "Central",   "nba_id": 1610612749},
    # Southeast
    {"name": "Atlanta Hawks",         "abbreviation": "ATL", "conference": "East", "division": "Southeast", "nba_id": 1610612737},
    {"name": "Charlotte Hornets",     "abbreviation": "CHA", "conference": "East", "division": "Southeast", "nba_id": 1610612766},
    {"name": "Miami Heat",            "abbreviation": "MIA", "conference": "East", "division": "Southeast", "nba_id": 1610612748},
    {"name": "Orlando Magic",         "abbreviation": "ORL", "conference": "East", "division": "Southeast", "nba_id": 1610612753},
    {"name": "Washington Wizards",    "abbreviation": "WAS", "conference": "East", "division": "Southeast", "nba_id": 1610612764},

    # ── Western Conference ─────────────────────────────────────
    # Northwest
    {"name": "Denver Nuggets",        "abbreviation": "DEN", "conference": "West", "division": "Northwest", "nba_id": 1610612743},
    {"name": "Minnesota Timberwolves","abbreviation": "MIN", "conference": "West", "division": "Northwest", "nba_id": 1610612750},
    {"name": "Oklahoma City Thunder", "abbreviation": "OKC", "conference": "West", "division": "Northwest", "nba_id": 1610612760},
    {"name": "Portland Trail Blazers","abbreviation": "POR", "conference": "West", "division": "Northwest", "nba_id": 1610612757},
    {"name": "Utah Jazz",             "abbreviation": "UTA", "conference": "West", "division": "Northwest", "nba_id": 1610612762},
    # Pacific
    {"name": "Golden State Warriors", "abbreviation": "GSW", "conference": "West", "division": "Pacific",   "nba_id": 1610612744},
    {"name": "LA Clippers",           "abbreviation": "LAC", "conference": "West", "division": "Pacific",   "nba_id": 1610612746},
    {"name": "Los Angeles Lakers",    "abbreviation": "LAL", "conference": "West", "division": "Pacific",   "nba_id": 1610612747},
    {"name": "Phoenix Suns",          "abbreviation": "PHX", "conference": "West", "division": "Pacific",   "nba_id": 1610612756},
    {"name": "Sacramento Kings",      "abbreviation": "SAC", "conference": "West", "division": "Pacific",   "nba_id": 1610612758},
    # Southwest
    {"name": "Dallas Mavericks",      "abbreviation": "DAL", "conference": "West", "division": "Southwest", "nba_id": 1610612742},
    {"name": "Houston Rockets",       "abbreviation": "HOU", "conference": "West", "division": "Southwest", "nba_id": 1610612745},
    {"name": "Memphis Grizzlies",     "abbreviation": "MEM", "conference": "West", "division": "Southwest", "nba_id": 1610612763},
    {"name": "New Orleans Pelicans",  "abbreviation": "NOP", "conference": "West", "division": "Southwest", "nba_id": 1610612740},
    {"name": "San Antonio Spurs",     "abbreviation": "SAS", "conference": "West", "division": "Southwest", "nba_id": 1610612759},
]

# NBA team ID → abbreviation (used by nba_schedule.py to map ScoreboardV2 IDs)
NBA_ID_TO_ABBREV: dict[int, str] = {t["nba_id"]: t["abbreviation"] for t in NBA_TEAMS}


def seed_teams(db: DatabaseManager) -> None:
    for team in NBA_TEAMS:
        logo_url = f"https://cdn.nba.com/logos/nba/{team['nba_id']}/global/L/logo.svg"
        upsert_nba_team(
            db,
            name=team["name"],
            abbreviation=team["abbreviation"],
            conference=team["conference"],
            division=team["division"],
            logo_url=logo_url,
        )
    print(f"Seeded {len(NBA_TEAMS)} NBA teams.")


if __name__ == "__main__":
    config = load_config()
    db = DatabaseManager(config.database_url)
    seed_teams(db)
