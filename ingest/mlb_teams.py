"""Seed the 30 MLB teams and 2025 park factors into Neon.

One-time idempotent operation — safe to re-run when park factors or
ballpark data changes.

Usage:
    python -m ingest.mlb_teams                    # seed with 2025 park factors
    python -m ingest.mlb_teams --season 2026      # seed for a different season
"""

from __future__ import annotations

import argparse
from datetime import date

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_mlb_team, upsert_mlb_park_factors

# ── 30 MLB teams ──────────────────────────────────────────────────────────────
# Abbreviations follow the Baseball Reference / DraftKings standard.
# These match what pybaseball returns for team stats and what DK uses in
# CSV exports.  The canonical abbreviation is stored in `abbreviation`;
# `dk_abbrev` is only set when DK deviates from our canonical (rare).
MLB_TEAMS: list[dict] = [
    # ── AL East ───────────────────────────────────────────────
    {"name": "New York Yankees",      "abbreviation": "NYY", "dk_abbrev": None,
     "ballpark": "Yankee Stadium",              "city": "Bronx, NY",          "division": "AL East",    "mlb_id": 147},
    {"name": "Boston Red Sox",        "abbreviation": "BOS", "dk_abbrev": None,
     "ballpark": "Fenway Park",                 "city": "Boston, MA",         "division": "AL East",    "mlb_id": 111},
    {"name": "Tampa Bay Rays",        "abbreviation": "TB",  "dk_abbrev": None,
     "ballpark": "Tropicana Field",             "city": "St. Petersburg, FL", "division": "AL East",    "mlb_id": 139},
    {"name": "Toronto Blue Jays",     "abbreviation": "TOR", "dk_abbrev": None,
     "ballpark": "Rogers Centre",               "city": "Toronto, ON",        "division": "AL East",    "mlb_id": 141},
    {"name": "Baltimore Orioles",     "abbreviation": "BAL", "dk_abbrev": None,
     "ballpark": "Camden Yards",                "city": "Baltimore, MD",      "division": "AL East",    "mlb_id": 110},
    # ── AL Central ────────────────────────────────────────────
    {"name": "Chicago White Sox",     "abbreviation": "CWS", "dk_abbrev": None,
     "ballpark": "Guaranteed Rate Field",       "city": "Chicago, IL",        "division": "AL Central", "mlb_id": 145},
    {"name": "Cleveland Guardians",   "abbreviation": "CLE", "dk_abbrev": None,
     "ballpark": "Progressive Field",           "city": "Cleveland, OH",      "division": "AL Central", "mlb_id": 114},
    {"name": "Minnesota Twins",       "abbreviation": "MIN", "dk_abbrev": None,
     "ballpark": "Target Field",                "city": "Minneapolis, MN",    "division": "AL Central", "mlb_id": 142},
    {"name": "Kansas City Royals",    "abbreviation": "KC",  "dk_abbrev": None,
     "ballpark": "Kauffman Stadium",            "city": "Kansas City, MO",    "division": "AL Central", "mlb_id": 118},
    {"name": "Detroit Tigers",        "abbreviation": "DET", "dk_abbrev": None,
     "ballpark": "Comerica Park",               "city": "Detroit, MI",        "division": "AL Central", "mlb_id": 116},
    # ── AL West ───────────────────────────────────────────────
    {"name": "Houston Astros",        "abbreviation": "HOU", "dk_abbrev": None,
     "ballpark": "Minute Maid Park",            "city": "Houston, TX",        "division": "AL West",    "mlb_id": 117},
    {"name": "Los Angeles Angels",    "abbreviation": "LAA", "dk_abbrev": None,
     "ballpark": "Angel Stadium",               "city": "Anaheim, CA",        "division": "AL West",    "mlb_id": 108},
    {"name": "Athletics",             "abbreviation": "OAK", "dk_abbrev": None,
     "ballpark": "Sutter Health Park",          "city": "Sacramento, CA",     "division": "AL West",    "mlb_id": 133},
    {"name": "Seattle Mariners",      "abbreviation": "SEA", "dk_abbrev": None,
     "ballpark": "T-Mobile Park",               "city": "Seattle, WA",        "division": "AL West",    "mlb_id": 136},
    {"name": "Texas Rangers",         "abbreviation": "TEX", "dk_abbrev": None,
     "ballpark": "Globe Life Field",            "city": "Arlington, TX",      "division": "AL West",    "mlb_id": 140},
    # ── NL East ───────────────────────────────────────────────
    {"name": "New York Mets",         "abbreviation": "NYM", "dk_abbrev": None,
     "ballpark": "Citi Field",                  "city": "Flushing, NY",       "division": "NL East",    "mlb_id": 121},
    {"name": "Atlanta Braves",        "abbreviation": "ATL", "dk_abbrev": None,
     "ballpark": "Truist Park",                 "city": "Cumberland, GA",     "division": "NL East",    "mlb_id": 144},
    {"name": "Philadelphia Phillies", "abbreviation": "PHI", "dk_abbrev": None,
     "ballpark": "Citizens Bank Park",          "city": "Philadelphia, PA",   "division": "NL East",    "mlb_id": 143},
    {"name": "Miami Marlins",         "abbreviation": "MIA", "dk_abbrev": None,
     "ballpark": "loanDepot park",              "city": "Miami, FL",          "division": "NL East",    "mlb_id": 146},
    {"name": "Washington Nationals",  "abbreviation": "WSH", "dk_abbrev": None,
     "ballpark": "Nationals Park",              "city": "Washington, D.C.",   "division": "NL East",    "mlb_id": 120},
    # ── NL Central ────────────────────────────────────────────
    {"name": "Chicago Cubs",          "abbreviation": "CHC", "dk_abbrev": None,
     "ballpark": "Wrigley Field",               "city": "Chicago, IL",        "division": "NL Central", "mlb_id": 112},
    {"name": "St. Louis Cardinals",   "abbreviation": "STL", "dk_abbrev": None,
     "ballpark": "Busch Stadium",               "city": "St. Louis, MO",      "division": "NL Central", "mlb_id": 138},
    {"name": "Milwaukee Brewers",     "abbreviation": "MIL", "dk_abbrev": None,
     "ballpark": "American Family Field",       "city": "Milwaukee, WI",      "division": "NL Central", "mlb_id": 158},
    {"name": "Cincinnati Reds",       "abbreviation": "CIN", "dk_abbrev": None,
     "ballpark": "Great American Ball Park",    "city": "Cincinnati, OH",     "division": "NL Central", "mlb_id": 113},
    {"name": "Pittsburgh Pirates",    "abbreviation": "PIT", "dk_abbrev": None,
     "ballpark": "PNC Park",                    "city": "Pittsburgh, PA",     "division": "NL Central", "mlb_id": 134},
    # ── NL West ───────────────────────────────────────────────
    {"name": "Los Angeles Dodgers",   "abbreviation": "LAD", "dk_abbrev": None,
     "ballpark": "Dodger Stadium",              "city": "Los Angeles, CA",    "division": "NL West",    "mlb_id": 119},
    {"name": "San Francisco Giants",  "abbreviation": "SF",  "dk_abbrev": None,
     "ballpark": "Oracle Park",                 "city": "San Francisco, CA",  "division": "NL West",    "mlb_id": 137},
    {"name": "San Diego Padres",      "abbreviation": "SD",  "dk_abbrev": None,
     "ballpark": "Petco Park",                  "city": "San Diego, CA",      "division": "NL West",    "mlb_id": 135},
    {"name": "Colorado Rockies",      "abbreviation": "COL", "dk_abbrev": None,
     "ballpark": "Coors Field",                 "city": "Denver, CO",         "division": "NL West",    "mlb_id": 115},
    {"name": "Arizona Diamondbacks",  "abbreviation": "ARI", "dk_abbrev": None,
     "ballpark": "Chase Field",                 "city": "Phoenix, AZ",        "division": "NL West",    "mlb_id": 109},
]

# MLB official team ID → our abbreviation.
# Used by mlb_schedule.py to map statsapi.mlb.com team IDs to mlb_teams rows.
MLB_ID_TO_ABBREV: dict[int, str] = {t["mlb_id"]: t["abbreviation"] for t in MLB_TEAMS}

# ── Park factors (2025 season) ────────────────────────────────────────────────
# Source: FanGraphs multi-year park factor averages, adjusted for 2025 context.
# Format: {abbreviation: (runs_factor, hr_factor)}
#   runs_factor > 1.0 = more runs scored than league average
#   hr_factor   > 1.0 = more home runs hit than league average
#
# NOTE: Wrigley Field (CHC) is highly wind-dependent.  The value here is the
# season-average; individual games with "wind blowing out" can push hr_factor
# to 1.20+.  A wind adjustment layer belongs in Phase 3 stats ingestion.
#
# Update annually after each season's final park factors are published on
# FanGraphs (https://www.fangraphs.com/guts.aspx?type=pf&teamid=0).
MLB_PARK_FACTORS_2025: dict[str, tuple[float, float]] = {
    # Hitter-friendly
    "COL": (1.14, 1.20),   # Coors Field — most extreme park factor in MLB
    "CIN": (1.05, 1.08),   # Great American Ball Park
    "BOS": (1.04, 1.00),   # Fenway — Green Monster inflates hits, suppresses HR
    "PHI": (1.03, 1.06),   # Citizens Bank Park
    "HOU": (1.03, 1.04),   # Minute Maid Park — Crawford Boxes short LF wall
    "CHC": (1.02, 1.01),   # Wrigley Field (season average; see note above)
    "BAL": (1.02, 1.04),   # Camden Yards
    "NYY": (1.01, 1.08),   # Yankee Stadium — short porch in RF
    "ARI": (1.01, 1.03),   # Chase Field — domed, hot/dry air favors power
    "TEX": (1.01, 1.02),   # Globe Life Field
    "ATL": (1.01, 1.02),   # Truist Park
    "CWS": (1.01, 1.04),   # Guaranteed Rate Field
    "TOR": (1.01, 1.00),   # Rogers Centre
    "LAA": (1.01, 1.00),   # Angel Stadium
    # Neutral
    "OAK": (1.00, 1.00),   # Sutter Health Park (2025 debut — using neutral)
    "MIL": (1.00, 1.02),   # American Family Field
    "KC":  (1.00, 0.98),   # Kauffman Stadium
    # Pitcher-friendly
    "MIN": (0.99, 0.97),   # Target Field — cold early-season suppresses HR
    "TB":  (0.99, 0.99),   # Tropicana Field — neutral dome
    "WSH": (0.98, 0.97),   # Nationals Park
    "CLE": (0.98, 0.96),   # Progressive Field
    "LAD": (0.98, 0.96),   # Dodger Stadium
    "NYM": (0.97, 0.88),   # Citi Field — spacious, pitcher-friendly
    "SEA": (0.97, 0.92),   # T-Mobile Park
    "STL": (0.97, 0.93),   # Busch Stadium
    "PIT": (0.97, 0.93),   # PNC Park
    "SF":  (0.96, 0.84),   # Oracle Park — marine layer, very pitcher-friendly
    "DET": (0.96, 0.86),   # Comerica Park — deep outfield
    "SD":  (0.93, 0.82),   # Petco Park — large park, ocean air
    "MIA": (0.93, 0.82),   # loanDepot park — very pitcher-friendly
}


def seed_teams(db: DatabaseManager, season: str = "2025") -> None:
    """Upsert all 30 MLB teams and park factors into Neon.

    Safe to re-run — all upserts are idempotent via ON CONFLICT DO UPDATE.
    """
    seeded = 0
    for team in MLB_TEAMS:
        logo_url = f"https://www.mlbstatic.com/team-logos/{team['mlb_id']}.svg"
        team_id = upsert_mlb_team(
            db,
            name=team["name"],
            abbreviation=team["abbreviation"],
            dk_abbrev=team["dk_abbrev"],
            ballpark=team["ballpark"],
            city=team["city"],
            division=team["division"],
            mlb_id=team["mlb_id"],
            logo_url=logo_url,
        )
        if team_id:
            pf = MLB_PARK_FACTORS_2025.get(team["abbreviation"], (1.0, 1.0))
            upsert_mlb_park_factors(
                db, team_id, season,
                runs_factor=pf[0],
                hr_factor=pf[1],
            )
            seeded += 1

    print(f"Seeded {seeded}/30 MLB teams and park factors for {season}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed MLB teams and park factors")
    parser.add_argument("--season", default=str(date.today().year), help="Season year (defaults to current year)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    seed_teams(db, season=args.season)
