"""Seed World Cup national teams into Neon.

Unlike NBA/MLB, the soccer team list is not fully fixed: the 2026 World Cup
has 48 nations and qualification finalizes late.  This seed provides clean
FIFA codes + confederations for well-known sides so abbreviations look right;
``soccer_schedule.py`` auto-creates any nation missing from this list directly
from the odds feed (keyed on the feed's canonical name), so a game is never
dropped just because a nation wasn't pre-seeded.

One-time idempotent operation — safe to re-run.

Usage:
    python -m ingest.soccer_teams
"""

from __future__ import annotations

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_soccer_team

# Canonical name → (FIFA code, confederation).
# Names follow The Odds API / common sportsbook conventions.  When a book names
# a side differently (e.g. "Korea Republic" vs "South Korea"), the schedule
# ingest will auto-create under the feed's name — adjust this map if duplicates
# appear so the seeded metadata wins.
SOCCER_TEAMS: dict[str, tuple[str, str]] = {
    # ── Hosts ────────────────────────────────────────────────
    "USA": ("USA", "CONCACAF"),
    "Canada": ("CAN", "CONCACAF"),
    "Mexico": ("MEX", "CONCACAF"),
    # ── UEFA (Europe) ────────────────────────────────────────
    "France": ("FRA", "UEFA"),
    "England": ("ENG", "UEFA"),
    "Spain": ("ESP", "UEFA"),
    "Germany": ("GER", "UEFA"),
    "Portugal": ("POR", "UEFA"),
    "Netherlands": ("NED", "UEFA"),
    "Italy": ("ITA", "UEFA"),
    "Belgium": ("BEL", "UEFA"),
    "Croatia": ("CRO", "UEFA"),
    "Denmark": ("DEN", "UEFA"),
    "Switzerland": ("SUI", "UEFA"),
    "Austria": ("AUT", "UEFA"),
    "Serbia": ("SRB", "UEFA"),
    "Poland": ("POL", "UEFA"),
    "Ukraine": ("UKR", "UEFA"),
    "Turkey": ("TUR", "UEFA"),
    "Norway": ("NOR", "UEFA"),
    "Scotland": ("SCO", "UEFA"),
    # ── CONMEBOL (South America) ─────────────────────────────
    "Argentina": ("ARG", "CONMEBOL"),
    "Brazil": ("BRA", "CONMEBOL"),
    "Uruguay": ("URU", "CONMEBOL"),
    "Colombia": ("COL", "CONMEBOL"),
    "Ecuador": ("ECU", "CONMEBOL"),
    "Paraguay": ("PAR", "CONMEBOL"),
    # ── CONCACAF (North/Central America) ─────────────────────
    "Costa Rica": ("CRC", "CONCACAF"),
    "Panama": ("PAN", "CONCACAF"),
    "Jamaica": ("JAM", "CONCACAF"),
    # ── CAF (Africa) ─────────────────────────────────────────
    "Morocco": ("MAR", "CAF"),
    "Senegal": ("SEN", "CAF"),
    "Nigeria": ("NGA", "CAF"),
    "Egypt": ("EGY", "CAF"),
    "Algeria": ("ALG", "CAF"),
    "Tunisia": ("TUN", "CAF"),
    "Ghana": ("GHA", "CAF"),
    "Cameroon": ("CMR", "CAF"),
    "Ivory Coast": ("CIV", "CAF"),
    "South Africa": ("RSA", "CAF"),
    # ── AFC (Asia) ───────────────────────────────────────────
    "Japan": ("JPN", "AFC"),
    "South Korea": ("KOR", "AFC"),
    "Iran": ("IRN", "AFC"),
    "Australia": ("AUS", "AFC"),
    "Saudi Arabia": ("KSA", "AFC"),
    "Qatar": ("QAT", "AFC"),
    "Uzbekistan": ("UZB", "AFC"),
    "Jordan": ("JOR", "AFC"),
    # ── OFC (Oceania) ────────────────────────────────────────
    "New Zealand": ("NZL", "OFC"),
}


def seed_teams(db: DatabaseManager) -> None:
    """Upsert known World Cup national teams.  Idempotent."""
    seeded = 0
    for name, (code, confed) in SOCCER_TEAMS.items():
        team_id = upsert_soccer_team(
            db,
            name=name,
            abbreviation=code,
            confederation=confed,
        )
        if team_id:
            seeded += 1
    print(f"Seeded {seeded}/{len(SOCCER_TEAMS)} soccer teams "
          f"(remaining nations auto-create from the odds feed)")


if __name__ == "__main__":
    config = load_config()
    db = DatabaseManager(config.database_url)
    seed_teams(db)
