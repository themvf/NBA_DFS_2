"""Seed soccer_groups with the official 2026 FIFA World Cup group assignments.

Overrides the algorithm-derived assignments in soccer_futures.derive_groups()
which can assign wrong letters when some group-stage fixtures are missing from
the DB (the K4 detector needs all 6 round-robin pairings present).

Official groups confirmed from live tournament standings (June 2026).
Run once after DB is seeded with teams + fixtures:
    python -m ingest.seed_soccer_groups
"""

from __future__ import annotations

import logging

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# Official 2026 FIFA World Cup groups (A–L, 48 teams, 12 groups of 4).
# Team names must match soccer_teams.name exactly.
OFFICIAL_GROUPS: dict[str, list[str]] = {
    "A": ["Mexico", "South Korea", "Czech Republic", "South Africa"],
    "B": ["Switzerland", "Canada", "Qatar", "Bosnia & Herzegovina"],
    "C": ["Scotland", "Morocco", "Brazil", "Haiti"],
    "D": ["USA", "Australia", "Turkey", "Paraguay"],
    "E": ["Germany", "Ivory Coast", "Ecuador", "Curaçao"],
    "F": ["Sweden", "Japan", "Netherlands", "Tunisia"],
    "G": ["New Zealand", "Iran", "Belgium", "Egypt"],
    "H": ["Uruguay", "Saudi Arabia", "Spain", "Cape Verde"],
    "I": ["Norway", "France", "Senegal", "Iraq"],
    "J": ["Argentina", "Austria", "Jordan", "Algeria"],
    "K": ["Colombia", "DR Congo", "Portugal", "Uzbekistan"],
    "L": ["England", "Ghana", "Panama", "Croatia"],
}


def seed_groups(db: DatabaseManager) -> None:
    """Clear and re-seed soccer_groups with the official 2026 WC structure."""
    # Build name → team_id map.
    rows = db.execute("SELECT team_id, name FROM soccer_teams")
    by_name: dict[str, int] = {r["name"]: r["team_id"] for r in rows}

    # Wipe existing assignments so re-running is idempotent.
    db.execute("DELETE FROM soccer_groups")
    logger.info("Cleared soccer_groups")

    inserted = 0
    for label, teams in OFFICIAL_GROUPS.items():
        for team_name in teams:
            team_id = by_name.get(team_name)
            if team_id is None:
                logger.warning("Team not found in soccer_teams: %r (group %s)", team_name, label)
                continue
            db.execute(
                "INSERT INTO soccer_groups (team_id, group_label, derived_at) "
                "VALUES (%s, %s, NOW()) "
                "ON CONFLICT (team_id) DO UPDATE SET group_label = EXCLUDED.group_label, derived_at = NOW()",
                (team_id, label),
            )
            inserted += 1

    print(f"Seeded {inserted} team-group assignments across {len(OFFICIAL_GROUPS)} groups")

    # Verify.
    rows = db.execute(
        "SELECT sg.group_label, st.name "
        "FROM soccer_groups sg JOIN soccer_teams st ON st.team_id = sg.team_id "
        "ORDER BY sg.group_label, st.name"
    )
    from collections import defaultdict
    groups: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        groups[r["group_label"]].append(r["name"])
    print("\nVerification:")
    for lbl in sorted(groups):
        print(f"  Group {lbl}: {', '.join(groups[lbl])}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = load_config()
    db = DatabaseManager(config.database_url)
    seed_groups(db)
