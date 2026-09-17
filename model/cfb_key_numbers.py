"""Audit the key numbers the CFB `key_cross` detector fires on.

`_cfb_market_signals` flags a spread crossing 3, 7, 10 or 14 -- a set inherited
from the NFL, where this repo's other detectors were written. A crossing only
matters if real margins pile up on that number, so the set is a claim about the
CFB margin distribution that had never been checked against it.

This is an audit, not a signal. It reads completed games and reports where the
mass actually is. Nothing here changes a detector; a change to DETECTOR_KEY_
NUMBERS is a trigger change and starts a new evidence cohort, per the standing
rule that a detector's trigger stays frozen while it is being graded.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter

from config import load_config
from db.database import DatabaseManager

AUDIT_VERSION = "cfb-key-numbers-v1"

# What the live detector actually crosses on. Read, never written, by this module.
DETECTOR_KEY_NUMBERS = (3, 7, 10, 14)


def margin_counts(rows: list[dict]) -> Counter:
    """Absolute final margins. Ties are excluded -- a 0 margin crosses nothing."""
    counts: Counter = Counter()
    for row in rows:
        home, away = row.get("home_score"), row.get("away_score")
        if home is None or away is None:
            continue
        margin = abs(int(home) - int(away))
        if margin:
            counts[margin] += 1
        # A 0 margin means the game is unresolved or a genuine tie; either way it
        # carries no information about which numbers a spread should respect.
    return counts


def rank_table(counts: Counter, top: int = 10) -> list[dict]:
    total = sum(counts.values())
    if not total:
        return []
    return [
        {
            "rank": i,
            "margin": margin,
            "games": games,
            "share_pct": round(100 * games / total, 2),
            "in_detector_set": margin in DETECTOR_KEY_NUMBERS,
        }
        for i, (margin, games) in enumerate(counts.most_common(top), 1)
    ]


def audit_counts(counts: Counter, per_season: dict[int, Counter] | None = None) -> dict:
    """Grade the detector's set against an observed margin distribution."""
    total = sum(counts.values())
    if not total:
        return {"audit_version": AUDIT_VERSION, "games": 0, "verdict": "NO_DATA"}

    ordered = [margin for margin, _ in counts.most_common()]
    ranks = {margin: i for i, margin in enumerate(ordered, 1)}
    in_set_shares = {k: 100 * counts.get(k, 0) / total for k in DETECTOR_KEY_NUMBERS}
    weakest_in_set = min(in_set_shares.values())

    # A number outside the set that carries more mass than the weakest member is
    # the specific defect this audit exists to find.
    outranking = [
        {"margin": m, "share_pct": round(100 * c / total, 2)}
        for m, c in counts.most_common()
        if m not in DETECTOR_KEY_NUMBERS and 100 * c / total > weakest_in_set
    ]

    stability = None
    if per_season:
        matches = sum(
            1 for season_counts in per_season.values()
            if {m for m, _ in season_counts.most_common(4)} == set(DETECTOR_KEY_NUMBERS)
        )
        top2 = sum(
            1 for season_counts in per_season.values()
            if {m for m, _ in season_counts.most_common(2)} == {3, 7}
        )
        stability = {
            "seasons": len(per_season),
            "exact_top4_match": matches,
            "top2_is_3_and_7": top2,
        }

    return {
        "audit_version": AUDIT_VERSION,
        "games": total,
        "top_margins": rank_table(counts),
        "detector_set": {
            str(k): {"share_pct": round(v, 2), "rank": ranks.get(k)}
            for k, v in in_set_shares.items()
        },
        "outranking_excluded_numbers": outranking,
        "set_total_mass_pct": round(sum(in_set_shares.values()), 2),
        "stability": stability,
        "verdict": "SET_CONFIRMED" if not outranking else "SET_INCOMPLETE",
    }


def _load(db: DatabaseManager, *, fbs_only: bool) -> tuple[Counter, dict[int, Counter]]:
    filters = [
        "completed=TRUE", "home_score IS NOT NULL", "away_score IS NOT NULL",
    ]
    if fbs_only:
        # cfb_teams carries classification; fall back to all games when absent.
        filters.append(
            "EXISTS (SELECT 1 FROM cfb_teams t WHERE t.team_id=m.home_team_id "
            "AND t.classification='fbs')"
        )
        filters.append(
            "EXISTS (SELECT 1 FROM cfb_teams t WHERE t.team_id=m.away_team_id "
            "AND t.classification='fbs')"
        )
    rows = db.execute(
        f"SELECT season, home_score, away_score FROM cfb_matchups m "
        f"WHERE {' AND '.join(filters)}"
    )
    per_season: dict[int, Counter] = {}
    for row in rows:
        per_season.setdefault(int(row["season"]), Counter())
        margin = abs(int(row["home_score"]) - int(row["away_score"]))
        if margin:
            per_season[int(row["season"])][margin] += 1
    return margin_counts(rows), per_season


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all-divisions", action="store_true",
                        help="include FCS opponents (default: FBS vs FBS only)")
    args = parser.parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    counts, per_season = _load(db, fbs_only=not args.all_divisions)
    print(json.dumps(audit_counts(counts, per_season), indent=2))


if __name__ == "__main__":
    main()
