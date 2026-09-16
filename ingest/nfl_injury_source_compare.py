"""Compare Sleeper and FantasyPros injury status for one game week. Read-only.

Answers one question before the pre-kickoff availability rule picks a source:
do the two feeds disagree, and about whom? Writes nothing and runs no DDL.

Usage:
    python -m ingest.nfl_injury_source_compare [--season 2026] [--week 1]
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager

# Statuses the availability rule would zero. The disagreements that matter are
# the ones that cross this line, not OUT-vs-IR bookkeeping differences.
OUT_CLASS = {"OUT", "IR", "PUP", "NFI", "SUSPENDED"}


def target_season(value, now):
    return value or (now.year - 1 if now.month <= 3 else now.year)


def latest_per_player(db, season, source, dataset_like):
    """One row per player: the newest observation from that source for the week."""
    return db.execute(
        """SELECT DISTINCT ON (o.player_id)
                  o.player_id, p.canonical_name, p.position, p.team_abbrev,
                  o.normalized_status, o.source_status, o.practice_status,
                  s.fetched_at, s.dataset
           FROM ff_player_injury_observations o
           JOIN ff_source_snapshots s ON s.id = o.source_snapshot_id
           JOIN ff_players p ON p.id = o.player_id
           WHERE o.season = %s AND o.source = %s AND s.dataset LIKE %s
             AND p.position = ANY(%s)
           ORDER BY o.player_id, s.fetched_at DESC, o.id DESC""",
        (season, source, dataset_like, ["QB", "RB", "WR", "TE"]),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int, default=1)
    args = parser.parse_args()
    season = target_season(args.season, datetime.now(timezone.utc))
    db = DatabaseManager(load_config().database_url, initialize_schema=False)

    fp = {r["player_id"]: r for r in latest_per_player(
        db, season, "fantasypros", f"game-week-injuries-v2-{season}-{args.week}%")}
    # Sleeper is not week-scoped; take everything this season and note the caveat.
    sl = {r["player_id"]: r for r in latest_per_player(db, season, "sleeper", "%")}

    print(f"## Injury source comparison — {season} week {args.week}\n")
    print(f"FantasyPros week-scoped rows: **{len(fp)}** · Sleeper rows (season, not week-scoped): **{len(sl)}**\n")
    if not fp and not sl:
        print("_No injury observations stored for either source. Nothing to compare._")
        return 0

    both = sorted(set(fp) & set(sl))
    print(f"Players present in both feeds: **{len(both)}** · "
          f"FantasyPros only: {len(set(fp) - set(sl))} · Sleeper only: {len(set(sl) - set(fp))}\n")

    agree, differ, crossing = 0, [], []
    for pid in both:
        a, b = fp[pid], sl[pid]
        if a["normalized_status"] == b["normalized_status"]:
            agree += 1
            continue
        differ.append((a, b))
        # Only a disagreement that crosses the zeroing line changes a projection.
        if (a["normalized_status"] in OUT_CLASS) != (b["normalized_status"] in OUT_CLASS):
            crossing.append((a, b))

    rate = f"{agree / len(both) * 100:.1f}%" if both else "—"
    print(f"**Agreement on normalized status: {agree}/{len(both)} ({rate})**\n")
    print(f"**Disagreements that cross the OUT-class line: {len(crossing)}** "
          f"— these are the only ones that would change a projection.\n")

    if crossing:
        print("| Player | Pos | Team | FantasyPros | Sleeper |")
        print("|---|---|---|---|---|")
        for a, b in crossing[:40]:
            print(f"| {a['canonical_name']} | {a['position']} | {a['team_abbrev'] or '—'} "
                  f"| {a['normalized_status']} | {b['normalized_status']} |")
        print()

    if differ:
        pairs = Counter((a["normalized_status"], b["normalized_status"]) for a, b in differ)
        print("Other status pairs seen (FantasyPros → Sleeper):\n")
        print("| FantasyPros | Sleeper | n |")
        print("|---|---|--:|")
        for (x, y), n in pairs.most_common(15):
            print(f"| {x} | {y} | {n} |")
        print()

    for label, rows in (("FantasyPros", fp), ("Sleeper", sl)):
        out = [r for r in rows.values() if r["normalized_status"] in OUT_CLASS]
        print(f"{label}: **{len(out)}** skill-position players in the OUT class.")
    print("\n_Caveat: Sleeper observations are not week-scoped, so its row is the latest "
          "of the season rather than the one that stood at this week's kickoff. "
          "Treat a disagreement as a lead to check, not a proven provider error._")
    return 0


if __name__ == "__main__":
    sys.exit(main())
