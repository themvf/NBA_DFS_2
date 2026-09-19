"""Why did these players miss their projection — did they leave, or just not produce?

Read-only. Joins the frozen weekly report card to play-by-play participation
and prints one proposed verdict per player for a human to confirm or reject.
It writes nothing and changes no projection or score.

Usage:
    python -m ingest.nfl_dfs_removal_probe --season 2026 --week 1
    python -m ingest.nfl_dfs_removal_probe --player Loveland
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_removal import (
    VERSION, appearances, classify, find, injuries_for, injury_events,
    team_offensive_plays,
)
from ingest.nfl_dfs_review_digest import latest_report, target_season

_PARTICIPANTS = """SELECT game_id, play_id, team, side, role, player_name
    FROM nfl_pbp_play_participants
    WHERE season=%s AND week=%s AND role IN ('passer','rusher','receiver')"""

# Quarter comes from the play, and the injured player's NAME comes from the
# play text -- `injury_on_play` is derived from the same phrase but drops the
# name, which is the only part that lets an injury reach a projection. The
# description filter keeps this to the handful of plays that carry one.
_PLAYS = """SELECT game_id, play_id, quarter, clock FROM nfl_pbp_archetypes
    WHERE season=%s AND week=%s"""
_INJURY_PLAYS = """SELECT game_id, play_id, quarter, clock, description
    FROM nfl_pbp_archetypes
    WHERE season=%s AND week=%s AND description ILIKE %s"""

_MARK = {"INJURED_OUT": "🔴", "LAST_SEEN_EARLY": "🟠", "NO_OPPORTUNITY": "⚪",
         "INJURED_RETURNED": "🟡", "PLAYED_LATE": "🟢"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--variant", default="production")
    parser.add_argument("--player", help="Substring search; overrides the worst-misses list.")
    parser.add_argument("--limit", type=int, default=15)
    args = parser.parse_args()

    season = target_season(args.season, datetime.now(timezone.utc))
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report, week = latest_report(db, season, args.week)
    if report is None:
        print(f"## Removal probe — {season}\n\n_No stored report card._")
        return 0

    rows = [r for r in report.get("rows", []) if r.get("variant") == args.variant]
    part = db.execute(_PARTICIPANTS, (season, week))
    if not part:
        print(f"## Removal probe — {season} week {week}\n\n"
              "_No play-by-play participation rows for this week. "
              "`refresh_nfl_pbp_archetypes` has not run for it yet._")
        return 0

    plays = db.execute(_PLAYS, (season, week))
    quarters = {(str(p["game_id"]), int(p["play_id"])): p["quarter"] for p in plays}
    events = injury_events(db.execute(_INJURY_PLAYS, (season, week, "%was injured during the play%")))

    index = appearances(part, quarters)
    team_plays = team_offensive_plays(part)
    # The report card's game_id is our internal numeric id, not the nflverse
    # text one, so a player is located by (team, name) across the week's games.
    games_by_team: dict[str, set[str]] = {}
    for row in part:
        games_by_team.setdefault(str(row["team"]), set()).add(str(row["game_id"]))

    def locate(name: str, team: str | None):
        """The one game this player appeared in, with his footprint."""
        for game in sorted(games_by_team.get(str(team), set())):
            found = find(index, game, name)
            if found is not None:
                return game, found
        # Team abbreviations can disagree between sources; fall back to a
        # league-wide search rather than reporting a real player as absent.
        for game in sorted({g for games in games_by_team.values() for g in games}):
            found = find(index, game, name)
            if found is not None:
                return game, found
        return None, None

    if args.player:
        needle = args.player.lower()
        rows = [r for r in rows if needle in (r.get("name") or "").lower()]
        title = f"players matching \u201c{args.player}\u201d"
    else:
        scored = [r for r in rows if r.get("actual") is not None and r.get("error") is not None]
        rows = sorted(scored, key=lambda r: r["error"])[: args.limit]
        title = f"{len(rows)} largest shortfalls"

    print(f"## Removal probe — {season} week {week} · {title}")
    print(f"\n_{VERSION}. {len(part)} participation rows, {len(events)} named injuries "
          f"this week. Proposals only — every verdict needs a human to confirm it._\n")
    print("_🔴 injured out · 🟠 last seen early · 🟡 injured, returned · "
          "🟢 played late · ⚪ no opportunity_\n")
    if not rows:
        print("_No matching player in the report card._")
        return 0

    print("| | Player | Proj | Final | Δ | Tgt | Car | Q | Verdict | Why |")
    print("|---|---|--:|--:|--:|--:|--:|--:|---|---|")
    num = lambda v: "—" if v is None else f"{v:.1f}"
    for r in rows:
        game, app = locate(r.get("name") or "", r.get("team"))
        verdict = classify(
            position=r.get("position") or "",
            appearance=app,
            team_plays=team_plays.get((game or "", str(app.team if app else r.get("team"))), []),
            injuries=injuries_for(events, game, r.get("name") or "") if game else (),
        )
        f, e = r.get("forecast") or {}, verdict["evidence"]
        print(f"| {_MARK.get(verdict['verdict'],'')} | {r.get('name','?')} "
              f"({r.get('position','?')} · {r.get('team','?')}) "
              f"| {num(f.get('mean'))} | {num(r.get('actual'))} | {num(r.get('error'))} "
              f"| {e['targets']} | {e['carries']} "
              f"| {e['last_quarter'] if e['last_quarter'] is not None else '—'} "
              f"| {verdict['verdict']} | {verdict['reason']} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
