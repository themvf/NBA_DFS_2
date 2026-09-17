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
    VERSION, appearances, classify, team_offensive_plays,
)
from ingest.nfl_dfs_review_digest import latest_report, target_season

_PARTICIPANTS = """SELECT game_id, play_id, team, side, role, player_id, player_name
    FROM nfl_pbp_play_participants WHERE season=%s AND week=%s"""

_MARK = {"LIKELY_REMOVED": "🔴", "OPPORTUNITY_NO_CONVERSION": "🟡",
         "NO_OPPORTUNITY": "⚪", "NORMAL": "🟢", "UNKNOWN": "❔"}


def _norm(name: str) -> str:
    """Match on surname-insensitive-ish normalisation. The two sources spell
    names differently often enough (suffixes, punctuation, initials) that an
    exact join silently drops players — and a dropped player looks identical
    to a player with no participation rows, which is the one distinction this
    whole report exists to make."""
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def _index(apps: dict) -> dict[str, object]:
    by_name: dict[str, object] = {}
    for app in apps.values():
        by_name.setdefault(_norm(app.player_name), app)
        parts = (app.player_name or "").split()
        if len(parts) >= 2:
            # "A.Loveland" and "Colston Loveland" share a surname and initial.
            by_name.setdefault(_norm(parts[0][:1] + parts[-1]), app)
    return by_name


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

    apps = appearances(part)
    by_name = _index(apps)
    team_plays = team_offensive_plays(part)

    if args.player:
        needle = args.player.lower()
        rows = [r for r in rows if needle in (r.get("name") or "").lower()]
        title = f"players matching “{args.player}”"
    else:
        scored = [r for r in rows if r.get("actual") is not None and r.get("error") is not None]
        rows = sorted(scored, key=lambda r: r["error"])[: args.limit]
        title = f"{len(rows)} largest shortfalls"

    print(f"## Removal probe — {season} week {week} · {title}")
    print(f"\n_{VERSION}. Proposals only — every verdict needs a human to confirm it. "
          "Participation rows record touches, not snaps, so silence is strong "
          "evidence at QB and weak evidence everywhere else._\n")
    if not rows:
        print("_No matching player in the report card._")
        return 0

    print("| | Player | Proj | Final | Δ | Tgt | Car | Verdict | Why |")
    print("|---|---|--:|--:|--:|--:|--:|---|---|")
    for r in rows:
        app = by_name.get(_norm(r.get("name") or ""))
        verdict = classify(
            position=r.get("position") or "",
            appearance=app,
            team_plays=team_plays.get(app.team if app else r.get("team"), []),
            receptions=((r.get("evidence") or {}).get("scoring_input") or {}).get("receptions"),
        )
        f = r.get("forecast") or {}
        e = verdict["evidence"]
        print(f"| {_MARK.get(verdict['verdict'],'')} | {r.get('name','?')} "
              f"({r.get('position','?')} · {r.get('team','?')}) "
              f"| {f.get('mean') if f.get('mean') is None else round(f['mean'],1)} "
              f"| {r.get('actual') if r.get('actual') is None else round(r['actual'],1)} "
              f"| {r.get('error') if r.get('error') is None else round(r['error'],1)} "
              f"| {e['targets']} | {e['carries']} "
              f"| {verdict['verdict']} | {verdict['reason']} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
