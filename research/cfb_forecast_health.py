"""Read-only checks of upcoming CFB forecast inputs and publication receipts."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from research.cfb_forecast_v1 import MIN_CURRENT_GAMES, load_rows
from ingest.cfb_plays import MIN_COMPLETE_GAME_DRIVES, MIN_COMPLETE_GAME_PPA_PLAYS, football_season_year

VERSIONS = ("cfb-score-context-v1", "cfb-score-possession-v2", "cfb-score-opponent-ppa-v3")


def assess(games, plays, drives, forecasts, now, since=None):
    failures, excluded, checked = [], [], []
    for game in games:
        kickoff = game["commence_time"]
        if game["completed"] or not 0 < (kickoff - now).total_seconds() <= 86400:
            continue
        counts = {}
        missing = []
        for team in (game["home_team_id"], game["away_team_id"]):
            previous = [g for g in games if g["season"] == game["season"]
                        and g["completed"] and g["commence_time"] < now
                        and team in (g["home_team_id"], g["away_team_id"])]
            counts[team] = len(previous)
            for prior in previous:
                ids = (prior["home_team_id"], prior["away_team_id"])
                ppa = sum(plays.get((prior["id"], t), {}).get("ppa_plays", 0) for t in ids)
                ndrives = sum(drives.get((prior["id"], t), 0) for t in ids)
                if (prior["home_score"] is None or prior["away_score"] is None
                        or ppa < MIN_COMPLETE_GAME_PPA_PLAYS or ndrives < MIN_COMPLETE_GAME_DRIVES
                        or any(plays.get((prior["id"], t), {}).get("ppa_plays", 0) < 30 or not drives.get((prior["id"], t)) for t in ids)):
                    missing.append(prior["id"])
        detail = {"game_id": game["id"], "kickoff": kickoff, "expected_current_games": counts,
                  "missing_input_game_ids": sorted(set(missing))}
        checked.append(detail)
        if missing:
            failures.append({**detail, "reason": "incomplete prior-game scores, PPA or drives"})
        if min(counts.values()) < MIN_CURRENT_GAMES:
            excluded.append({**detail, "reason": "insufficient current-season FBS games"})
            continue
        if since is not None:
            for version in VERSIONS:
                rows = [f for f in forecasts if f["game_id"] == game["id"] and f["version"] == version
                        and f["kickoff"] == kickoff and since <= f["generated_at"] <= now
                        and f["generated_at"] < kickoff]
                if not rows:
                    failures.append({"game_id": game["id"], "version": version, "reason": "no forecast published during this refresh"})
                    continue
                f = max(rows, key=lambda f: f["generated_at"])
                if (f["home_current_games"] != counts[game["home_team_id"]]
                        or f["away_current_games"] != counts[game["away_team_id"]]):
                    failures.append({"game_id": game["id"], "version": version, "reason": "forecast omits eligible completed games"})
    return {"checked_at": now, "refresh_started": since, "status": "fail" if failures else "pass",
            "games": checked, "excluded": excluded, "failures": failures,
            "limitations": ["Injuries and weather are not model inputs.", "Freshness is not predictive validation."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", type=lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    now = datetime.now(timezone.utc)
    games, plays, drives, _ = load_rows(db, football_season_year())
    forecasts = db.execute("""SELECT f.*, r.version, r.generated_at FROM cfb_game_forecasts f
        JOIN cfb_forecast_runs r ON r.id=f.run_id WHERE r.generated_at >= %s""", (args.since,)) if args.since else []
    report = assess(games, plays, drives, forecasts, now, args.since)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
    print(json.dumps(report, default=str))
    if report["failures"]:
        raise SystemExit("CFB forecast health failed; see saved report for affected games")


if __name__ == "__main__":
    main()
