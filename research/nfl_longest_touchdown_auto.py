"""Refresh the next NFL week with games still ahead of kickoff.

The tracked index is replaced only after complete source capture, modeling and
publication checks. Started games retain their frozen pregame forecast.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile

from model.nfl_longest_touchdown import Settings, timestamp
from research.nfl_longest_touchdown import capture, read, write
from research.nfl_longest_touchdown_weekly import build_week, season_index


INDEX = Path("web/src/data/longest-touchdown-weeks.json")


def current_nfl_season(now: datetime) -> int:
    return now.year if now.month >= 3 else now.year - 1


def target_week(snapshot: dict, now: datetime, season: int) -> int | None:
    upcoming = sorted((g for g in snapshot["games"] if g["season"] == season
        and timestamp(g["kickoff"]) > now), key=lambda g: timestamp(g["kickoff"]))
    if not upcoming or timestamp(upcoming[0]["kickoff"]) > now + timedelta(days=7):
        return None
    return upcoming[0]["week"]


def prior_started(index: dict | None, season: int, week: int, now: datetime) -> list[dict]:
    weeks = index["weeks"] if index and index["season"] == season else []
    saved = next((item for item in weeks if item["week"] == week), None)
    return [game for game in saved["games"] if timestamp(game["game"]["kickoff"]) <= now] if saved else []


def validate(week: dict, snapshot: dict, requests: dict, now: datetime) -> None:
    games = week["games"]
    canonical = {g["game_id"] for g in snapshot["games"]
        if (g["season"], g["week"]) == (week["season"], week["week"])}
    if not canonical or {g["game"]["game_id"] for g in games} != canonical or len(games) != len(canonical):
        raise ValueError("Publication does not cover every canonical game exactly once")
    for game in games:
        gid = game["game"]["game_id"]
        kickoff = timestamp(game["game"]["kickoff"])
        if timestamp(game["decisionAt"]) >= kickoff:
            raise ValueError(f"Forecast was not captured before kickoff: {gid}")
        share = sum(p["longestShare"] for p in game["players"] + game["residual"]) + game["noTd"]
        if abs(share - 1) > 1e-7:
            raise ValueError(f"Longest-touchdown field does not sum to one: {gid}")
        if kickoff <= now:
            continue
        evidence = requests.get(gid)
        if evidence is None or evidence.get("capture_error"):
            raise ValueError(f"Current evidence missing for upcoming game: {gid}")
        out = {p["identity"] for p in evidence["players"] if p["status"] == "out"}
        if out.intersection(game.get("forecastRosterIds", [])):
            raise ValueError(f"A dual-source Out player remains modeled: {gid}")


def refresh(snapshot: dict, prior_index: dict | None, now: datetime, season: int,
            draws: int, request_capture) -> dict | None:
    week = target_week(snapshot, now, season)
    if week is None:
        return None
    games = [g for g in snapshot["games"] if (g["season"], g["week"]) == (season, week)]
    requests = {}
    for game in games:
        if timestamp(game["kickoff"]) > now:
            print(f"Capturing availability {game['game_id']}", flush=True)
            requests[game["game_id"]] = request_capture(snapshot, game["game_id"])
    publication = build_week(snapshot, requests, season, week,
        Settings(draws=draws, newcomer_reserve=True), prior_started(prior_index, season, week, now))
    validate(publication, snapshot, requests, now)
    return season_index(publication, prior_index)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=current_nfl_season(datetime.now(timezone.utc)))
    parser.add_argument("--draws", type=int, default=500)
    parser.add_argument("--index", type=Path, default=INDEX)
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/nfl-longest-td/auto"))
    args = parser.parse_args()
    from config import load_config
    from db.database import DatabaseManager
    from research.nfl_game_leaders import evidence_request

    now = datetime.now(timezone.utc)
    prior = read(args.index) if args.index.exists() else None
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        snapshot = capture(db, 2023, args.season)
    args.artifacts.mkdir(parents=True, exist_ok=True)
    stem = f"{args.season}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    write(args.artifacts / f"{stem}-capture.json.gz", snapshot)
    selected_week = target_week(snapshot, now, args.season)
    next_index = refresh(snapshot, prior, now, args.season, args.draws, evidence_request)
    if next_index is None:
        print("No game within seven days; no publication change")
        return
    finished = datetime.now(timezone.utc)
    if any(g["season"] == args.season and g["week"] == selected_week
            and now < timestamp(g["kickoff"]) <= finished for g in snapshot["games"]):
        raise ValueError("A game kicked off during the refresh; publication withheld")
    write(args.artifacts / f"{stem}-index.json", next_index)
    args.index.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=args.index.parent,
            prefix=".longest-touchdown-", suffix=".json", delete=False) as file:
        temporary = Path(file.name)
    try:
        temporary.unlink()
        write(temporary, next_index)
        os.replace(temporary, args.index)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Published {len(next_index['weeks'][-1]['games'])} games for Week {next_index['weeks'][-1]['week']}")


if __name__ == "__main__":
    main()
