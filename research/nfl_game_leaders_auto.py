"""Refresh the Game Leaders page for the next NFL week, unattended.

One run: pick the week of the next game inside seven days, reuse that week's
source capture (or take it on the first run of the week), capture fresh
availability for every game not yet started, run the batch with the production
settings, gate each game separately, and write the page file.

Started games keep their frozen pregame entry. An unstarted game whose new
forecast fails a gate keeps its previous entry when the page has one, and is
listed under `skipped` with the reason either way, so nothing disappears
silently. The page is replaced only after every check passes.

    python -m research.nfl_game_leaders_auto [--season 2026] [--draws N]
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile

from model.nfl_game_leaders import VERSION
from model.nfl_game_leaders_calibration import production_settings
from model.nfl_longest_touchdown import timestamp
from research.nfl_game_leaders_publish import publish
from research.nfl_longest_touchdown import read, write

PAGE = Path("web/src/data/game-leaders.json")
# One file per week, the last version published for it, so the page can show past weeks.
ARCHIVE = Path("web/src/data/game-leaders-weeks")
ROOT = Path("artifacts/nfl-game-leaders/auto")
EVALUATIONS = (Path("artifacts/nfl-game-leaders/final-evaluation-2025.json"),
               Path("artifacts/nfl-game-leaders/final-evaluation-2026.json"))
LOOKAHEAD = timedelta(days=7)


def current_nfl_season(now: datetime) -> int:
    return now.year if now.month >= 3 else now.year - 1


def target_week(snapshot: dict, now: datetime, season: int) -> int | None:
    upcoming = sorted((g for g in snapshot["games"] if g["season"] == season
        and timestamp(g["kickoff"]) > now), key=lambda g: timestamp(g["kickoff"]))
    if not upcoming or timestamp(upcoming[0]["kickoff"]) > now + LOOKAHEAD:
        return None
    return upcoming[0]["week"]


def take_capture(path: Path, season: int) -> dict:
    """The same capture as `research.nfl_game_leaders capture`."""
    from config import load_config
    from db.database import DatabaseManager
    from research.nfl_game_leaders import capture_pbp, full_box_capture
    from research.nfl_game_leaders_source import capture_box_verification, capture_stat_credits
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        result = capture_pbp(db, 2023, season)
    result = full_box_capture(result, 2023, season)
    result = capture_box_verification(capture_stat_credits(result, path.parent / "raw-stat-credits"))
    result["created_at"] = datetime.now(timezone.utc).isoformat()
    write(path, result)
    return result


def drop_moved_players(request: dict) -> dict:
    """Remove from a team's role scenario anyone the request rosters on the other team.

    The roles rule lists every identity in a team's recent box scores, so a player
    who moved between the two teams in this game (Ryan Miller, MIA weeks 2-3, NE in
    week 8) appears in his old team's field. The model leaves request players out
    of a team's residual field, so the two lists differ and the game fails. Only a
    zero share is dropped; a moved player carrying a share is an error, since
    removing it would silently hand his mass to someone else."""
    team_of = {p["identity"]: p["team"] for p in request.get("players", [])}
    dropped = []
    for team, actions in (request.get("role_scenarios") or {}).items():
        for action, shares in actions.items():
            for identity in [i for i in shares if team_of.get(i, team) != team]:
                if shares[identity] != 0:
                    raise ValueError(f"{identity} is rostered on {team_of[identity]} but holds "
                                     f"{shares[identity]:.3f} of {team} {action} in the role scenario")
                del shares[identity]
                dropped.append({"identity": identity, "old_team": team, "now": team_of[identity],
                                "action": action})
    if dropped:
        request["scenario_evidence"] = {**request["scenario_evidence"], "moved_players_dropped": dropped}
    return request


def capture_requests(snapshot: dict, games: list[dict]) -> dict:
    """Per-game availability, as `week-requests` does; a failed game keeps its error."""
    import psycopg2
    import requests as http
    from research.nfl_game_leaders import attach_roles, evidence_request
    from research.nfl_game_leaders_reviewed import apply_reviewed, load_reviewed
    reviewed = load_reviewed()
    out = {}
    for g in games:
        try:
            gid = g["game_id"]
            # A reviewed depth override (research/nfl_game_leaders_reviewed.py) survives every rebuild.
            out[gid] = drop_moved_players(apply_reviewed(
                gid, evidence_request(snapshot, gid),
                lambda r, gid=gid: attach_roles(snapshot, {gid: r})[gid], reviewed))
            print(json.dumps({"captured": g["game_id"]}), flush=True)
        except (ValueError, RuntimeError, http.RequestException, psycopg2.Error) as exc:
            out[g["game_id"]] = {"capture_error": str(exc)}
            print(json.dumps({"capture_error": g["game_id"], "reason": str(exc)}), flush=True)
    return out


def gate_games(batch: dict, evaluations: list[dict], sources: dict) -> tuple[dict, dict]:
    """Publish each forecast on its own so one bad game cannot block the rest."""
    passed, failed = {}, {}
    for forecast in batch["forecasts"]:
        single = {**batch, "forecasts": [forecast]}
        gid = forecast["game"]["game_id"]
        try:
            passed[gid] = publish(single, evaluations, sources)["games"][0]
        except ValueError as exc:
            failed[gid] = str(exc)
    return passed, failed


def merge(week_games: list[dict], prior: dict | None, passed: dict, failed: dict,
          batch_skipped: list[dict], now: datetime) -> tuple[list[dict], list[dict]]:
    """One entry per canonical game: new forecast, else frozen or previous entry."""
    prior_games = {g["game"]["game_id"]: g for g in (prior or {}).get("games", [])} \
        if prior and (prior["season"], prior["week"]) == (week_games[0]["season"], week_games[0]["week"]) else {}
    reasons = {s["game_id"]: s["reason"] for s in batch_skipped}
    reasons.update(failed)
    games, skipped = [], []
    for g in sorted(week_games, key=lambda g: (timestamp(g["kickoff"]), g["game_id"])):
        gid = g["game_id"]
        started = timestamp(g["kickoff"]) <= now
        if not started and gid in passed:
            games.append(passed[gid])
        elif gid in prior_games:
            games.append(prior_games[gid])
            if not started:
                skipped.append({"game_id": gid, "reason": f"kept previous forecast: {reasons.get(gid, 'no new forecast')}"})
        else:
            skipped.append({"game_id": gid, "reason": "started before any forecast was saved" if started
                            else reasons.get(gid, "no forecast produced")})
    return games, skipped


def validate(page: dict, week_games: list[dict]) -> None:
    ids = [g["game"]["game_id"] for g in page["games"]]
    if len(ids) != len(set(ids)) or not set(ids) <= {g["game_id"] for g in week_games}:
        raise ValueError("Page games must be unique canonical games of the target week")
    for g in page["games"]:
        if timestamp(g["decision_at"]) >= timestamp(g["game"]["kickoff"]):
            raise ValueError(f"Forecast not frozen before kickoff: {g['game']['game_id']}")
        if not g.get("sources"):
            raise ValueError(f"Game has no sources for the DK pricing refresh: {g['game']['game_id']}")


def replace_page(path: Path, page: dict) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
            prefix=".game-leaders-", suffix=".json", delete=False) as f:
        temporary = Path(f.name)
    try:
        temporary.unlink()
        write(temporary, page)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def archive_page(page: dict, root: Path = ARCHIVE) -> Path:
    """Keep this week's page as `{season}-w{week}.json` and list every archived week in `index.json`.

    A week's file is replaced on every publish, so it ends as the last pregame
    version (started games are frozen in the page, never rewritten)."""
    key = f"{page['season']}-w{page['week']:02d}"
    path = root / f"{key}.json"
    root.mkdir(parents=True, exist_ok=True)
    replace_page(path, page)
    index_path = root / "index.json"
    weeks = {w["key"]: w for w in (read(index_path)["weeks"] if index_path.exists() else [])}
    weeks[key] = {"key": key, "season": page["season"], "week": page["week"],
                  "decision_at": page["decision_at"], "games": len(page["games"])}
    ordered = sorted(weeks.values(), key=lambda w: (w["season"], w["week"]), reverse=True)
    replace_page(index_path, {"weeks": ordered})
    return path


def capture_dir(season: int, week: int, stamp: str) -> Path:
    return ROOT / f"{season}-w{week:02d}-{stamp}"


def reusable_capture(prior: dict | None, now: datetime, season: int) -> tuple[Path, dict, int] | None:
    """The capture the live page was built on, when it is still the target week's."""
    path = Path(((prior or {}).get("sources") or {}).get("snapshot", ""))
    if not prior or prior.get("season") != season or not path.is_file():
        return None
    snapshot = read(path)
    week = target_week(snapshot, now, season)
    return (path, snapshot, week) if week == prior.get("week") else None


def run(season: int, draws: int | None, page_path: Path = PAGE) -> str:
    started_at = datetime.now(timezone.utc)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    prior = read(page_path) if page_path.exists() else None
    # Reuse this week's capture: training history only changes week to week,
    # and each capture is ~11 MB, committed so the DK pricing refresh can
    # re-create every forecast from the repo.
    reuse = reusable_capture(prior, started_at, season)
    if reuse:
        capture_path, snapshot, week = reuse
    else:
        pending = ROOT / f"pending-{stamp}"
        snapshot = take_capture(pending / "capture.json.gz", season)
        week = target_week(snapshot, started_at, season)
        if week is None:
            return "No NFL game within seven days; page unchanged"
        folder = capture_dir(season, week, stamp)
        os.replace(pending, folder)
        capture_path = folder / "capture.json.gz"
    week_games = [g for g in snapshot["games"] if (g["season"], g["week"]) == (season, week)]
    upcoming = [g for g in week_games if timestamp(g["kickoff"]) > datetime.now(timezone.utc)]
    if not upcoming:
        return f"Week {week}: every game has started; page unchanged"
    requests_path = capture_path.parent / f"requests-{stamp}.json"
    requests = capture_requests(snapshot, upcoming)
    write(requests_path, requests)
    decision_at = datetime.now(timezone.utc)
    from research.nfl_game_leaders import batch as run_batch
    cfg = production_settings(**({"draws": draws} if draws else {}))
    result = run_batch(snapshot, season, week, decision_at.isoformat(), cfg, requests)
    if result["version"] != VERSION:
        raise ValueError("Batch version does not match the model")
    sources = {"snapshot": capture_path.as_posix(), "requests": requests_path.as_posix()}
    evaluations = [read(p) for p in EVALUATIONS]
    passed, failed = gate_games(result, evaluations, sources)
    finished = datetime.now(timezone.utc)
    games, skipped = merge(week_games, prior, passed, failed, result["skipped"], finished)
    if not games:
        raise ValueError(f"Week {week}: no game produced a publishable forecast; page unchanged. {skipped}")
    shell = publish({**result, "forecasts": []}, evaluations)
    page = {**shell, "decision_at": decision_at.isoformat(), "games": games, "skipped": skipped,
            "sources": sources}
    validate(page, week_games)
    replace_page(page_path, page)
    if page_path == PAGE:
        archive_page(page)
    fresh = sum(1 for g in games if passed.get(g["game"]["game_id"]) is g)
    summary = f"Week {week}: {len(games)} games on the page ({fresh} refreshed now), {len(skipped)} noted in skipped"
    for s in skipped:
        summary += f"\n  {s['game_id']}: {s['reason']}"
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=current_nfl_season(datetime.now(timezone.utc)))
    parser.add_argument("--draws", type=int, help="override production draws (testing only)")
    args = parser.parse_args()
    print(run(args.season, args.draws), flush=True)


if __name__ == "__main__":
    main()
