"""Build an immutable, multi-game Longest Touchdown publication.

Use week-matched, dual-provider requests from ``research.nfl_game_leaders``.
The request's unresolved players are conditional participants, not confirmed
game-day actives. Previously published games can be carried forward unchanged.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from model.nfl_longest_touchdown import Settings, prepare, simulate, timestamp
from research.nfl_longest_touchdown import read, write
from research.nfl_longest_touchdown_publish import publish


GAME_KEYS = ("game_id", "season", "week", "kickoff", "away", "home")
OUT = {"OUT", "IR", "PUP", "NFI", "SUSPENDED"}


def canonical_game(game: dict) -> dict:
    return {key: game[key] for key in GAME_KEYS}


def current_role_support(snapshot: dict, request: dict) -> dict:
    """Count eligible current-season opportunities before the decision time."""
    rows, _ = prepare(snapshot, request["decision_at"])
    game = request["game"]
    teams = {game["away"], game["home"]}
    counts = Counter((row["team"], row["actor"]) for row in rows
        if row["season"] == game["season"] and row["team"] in teams and row["actor"])
    return {p["identity"]: counts[(p["team"], p["identity"])] for p in request["players"]}


def model_request(evidence: dict, game: dict) -> dict:
    if evidence.get("capture_error") or not evidence.get("dual_depth") or "week_injuries" not in evidence:
        raise ValueError(f"Missing dual-provider evidence for {game['game_id']}")
    saved = canonical_game(evidence["game"])
    if any(saved[key] != game[key] for key in GAME_KEYS if key != "kickoff") or timestamp(saved["kickoff"]) != timestamp(game["kickoff"]):
        raise ValueError(f"Request does not match canonical game {game['game_id']}")
    decision = evidence["decision_at"]
    if timestamp(decision) >= timestamp(game["kickoff"]):
        raise ValueError(f"Request is not pregame for {game['game_id']}")
    for team in (game["away"], game["home"]):
        depth = evidence["dual_depth"].get(team)
        if not depth or depth.get("season") != game["season"]:
            raise ValueError(f"Missing team depth evidence for {team}")
        sources = depth["sources"]
        for source, field in (("sleeper", "snapshot_fetched_at"), ("fantasypros", "retrieved_at")):
            captured = timestamp(sources[source][field])
            if captured > timestamp(decision) or timestamp(decision) - captured > timedelta(hours=36):
                raise ValueError(f"Stale {source} depth evidence for {team}")
    injuries = evidence["week_injuries"]
    injury_snapshot = evidence.get("week_injury_snapshot")
    injury_time = (timestamp(injury_snapshot["fetched_at"]) if injury_snapshot
        else max((timestamp(row["observed_at"]) for row in injuries), default=None))
    if not injury_time or injury_time > timestamp(decision) or timestamp(decision) - injury_time > timedelta(hours=36):
        raise ValueError(f"Missing fresh week-matched FantasyPros injury evidence for {game['game_id']}")
    players = []
    for player in evidence["players"]:
        if player["team"] not in (game["away"], game["home"]) or player["position"] not in ("QB", "RB", "WR", "TE"):
            raise ValueError(f"Invalid player in {game['game_id']}")
        dual_out = (str(player.get("sleeper_status", "")).upper() in OUT
            and str(player.get("fantasypros_injury_status", "")).upper() in OUT)
        official_out = str(player.get("official_status", "")).upper() in OUT | {"INACTIVE"}
        if (player["status"] == "out") != (dual_out or official_out):
            raise ValueError(f"Inconsistent Out evidence for {player['identity']}")
        players.append({key: player[key] for key in ("identity", "name", "team", "position")}
            | {"status": "out" if dual_out or official_out else "active"})
    if len({p["identity"] for p in players}) != len(players):
        raise ValueError(f"Duplicate player in {game['game_id']}")
    return {"game": game, "decision_at": decision, "players": players,
        "roster_evidence": {"source": "week-matched Sleeper and FantasyPros request",
            "availability_verified": False,
            "unresolved_players_are_conditional": True,
            "game_leaders_request_at": evidence["decision_at"]}}


def modeled_roster_ids(publication: dict) -> list[str]:
    """Only modeled contributors can make the saved probability field stale."""
    return sorted({p["identity"] for p in publication["players"] + publication["residual"]
        if not p["identity"].startswith("OTHER:")
        and (p["anyTd"] > 0 or p["longestShare"] > 0)})


def publish_game(snapshot: dict, evidence: dict, game: dict, settings: Settings) -> dict:
    request = model_request(evidence, game)
    forecast = simulate(snapshot, request, settings)
    support = current_role_support(snapshot, request)
    result = publish(forecast, {"decision_at": request["decision_at"],
        "support": [{"identity": identity, "current_opportunities": count}
            for identity, count in support.items()]})
    result["forecastRosterIds"] = modeled_roster_ids(result)
    result["availabilityVerified"] = False
    result["rosterEvidence"] = request["roster_evidence"]
    return result


def build_week(snapshot: dict, requests: dict, season: int, week: int,
               settings: Settings, prior_games: list[dict] | None = None) -> dict:
    games = sorted((canonical_game(g) for g in snapshot["games"]
        if g["season"] == season and g["week"] == week),
        key=lambda g: (timestamp(g["kickoff"]), g["game_id"]))
    if not games or len({g["game_id"] for g in games}) != len(games):
        raise ValueError("Week has no unique canonical games")
    prior = {item["game"]["game_id"]: item for item in prior_games or []}
    if len(prior) != len(prior_games or []):
        raise ValueError("Duplicate prior game")
    if set(prior) - {game["game_id"] for game in games}:
        raise ValueError("Prior publication is outside the selected week")
    output = []
    for game in games:
        old = prior.get(game["game_id"])
        if old is not None:
            if any(old["game"][key] != game[key] for key in GAME_KEYS if key != "kickoff") or timestamp(old["game"]["kickoff"]) != timestamp(game["kickoff"]):
                raise ValueError(f"Prior publication does not match {game['game_id']}")
            output.append(old)
        else:
            if game["game_id"] not in requests:
                raise ValueError(f"Missing pregame request for {game['game_id']}")
            print(f"Forecasting {game['game_id']}", flush=True)
            output.append(publish_game(snapshot, requests[game["game_id"]], game, settings))
            print(f"Completed {game['game_id']}", flush=True)
    return {"schemaVersion": 2, "season": season, "week": week,
        "publishedAt": datetime.now(timezone.utc).isoformat(),
        "games": output,
        "coverage": {"scheduledGames": len(games), "publishedGames": len(output)},
        "limits": ["Saved, exploratory forecasts. No demonstrated calibration or betting edge.",
            "Unresolved players are conditional participants, not confirmed game-day actives.",
            "New injury evidence can hide a stale game until the whole forecast is rerun."]}


def season_index(week_publication: dict, previous: dict | None = None) -> dict:
    prior_seasons = (previous or {}).get("priorSeasons", [])
    if previous and previous["season"] != week_publication["season"]:
        prior_seasons = prior_seasons + [{"season": previous["season"], "weeks": previous["weeks"]}]
    same_season = previous if previous and previous["season"] == week_publication["season"] else None
    weeks = {item["week"]: item for item in (same_season or {}).get("weeks", [])}
    if len(weeks) != len((same_season or {}).get("weeks", [])):
        raise ValueError("Duplicate week in prior index")
    weeks[week_publication["week"]] = week_publication
    return {"schemaVersion": 2, "season": week_publication["season"],
        "weeks": [weeks[key] for key in sorted(weeks)],
        "priorSeasons": sorted(prior_seasons, key=lambda item: item["season"])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    parser.add_argument("--draws", type=int, default=2000)
    parser.add_argument("--prior-game", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous-index", type=Path)
    parser.add_argument("--index-output", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Choose a new output path; prior publications are immutable")
    if args.index_output and args.index_output.exists():
        raise FileExistsError("Choose a new index output path")
    prior = [read(path) for path in args.prior_game]
    result = build_week(read(args.input), read(args.requests), args.season, args.week,
        Settings(draws=args.draws, newcomer_reserve=True), prior)
    write(args.output, result)
    if args.index_output:
        write(args.index_output, season_index(result,
            read(args.previous_index) if args.previous_index else None))
    print({"output": str(args.output), "coverage": result["coverage"]})


if __name__ == "__main__":
    main()
