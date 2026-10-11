"""PBP-conditioned, shadow-only transport of coherent NFL scenario banks.

Drive paths are sampled from labeled pre-decision PBP. Each sampled game's
score/FG/turnover/pace signature selects a *whole* scenario from an existing
coherent bank. We never splice independent player outcomes together. This is
an empirical script challenger, not a calibrated possession forecast.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
import json
import math
from typing import Any, Iterable

import numpy as np

VERSION = "nfl-pbp-script-transport-shadow-v1"
TERMINALS = {"TOUCHDOWN", "FIELD_GOAL", "THREE_AND_OUT", "STALLED", "MISSED_FG",
             "TURNOVER_GIVEAWAY", "TURNOVER_ON_DOWNS", "SCORE_AGAINST", "CLOCK_EXPIRED", "KNEEL_DOWN"}


@dataclass(frozen=True)
class Drive:
    game_id: str
    team: str
    season: int
    week: int
    state: str
    terminal: str
    seconds: int
    plays: int
    dropbacks: int
    sacks: int


def state(score_diff: int) -> str:
    return "leading" if score_diff > 7 else "trailing" if score_diff < -7 else "close"


def drive_catalog(rows: Iterable[dict[str, Any]]) -> list[Drive]:
    """Count each (game, offense, drive) once; never count a label per play."""
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("drive") is not None and row.get("posteam"):
            groups[(str(row["game_id"]), str(row["posteam"]), int(row["drive"]))].append(row)
    result = []
    for (game_id, team, _), plays in sorted(groups.items()):
        plays.sort(key=lambda r: int(r["play_id"]))
        labels = {r["drive_archetype"] for r in plays if r.get("drive_archetype")}
        if len(labels) != 1 or next(iter(labels)) not in TERMINALS:
            continue
        first = next((r for r in plays if r.get("score_differential") is not None), None)
        clocks = [int(r["game_seconds_remaining"]) for r in plays if r.get("game_seconds_remaining") is not None]
        scrimmage = [r for r in plays if r.get("play_type") in ("run", "pass")]
        if first is None or not clocks or not scrimmage:
            continue
        elapsed = max(clocks) - min(clocks)
        # The terminal play consumes time after its timestamp; this fixed
        # residual is explicit and later must be estimated from snap clocks.
        seconds = max(15, min(480, elapsed + 25))
        result.append(Drive(game_id, team, int(first["season"]), int(first["week"]),
                            state(int(first["score_differential"])), next(iter(labels)), seconds,
                            len(scrimmage), sum(r.get("qb_dropback") is True for r in scrimmage),
                            sum(r.get("had_sack") is True for r in plays)))
    return result


def _recent(catalog: list[Drive], team: str, current_state: str, max_games: int = 8) -> tuple[list[Drive], str]:
    team_games = sorted({(d.season, d.week, d.game_id) for d in catalog if d.team == team})[-max_games:]
    recent = [d for d in catalog if d.team == team and (d.season, d.week, d.game_id) in team_games]
    matched = [d for d in recent if d.state == current_state and d.terminal != "KNEEL_DOWN"]
    if len(matched) >= 8:
        return matched, "team_state"
    league = [d for d in catalog if d.state == current_state and d.terminal != "KNEEL_DOWN"]
    if league:
        return league, "league_state"
    raise ValueError(f"No qualified PBP drives for {current_state}")


def sample_scripts(catalog: list[Drive], teams: tuple[str, str], count: int, seed: int) -> tuple[list[dict], dict]:
    if len(set(teams)) != 2 or count < 2 or not catalog:
        raise ValueError("Two teams, at least two draws, and qualified PBP drives required")
    rng = np.random.default_rng(seed)
    scripts, fallback = [], defaultdict(int)
    pools: dict[tuple[str, str], tuple[list[Drive], str]] = {}
    for index in range(count):
        score = {team: 0 for team in teams}
        totals = {team: {"fg": 0, "turnovers": 0, "sacks": 0, "plays": 0, "dropbacks": 0} for team in teams}
        ledger = []
        remaining = 3600
        side = int(rng.integers(2))
        while remaining > 0 and len(ledger) < 40:
            team, opponent = teams[side], teams[1 - side]
            pool_key = (team, state(score[team] - score[opponent]))
            if pool_key not in pools:
                pools[pool_key] = _recent(catalog, *pool_key)
            options, source = pools[pool_key]
            fallback[source] += 1
            drive = options[int(rng.integers(len(options)))]
            terminal = drive.terminal
            entering_state = state(score[team] - score[opponent])
            if terminal == "TOUCHDOWN":
                score[team] += 7
            elif terminal == "FIELD_GOAL":
                score[team] += 3
                totals[team]["fg"] += 1
            elif terminal == "SCORE_AGAINST":
                score[opponent] += 7
                totals[team]["turnovers"] += 1
            elif terminal == "TURNOVER_GIVEAWAY":
                totals[team]["turnovers"] += 1
            totals[team]["sacks"] += drive.sacks
            totals[team]["plays"] += drive.plays
            totals[team]["dropbacks"] += drive.dropbacks
            remaining -= drive.seconds
            ledger.append({"team": team, "state": entering_state,
                           "terminal": terminal, "source_game": drive.game_id, "seconds": drive.seconds})
            side = 1 - side
        scripts.append({"id": index, "score": score, "totals": totals, "drives": ledger})
    return scripts, dict(fallback)


def _signature(team_events: list[dict], teams: tuple[str, str]) -> dict:
    by_team = {event["team"]: event for event in team_events}
    if set(by_team) != set(teams):
        raise ValueError("Coherent ledger teams do not match target game")
    return {team: {"points": int(by_team[team]["final_points"]),
                   "fg": sum(int(by_team[team][key]) for key in ("fg_short", "fg_medium", "fg_long")),
                   "turnovers": int(by_team[team]["interceptions_thrown"]) + int(by_team[team]["fumbles_lost"]),
                   "sacks": int(by_team[team]["sacks_suffered"]),
                   "pass_rate": int(by_team[team]["opportunities"]["attempts"]) /
                                max(1, int(by_team[team]["opportunities"]["attempts"]) + int(by_team[team]["opportunities"]["carries"]))}
            for team in teams}


def _distance(script: dict, signature: dict, teams: tuple[str, str]) -> float:
    total = 0.0
    for team in teams:
        own = script["totals"][team]
        want = ("points", script["score"][team]), ("fg", own["fg"]), ("turnovers", own["turnovers"]), ("sacks", own["sacks"])
        total += sum(abs(float(signature[team][key]) - value) / scale for (key, value), scale in zip(want, (7, 1.5, 1.5, 2.5)))
        total += abs(signature[team]["pass_rate"] - own["dropbacks"] / max(1, own["plays"])) / 0.2
    return total


def transport_bank(bank: dict, ledger: list, scripts: list[dict], teams: tuple[str, str], seed: int,
                   *, max_mean_distance: float = 7.0, provenance_digest: str = "") -> tuple[dict, dict]:
    """Select whole joint draws. Reject poor transport coverage rather than hide it."""
    base = bank["scenarios"]
    if bank.get("source") != "model" or len(base) != len(ledger) or not scripts:
        raise ValueError("A model-source bank and aligned event ledger are required")
    signatures = []
    for entry in ledger:
        if len(entry) != 1:
            raise ValueError("Showdown transport requires exactly one game per draw")
        signatures.append(_signature(entry[0]["teams"], teams))
    rng = np.random.default_rng(seed)
    selected, distances = [], []
    for script in scripts:
        nearest = sorted(((float(_distance(script, sig, teams)), i) for i, sig in enumerate(signatures)))[:5]
        weights = np.array([math.exp(-(d - nearest[0][0])) for d, _ in nearest])
        weights /= weights.sum()
        pick = nearest[int(rng.choice(len(nearest), p=weights))]
        distances.append(pick[0])
        selected.append({**base[pick[1]], "id": f"pbp-script:{seed}:{script['id']}"})
    mean_distance = float(np.mean(distances))
    diagnostics = {"mean_match_distance": mean_distance, "p90_match_distance": float(np.quantile(distances, .9)),
                   "unique_base_draws": len({json.dumps(s["stats"], sort_keys=True) for s in selected}),
                   "max_mean_distance": max_mean_distance, "accepted": mean_distance <= max_mean_distance}
    if not diagnostics["accepted"]:
        raise ValueError(f"PBP script transport coverage failed: mean distance {mean_distance:.3f} > {max_mean_distance}")
    snapshot = sha256(json.dumps({"source": bank["snapshotId"], "pbp": provenance_digest}, sort_keys=True).encode()).hexdigest()
    digest = sha256(json.dumps({"snapshot": snapshot, "seed": seed, "script_count": len(scripts)}, sort_keys=True).encode()).hexdigest()
    return {**bank, "runId": f"{VERSION}:{digest}", "modelVersion": VERSION,
            "snapshotId": f"{VERSION}:{snapshot}", "streamId": f"{VERSION}:{seed}",
            "sampling": "iid", "seed": seed, "scenarios": selected}, diagnostics
