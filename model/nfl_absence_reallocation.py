"""Fixed-pie reallocation of an absent player's volume to his teammates.

Study `nfl-absence-reallocation-v1`, registered in
docs/nfl-absence-reallocation-study.md before any outcome was computed.

The withdrawn additive transfer handed every ruled-out player's historical
average to his teammates. It had no team budget, so it could hand out volume
that was never vacated. The fixed pie hands out only what is left over:

    V = phi * min(D, max(0, T_hat - A - R_hat))

T_hat is the team's recent volume, A what the active players are already
projected to take, R_hat what a full-strength team normally leaves to players
we do not model, and D the absent players' own baselines. If teammates'
baselines already reflect the absence, the leftover is small and so is V.

Result (confirmation 2024-2025): targets NOT_PROMOTED, carries INSUFFICIENT.
Neither pool is applied live; see the study doc for the numbers.

Usage:
    python -m model.nfl_absence_reallocation study --discovery-only
    python -m model.nfl_absence_reallocation study
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

STUDY_ID = "nfl-absence-reallocation-v1"

WINDOW_GAMES = 8
HALF_LIFE = 4.0
MIN_ACT_GAMES = 2
MIN_BUDGET_GAMES = 4
MIN_RESERVE_GAMES = 3
MIN_FULL_STRENGTH_GAMES = 3
MAX_MULTIPLIER = 4.0
ABSENT_STATUSES = frozenset({"INA", "RES"})

PHI_GRID = (0.5, 1.0)
ALPHA_GRID = (0.0, 0.5, 1.0)
DISCOVERY_SEASONS = (2022, 2023)
CONFIRMATION_SEASONS = (2024, 2025)
WARMUP_SEASONS = (2021,)
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260928

GATE_MIN_EVENTS = 200
GATE_MIN_ROWS = 800
GATE_POINTS_MARGIN = 0.05


@dataclass(frozen=True)
class PoolSpec:
    name: str
    unit: str
    donors: frozenset
    recipients: frozenset
    budget_excludes: frozenset
    material: float


POOLS = {
    "targets": PoolSpec("targets", "targets", frozenset({"RB", "FB", "WR", "TE"}),
                        frozenset({"RB", "FB", "WR", "TE"}), frozenset(), 3.0),
    "carries": PoolSpec("carries", "carries", frozenset({"RB", "FB", "WR", "TE"}),
                        frozenset({"RB", "FB"}), frozenset({"QB"}), 5.0),
}


def position_group(position: str) -> str:
    return "RB" if position in ("RB", "FB") else position


def recency_weights(n: int) -> np.ndarray:
    """k = 0 is the most recent game."""
    return 0.5 ** (np.arange(n) / HALF_LIFE)


# ---------------------------------------------------------------------------
# Allocation
# ---------------------------------------------------------------------------

def leftover_volume(team_budget: float, active_sum: float, reserve: float, donor_sum: float) -> float:
    """Volume actually left on the table: never more than the donors had."""
    for value in (team_budget, active_sum, reserve, donor_sum):
        if not np.isfinite(value):
            raise ValueError("non-finite allocation input")
    return min(donor_sum, max(0.0, team_budget - active_sum - reserve))


def allocate_fixed_pie(recipients, donors, team_budget, active_sum, reserve, phi, alpha):
    """Split the leftover among recipients.

    recipients: [{"id", "group", "base"}]  (base > 0)
    donors:     [{"id", "group", "base"}]  (base > 0)
    Returns {"leftover", "allocated", "dropped", "gains": {id: gain}}.
    """
    if not (0.0 <= phi <= 1.0 and 0.0 <= alpha <= 1.0):
        raise ValueError("phi and alpha must lie in [0, 1]")
    if any(r["base"] <= 0 or not np.isfinite(r["base"]) for r in recipients):
        raise ValueError("recipient baselines must be positive")
    if any(d["base"] <= 0 or not np.isfinite(d["base"]) for d in donors):
        raise ValueError("donor baselines must be positive")
    donor_sum = sum(d["base"] for d in donors)
    leftover = leftover_volume(team_budget, active_sum, reserve, donor_sum)
    volume = phi * leftover
    raw = {r["id"]: 0.0 for r in recipients}
    total_base = sum(r["base"] for r in recipients)
    if volume > 0 and total_base > 0:
        for donor in sorted(donors, key=lambda d: str(d["id"])):
            part = volume * donor["base"] / donor_sum
            same = [r for r in recipients if r["group"] == donor["group"]]
            same_base = sum(r["base"] for r in same)
            if same and same_base > 0:
                for r in same:
                    raw[r["id"]] += alpha * part * r["base"] / same_base
                spread = (1.0 - alpha) * part
            else:
                spread = part
            for r in recipients:
                raw[r["id"]] += spread * r["base"] / total_base
    gains, dropped = {}, 0.0
    for r in recipients:
        cap = (MAX_MULTIPLIER - 1.0) * r["base"]
        gains[r["id"]] = min(raw[r["id"]], cap)
        dropped += raw[r["id"]] - gains[r["id"]]
    return {"leftover": leftover, "allocated": volume - dropped, "dropped": dropped, "gains": gains}


def allocate_additive(recipients, donors):
    """The withdrawn transfer, kept only to reproduce its error."""
    donor_sum = sum(d["base"] for d in donors)
    total_base = sum(r["base"] for r in recipients)
    gains = {}
    for r in recipients:
        gains[r["id"]] = min(donor_sum * r["base"] / total_base, (MAX_MULTIPLIER - 1.0) * r["base"]) if total_base else 0.0
    return gains


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"
SOURCES = {
    "stats": NFLVERSE + "/stats_player/stats_player_week_{season}.csv",
    "roster": NFLVERSE + "/weekly_rosters/roster_weekly_{season}.csv",
}
GAMES_URL = NFLVERSE + "/schedules/games.csv"
DEFAULT_CACHE = Path(__file__).resolve().parents[1] / "data" / "nflverse_cache"


def fetch(url: str, cache: Path) -> bytes:
    import requests

    cache.mkdir(parents=True, exist_ok=True)
    path = cache / url.rsplit("/", 1)[-1]
    if path.exists():
        return path.read_bytes()
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    path.write_bytes(response.content)
    return response.content


@dataclass
class TeamGame:
    season: int
    week: int
    team: str
    opponent: str
    status: dict = field(default_factory=dict)      # gsis -> roster status
    position: dict = field(default_factory=dict)    # gsis -> position
    name: dict = field(default_factory=dict)        # gsis -> display name
    stats: dict = field(default_factory=dict)       # gsis -> stat dict
    has_stats: bool = False


STAT_FIELDS = ("targets", "carries", "receptions", "receiving_yards", "receiving_tds",
               "rushing_yards", "rushing_tds", "fantasy_points_ppr")


def load(seasons, cache: Path = DEFAULT_CACHE):
    """Team games keyed by team, chronological, plus source digests."""
    import io

    import pandas as pd

    digests = {}
    games_bytes = fetch(GAMES_URL, cache)
    digests["games.csv"] = hashlib.sha256(games_bytes).hexdigest()
    schedule = pd.read_csv(io.BytesIO(games_bytes), low_memory=False)
    schedule = schedule[(schedule["game_type"] == "REG") & schedule["season"].isin(seasons)]

    by_key: dict[tuple, TeamGame] = {}
    for row in schedule.itertuples(index=False):
        for team, opp in ((row.home_team, row.away_team), (row.away_team, row.home_team)):
            by_key[(int(row.season), int(row.week), team)] = TeamGame(int(row.season), int(row.week), team, opp)

    for season in seasons:
        roster_bytes = fetch(SOURCES["roster"].format(season=season), cache)
        stats_bytes = fetch(SOURCES["stats"].format(season=season), cache)
        digests[f"roster_weekly_{season}.csv"] = hashlib.sha256(roster_bytes).hexdigest()
        digests[f"stats_player_week_{season}.csv"] = hashlib.sha256(stats_bytes).hexdigest()
        roster = pd.read_csv(io.BytesIO(roster_bytes), low_memory=False,
                             usecols=["season", "week", "team", "gsis_id", "status", "position", "full_name", "game_type"])
        roster = roster[(roster["game_type"] == "REG") & roster["gsis_id"].notna()]
        for row in roster.itertuples(index=False):
            game = by_key.get((int(row.season), int(row.week), row.team))
            if game is None:
                continue
            game.status[row.gsis_id] = str(row.status)
            game.position[row.gsis_id] = str(row.position)
            game.name[row.gsis_id] = str(row.full_name)
        stats = pd.read_csv(io.BytesIO(stats_bytes), low_memory=False)
        stats = stats[stats["season_type"] == "REG"]
        for row in stats.itertuples(index=False):
            game = by_key.get((int(row.season), int(row.week), row.team))
            if game is None or not isinstance(row.player_id, str):
                continue
            game.stats[row.player_id] = {f: float(getattr(row, f) or 0.0) if pd.notna(getattr(row, f)) else 0.0
                                         for f in STAT_FIELDS}
            game.has_stats = True
            game.position.setdefault(row.player_id, str(row.position))
            game.name.setdefault(row.player_id, str(row.player_display_name))

    teams: dict[str, list[TeamGame]] = defaultdict(list)
    for game in by_key.values():
        teams[game.team].append(game)
    for games in teams.values():
        games.sort(key=lambda g: (g.season, g.week))
    return dict(teams), digests


# ---------------------------------------------------------------------------
# Walk-forward quantities
# ---------------------------------------------------------------------------

def _stat(game: TeamGame, pid: str, field_name: str) -> float:
    return game.stats.get(pid, {}).get(field_name, 0.0)


def _linear(game: TeamGame, pid: str, pool: str) -> float:
    s = game.stats.get(pid)
    if not s:
        return 0.0
    if pool == "targets":
        return s["receptions"] + 0.1 * s["receiving_yards"] + 6.0 * s["receiving_tds"]
    return 0.1 * s["rushing_yards"] + 6.0 * s["rushing_tds"]


def team_total(game: TeamGame, spec: PoolSpec) -> float:
    return sum(v[spec.unit] for pid, v in game.stats.items()
               if game.position.get(pid, "") not in spec.budget_excludes)


@dataclass
class Baseline:
    base: float
    act_games: int
    points: float
    points_per_unit: float


def player_baselines(window: list[TeamGame], spec: PoolSpec) -> dict[str, Baseline]:
    """window is most-recent-first. Only ACT games for this team count."""
    weights = recency_weights(len(window))
    acc = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0])  # w, w*u, w*pts, w*linear, n
    for k, game in enumerate(window):
        w = weights[k]
        for pid, status in game.status.items():
            if status != "ACT":
                continue
            a = acc[pid]
            a[0] += w
            a[1] += w * _stat(game, pid, spec.unit)
            a[2] += w * _stat(game, pid, "fantasy_points_ppr")
            a[3] += w * _linear(game, pid, spec.name)
            a[4] += 1
    out = {}
    for pid, (w, wu, wp, wl, n) in acc.items():
        if n < MIN_ACT_GAMES:
            continue
        out[pid] = Baseline(base=wu / w, act_games=n, points=wp / w, points_per_unit=(wl / wu) if wu > 0 else 0.0)
    return out


def team_budget(window: list[TeamGame], spec: PoolSpec) -> float | None:
    if len(window) < MIN_BUDGET_GAMES:
        return None
    weights = recency_weights(len(window))
    totals = np.array([team_total(g, spec) for g in window])
    return float(np.dot(weights, totals) / weights.sum())


def counted(game: TeamGame, pid: str, spec: PoolSpec) -> bool:
    return game.position.get(pid, "") not in spec.budget_excludes


def donors_in(game: TeamGame, baselines: dict[str, Baseline], spec: PoolSpec):
    return [pid for pid, status in game.status.items()
            if status in ABSENT_STATUSES and pid in baselines and baselines[pid].base > 0
            and game.position.get(pid, "") in spec.donors]


def active_sum(game: TeamGame, baselines: dict[str, Baseline], spec: PoolSpec) -> float:
    return sum(b.base for pid, b in baselines.items()
               if game.status.get(pid) == "ACT" and counted(game, pid, spec))


@dataclass
class GameState:
    """Per team-game walk-forward state, computed once and reused."""
    baselines: dict
    budget: float | None
    residual: float | None       # r_h (needs the game's own actuals)
    full_strength: bool


def team_states(games: list[TeamGame], spec: PoolSpec) -> list[GameState]:
    states = []
    for n, game in enumerate(games):
        window = list(reversed(games[max(0, n - WINDOW_GAMES):n]))
        window = [g for g in window if g.has_stats]
        baselines = player_baselines(window, spec)
        budget = team_budget(window, spec)
        residual = None
        if budget is not None and game.has_stats:
            residual = team_total(game, spec) - active_sum(game, baselines, spec)
        full = not any(baselines[pid].base >= spec.material for pid in donors_in(game, baselines, spec))
        states.append(GameState(baselines, budget, residual, full))
    return states


def reserve_for(states: list[GameState], n: int) -> float | None:
    """R_hat for game n from the prior window's residuals."""
    idx = [i for i in range(n - 1, max(-1, n - 1 - WINDOW_GAMES), -1)]
    weights = recency_weights(len(idx))
    defined = [(weights[k], states[i]) for k, i in enumerate(idx) if states[i].residual is not None]
    full = [(w, s) for w, s in defined if s.full_strength]
    use = full if len(full) >= MIN_FULL_STRENGTH_GAMES else defined
    if len(use) < MIN_RESERVE_GAMES:
        return None
    return sum(w * s.residual for w, s in use) / sum(w for w, _ in use)


# ---------------------------------------------------------------------------
# Events and evaluation
# ---------------------------------------------------------------------------

CONFIGS = [(phi, alpha) for phi in PHI_GRID for alpha in ALPHA_GRID]


def config_key(phi: float, alpha: float) -> str:
    return f"phi={phi:g},alpha={alpha:g}"


def build_events(teams: dict[str, list[TeamGame]], spec: PoolSpec, seasons) -> tuple[list[dict], dict]:
    events, quality = [], defaultdict(int)
    for team, games in sorted(teams.items()):
        states = team_states(games, spec)
        for n, game in enumerate(games):
            if game.season not in seasons or not game.has_stats:
                continue
            state = states[n]
            donors = donors_in(game, state.baselines, spec)
            material = [pid for pid in donors if state.baselines[pid].base >= spec.material]
            if not material:
                continue
            quality["candidate_events"] += 1
            quality["donor_with_stat_row"] += sum(1 for pid in donors if pid in game.stats)
            reserve = reserve_for(states, n)
            if state.budget is None or reserve is None:
                quality["excluded_insufficient_history"] += 1
                continue
            recipients = [pid for pid, b in state.baselines.items()
                          if game.status.get(pid) == "ACT" and game.position.get(pid, "") in spec.recipients and b.base > 0]
            if not recipients:
                quality["excluded_no_recipients"] += 1
                continue
            rows = [{"id": pid, "group": position_group(game.position[pid]), "base": state.baselines[pid].base,
                     "points": state.baselines[pid].points, "ppu": state.baselines[pid].points_per_unit,
                     "actual": _stat(game, pid, spec.unit), "actual_points": _stat(game, pid, "fantasy_points_ppr")}
                    for pid in sorted(recipients)]
            events.append({
                "season": game.season, "week": game.week, "team": team,
                "donors": [{"id": pid, "group": position_group(game.position[pid]), "base": state.baselines[pid].base,
                            "position": game.position[pid], "name": game.name.get(pid, pid)} for pid in sorted(donors)],
                "budget": state.budget, "reserve": reserve,
                "active": active_sum(game, state.baselines, spec),
                "actual_total": team_total(game, spec),
                "recipients": rows,
            })
    return events, dict(quality)


def predictions(event: dict, method: str, phi: float = 1.0, alpha: float = 0.0) -> tuple[np.ndarray, float]:
    recipients, donors = event["recipients"], event["donors"]
    if method == "BASE":
        gains = {r["id"]: 0.0 for r in recipients}
        dropped = 0.0
    elif method == "OLD":
        gains = allocate_additive(recipients, donors)
        dropped = 0.0
    else:
        result = allocate_fixed_pie(recipients, donors, event["budget"], event["active"], event["reserve"], phi, alpha)
        gains, dropped = result["gains"], result["dropped"]
    return np.array([r["base"] + gains[r["id"]] for r in recipients]), dropped


def event_errors(event: dict, method: str, phi: float = 1.0, alpha: float = 0.0) -> dict:
    pred, dropped = predictions(event, method, phi, alpha)
    base = np.array([r["base"] for r in event["recipients"]])
    actual = np.array([r["actual"] for r in event["recipients"]])
    pts_base = np.array([r["points"] for r in event["recipients"]])
    ppu = np.array([r["ppu"] for r in event["recipients"]])
    actual_pts = np.array([r["actual_points"] for r in event["recipients"]])
    pred_pts = pts_base + (pred - base) * ppu
    return {"abs": np.abs(actual - pred), "err": actual - pred,
            "abs_pts": np.abs(actual_pts - pred_pts), "err_pts": actual_pts - pred_pts,
            "gain": float((pred - base).sum()), "dropped": dropped}


def mae(events: list[dict], method: str, phi: float = 1.0, alpha: float = 0.0) -> float:
    total = n = 0.0
    for event in events:
        e = event_errors(event, method, phi, alpha)
        total += e["abs"].sum()
        n += len(e["abs"])
    return total / n if n else float("nan")


def paired(events: list[dict], a: tuple, b: tuple, key: str) -> dict:
    """Paired MAE delta a - b with an event-clustered bootstrap."""
    sums, counts, bias_a = [], [], []
    for event in events:
        ea, eb = event_errors(event, *a), event_errors(event, *b)
        sums.append(float((ea[key] - eb[key]).sum()))
        counts.append(len(ea[key]))
        bias_a.append(float(ea["err" if key == "abs" else "err_pts"].sum()))
    sums, counts = np.array(sums), np.array(counts, dtype=float)
    delta = sums.sum() / counts.sum()
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    idx = rng.integers(0, len(events), size=(BOOTSTRAP_DRAWS, len(events)))
    draws = sums[idx].sum(axis=1) / counts[idx].sum(axis=1)
    lo, hi = np.percentile(draws, [2.5, 97.5])
    return {"delta": float(delta), "ci": [float(lo), float(hi)], "rows": int(counts.sum()),
            "events": len(events), "bias_actual_minus_pred": float(sum(bias_a) / counts.sum())}


def select_config(discovery: list[dict]) -> dict:
    scores = {config_key(phi, alpha): round(mae(discovery, "FIX", phi, alpha), 4) for phi, alpha in CONFIGS}
    best = sorted(CONFIGS, key=lambda c: (scores[config_key(*c)], c[0], c[1]))[0]
    return {"phi": best[0], "alpha": best[1], "discovery_mae": scores,
            "discovery_base_mae": round(mae(discovery, "BASE"), 4),
            "discovery_old_mae": round(mae(discovery, "OLD"), 4), "discovery_events": len(discovery),
            "discovery_rows": sum(len(e["recipients"]) for e in discovery)}


def verdict(units: dict, points: dict) -> dict:
    g3 = units["events"] >= GATE_MIN_EVENTS and units["rows"] >= GATE_MIN_ROWS
    g1 = units["ci"][1] < 0
    g2 = points["delta"] < 0 and points["ci"][1] < GATE_POINTS_MARGIN
    status = "INSUFFICIENT" if not g3 else ("PROMOTE" if g1 and g2 else "NOT_PROMOTED")
    return {"G1_units": g1, "G2_points": g2, "G3_sample": g3, "verdict": status}


def slice_by(events: list[dict], key) -> dict:
    groups = defaultdict(list)
    for e in events:
        groups[key(e)].append(e)
    return groups


def run_study(teams, digests) -> dict:
    result = {"study": STUDY_ID, "registered": "docs/nfl-absence-reallocation-study.md",
              "generated_at": datetime.now(timezone.utc).isoformat(), "sources": digests,
              "constants": {"window_games": WINDOW_GAMES, "half_life": HALF_LIFE, "min_act_games": MIN_ACT_GAMES,
                            "max_multiplier": MAX_MULTIPLIER, "absent_statuses": sorted(ABSENT_STATUSES),
                            "bootstrap_draws": BOOTSTRAP_DRAWS, "seed": BOOTSTRAP_SEED}, "pools": {}}
    for name, spec in POOLS.items():
        discovery, dq = build_events(teams, spec, DISCOVERY_SEASONS)
        confirmation, cq = build_events(teams, spec, CONFIRMATION_SEASONS)
        live, lq = build_events(teams, spec, (2026,))
        selection = select_config(discovery)
        chosen = ("FIX", selection["phi"], selection["alpha"])
        base = ("BASE", 1.0, 0.0)
        old = ("OLD", 1.0, 0.0)
        units = paired(confirmation, chosen, base, "abs")
        points = paired(confirmation, chosen, base, "abs_pts")
        cap_dropped = sum(event_errors(e, *chosen)["dropped"] for e in confirmation)
        allocated = sum(event_errors(e, *chosen)["gain"] for e in confirmation)
        pool = {
            "spec": {"unit": spec.unit, "donors": sorted(spec.donors), "recipients": sorted(spec.recipients),
                     "budget_excludes": sorted(spec.budget_excludes), "material": spec.material},
            "data_quality": {"discovery": dq, "confirmation": cq, "2026": lq},
            "selection": selection,
            "confirmation": {
                "config": {"phi": selection["phi"], "alpha": selection["alpha"]},
                "mae": {"BASE": mae(confirmation, "BASE"), "FIX": mae(confirmation, *chosen),
                        "OLD": mae(confirmation, "OLD")},
                "units_fix_minus_base": units,
                "points_fix_minus_base": points,
                "units_old_minus_base": paired(confirmation, old, base, "abs"),
                "points_old_minus_base": paired(confirmation, old, base, "abs_pts"),
                "units_base_bias": paired(confirmation, base, chosen, "abs")["bias_actual_minus_pred"],
                "allocated_volume": allocated, "cap_dropped_volume": cap_dropped,
                "by_season": {str(k): paired(v, chosen, base, "abs")
                              for k, v in sorted(slice_by(confirmation, lambda e: e["season"]).items())},
                "by_donor_group": {k: paired(v, chosen, base, "abs")
                                   for k, v in sorted(slice_by(confirmation, lambda e: max(
                                       e["donors"], key=lambda d: d["base"])["group"]).items()) if len(v) >= 20},
                "gate": verdict(units, points),
            },
            "descriptive_2026": ({"events": len(live), "rows": sum(len(e["recipients"]) for e in live),
                                  "mae": {"BASE": mae(live, "BASE"), "FIX": mae(live, *chosen), "OLD": mae(live, "OLD")}}
                                 if live else {"events": 0}),
        }
        result["pools"][name] = pool
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("study")
    s.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    s.add_argument("--out", type=Path, default=Path("artifacts/nfl_absence_reallocation_v1.json"))
    s.add_argument("--discovery-only", action="store_true",
                   help="data quality and discovery selection only; never touches confirmation seasons")
    args = parser.parse_args()

    if args.command == "study":
        if args.discovery_only:
            teams, digests = load(WARMUP_SEASONS + DISCOVERY_SEASONS, args.cache)
            for name, spec in POOLS.items():
                events, quality = build_events(teams, spec, DISCOVERY_SEASONS)
                print(name, quality, json.dumps(select_config(events), indent=1))
            return
        seasons = WARMUP_SEASONS + DISCOVERY_SEASONS + CONFIRMATION_SEASONS + (2026,)
        teams, digests = load(seasons, args.cache)
        result = run_study(teams, digests)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=1, default=float) + "\n")
        for name, pool in result["pools"].items():
            c = pool["confirmation"]
            print(f"{name}: config {c['config']}  units {c['units_fix_minus_base']['delta']:+.4f} "
                  f"{c['units_fix_minus_base']['ci']}  points {c['points_fix_minus_base']['delta']:+.4f} "
                  f"{c['points_fix_minus_base']['ci']}  -> {c['gate']['verdict']}")


if __name__ == "__main__":
    main()
