"""``nfl-dfs-range-calibration-v1``: are the historical model's P10/P90 the right width?

Registered in ``docs/nfl-dfs-range-calibration-study.md`` before any held-out
(2024, 2025) row was scored. Research only; production projections are not
touched and ``model/nfl_dfs_historical.py`` is not edited.

The population is players who PLAYED (a target-week stat line), because the
participation-ledger study (``nfl-dfs-participation-ledger-v1``) owns the
players who never take the field. Two candidates, each compared with the
baseline (production v5, half-life 3) on identical rows:

* A, half-life by quantile loss: the half-life in ``HALF_LIFE_GRID`` with the
  lowest pooled RB+TE+WR range loss (P10 pinball + P90 pinball) on the tuning
  seasons. If that is 3, candidate A is the baseline and is dropped.
* B, tail scaling: ``P10' = m - a_pos (m - P10)``, ``P90' = m + b_pos (P90 - m)``
  with ``m`` the projection, ``a``/``b`` per position fitted on the tuning
  seasons by pinball loss at tau 0.10 / 0.90. The mean does not move.

Usage::

    python -m research.nfl_dfs_range_calibration replay --season 2022   # tuning
    python -m research.nfl_dfs_range_calibration replay --season 2023   # tuning
    python -m research.nfl_dfs_range_calibration tune                    # writes the frozen constants
    python -m research.nfl_dfs_range_calibration replay --season 2024   # held out
    python -m research.nfl_dfs_range_calibration replay --season 2025   # held out
    python -m research.nfl_dfs_range_calibration grade                   # verdict, once

``replay`` on a held-out season refuses to run until the frozen constants file
exists. Every artifact lands in ``artifacts/nfl-dfs-range-calibration/``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from model.nfl_absence_reallocation import DEFAULT_CACHE
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    HistoricalWeek,
    ProjectionContext,
    _peer_rows,
    _recency_weights,
    adjust_stat_line,
    before_cutoff,
    draftkings_points,
)
from model.nfl_dfs_participation_ledger_gate_h import (
    MODEL_FILE_SHA256,
    game_bootstrap,
    load_sources,
    model_file_matches_pin,
    model_file_sha256,
    pinball,
)

STUDY_ID = "nfl-dfs-range-calibration-v1"
SEED = 20260902                       # production default projection seed
BOOTSTRAP_SEED = 20261025
BOOTSTRAP_DRAWS = 10_000
HISTORY_SEASONS_BACK = 2              # same history depth as the ledger study's Gate H
TARGET_WEEKS = tuple(range(4, 19))
POSITIONS = ("QB", "RB", "TE", "WR")  # QB is reported, never gated
GATED = ("RB", "TE", "WR")
MIN_BASELINE = 5.0
BASE_HALF_LIFE = 3.0
HALF_LIFE_GRID = (2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 17.0, 34.0)
SCALE_GRID = tuple(round(x, 2) for x in np.arange(0.50, 3.0001, 0.02))
TUNING_SEASONS = (2022, 2023)
HELD_OUT_SEASONS = (2024, 2025)
MAE_GUARD_UPPER = 0.05                # pooled MAE delta, 95% upper bound, DK points
POSITION_GUARD_MAX_POINT = 0.01       # per-position range-loss delta, point estimate
ARTIFACT_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "nfl-dfs-range-calibration"
FROZEN_PATH = Path(__file__).resolve().parent / "nfl_dfs_range_calibration_frozen.json"


# ---------------------------------------------------------------------------
# Draws: project_player, step for step, but returning the draw array
# ---------------------------------------------------------------------------

class PeerIndex:
    """``_peer_rows`` for many players over one fixed prior set, without rescoring every peer per call.

    Same grouping, same ``np.mean`` over the same floats in the same order, same
    sort key, so the selection is identical (checked in the tests).
    """

    def __init__(self, position: str, prior: Sequence[HistoricalWeek]):
        self.position = position
        self.points = {id(row): row.dk_points for row in prior}
        by_player: dict[int, list[HistoricalWeek]] = {}
        for row in prior:
            if row.position == position:
                by_player.setdefault(row.player_id, []).append(row)
        self.by_player = by_player
        self.means = {pid: float(np.mean([self.points[id(r)] for r in rows])) for pid, rows in by_player.items()}
        self.recent = {pid: sorted(rows, key=lambda r: r.chronological_key, reverse=True) for pid, rows in by_player.items()}
        self.rows = [row for row in prior if row.position == position]

    def peers(self, own: Sequence[HistoricalWeek], limit: int) -> list[HistoricalWeek]:
        exclude = own[-1].player_id if own else None
        ids = [pid for pid in self.by_player if pid != exclude]
        if not ids:
            return []
        if own:
            player_mean = np.mean([self.points[id(r)] for r in own])
            ids.sort(key=lambda pid: (abs(self.means[pid] - player_mean), pid))
            selected: list[HistoricalWeek] = []
            for pid in ids:
                selected.extend(self.recent[pid])
                if len(selected) >= limit:
                    break
            return selected[:limit]
        rows = [r for r in self.rows if r.player_id != exclude]
        rows.sort(key=lambda row: (-row.season, -row.week, row.player_id))
        return rows[:limit]


def projection_draws(
    *,
    player_id: int,
    player_gsis_id: str,
    player_name: str,
    position: str,
    prior: Sequence[HistoricalWeek],
    cutoff_season: int,
    cutoff_week: int,
    context: ProjectionContext,
    half_lives: Sequence[float],
    seed: int = SEED,
    config: Mapping[str, float] = MODEL_CONFIG,
    peer_index: PeerIndex | None = None,
) -> dict[float, np.ndarray] | None:
    """Score arrays keyed by half-life, identical to ``project_player`` at each half-life.

    ``prior`` must already be same-position rows before the cutoff, sorted
    chronologically (``project_player`` filters and sorts the same way; peers
    are restricted to the position inside ``_peer_rows`` either way). Each row
    is scored once instead of once per draw, which is the only difference.
    """
    own = [row for row in prior if row.player_id == player_id]
    if not own:
        own = [row for row in prior if row.player_gsis_id == player_gsis_id]
    own = own[-int(config["max_player_games"]):]
    if peer_index is None:
        peers = _peer_rows(position, own, prior, int(config["max_prior_games"]))
    else:
        peers = peer_index.peers(own, int(config["max_prior_games"]))
    status = "historical" if len(own) >= int(config["minimum_historical_games"]) else ("position_prior" if peers else "unavailable")
    if status == "unavailable":
        return None
    own_scores = np.array([draftkings_points(position, adjust_stat_line(position, r.stats, context, config)) for r in own])
    peer_scores = np.array([draftkings_points(position, adjust_stat_line(position, r.stats, context, config)) for r in peers])
    strength = 0.0 if status == "position_prior" else len(own) / (len(own) + float(config["prior_equivalent_games"]))
    identity = int(hashlib.sha256(f"{seed}:{player_id}:{player_gsis_id}:{player_name}".encode()).hexdigest()[:16], 16)
    n = int(config["draws"])
    out: dict[float, np.ndarray] = {}
    for half_life in half_lives:
        rng = np.random.default_rng(identity)
        weights = _recency_weights(own, float(half_life))
        choose = rng.random(n) < strength
        own_idx = rng.choice(len(own), size=n, p=weights) if own else np.zeros(n, dtype=int)
        peer_idx = rng.choice(len(peers), size=n) if peers else np.zeros(n, dtype=int)
        if own and peers:
            out[half_life] = np.where(choose, own_scores[own_idx], peer_scores[peer_idx])
        elif own:
            out[half_life] = own_scores[own_idx]
        else:
            out[half_life] = peer_scores[peer_idx]
    return out


def summarize_draws(draws: np.ndarray) -> dict[str, float]:
    return {
        "mean": round(float(draws.mean()), 4),
        "p10": round(float(np.quantile(draws, 0.10)), 4),
        "p50": round(float(np.quantile(draws, 0.50)), 4),
        "p90": round(float(np.quantile(draws, 0.90)), 4),
    }


# ---------------------------------------------------------------------------
# Candidate B and the losses
# ---------------------------------------------------------------------------

def scale_tails(mean: float, p10: float, p90: float, a: float, b: float) -> tuple[float, float]:
    """Mean-preserving tail scaling. Commutes with the market anchor's k-scaling."""
    return mean - a * (mean - p10), mean + b * (p90 - mean)


def range_loss(actual: float, p10: float, p90: float) -> float:
    return pinball(0.10, p10, actual) + pinball(0.90, p90, actual)


def fit_scale(rows: Sequence[Mapping[str, Any]], tau: float, grid: Sequence[float] = SCALE_GRID) -> dict[str, float]:
    """The grid value minimising mean pinball at tau for P10 (tau .1) or P90 (tau .9) of the baseline."""
    mean = np.array([r["base"]["mean"] for r in rows])
    actual = np.array([r["actual"] for r in rows])
    edge = np.array([r["base"]["p10" if tau < 0.5 else "p90"] for r in rows])
    best = None
    for s in grid:
        q = mean - s * (mean - edge) if tau < 0.5 else mean + s * (edge - mean)
        loss = float(np.mean((tau - (actual < q)) * (actual - q)))
        if best is None or loss < best[1] - 1e-12:
            best = (s, loss)
    return {"scale": best[0], "loss": best[1]}


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def replay_season(season: int, *, cache: Path = DEFAULT_CACHE, weeks: Sequence[int] = TARGET_WEEKS,
                  half_lives: Sequence[float] = HALF_LIFE_GRID, log=print) -> dict[str, Any]:
    started = time.time()
    sources = load_sources(season, cache, HISTORY_SEASONS_BACK)
    by_pos: dict[str, list[HistoricalWeek]] = defaultdict(list)
    for row in sorted(sources.stat_rows, key=lambda r: r.chronological_key):
        by_pos[row.position].append(row)
    target = {(r.player_gsis_id, r.week): r for r in sources.stat_rows if r.season == season}
    rows: list[dict[str, Any]] = []
    excluded: Counter = Counter()
    for week in weeks:
        t0 = time.time()
        for position in POSITIONS:
            prior = [r for r in by_pos[position] if before_cutoff(r, season, week)]
            index = PeerIndex(position, prior)
            for (gsis, w), stat in target.items():
                if w != week or stat.position != position:
                    continue
                game = sources.games.get((season, week, stat.team))
                if game is None:
                    excluded["no_team_game"] += 1
                    continue
                name = sources.roster_names.get(gsis, gsis)
                draws = projection_draws(
                    player_id=sources.player_ids[gsis], player_gsis_id=gsis, player_name=name, position=position,
                    prior=prior, cutoff_season=season, cutoff_week=week,
                    context=ProjectionContext(team_implied_total=game.implied_total), half_lives=half_lives,
                    peer_index=index,
                )
                if draws is None:
                    excluded["unavailable"] += 1
                    continue
                base = summarize_draws(draws[BASE_HALF_LIFE])
                if base["mean"] < MIN_BASELINE:
                    excluded["baseline_under_5"] += 1
                    continue
                own_n = sum(1 for r in prior if r.player_gsis_id == gsis)
                rows.append({
                    "season": season, "week": week, "game_id": game.game_id, "team": stat.team, "gsis": gsis,
                    "name": name, "position": position, "actual": round(stat.dk_points, 4),
                    "touches": stat.stats.get("carries", 0.0) + stat.stats.get("receptions", 0.0),
                    "history_rows": min(own_n, int(MODEL_CONFIG["max_player_games"])),
                    "base": base,
                    "half_life": {str(h): summarize_draws(d) for h, d in draws.items() if h != BASE_HALF_LIFE},
                })
        log(f"[{season}] week {week}: {sum(1 for r in rows if r['week'] == week)} rows, {time.time() - t0:.0f}s")
    return {
        "study_id": STUDY_ID, "season": season, "weeks": list(weeks), "positions": list(POSITIONS),
        "generated_at": datetime.now(timezone.utc).isoformat(), "runtime_seconds": round(time.time() - started, 1),
        "model_file_sha256": model_file_sha256(), "seed": SEED, "half_life_grid": list(half_lives),
        "history_seasons": list(sources.seasons), "source_digests": sources.digests,
        "excluded": dict(excluded), "rows": rows,
    }


# ---------------------------------------------------------------------------
# Tuning and grading
# ---------------------------------------------------------------------------

def coverage(rows, p10_key, p90_key) -> dict[str, float]:
    a = np.array([r["actual"] for r in rows])
    lo = np.array([p10_key(r) for r in rows]); hi = np.array([p90_key(r) for r in rows])
    return {"n": len(rows), "below_p10": float(np.mean(a < lo)), "above_p90": float(np.mean(a > hi)),
            "inside": float(np.mean((a >= lo) & (a <= hi)))}


def _hl_quant(r: Mapping[str, Any], half_life: float) -> Mapping[str, float]:
    return r["base"] if half_life == BASE_HALF_LIFE else r["half_life"][str(half_life)]


def tune(directory: Path = ARTIFACT_DIR) -> dict[str, Any]:
    rows = []
    for season in TUNING_SEASONS:
        rows += json.loads((directory / f"replay-{season}.json").read_text(encoding="utf-8"))["rows"]
    gated = [r for r in rows if r["position"] in GATED]
    grid = []
    for h in HALF_LIFE_GRID:
        q = [_hl_quant(r, h) for r in gated]
        loss = float(np.mean([range_loss(r["actual"], x["p10"], x["p90"]) for r, x in zip(gated, q)]))
        mae = float(np.mean([abs(x["mean"] - r["actual"]) for r, x in zip(gated, q)]))
        per_pos = {}
        for pos in GATED:
            sub = [(r, _hl_quant(r, h)) for r in gated if r["position"] == pos]
            per_pos[pos] = {"range_loss": float(np.mean([range_loss(r["actual"], x["p10"], x["p90"]) for r, x in sub])),
                            **coverage([r for r, _ in sub], lambda r: _hl_quant(r, h)["p10"], lambda r: _hl_quant(r, h)["p90"])}
        grid.append({"half_life": h, "range_loss": loss, "mae": mae, "by_position": per_pos})
    best = min(grid, key=lambda g: (g["range_loss"], g["half_life"]))
    scales = {}
    for pos in GATED:
        sub = [r for r in gated if r["position"] == pos]
        scales[pos] = {"a": fit_scale(sub, 0.10)["scale"], "b": fit_scale(sub, 0.90)["scale"], "n": len(sub)}
    frozen = {
        "study_id": STUDY_ID,
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "tuning_seasons": list(TUNING_SEASONS),
        "tuning_rows": len(gated),
        "candidate_a": {"half_life": best["half_life"], "dropped": best["half_life"] == BASE_HALF_LIFE},
        "candidate_b": scales,
        "model_file_sha256": model_file_sha256(),
    }
    report = {"grid": grid, "frozen": frozen,
              "baseline_coverage_tuning": {pos: coverage([r for r in rows if r["position"] == pos],
                                                         lambda r: r["base"]["p10"], lambda r: r["base"]["p90"])
                                           for pos in POSITIONS}}
    return report


def arm_quantiles(r: Mapping[str, Any], arm: str, frozen: Mapping[str, Any]) -> tuple[float, float, float]:
    """(mean, p10, p90) of one row under 'base', 'A' or 'B'."""
    b = r["base"]
    if arm == "base" or r["position"] not in GATED:
        return b["mean"], b["p10"], b["p90"]
    if arm == "A":
        x = _hl_quant(r, float(frozen["candidate_a"]["half_life"]))
        return x["mean"], x["p10"], x["p90"]
    s = frozen["candidate_b"][r["position"]]
    lo, hi = scale_tails(b["mean"], b["p10"], b["p90"], s["a"], s["b"])
    return b["mean"], lo, hi


def grade_season(rows: Sequence[Mapping[str, Any]], arm: str, frozen: Mapping[str, Any]) -> dict[str, Any]:
    gated = [r for r in rows if r["position"] in GATED]
    games = [r["game_id"] for r in gated]
    weeks = [f"{r['season']}-{r['week']}" for r in gated]
    loss_d, mae_d, p10_d, p90_d = [], [], [], []
    for r in gated:
        bm, b10, b90 = arm_quantiles(r, "base", frozen)
        cm, c10, c90 = arm_quantiles(r, arm, frozen)
        a = r["actual"]
        loss_d.append(range_loss(a, c10, c90) - range_loss(a, b10, b90))
        p10_d.append(pinball(0.10, c10, a) - pinball(0.10, b10, a))
        p90_d.append(pinball(0.90, c90, a) - pinball(0.90, b90, a))
        mae_d.append(abs(cm - a) - abs(bm - a))
    out: dict[str, Any] = {
        "arm": arm, "n": len(gated), "games": len(set(games)),
        "range_loss_delta": game_bootstrap(loss_d, games, draws=BOOTSTRAP_DRAWS, seed=BOOTSTRAP_SEED),
        "range_loss_delta_week_clustered": game_bootstrap(loss_d, weeks, draws=BOOTSTRAP_DRAWS, seed=BOOTSTRAP_SEED),
        "p10_pinball_delta": game_bootstrap(p10_d, games, draws=2000, seed=BOOTSTRAP_SEED),
        "p90_pinball_delta": game_bootstrap(p90_d, games, draws=2000, seed=BOOTSTRAP_SEED),
        "mae_delta": game_bootstrap(mae_d, games, draws=BOOTSTRAP_DRAWS, seed=BOOTSTRAP_SEED),
        "coverage": {
            "baseline": coverage(gated, lambda r: arm_quantiles(r, "base", frozen)[1], lambda r: arm_quantiles(r, "base", frozen)[2]),
            "candidate": coverage(gated, lambda r: arm_quantiles(r, arm, frozen)[1], lambda r: arm_quantiles(r, arm, frozen)[2]),
        },
        "by_position": {},
    }
    for pos in POSITIONS:
        idx = [i for i, r in enumerate(gated) if r["position"] == pos]
        sub = [r for r in rows if r["position"] == pos]
        if pos not in GATED:
            out["by_position"][pos] = {"baseline_coverage": coverage(sub, lambda r: r["base"]["p10"], lambda r: r["base"]["p90"])}
            continue
        out["by_position"][pos] = {
            "n": len(idx),
            "range_loss_delta": game_bootstrap([loss_d[i] for i in idx], [games[i] for i in idx], draws=2000, seed=BOOTSTRAP_SEED),
            "p10_pinball_delta_point": float(np.mean([p10_d[i] for i in idx])),
            "p90_pinball_delta_point": float(np.mean([p90_d[i] for i in idx])),
            "mae_delta_point": float(np.mean([mae_d[i] for i in idx])),
            "coverage": {
                "baseline": coverage(sub, lambda r: arm_quantiles(r, "base", frozen)[1], lambda r: arm_quantiles(r, "base", frozen)[2]),
                "candidate": coverage(sub, lambda r: arm_quantiles(r, arm, frozen)[1], lambda r: arm_quantiles(r, arm, frozen)[2]),
            },
        }
    return out


def calibration_error(cov: Mapping[str, float]) -> float:
    return abs(cov["below_p10"] - 0.10) + abs(cov["above_p90"] - 0.10)


def guards(g: Mapping[str, Any]) -> dict[str, Any]:
    pos_ok = {pos: g["by_position"][pos]["range_loss_delta"]["mean"] <= POSITION_GUARD_MAX_POINT for pos in GATED}
    cov_ok = calibration_error(g["coverage"]["candidate"]) < calibration_error(g["coverage"]["baseline"])
    mae_ok = g["mae_delta"]["hi"] < MAE_GUARD_UPPER
    return {
        "primary_ci_wholly_below_zero": g["range_loss_delta"]["hi"] < 0.0,
        "no_position_worse": all(pos_ok.values()), "position_readings": pos_ok,
        "calibration_closer_to_target": cov_ok, "mae_upper_below_bound": mae_ok,
        "all_guards_hold": all(pos_ok.values()) and cov_ok and mae_ok,
    }


def verdict(per_season: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    readings = {s: guards(per_season[s]) for s in HELD_OUT_SEASONS}
    if any(not r["primary_ci_wholly_below_zero"] for r in readings.values()):
        return {"verdict": "KILL", "reason": "pooled range-loss interval reaches zero in at least one held-out season", "readings": readings}
    if all(r["all_guards_hold"] for r in readings.values()):
        return {"verdict": "H_CLEARED", "reason": "interval wholly below zero and every guard holds in 2024 and 2025; the 2026 forward gate still runs", "readings": readings}
    return {"verdict": "INCONCLUSIVE", "reason": "interval below zero but a guard failed", "readings": readings}


def grade(directory: Path = ARTIFACT_DIR) -> dict[str, Any]:
    frozen = json.loads(FROZEN_PATH.read_text(encoding="utf-8"))
    seasons = {s: json.loads((directory / f"replay-{s}.json").read_text(encoding="utf-8"))["rows"] for s in HELD_OUT_SEASONS}
    result: dict[str, Any] = {"study_id": STUDY_ID, "decided_at": datetime.now(timezone.utc).isoformat(),
                              "frozen": frozen, "arms": {}}
    arms = ["B"] if frozen["candidate_a"]["dropped"] else ["A", "B"]
    for arm in arms:
        per = {s: grade_season(rows, arm, frozen) for s, rows in seasons.items()}
        result["arms"][arm] = {"per_season": per, **verdict(per)}
    if frozen["candidate_a"]["dropped"]:
        result["arms"]["A"] = {"verdict": "DROPPED", "reason": "tuning chose the baseline half-life (3)"}
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _ci(x: Mapping[str, Any]) -> str:
    return f"{x['mean']:+.4f} [{x['lo']:+.4f}, {x['hi']:+.4f}]"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("replay"); p.add_argument("--season", type=int, required=True); p.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    p.add_argument("--weeks", default=None, help="smoke run, e.g. 4-5; written as smoke-<season>.json")
    sub.add_parser("tune")
    sub.add_parser("grade")
    args = parser.parse_args(argv)
    if not model_file_matches_pin():
        raise SystemExit(f"model/nfl_dfs_historical.py is {model_file_sha256()}, not the pinned {MODEL_FILE_SHA256}")
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    if args.cmd == "replay":
        if args.season in HELD_OUT_SEASONS and not FROZEN_PATH.exists():
            raise SystemExit("held-out season: freeze the tuning constants first (tune), and commit them")
        weeks = TARGET_WEEKS
        if args.weeks:
            lo, hi = (int(x) for x in args.weeks.split("-")); weeks = tuple(range(lo, hi + 1))
        art = replay_season(args.season, cache=args.cache, weeks=weeks, log=lambda m: print(m, flush=True))
        name = f"replay-{args.season}.json" if weeks == TARGET_WEEKS else f"smoke-{args.season}.json"
        (ARTIFACT_DIR / name).write_text(json.dumps(art, default=str), encoding="utf-8")
        print(f"{len(art['rows'])} rows, excluded {art['excluded']}, {art['runtime_seconds']}s -> {ARTIFACT_DIR / name}")
    elif args.cmd == "tune":
        if FROZEN_PATH.exists():
            raise SystemExit(f"{FROZEN_PATH} already exists; the constants are frozen once")
        rep = tune()
        (ARTIFACT_DIR / "tune.json").write_text(json.dumps(rep, indent=1), encoding="utf-8")
        FROZEN_PATH.write_text(json.dumps(rep["frozen"], indent=1), encoding="utf-8")
        print("Tuning seasons", TUNING_SEASONS, "baseline coverage (played rows):")
        for pos, c in rep["baseline_coverage_tuning"].items():
            print(f"  {pos}: n={c['n']} below P10 {c['below_p10']:.3f} above P90 {c['above_p90']:.3f} inside {c['inside']:.3f}")
        print("Half-life grid (RB+TE+WR pooled): range loss, MAE")
        for g in rep["grid"]:
            print(f"  {g['half_life']:>5}: {g['range_loss']:.4f}  MAE {g['mae']:.4f}  "
                  + "  ".join(f"{p} {v['range_loss']:.3f} ({v['below_p10']:.2f}/{v['above_p90']:.2f})" for p, v in g["by_position"].items()))
        print("Frozen:", json.dumps(rep["frozen"], indent=1))
    else:
        if (ARTIFACT_DIR / "verdict.json").exists():
            raise SystemExit("verdict already written; the grade is computed once")
        res = grade()
        (ARTIFACT_DIR / "verdict.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
        for arm, a in res["arms"].items():
            print(f"Candidate {arm}: {a['verdict']} -- {a['reason']}")
            for s, g in a.get("per_season", {}).items():
                print(f"  {s}: n={g['n']} games={g['games']} range loss {_ci(g['range_loss_delta'])}"
                      f"  week-clustered {_ci(g['range_loss_delta_week_clustered'])}  MAE {_ci(g['mae_delta'])}")
                print(f"     coverage base {g['coverage']['baseline']}  cand {g['coverage']['candidate']}")
                for pos, v in g["by_position"].items():
                    if pos in GATED:
                        print(f"     {pos}: n={v['n']} loss {_ci(v['range_loss_delta'])} p10 {v['p10_pinball_delta_point']:+.4f} "
                              f"p90 {v['p90_pinball_delta_point']:+.4f} cov base {v['coverage']['baseline']['below_p10']:.3f}/"
                              f"{v['coverage']['baseline']['above_p90']:.3f} cand {v['coverage']['candidate']['below_p10']:.3f}/"
                              f"{v['coverage']['candidate']['above_p90']:.3f}")
                    else:
                        c = v["baseline_coverage"]
                        print(f"     {pos} (reported): baseline below {c['below_p10']:.3f} above {c['above_p90']:.3f} n={c['n']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
