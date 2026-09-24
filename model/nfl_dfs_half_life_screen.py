"""Walk-forward screen for the NFL DFS recency half-life.

`player_half_life_games = 6.0` in `model/nfl_dfs_historical.py` was set by
judgment and never fitted. The 2025 walk-forward backtest showed the full model
is, to within noise, a recency-weighted average of each player's own games --
so this constant is the model's main lever. The question raised on the week-3
ATL@GB slate was whether it reacts too slowly: with 34 games of history, a
veteran's current season carries only ~21% of the weight.

PRE-REGISTERED (written before any result was computed; do not edit the rule
after seeing output -- a different rule is a new, separately registered screen)
------------------------------------------------------------------------------
Constant under test:  `player_half_life_games`.
Grid:                 2, 3, 4, 6 (incumbent), 8, 12, 24 games.
Tuning seasons:       2023, 2024 (targets predicted from strictly earlier weeks,
                      history from 2020).
Held-out season:      2025, graded ONCE, for ONE candidate.
Cohort:               QB/RB/WR/TE player-weeks, target weeks 2-18, player needs
                      >= 2 prior games of his own (the model's own minimum).
                      Weeks 2-4 are included deliberately: the question came
                      from a week-3 slate. The backtest artifact's weeks 5-18
                      cohort is reported alongside as a descriptive split.
Metric:               MAE of the projection against realized DK points.
Candidate:            the grid value with the lowest pooled tuning MAE.
Ship rule:            candidate != 6.0 AND the 2025 paired delta
                      (candidate - incumbent) has a 95% bootstrap CI entirely
                      below zero, resampling PLAYERS (a player's errors are
                      correlated across weeks; that is the dominant dependence).
                      Week-clustered CI is reported as a sensitivity.
Not tested:           per-position half-lives. Descriptive only -- production
                      uses one constant, and four more tests would be fishing.

RESULT (2026-09-24, first and only run under this rule)
-------------------------------------------------------
Tuning MAE fell monotonically as the half-life shortened, bottoming at 3 games
(4.7027 vs incumbent 4.7591). Held out on 2025, n=5,498 player-weeks:
3.0 scored 4.6511 against the incumbent's 4.6932 -- paired delta -0.0422,
95% CI [-0.0697, -0.0160] over 576 players; week-clustered sensitivity
[-0.0634, -0.0207]. Both exclude zero. SHIPPED as nfl-dfs-historical-v5.

Read it at its real size: ~0.9% of MAE. Within-week rank correlation also
improved (0.668 -> 0.677 in tuning), which is the number a lineup consumes.
The gain is smallest in weeks 2-4 (4.848 -> 4.833 on 2025), because early in a
season almost all of a veteran's weight still sits in last year's games under
ANY half-life -- so this does not by itself fix the week-3 case that prompted
it. The model got faster at following form; it did not become a form model.

WHAT IS HELD CONSTANT, SO THIS MEASURES ONE THING
-------------------------------------------------
* Expected projection, not a Monte Carlo draw. The model mixes own and peer
  games with probability `n / (n + prior_equivalent_games)`; its projection is
  the mean of that mixture. Simulating it with a few hundred draws adds ~0.5
  points of noise per player -- larger than the effects being measured -- so
  the screen computes the mixture's expectation exactly. `check_analytic_mean`
  confirms it agrees with the simulated model.
* No environment or opponent adjustment. Production passes no opponent factor
  (see CLAUDE.md, 2026-09-24), and historical implied totals are not archived,
  so both factors are 1.0 here. The half-life acts on the player's own-history
  mean, which is upstream of both.
* The peer term is the same for every arm. It depends on the player's mean, not
  on how his games are weighted, so it cannot favour one half-life.

Usage:
    python -m model.nfl_dfs_half_life_screen
    python -m model.nfl_dfs_half_life_screen --output artifacts/nfl_dfs_half_life_screen.json
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from model.nfl_dfs_backtest import load_source_rows, source_paths
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    HistoricalWeek,
    ProjectionContext,
    _recency_weights,
    artifact_digest,
    project_player,
)

SCREEN_VERSION = "nfl-dfs-half-life-screen-v1"
POSITIONS = ("QB", "RB", "WR", "TE")
GRID = (2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 24.0)
INCUMBENT = 6.0
TUNING_SEASONS = (2023, 2024)
HELD_OUT_SEASON = 2025
HISTORY_START = 2020
FIRST_TARGET_WEEK = 2
BOOTSTRAP_ITERATIONS = 4000
SEED = 20260924


_POINTS: dict[int, float] = {}


def points_of(row: HistoricalWeek) -> float:
    """DK points for a row, scored once. The model's property rescores on every access."""
    key = id(row)
    if key not in _POINTS:
        _POINTS[key] = row.dk_points
    return _POINTS[key]


def _peer_mean_cache(prior_by_position):
    """Peer mean by (position, own-mean bucket), mirroring the backtest harness.

    Per-peer means are computed once per position per target week; the harness
    recomputes them per bucket, which is the same answer much more slowly.
    """
    peers_by_position: dict[str, list[tuple[float, int, list[float]]]] = {}
    for position, rows in prior_by_position.items():
        by_peer: dict[int, list[float]] = defaultdict(list)
        for row in rows:
            by_peer[row.player_id].append(points_of(row))
        peers_by_position[position] = [(float(np.mean(v)), pid, v) for pid, v in by_peer.items()]
    cache: dict[tuple[str, int, int], float | None] = {}

    def lookup(position: str, own: list[HistoricalWeek], player_id: int) -> float | None:
        bucket = int(round(float(np.mean([points_of(row) for row in own])) * 2))
        key = (position, bucket, player_id)
        center = bucket / 2.0
        ordered = sorted(peers_by_position[position], key=lambda item: (abs(item[0] - center), item[1]))
        pool: list[float] = []
        for _, pid, points in ordered:
            if pid == player_id:
                continue
            pool.extend(points)
            if len(pool) >= int(MODEL_CONFIG["max_prior_games"]):
                break
        pool = pool[: int(MODEL_CONFIG["max_prior_games"])]
        cache[key] = float(np.mean(pool)) if pool else None
        return cache[key]

    return lookup


def expected_projection(own_points: list[float], peer_mean: float | None, half_life: float) -> float:
    """The mean of the model's own/peer mixture, computed exactly."""
    weights = _recency_weights(own_points, half_life)
    own_mean = float(np.dot(np.asarray(own_points, dtype=float), weights))
    if peer_mean is None:
        return own_mean
    strength = len(own_points) / (len(own_points) + float(MODEL_CONFIG["prior_equivalent_games"]))
    return strength * own_mean + (1.0 - strength) * peer_mean


def collect(rows: list[HistoricalWeek], season: int) -> list[dict[str, Any]]:
    """Every eligible target in one season, with a projection per grid value."""
    targets = [r for r in rows if r.season == season and r.position in POSITIONS and r.week >= FIRST_TARGET_WEEK]
    out: list[dict[str, Any]] = []
    for week in sorted({r.week for r in targets}):
        prior = [r for r in rows if r.season < season or (r.season == season and r.week < week)]
        prior_by_position = {p: [r for r in prior if r.position == p] for p in POSITIONS}
        prior_by_player: dict[int, list[HistoricalWeek]] = defaultdict(list)
        for r in prior:
            prior_by_player[r.player_id].append(r)
        peer_mean = _peer_mean_cache(prior_by_position)
        for target in (r for r in targets if r.week == week):
            own = sorted(prior_by_player.get(target.player_id, []), key=lambda r: r.chronological_key)
            own = own[-int(MODEL_CONFIG["max_player_games"]):]
            if len(own) < int(MODEL_CONFIG["minimum_historical_games"]):
                continue
            points = [points_of(r) for r in own]
            peer = peer_mean(target.position, own, target.player_id)
            out.append({
                "season": season, "week": week, "position": target.position,
                "player": target.player_gsis_id, "history_games": len(own),
                "actual": float(points_of(target)),
                "proj": {hl: expected_projection(points, peer, hl) for hl in GRID},
            })
    return out


def mae(cohort: list[dict[str, Any]], half_life: float) -> float:
    return float(np.mean([abs(r["proj"][half_life] - r["actual"]) for r in cohort]))


def spearman_by_week(cohort: list[dict[str, Any]], half_life: float) -> float | None:
    """Mean within-week rank correlation -- what a lineup actually consumes."""
    values = []
    by_week: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for r in cohort:
        by_week[(r["season"], r["week"])].append(r)
    for rows in by_week.values():
        if len(rows) < 3:
            continue
        a = np.argsort(np.argsort([r["actual"] for r in rows]))
        p = np.argsort(np.argsort([r["proj"][half_life] for r in rows]))
        corr = np.corrcoef(a, p)[0, 1]
        if not np.isnan(corr):
            values.append(corr)
    return float(np.mean(values)) if values else None


def paired_ci(cohort: list[dict[str, Any]], candidate: float, cluster: str) -> dict[str, float]:
    """Bootstrap CI of mean(|cand err| - |incumbent err|), resampling clusters."""
    deltas_by_cluster: dict[Any, list[float]] = defaultdict(list)
    for r in cohort:
        key = r["player"] if cluster == "player" else (r["season"], r["week"])
        deltas_by_cluster[key].append(abs(r["proj"][candidate] - r["actual"]) - abs(r["proj"][INCUMBENT] - r["actual"]))
    clusters = list(deltas_by_cluster.values())
    sums = np.array([sum(c) for c in clusters])
    counts = np.array([len(c) for c in clusters])
    rng = np.random.default_rng(SEED)
    draws = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        pick = rng.integers(0, len(clusters), len(clusters))
        draws.append(sums[pick].sum() / counts[pick].sum())
    return {
        "delta": float(sums.sum() / counts.sum()),
        "ci_low": float(np.quantile(draws, 0.025)),
        "ci_high": float(np.quantile(draws, 0.975)),
        "clusters": len(clusters),
    }


def check_analytic_mean(rows: list[HistoricalWeek], samples: int = 40) -> dict[str, float]:
    """Confirm the exact expectation agrees with the model's own simulation."""
    season, week = 2024, 10
    prior = [r for r in rows if r.season < season or (r.season == season and r.week < week)]
    targets = [r for r in rows if r.season == season and r.week == week and r.position in POSITIONS][:samples]
    by_player: dict[int, list[HistoricalWeek]] = defaultdict(list)
    for r in prior:
        by_player[r.player_id].append(r)
    gaps = []
    for t in targets:
        own = sorted(by_player.get(t.player_id, []), key=lambda r: r.chronological_key)[-34:]
        if len(own) < 2:
            continue
        # No peers: the mixture reduces to the recency-weighted own mean, which
        # isolates exactly the term the half-life controls.
        sim = project_player(
            player_id=t.player_id, player_gsis_id=t.player_gsis_id, player_name=t.player_name,
            position=t.position, historical_rows=own, cutoff_season=season, cutoff_week=week,
            context=ProjectionContext(), seed=SEED, config={**MODEL_CONFIG, "draws": 20000},
        )
        exact = expected_projection([r.dk_points for r in own], None, INCUMBENT)
        gaps.append(abs(sim.model_proj_fpts - exact))
    return {"n": len(gaps), "mean_abs_gap": float(np.mean(gaps)), "max_abs_gap": float(np.max(gaps))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    paths = source_paths(args.source_root, range(HISTORY_START, HELD_OUT_SEASON + 1))
    rows, evidence = load_source_rows(paths)

    sanity = check_analytic_mean(rows)
    print(f"analytic vs simulated projection: mean |gap| {sanity['mean_abs_gap']:.3f}, "
          f"max {sanity['max_abs_gap']:.3f} over {sanity['n']} players (20,000 draws)")

    tuning = [r for season in TUNING_SEASONS for r in collect(rows, season)]
    print(f"\nTUNING {TUNING_SEASONS}: n={len(tuning)} player-weeks")
    print(f"  {'half-life':>9s} {'MAE':>7s}  {'QB':>6s} {'RB':>6s} {'WR':>6s} {'TE':>6s}  {'wk rho':>6s}")
    tuning_table = {}
    for hl in GRID:
        per_pos = {p: mae([r for r in tuning if r["position"] == p], hl) for p in POSITIONS}
        tuning_table[hl] = {"mae": mae(tuning, hl), "by_position": per_pos, "week_spearman": spearman_by_week(tuning, hl)}
        mark = "  <- incumbent" if hl == INCUMBENT else ""
        print(f"  {hl:9.0f} {tuning_table[hl]['mae']:7.4f}  " + " ".join(f"{per_pos[p]:6.3f}" for p in POSITIONS)
              + f"  {tuning_table[hl]['week_spearman']:6.4f}{mark}")
    candidate = min(GRID, key=lambda hl: tuning_table[hl]["mae"])
    print(f"\n  candidate (lowest pooled tuning MAE): {candidate:g} games")

    held = collect(rows, HELD_OUT_SEASON)
    print(f"\nHELD OUT {HELD_OUT_SEASON}: n={len(held)} player-weeks  (graded once, for the candidate only)")
    result: dict[str, Any] = {"candidate": candidate}
    if candidate == INCUMBENT:
        verdict = "NO CHANGE -- the incumbent 6.0 won tuning; nothing to grade."
    else:
        primary = paired_ci(held, candidate, "player")
        sensitivity = paired_ci(held, candidate, "week")
        result.update({"primary_player_clustered": primary, "sensitivity_week_clustered": sensitivity})
        print(f"  MAE incumbent {mae(held, INCUMBENT):.4f}  candidate {mae(held, candidate):.4f}")
        print(f"  paired delta {primary['delta']:+.4f}  95% CI [{primary['ci_low']:+.4f}, {primary['ci_high']:+.4f}]"
              f"  ({primary['clusters']} players)")
        print(f"  sensitivity, week clusters: CI [{sensitivity['ci_low']:+.4f}, {sensitivity['ci_high']:+.4f}]"
              f"  ({sensitivity['clusters']} weeks)")
        passed = primary["ci_high"] < 0
        verdict = (f"SHIP {candidate:g} -- held-out CI excludes zero." if passed else
                   f"DO NOT SHIP -- {candidate:g} won tuning but the held-out CI includes zero or favours the incumbent.")

    # Descriptive only: every grid value on 2025, and the early/late split.
    descriptive = {}
    for hl in GRID:
        early = [r for r in held if r["week"] <= 4]
        late = [r for r in held if r["week"] >= 5]
        descriptive[hl] = {"mae": mae(held, hl), "mae_weeks_2_4": mae(early, hl), "mae_weeks_5_18": mae(late, hl),
                           "by_position": {p: mae([r for r in held if r["position"] == p], hl) for p in POSITIONS},
                           "week_spearman": spearman_by_week(held, hl)}
    print(f"\n  descriptive, all grid values on {HELD_OUT_SEASON} (NOT the test):")
    print(f"  {'half-life':>9s} {'MAE':>7s} {'wk 2-4':>7s} {'wk 5-18':>7s}  {'QB':>6s} {'RB':>6s} {'WR':>6s} {'TE':>6s}")
    for hl in GRID:
        d = descriptive[hl]
        print(f"  {hl:9.0f} {d['mae']:7.4f} {d['mae_weeks_2_4']:7.4f} {d['mae_weeks_5_18']:7.4f}  "
              + " ".join(f"{d['by_position'][p]:6.3f}" for p in POSITIONS))

    print(f"\nVERDICT: {verdict}")
    payload = {
        "screen_version": SCREEN_VERSION, "grid": GRID, "incumbent": INCUMBENT,
        "tuning_seasons": TUNING_SEASONS, "held_out_season": HELD_OUT_SEASON,
        "analytic_check": sanity, "tuning": {str(k): v for k, v in tuning_table.items()},
        "held_out": result, "held_out_descriptive": {str(k): v for k, v in descriptive.items()},
        "n_tuning": len(tuning), "n_held_out": len(held), "verdict": verdict,
        "source_evidence": evidence,
    }
    payload["output_digest"] = artifact_digest(payload)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
