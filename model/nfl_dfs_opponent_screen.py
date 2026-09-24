"""Walk-forward screen for the NFL DFS opponent (defense) adjustment.

`model/nfl_dfs_historical.py` has an opponent term -- `opponent_factor ** 0.35`
on yardage and receptions -- but production never passed it: `build_week`
constructed `ProjectionContext(team_implied_total=...)` only. The 2025
backtest artifact DID include it, so production has been running a different
model than the one that was measured. And that backtest compared the full
model against a plain average; it never tested opponent-ON against
opponent-OFF. So the term's worth is unknown, and there is a specific reason to
doubt it: production already prices the defense through the Vegas team implied
total, and adding a defense term on top of that may count the same defense
twice.

PRE-REGISTERED (written before any result was computed; a different rule is a
new, separately registered screen)
------------------------------------------------------------------------------
Arms, all at nfl-dfs-historical-v5 (half-life 3) WITH the team implied total,
i.e. exactly what production runs:
  OFF   no opponent term -- production today.
  ALL   opponent term over all prior history since 2020 -- the function as
        written in model/nfl_dfs_backtest.py (16 equivalent league-average games
        of shrinkage, clipped 0.80-1.20).
  RECENT  the same function over the current and previous season only.
        Defensive fantasy points allowed carry over weakly year to year (see
        the DST screens in CLAUDE.md), so averaging a defense since 2020 may be
        mostly stale.
Team environment: the historical closing total and spread from
                  nfl_season_games (nflverse convention, positive = home
                  favoured), split the same way production now does.
Tuning seasons:   2023, 2024.  Held out: 2025, graded ONCE, for ONE candidate.
Cohort:           QB/RB/WR/TE player-weeks, target weeks 2-18, >= 2 own prior
                  games, and a closing line for the game.
Metric:           MAE against realized DK points.
Candidate:        the arm with the lowest pooled tuning MAE.
Ship rule:        candidate != OFF AND the 2025 paired delta (candidate - OFF)
                  has a 95% bootstrap CI entirely below zero, resampling
                  players; week-clustered CI reported as a sensitivity.
                  If OFF wins tuning, the term stays wired but DISABLED.
Not tested:       the exponent (0.35), the shrinkage (16), the clip -- one
                  question at a time.

WHAT IS HELD CONSTANT
---------------------
Expected projection, computed exactly, as in the half-life screen: the mean of
the model's own/peer mixture with every drawn stat line adjusted by the same
factors production would apply. A DK stat line scores as
    constant + yardage_factor * yardage_points + td_factor * td_points + bonuses
so each row is decomposed once and rescored in numpy per arm.
`check_decomposition` confirms the decomposition reproduces
`draftkings_points` exactly at factor 1.

Peer pool: the 400 prior games of the position's players nearest the target's
own mean, cached per (position, half-point bucket), with the target's own rows
removed -- the backtest harness's scheme, to within that exclusion.

Usage:
    python -m model.nfl_dfs_opponent_screen --source-root <main checkout>
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_projections import implied_from_lines
from model.nfl_dfs_backtest import load_source_rows, source_paths
from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    MODEL_VERSION,
    HistoricalWeek,
    _recency_weights,
    artifact_digest,
    draftkings_points,
)
from model.nfl_team_aliases import normalize_team

SCREEN_VERSION = "nfl-dfs-opponent-screen-v1"
POSITIONS = ("QB", "RB", "WR", "TE")
ARMS = ("OFF", "ALL", "RECENT")
TUNING_SEASONS = (2023, 2024)
HELD_OUT_SEASON = 2025
HISTORY_START = 2020
FIRST_TARGET_WEEK = 2
SHRINK_GAMES = 16.0
BOOTSTRAP_ITERATIONS = 4000
SEED = 20260924
CFG = MODEL_CONFIG


def _n(stats, key) -> float:
    value = stats.get(key)
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if value != value else value


def decompose(row: HistoricalWeek) -> tuple[float, ...]:
    """(constant, yardage points, td points, pass yds, rush yds, rec yds)."""
    s = row.stats
    py, ry, rey = _n(s, "passing_yards"), _n(s, "rushing_yards"), _n(s, "receiving_yards")
    yardage = py / 25.0 + ry / 10.0 + rey / 10.0 + _n(s, "receptions")
    tds = 4.0 * _n(s, "passing_tds") + 6.0 * _n(s, "rushing_tds") + 6.0 * _n(s, "receiving_tds")
    constant = (
        -_n(s, "passing_interceptions")
        + 2.0 * (_n(s, "passing_2pt_conversions") + _n(s, "rushing_2pt_conversions") + _n(s, "receiving_2pt_conversions"))
        + 6.0 * (_n(s, "special_teams_tds") + _n(s, "fumble_recovery_tds"))
        - _n(s, "fumbles_lost_total")
    )
    return constant, yardage, tds, py, ry, rey


def rescore(parts: np.ndarray, yardage_factor: float, td_factor: float) -> np.ndarray:
    """DK points of decomposed rows after production's stat adjustment."""
    constant, yardage, tds, py, ry, rey = parts.T
    points = constant + yardage_factor * yardage + td_factor * tds
    points += 3.0 * (yardage_factor * py >= 300)
    points += 3.0 * (yardage_factor * ry >= 100)
    points += 3.0 * (yardage_factor * rey >= 100)
    return points


def factors(team_implied: float | None, opponent: float | None) -> tuple[float, float]:
    """Production's `_environment_factors`, returning (yardage, touchdown)."""
    team = 1.0 if not team_implied or team_implied <= 0 else team_implied / CFG["league_team_points"]
    team = float(np.clip(team, CFG["min_environment_factor"], CFG["max_environment_factor"]))
    opp = float(np.clip(opponent or 1.0, 0.80, 1.20))
    return (team ** CFG["environment_yardage_exponent"]) * (opp ** CFG["opponent_exponent"]), \
        team ** CFG["environment_td_exponent"]


def check_decomposition(rows: list[HistoricalWeek], sample: int = 3000) -> float:
    step = max(1, len(rows) // sample)
    worst = 0.0
    for row in rows[::step]:
        parts = np.asarray([decompose(row)])
        worst = max(worst, abs(float(rescore(parts, 1.0, 1.0)[0]) - draftkings_points(row.position, row.stats)))
    return worst


def game_lines(db: DatabaseManager) -> dict[tuple[int, int, str], float]:
    """Implied team points for every historical game, keyed (season, week, team)."""
    out: dict[tuple[int, int, str], float] = {}
    for g in db.execute(
        """SELECT g.season, g.week, h.abbreviation home, a.abbreviation away,
                  g.quoted_total_line total, g.quoted_spread_line spread
             FROM nfl_season_games g
             JOIN nfl_teams h ON h.team_id = g.home_team_id
             JOIN nfl_teams a ON a.team_id = g.away_team_id
            WHERE g.game_type = 'REG' AND g.season BETWEEN %s AND %s""",
        (min(TUNING_SEASONS), HELD_OUT_SEASON),
    ):
        split = implied_from_lines(g["total"], None, g["spread"])
        if split:
            out[(int(g["season"]), int(g["week"]), normalize_team(g["home"]))] = split[0]
            out[(int(g["season"]), int(g["week"]), normalize_team(g["away"]))] = split[1]
    return out


def opponent_factors(prior, season: int) -> dict[str, dict[tuple[str, str], float]]:
    """Per arm: (position, defense) -> shrunk points-allowed ratio."""
    result: dict[str, dict[tuple[str, str], float]] = {"ALL": {}, "RECENT": {}}
    for arm, rows in (("ALL", prior), ("RECENT", [r for r in prior if r[0].season >= season - 1])):
        allowed: dict[tuple[str, str], list[float]] = defaultdict(list)
        league: dict[str, list[float]] = defaultdict(list)
        for row, points in rows:
            league[row.position].append(points)
            if row.opponent:
                allowed[(row.position, row.opponent)].append(points)
        league_mean = {p: float(np.mean(v)) for p, v in league.items()}
        for (position, defense), values in allowed.items():
            mean = league_mean.get(position, 0.0)
            if mean <= 0:
                continue
            shrunk = (sum(values) + SHRINK_GAMES * mean) / (len(values) + SHRINK_GAMES)
            result[arm][(position, defense)] = float(np.clip(shrunk / mean, 0.80, 1.20))
    return result


def collect(rows, parts, points, season: int, lines) -> list[dict[str, Any]]:
    targets = [i for i, r in enumerate(rows)
               if r.season == season and r.position in POSITIONS and r.week >= FIRST_TARGET_WEEK]
    out: list[dict[str, Any]] = []
    missing_line = 0
    for week in sorted({rows[i].week for i in targets}):
        prior_idx = [i for i, r in enumerate(rows) if r.season < season or (r.season == season and r.week < week)]
        prior_pairs = [(rows[i], points[i]) for i in prior_idx]
        opp = opponent_factors(prior_pairs, season)
        by_player: dict[int, list[int]] = defaultdict(list)
        by_position: dict[str, dict[int, list[int]]] = {p: defaultdict(list) for p in POSITIONS}
        for i in prior_idx:
            by_player[rows[i].player_id].append(i)
            if rows[i].position in by_position:
                by_position[rows[i].position][rows[i].player_id].append(i)
        peer_means = {p: sorted((float(np.mean(points[ix])), pid, ix) for pid, ix in d.items())
                      for p, d in by_position.items()}
        pools: dict[tuple[str, int], list[int]] = {}
        for t in (i for i in targets if rows[i].week == week):
            target = rows[t]
            own = sorted(by_player.get(target.player_id, []), key=lambda i: (rows[i].season, rows[i].week))
            own = own[-int(CFG["max_player_games"]):]
            if len(own) < int(CFG["minimum_historical_games"]):
                continue
            implied = lines.get((season, week, target.team))
            if implied is None:
                missing_line += 1
                continue
            bucket = int(round(float(np.mean(points[own])) * 2))
            key = (target.position, bucket)
            if key not in pools:
                center = bucket / 2.0
                ordered = sorted(peer_means[target.position], key=lambda m: (abs(m[0] - center), m[1]))
                pool: list[int] = []
                for _, _, ix in ordered:
                    pool.extend(ix)
                    if len(pool) >= int(CFG["max_prior_games"]) + 40:
                        break
                pools[key] = pool
            peers = [i for i in pools[key] if rows[i].player_id != target.player_id][: int(CFG["max_prior_games"])]
            weights = _recency_weights(own, float(CFG["player_half_life_games"]))
            strength = len(own) / (len(own) + float(CFG["prior_equivalent_games"]))
            proj = {}
            for arm in ARMS:
                opp_factor = None if arm == "OFF" else opp[arm].get((target.position, target.opponent))
                yf, tf = factors(implied, opp_factor)
                own_mean = float(np.dot(rescore(parts[own], yf, tf), weights))
                if peers:
                    proj[arm] = strength * own_mean + (1 - strength) * float(np.mean(rescore(parts[peers], yf, tf)))
                else:
                    proj[arm] = own_mean
            out.append({"season": season, "week": week, "position": target.position,
                        "player": target.player_gsis_id, "actual": float(points[t]), "proj": proj,
                        "opp_all": opp["ALL"].get((target.position, target.opponent)),
                        "opp_recent": opp["RECENT"].get((target.position, target.opponent))})
    print(f"  {season}: {len(out)} scored, {missing_line} skipped for a missing closing line")
    return out


def mae(cohort, arm) -> float:
    return float(np.mean([abs(r["proj"][arm] - r["actual"]) for r in cohort]))


def paired_ci(cohort, candidate: str, cluster: str) -> dict[str, float]:
    by: dict[Any, list[float]] = defaultdict(list)
    for r in cohort:
        key = r["player"] if cluster == "player" else (r["season"], r["week"])
        by[key].append(abs(r["proj"][candidate] - r["actual"]) - abs(r["proj"]["OFF"] - r["actual"]))
    clusters = list(by.values())
    sums = np.array([sum(c) for c in clusters])
    counts = np.array([len(c) for c in clusters])
    rng = np.random.default_rng(SEED)
    draws = [sums[p].sum() / counts[p].sum() for p in (rng.integers(0, len(clusters), len(clusters))
                                                       for _ in range(BOOTSTRAP_ITERATIONS))]
    return {"delta": float(sums.sum() / counts.sum()), "ci_low": float(np.quantile(draws, 0.025)),
            "ci_high": float(np.quantile(draws, 0.975)), "clusters": len(clusters)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    raw, evidence = load_source_rows(source_paths(args.source_root, range(HISTORY_START, HELD_OUT_SEASON + 1)))
    rows = [HistoricalWeek(**{**r.__dict__, "team": normalize_team(r.team), "opponent": normalize_team(r.opponent)})
            for r in raw]
    parts = np.asarray([decompose(r) for r in rows])
    points = np.asarray([draftkings_points(r.position, r.stats) for r in rows])
    worst = check_decomposition(rows)
    print(f"model {MODEL_VERSION}, half-life {CFG['player_half_life_games']}; "
          f"decomposition vs draftkings_points: max |gap| {worst:.2e}")
    if worst > 1e-9:
        raise SystemExit("decomposition does not reproduce DK scoring; refusing to grade")

    lines = game_lines(DatabaseManager(load_config().database_url, initialize_schema=False))
    print("collecting...")
    tuning = [r for s in TUNING_SEASONS for r in collect(rows, parts, points, s, lines)]
    held = collect(rows, parts, points, HELD_OUT_SEASON, lines)

    print(f"\nTUNING {TUNING_SEASONS}: n={len(tuning)}")
    print(f"  {'arm':7s} {'MAE':>7s}  {'QB':>6s} {'RB':>6s} {'WR':>6s} {'TE':>6s}")
    table = {}
    for arm in ARMS:
        by_pos = {p: mae([r for r in tuning if r['position'] == p], arm) for p in POSITIONS}
        table[arm] = {"mae": mae(tuning, arm), "by_position": by_pos}
        print(f"  {arm:7s} {table[arm]['mae']:7.4f}  " + " ".join(f"{by_pos[p]:6.3f}" for p in POSITIONS))
    candidate = min(ARMS, key=lambda a: table[a]["mae"])
    spread = {arm: (np.percentile([r[k] for r in tuning if r[k] is not None], [5, 95]).round(3).tolist())
              for arm, k in (("ALL", "opp_all"), ("RECENT", "opp_recent"))}
    print(f"  opponent factor 5th-95th pct: ALL {spread['ALL']}  RECENT {spread['RECENT']}")
    print(f"  candidate: {candidate}")

    print(f"\nHELD OUT {HELD_OUT_SEASON}: n={len(held)}")
    result: dict[str, Any] = {"candidate": candidate}
    if candidate == "OFF":
        verdict = "DO NOT ENABLE -- no opponent term beat OFF in tuning; wire it disabled."
    else:
        primary, sens = paired_ci(held, candidate, "player"), paired_ci(held, candidate, "week")
        result.update({"primary_player_clustered": primary, "sensitivity_week_clustered": sens})
        print(f"  MAE OFF {mae(held, 'OFF'):.4f}  {candidate} {mae(held, candidate):.4f}")
        print(f"  paired delta {primary['delta']:+.4f}  95% CI [{primary['ci_low']:+.4f}, {primary['ci_high']:+.4f}]"
              f"  ({primary['clusters']} players)")
        print(f"  week clusters: CI [{sens['ci_low']:+.4f}, {sens['ci_high']:+.4f}]  ({sens['clusters']} weeks)")
        verdict = (f"ENABLE {candidate} -- held-out CI excludes zero." if primary["ci_high"] < 0 else
                   f"DO NOT ENABLE -- {candidate} won tuning but the held-out CI includes zero or favours OFF.")
    print("\n  descriptive, every arm on 2025 (NOT the test):")
    descriptive = {arm: {"mae": mae(held, arm),
                         "by_position": {p: mae([r for r in held if r['position'] == p], arm) for p in POSITIONS}}
                   for arm in ARMS}
    for arm in ARMS:
        d = descriptive[arm]
        print(f"  {arm:7s} {d['mae']:7.4f}  " + " ".join(f"{d['by_position'][p]:6.3f}" for p in POSITIONS))
    print(f"\nVERDICT: {verdict}")

    payload = {"screen_version": SCREEN_VERSION, "model_version": MODEL_VERSION, "arms": ARMS,
               "tuning_seasons": TUNING_SEASONS, "held_out_season": HELD_OUT_SEASON,
               "tuning": table, "factor_spread": spread, "held_out": result,
               "held_out_descriptive": descriptive, "n_tuning": len(tuning), "n_held_out": len(held),
               "verdict": verdict, "source_evidence": evidence}
    payload["output_digest"] = artifact_digest(payload)
    if args.output:
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
