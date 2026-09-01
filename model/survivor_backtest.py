"""Does planning the whole path actually beat taking the biggest favorite?

The optimizer is provably optimal against the MODEL. That is a statement about
arithmetic, not about football, and it is worth exactly nothing until the
advantage is shown to survive contact with real outcomes. This replays every
season since 1999 and measures it.

WHAT MAKES THIS HONEST
----------------------
Each decision is made with only what was knowable at the time:

  * the CURRENT week's closing spread, which a real player has before kickoff
  * MODELED spreads for every future week, from ridge ratings fit on the
    weeks already played

That is exactly what the live tool does, so the backtest measures the tool
rather than a hindsight-fed idealization of it. Feeding future closing spreads
into the planner would inflate the result and measure nothing.

Strategies:
  B0  biggest weekly favorite, reuse allowed   -- not a legal survivor entry;
      a ceiling reference for how good weekly picking could ever be
  B1  biggest favorite among unused teams      -- the real naive baseline, and
      what a median pool entry does
  S1  the assignment-optimal path, re-solved every week

Usage:
    python -m model.survivor_backtest
    python -m model.survivor_backtest --from-season 2010 --trials 2000
"""

from __future__ import annotations

import argparse
import math
import random
from collections import defaultdict

import numpy as np
import pandas as pd

from ingest.nfl_season_schedule import fetch_schedule
from model.nfl_survivor_model import (
    RIDGE_LAMBDA,
    fit_ratings,
    fit_spread_prob,
    historical_games,
    spread_to_prob,
    widen,
)

BLOCKED = 1e9
STRATEGIES = ("B0", "B1", "S1")


# ---------------------------------------------------------------------------
# Hungarian assignment (mirrors web/src/lib/nfl/survivor-assignment.ts)
# ---------------------------------------------------------------------------

def solve_assignment(cost: np.ndarray) -> list[int]:
    """Minimum-cost assignment for a rows <= cols matrix. Returns col per row."""
    n, m = cost.shape
    if n > m:
        raise ValueError(f"assignment needs rows <= cols, got {n}x{m}")
    u = np.zeros(n + 1)
    v = np.zeros(m + 1)
    p = np.zeros(m + 1, dtype=int)
    way = np.zeros(m + 1, dtype=int)

    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = np.full(m + 1, np.inf)
        used = np.zeros(m + 1, dtype=bool)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = np.inf
            j1 = 0
            for j in range(1, m + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1, j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(m + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while j0:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1

    assignment = [-1] * n
    for j in range(1, m + 1):
        if p[j]:
            assignment[p[j] - 1] = j - 1
    return assignment


# ---------------------------------------------------------------------------
# One season replay
# ---------------------------------------------------------------------------

def replay_season(
    season_games: pd.DataFrame,
    teams: list[str],
    fit: dict,
    sigma_table: dict[int, float],
) -> dict[str, dict]:
    """Walk one season week by week under each strategy.

    Returns per-strategy survival week and the (predicted, actual) pairs each
    strategy generated, which feed the calibration report.
    """
    index = {team: i for i, team in enumerate(teams)}
    weeks = sorted(int(week) for week in season_games["week"].unique())

    # Per week: team index -> (p_win, actual_won, spread, is_quoted)
    known: dict[int, dict[int, tuple[float, bool | None, float]]] = {}
    for week in weeks:
        entries: dict[int, tuple[float, bool | None, float]] = {}
        for _, game in season_games[season_games["week"] == week].iterrows():
            spread = game["spread_line"]
            if pd.isna(spread):
                continue
            spread = float(spread)
            result = game["result"]
            home_won: bool | None = None
            if not pd.isna(result):
                # A tie is a loss under the default pool rule.
                home_won = float(result) > 0
            away_won: bool | None = None
            if home_won is not None:
                away_won = float(result) < 0
            entries[index[game["home_team"]]] = (spread_to_prob(spread, fit), home_won, spread)
            entries[index[game["away_team"]]] = (spread_to_prob(-spread, fit), away_won, -spread)
        known[week] = entries

    state = {
        name: {"alive": True, "survived": 0, "used": set(), "calls": []}
        for name in STRATEGIES
    }

    for position, week in enumerate(weeks):
        this_week = known.get(week, {})
        if not this_week:
            continue

        # Ratings from every week already played -- never from the future.
        played = season_games[season_games["week"] < week]
        played = played[played["spread_line"].notna()]
        ratings: dict[str, float] | None = None
        hfa = 0.0
        if len(played) >= 32:
            ratings, hfa, _ = fit_ratings(played, teams, RIDGE_LAMBDA)

        for name in STRATEGIES:
            entry = state[name]
            if not entry["alive"]:
                continue

            legal = {
                team: value
                for team, value in this_week.items()
                if value[1] is not None and (name == "B0" or team not in entry["used"])
            }
            if not legal:
                continue

            if name in ("B0", "B1"):
                choice = max(legal, key=lambda team: legal[team][0])
            else:
                choice = _plan(
                    weeks[position:], known, legal, entry["used"], teams, ratings, hfa,
                    fit, sigma_table, index, season_games,
                )

            probability, won, _ = this_week[choice]
            entry["calls"].append((probability, bool(won)))
            entry["used"].add(choice)
            if won:
                entry["survived"] += 1
            else:
                entry["alive"] = False

    return state


def _plan(
    remaining_weeks: list[int],
    known: dict[int, dict[int, tuple[float, bool | None, float]]],
    legal_now: dict[int, tuple[float, bool | None, float]],
    used: set[int],
    teams: list[str],
    ratings: dict[str, float] | None,
    hfa: float,
    fit: dict,
    sigma_table: dict[int, float],
    index: dict[str, int],
    season_games: pd.DataFrame,
) -> int:
    """Assignment-optimal pick for the current week.

    Future weeks are filled from the rating model, widened by its own measured
    error at that horizon -- the same treatment the live grid gives a modeled
    cell. With no ratings yet (week 1) the planner has nothing to plan with and
    correctly degrades to the greedy pick.
    """
    if ratings is None:
        return max(legal_now, key=lambda team: legal_now[team][0])

    horizon_weeks = remaining_weeks[:12]  # a full season of lookahead is noise
    matrix = np.full((len(horizon_weeks), len(teams)), BLOCKED)

    for row, week in enumerate(horizon_weeks):
        if row == 0:
            for team, (probability, _, _) in legal_now.items():
                matrix[row, team] = -math.log(max(probability, 1e-6))
            continue
        for _, game in season_games[season_games["week"] == week].iterrows():
            home, away = index[game["home_team"]], index[game["away_team"]]
            spread = ratings[game["home_team"]] - ratings[game["away_team"]] + hfa
            sigma = sigma_table.get(row, max(sigma_table.values()) if sigma_table else 0.0)
            home_prob = widen(spread, sigma, fit)
            for team, probability in ((home, home_prob), (away, 1.0 - home_prob)):
                if team in used:
                    continue
                matrix[row, team] = -math.log(max(probability, 1e-6))

    for team in used:
        matrix[:, team] = BLOCKED

    assignment = solve_assignment(matrix)
    pick = assignment[0]
    if pick < 0 or matrix[0, pick] >= BLOCKED:
        return max(legal_now, key=lambda team: legal_now[team][0])
    return pick


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def bootstrap_ci(
    differences: list[float], trials: int, rng: random.Random
) -> tuple[float, float]:
    if not differences:
        return (float("nan"), float("nan"))
    means = []
    size = len(differences)
    for _ in range(trials):
        means.append(sum(rng.choice(differences) for _ in range(size)) / size)
    means.sort()
    return (means[int(0.025 * trials)], means[int(0.975 * trials)])


def calibration_table(calls: list[tuple[float, bool]]) -> list[tuple[str, int, float, float]]:
    """Predicted vs realized advance rate by probability bucket."""
    buckets: dict[str, list[tuple[float, bool]]] = defaultdict(list)
    for probability, won in calls:
        floor = min(int(probability * 10) / 10, 0.9)
        buckets[f"{floor:.1f}-{floor + 0.1:.1f}"].append((probability, won))
    rows = []
    for label in sorted(buckets):
        entries = buckets[label]
        rows.append(
            (
                label,
                len(entries),
                sum(p for p, _ in entries) / len(entries),
                sum(1 for _, won in entries if won) / len(entries),
            )
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-season", type=int, default=1999)
    parser.add_argument("--to-season", type=int, default=2025)
    parser.add_argument("--trials", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=20260901)
    args = parser.parse_args()

    schedule = fetch_schedule()
    fit = fit_spread_prob(historical_games(schedule))
    print(
        f"spread->prob fit on {fit['n']} games: "
        f"sigmoid({fit['intercept']:.5f} + {fit['slope']:.5f} * spread)\n"
    )

    # Horizon sigmas, rounded from the measured table in the survivor spec.
    sigma_table = {1: 3.35, 2: 3.63, 3: 3.95, 4: 4.20, 5: 4.36, 6: 4.50,
                   7: 4.66, 8: 4.86, 9: 4.99, 10: 5.15, 11: 5.45, 12: 5.78}

    frame = schedule[
        (schedule["game_type"] == "REG")
        & (schedule["season"] >= args.from_season)
        & (schedule["season"] <= args.to_season)
        & schedule["spread_line"].notna()
        & schedule["result"].notna()
    ].copy()

    seasons: list[int] = []
    survived: dict[str, list[int]] = {name: [] for name in STRATEGIES}
    calls: dict[str, list[tuple[float, bool]]] = {name: [] for name in STRATEGIES}

    for season, season_games in frame.groupby("season"):
        teams = sorted(set(season_games["home_team"]) | set(season_games["away_team"]))
        if len(teams) < 30:
            continue
        state = replay_season(season_games, teams, fit, sigma_table)
        seasons.append(int(season))
        for name in STRATEGIES:
            survived[name].append(state[name]["survived"])
            calls[name].extend(state[name]["calls"])

    print(f"replayed {len(seasons)} seasons ({min(seasons)}-{max(seasons)})\n")

    print("weeks survived")
    print(f"  {'strategy':<24}{'mean':>7}{'median':>8}{'>=9':>7}{'>=13':>7}{'>=17':>7}")
    labels = {
        "B0": "B0 biggest fav (reuse)",
        "B1": "B1 biggest unused fav",
        "S1": "S1 planned path",
    }
    for name in STRATEGIES:
        values = survived[name]
        print(
            f"  {labels[name]:<24}{np.mean(values):>7.2f}{np.median(values):>8.1f}"
            f"{sum(1 for v in values if v >= 9) / len(values) * 100:>6.0f}%"
            f"{sum(1 for v in values if v >= 13) / len(values) * 100:>6.0f}%"
            f"{sum(1 for v in values if v >= 17) / len(values) * 100:>6.0f}%"
        )

    rng = random.Random(args.seed)
    differences = [s1 - b1 for s1, b1 in zip(survived["S1"], survived["B1"])]
    low, high = bootstrap_ci(differences, args.trials, rng)
    mean_difference = float(np.mean(differences))
    print(
        f"\nS1 - B1: {mean_difference:+.2f} weeks per season, "
        f"95% CI [{low:+.2f}, {high:+.2f}] over {len(differences)} seasons"
    )
    wins = sum(1 for d in differences if d > 0)
    losses = sum(1 for d in differences if d < 0)
    print(f"  S1 outlasted B1 in {wins} seasons, was outlasted in {losses}, "
          f"tied in {len(differences) - wins - losses}")
    if low <= 0 <= high:
        print("  CI includes zero -- no demonstrated advantage at this sample.")
    elif low > 0:
        print("  CI excludes zero -- the planned path outlasts the greedy one.")
    else:
        print("  CI excludes zero in the WRONG direction -- greedy wins. Report it.")

    print("\ncalibration of the picks each strategy actually made (S1)")
    print(f"  {'bucket':<12}{'n':>6}{'predicted':>11}{'realized':>10}")
    for label, n, predicted, realized in calibration_table(calls["S1"]):
        print(f"  {label:<12}{n:>6}{predicted * 100:>10.1f}%{realized * 100:>9.1f}%")

    print(
        "\nNote: every season is one observation, so n is 27 at most. A 27-season "
        "\nbootstrap is a real interval but a narrow evidence base; read the "
        "\nper-season win/loss count alongside it."
    )


if __name__ == "__main__":
    main()
