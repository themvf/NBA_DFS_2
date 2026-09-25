"""Projection compression study. Registered in docs/nfl-dfs-projection-tier-study.md.

Does the NFL DFS projection model project the top of the pool too low and the
bottom too high? Primary statistic: the calibration slope of actual on
projected for players with six or more games of history, with a bootstrap
that resamples games. Everything else printed here is descriptive.

    python -m model.nfl_dfs_projection_tier_study
"""

from __future__ import annotations

import random
from collections import defaultdict

REGISTRATION = "docs/nfl-dfs-projection-tier-study.md"
SEED = 20260925
DRAWS = 10_000
MIN_PLAYER_GAMES = 300
MIN_GAMES = 20
PRIMARY_COHORT = "hist_6_plus"
TIERS = (("18+", 18.0, None), ("12-18", 12.0, 18.0), ("6-12", 6.0, 12.0), ("under 6", None, 6.0))
POSITIONS = ("QB", "RB", "WR", "TE", "DST")


def eligible_rows(reports: list[dict], runs: dict[str, dict]) -> list[dict]:
    """One row per (player, game) from projection runs that could not have seen the game.

    reports: report-card payloads (season, week, projection_run_id, rows).
    runs:    projection_run_id -> {history_cutoff_season, history_cutoff_week, as_of_at}.
    """
    best: dict[tuple, tuple] = {}
    for report in reports:
        run = runs.get(report.get("projection_run_id"))
        if not run or run.get("history_cutoff_season") is None or run.get("history_cutoff_week") is None:
            continue
        slate = (int(report["season"]), int(report["week"]))
        if (int(run["history_cutoff_season"]), int(run["history_cutoff_week"])) >= slate:
            continue  # the run's history can include this slate's games
        for row in report.get("rows") or []:
            if row.get("cohort") == "out" or row.get("projected") is None or row.get("actual") is None:
                continue
            if float(row["projected"]) <= 0:
                continue
            key = (row.get("ff_player_id") or row.get("name"), row.get("game_key"))
            stamp = str(run.get("as_of_at") or "")
            if key not in best or stamp > best[key][0]:
                best[key] = (stamp, {**row, "season": slate[0], "week": slate[1]})
    return [value[1] for value in best.values()]


def ols_slope(rows: list[dict]) -> float | None:
    n = len(rows)
    if n < 3:
        return None
    xs = [float(r["projected"]) for r in rows]
    ys = [float(r["actual"]) for r in rows]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx


def mean_error(rows: list[dict]) -> float | None:
    return sum(float(r["actual"]) - float(r["projected"]) for r in rows) / len(rows) if rows else None


def clustered_interval(rows: list[dict], statistic, draws: int = DRAWS, seed: int = SEED):
    """95% interval for `statistic` from a bootstrap that resamples games."""
    by_game = defaultdict(list)
    for row in rows:
        by_game[row.get("game_key")].append(row)
    games = list(by_game)
    if len(games) < 2:
        return None
    rng = random.Random(seed)
    values = []
    for _ in range(draws):
        sample = [row for _ in games for row in by_game[rng.choice(games)]]
        value = statistic(sample)
        if value is not None:
            values.append(value)
    if not values:
        return None
    values.sort()
    return values[int(0.025 * len(values))], values[int(0.975 * len(values)) - 1]


def tier_of(projected: float) -> str:
    for name, low, high in TIERS:
        if (low is None or projected >= low) and (high is None or projected < high):
            return name
    raise ValueError(projected)


def verdict(rows: list[dict], interval) -> str:
    games = {r.get("game_key") for r in rows}
    if len(rows) < MIN_PLAYER_GAMES or len(games) < MIN_GAMES:
        return "INSUFFICIENT"
    if interval is None:
        return "INSUFFICIENT"
    low, high = interval
    if low > 1.0:
        return "CONFIRMED"
    if high < 1.0:
        return "REVERSED"
    return "NOT CONFIRMED"


def analyze(rows: list[dict], draws: int = DRAWS) -> dict:
    primary = [r for r in rows if r.get("cohort") == PRIMARY_COHORT]
    slope = ols_slope(primary)
    interval = clustered_interval(primary, ols_slope, draws)
    out = {
        "primary": {"n": len(primary), "games": len({r.get("game_key") for r in primary}),
                    "slope": slope, "interval": interval, "verdict": verdict(primary, interval)},
        "tiers": [], "positions": [],
        "all_cohorts": {"n": len(rows), "slope": ols_slope(rows), "interval": clustered_interval(rows, ols_slope, draws)},
    }
    for name, _, _ in TIERS:
        cell = [r for r in primary if tier_of(float(r["projected"])) == name]
        out["tiers"].append({"tier": name, "n": len(cell), "mean_error": mean_error(cell),
                             "interval": clustered_interval(cell, mean_error, draws) if cell else None})
    for position in POSITIONS:
        cell = [r for r in primary if r.get("position") == position]
        out["positions"].append({"position": position, "n": len(cell), "slope": ols_slope(cell),
                                 "interval": clustered_interval(cell, ols_slope, draws) if len(cell) >= 3 else None})
    return out


def _fmt(value, digits=2):
    return "  n/a" if value is None else f"{value:+.{digits}f}" if digits == 1 else f"{value:.{digits}f}"


def _ci(interval, digits=2):
    return "" if interval is None else f"[{interval[0]:.{digits}f}, {interval[1]:.{digits}f}]"


def main() -> None:
    import json
    from config import load_config
    from db.database import DatabaseManager

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT payload FROM nfl_dfs_slate_report_cards")
        reports = [r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]) for r in cur.fetchall()]
        cur.execute("SELECT run_id, history_cutoff_season, history_cutoff_week, as_of_at FROM nfl_dfs_projection_runs")
        runs = {str(r["run_id"]): {"history_cutoff_season": r["history_cutoff_season"], "history_cutoff_week": r["history_cutoff_week"],
                                   "as_of_at": r["as_of_at"].isoformat() if r["as_of_at"] else None} for r in cur.fetchall()}
    rows = eligible_rows(reports, runs)
    result = analyze(rows)
    p = result["primary"]
    print(f"Registration: {REGISTRATION}")
    print(f"Reports read: {len(reports)} | eligible player-games (all cohorts): {len(rows)}")
    print(f"\nPRIMARY ({PRIMARY_COHORT}): n={p['n']} games={p['games']}")
    print(f"  calibration slope {_fmt(p['slope'], 3)}  95% {_ci(p['interval'], 3)}  -> {p['verdict']}")
    print("\nDescriptive: mean actual - projected by projection tier")
    for t in result["tiers"]:
        print(f"  {t['tier']:>8}  n={t['n']:>4}  {_fmt(t['mean_error'], 1)}  {_ci(t['interval'], 1)}")
    print("\nDescriptive: slope within position")
    for pos in result["positions"]:
        print(f"  {pos['position']:>4}  n={pos['n']:>4}  {_fmt(pos['slope'], 3)}  {_ci(pos['interval'], 3)}")
    a = result["all_cohorts"]
    print(f"\nDescriptive: all non-out cohorts n={a['n']} slope {_fmt(a['slope'], 3)} {_ci(a['interval'], 3)}")


if __name__ == "__main__":
    main()
