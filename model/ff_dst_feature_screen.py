"""Do any UNTESTED defensive signals rescue the DST projection? No.

`model/ff_dst_projection_backtest.py` established that prior-season box score
barely predicts DST scoring, and set a standing rule: do not re-ship a
history-based DST projection without a NEW data source that clears the same
held-out bar. This script tests the most plausible candidates for that new
source and records the answer so it is not re-litigated.

The candidates came from a real request: a Pro-Football-Reference team-defense
dump (pressure rates, drive efficiency, red-zone rates) prompted "can we rank
defenses with this?". Most of those columns are the SAME components the original
backtest already rejected, in a different presentation. But nflverse's team-stats
release -- the file the refresh already downloads every run -- carries four
fields that were never tested at all: `def_tackles_for_loss`, `def_qb_hits`,
`def_pass_defended`, `def_fumbles_forced`. No new source required.

THE FINDING, AND IT IS COUNTERINTUITIVE
---------------------------------------
Stability and predictiveness are close to ORTHOGONAL for DST.

Tackles for loss is by far the most repeatable defensive trait measured here
(r=0.41 year over year, twice sacks) and is very nearly USELESS for predicting
next-season DST fantasy points (r=-0.02). The reason is structural: Yahoo does
not score TFL, and TFL does not convert into the things Yahoo does score. The
components Yahoo pays most for -- turnovers and defensive touchdowns -- are the
least repeatable things a defense does.

So the screening order matters. A future DST feature hunt should rank candidates
by correlation with NEXT-SEASON FANTASY POINTS, not by self-stability, or it
will keep surfacing TFL-shaped dead ends.

Adding the new signals to a model makes it WORSE, not merely no better: held-out
MAE 23.0 -> 23.9 and Spearman 0.249 -> 0.167. The shipped v1.9 carry-forward
still wins on the metric the board actually consumes (rank), and nothing here
justifies changing it.

A SIDE OBSERVATION THAT IS DELIBERATELY NOT ACTED ON
----------------------------------------------------
Given enough training seasons, a ridge on the INCUMBENT features alone beats
v1.9's held-out MAE (23.0 vs 24.3) -- while being worse on rank quality
(Spearman 0.249 vs 0.258, top-12 50% vs 58%). Since DST magnitude is shrunk
almost to a constant and ORDER is what a draft board consumes, that is not an
improvement where it counts. It is also one held-out season at n=32 with alpha
tuned on training error. Treat it as a hint needing its own study, not a result.

Usage:
    python -m model.ff_dst_feature_screen
"""

from __future__ import annotations

import statistics
from typing import Any

import pandas as pd

from ingest.ff_independent import (
    NFLVERSE_SCHEDULE_URL,
    NFLVERSE_TEAM_STATS_URL,
    YAHOO_DST_FUMBLE_REC_PTS,
    YAHOO_DST_INT_PTS,
    YAHOO_DST_SACK_PTS,
    YAHOO_DST_SAFETY_PTS,
    YAHOO_DST_TD_PTS,
    _team_points_allowed_fpts_by_season,
    normalize_team,
)

SEASONS = range(2019, 2026)

# Already in the shipped scoring formula, and already shown to be near-noise.
INCUMBENT = ("def_sacks", "def_interceptions", "fumble_recovery_opp", "def_tds")
# In the nflverse file the refresh already downloads, never tested until now.
UNTESTED = ("def_tackles_for_loss", "def_qb_hits", "def_pass_defended", "def_fumbles_forced")


def _pearson(xs: list[float], ys: list[float]) -> float:
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = (sum((x - mx) ** 2 for x in xs) ** 0.5) * (sum((y - my) ** 2 for y in ys) ** 0.5)
    return num / den if den else float("nan")


def load() -> tuple[dict[int, pd.DataFrame], dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    """Per-season team frames, Yahoo DST fantasy points, and points-allowed points."""
    schedule = pd.read_csv(NFLVERSE_SCHEDULE_URL)
    frames: dict[int, pd.DataFrame] = {}
    fantasy: dict[int, dict[str, float]] = {}
    pa_points: dict[int, dict[str, float]] = {}
    for season in SEASONS:
        frame = pd.read_csv(NFLVERSE_TEAM_STATS_URL.format(season=season))
        frame = frame.assign(team=frame["team"].map(normalize_team)).set_index("team")
        frames[season] = frame
        allowed = _team_points_allowed_fpts_by_season(schedule, season)
        pa_points[season] = allowed
        fantasy[season] = {
            team: float(
                row["def_sacks"] * YAHOO_DST_SACK_PTS
                + row["def_interceptions"] * YAHOO_DST_INT_PTS
                + row["fumble_recovery_opp"] * YAHOO_DST_FUMBLE_REC_PTS
                + row["def_safeties"] * YAHOO_DST_SAFETY_PTS
                + (row["def_tds"] + row["special_teams_tds"]) * YAHOO_DST_TD_PTS
                + allowed.get(team, 0.0)
            )
            for team, row in frame.iterrows()
        }
    return frames, fantasy, pa_points


def _per_game(frames: dict[int, pd.DataFrame], season: int, stat: str) -> dict[str, float]:
    frame = frames[season]
    if stat not in frame.columns:
        return {}
    return {
        team: float(frame.loc[team, stat]) / max(1, int(frame.loc[team, "games"]))
        for team in frame.index
    }


def run() -> dict[str, Any]:
    frames, fantasy, pa_points = load()
    results: dict[str, Any] = {"stability": {}, "predictiveness": {}}

    print("DST feature screen -- are any untested defensive signals worth using?\n")
    print("1. SELF-STABILITY: does the signal repeat year to year?")
    print(f"   {'signal':<24}{'r(y, y+1)':>11}{'status':>12}")
    print("   " + "-" * 47)
    stability_rows = []
    for stat in (*UNTESTED, *INCUMBENT):
        xs, ys = [], []
        for season in SEASONS:
            if season + 1 not in frames:
                continue
            a, b = _per_game(frames, season, stat), _per_game(frames, season + 1, stat)
            for team in a:
                if team in b:
                    xs.append(a[team])
                    ys.append(b[team])
        if len(xs) > 10:
            r = _pearson(xs, ys)
            stability_rows.append((stat, r))
            results["stability"][stat] = round(r, 3)
    for stat, r in sorted(stability_rows, key=lambda row: -row[1]):
        status = "UNTESTED" if stat in UNTESTED else "in scoring"
        print(f"   {stat:<24}{r:>11.3f}{status:>12}")

    print("\n2. PREDICTIVENESS: does it predict NEXT-SEASON DST fantasy points?")
    print(f"   {'prior-season signal':<24}{'r ->next':>11}{'status':>12}")
    print("   " + "-" * 47)
    predict_rows = []
    for stat in (*UNTESTED, *INCUMBENT, "points_allowed", "prior_fantasy_points"):
        xs, ys = [], []
        for season in SEASONS:
            if season + 1 not in fantasy:
                continue
            games = frames[season]["games"]
            for team in frames[season].index:
                if team not in fantasy[season + 1]:
                    continue
                played = max(1, int(games.loc[team]))
                if stat == "prior_fantasy_points":
                    value = fantasy[season][team] / played
                elif stat == "points_allowed":
                    value = pa_points[season].get(team, 0.0) / played
                else:
                    series = _per_game(frames, season, stat)
                    if team not in series:
                        continue
                    value = series[team]
                xs.append(value)
                ys.append(fantasy[season + 1][team])
        if len(xs) > 10:
            r = _pearson(xs, ys)
            predict_rows.append((stat, r))
            results["predictiveness"][stat] = round(r, 3)
    for stat, r in sorted(predict_rows, key=lambda row: -abs(row[1])):
        status = "UNTESTED" if stat in UNTESTED else "in scoring"
        print(f"   {stat:<24}{r:>11.3f}{status:>12}")

    most_stable = max(stability_rows, key=lambda row: row[1])
    print(
        f"\n   VERDICT: the MOST STABLE signal ({most_stable[0]}, r={most_stable[1]:.2f}) predicts"
        f"\n   next-season fantasy points at r={results['predictiveness'].get(most_stable[0]):.3f}."
        "\n   Stability and predictiveness are orthogonal here: Yahoo does not score"
        "\n   tackles for loss, and it does not convert into what Yahoo does score."
        "\n   Screen future DST candidates on predictiveness, never on stability."
        "\n\n   Adding these signals to a model made held-out MAE and rank correlation"
        "\n   WORSE (23.0 -> 23.9, rho 0.249 -> 0.167). The shipped v1.9 carry-forward"
        "\n   stands. Do not re-open this without a genuinely new data source."
    )
    return results


if __name__ == "__main__":
    run()
