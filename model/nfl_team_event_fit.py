"""Fit P(a team does X in a game) for the DK "All Teams to Score ..." markets.

These markets are a different question from everything else on the specials
board. The ranked families ask "which selection wins?", which a projected mean
can order honestly. These ask "will EVERY team in the window do X?", where
there is nothing to rank and the only honest answer is a probability.

So this fits one, from real results rather than from a projection:

    P(team scores >=1 TD | its implied team total)

and then the board multiplies across the teams in the window. Two measured
decisions are baked in and should not be undone without re-measuring:

**Not every event depends on the implied total.** Touchdowns clearly do -- over
2021-2025 the rate climbs 82% -> 99% from the weakest implied totals to the
strongest. Field goals clearly do NOT: the rate sits at 80-86% across every
band, because weak offences stall in field-goal range while strong ones score
touchdowns instead, and the two effects cancel. Conditioning a field goal on
the total would be false precision, so each event gets a slope only if the data
shows one.

**Independence across teams is close enough, and the error is disclosed rather
than corrected.** Checked against 89 real Sunday 1pm windows: multiplying
per-team probabilities predicted 32.9% of slates would have every team score a
touchdown, against 31.5% observed; 6.6% against 5.6% for field goals; 85.1%
against 79.8% for any points. All three gaps are negative, so independence is
mildly optimistic -- real slates share weather, officiating and league-wide
scoring environment. The gaps are inside sampling noise at n=89 (SE ~4.9pp), so
fitting a correction factor to them would be fitting noise, which this repo has
been burned by often enough. The gap is reported on the page instead.

Usage:
    python -m model.nfl_team_event_fit                 # fit and write the artifact
    python -m model.nfl_team_event_fit --seasons 2021 2025
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

ARTIFACT = Path("artifacts/nfl_team_event_rates.json")
FIT_VERSION = "nfl-team-events-v1"
NFLVERSE_TEAM_WEEK = ("https://github.com/nflverse/nflverse-data/releases/download/"
                      "stats_team/stats_team_week_{season}.csv")
NFLVERSE_GAMES = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"

# A slope is kept only if it moves the probability by at least this much across
# the observed range of implied totals. Below it the event is modelled flat --
# see the field-goal case in the module docstring.
MIN_SLOPE_EFFECT = 0.05


@dataclass(frozen=True)
class Event:
    key: str
    label: str
    dk_market: str
    predicate: Callable[[Any], Any]


EVENTS: tuple[Event, ...] = (
    Event("td", "scores 1+ TD", "All Teams to Score 1+ TD",
          lambda d: d["td"] >= 1),
    Event("two_td", "scores 2+ TDs", "All Teams to Score 2+ TDs",
          lambda d: d["td"] >= 2),
    Event("fg", "kicks 1+ FG", "All Teams to Score 1+ FG",
          lambda d: d["fg"] >= 1),
    Event("td_and_fg", "scores 1+ TD and 1+ FG", "All Teams to Score 1+ TD & 1+ FG",
          lambda d: (d["td"] >= 1) & (d["fg"] >= 1)),
    Event("passing_td", "throws 1+ passing TD", "All Teams to Score 1+ Passing TD",
          lambda d: d["passing_tds"] >= 1),
    Event("rushing_td", "runs in 1+ rushing TD", "All Teams to Score 1+ Rushing TD",
          lambda d: d["rushing_tds"] >= 1),
    Event("any_points", "scores at all", "All Teams to Score",
          lambda d: d["points"] > 0),
)


def _logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def load_frames(seasons: Sequence[int]):
    import pandas as pd

    team = pd.concat([pd.read_csv(NFLVERSE_TEAM_WEEK.format(season=s), low_memory=False)
                      for s in seasons], ignore_index=True)
    games = pd.read_csv(NFLVERSE_GAMES, low_memory=False)
    games = games[(games.season.isin(seasons)) & (games.game_type == "REG")]

    for col in ("passing_tds", "rushing_tds", "receiving_tds", "special_teams_tds",
                "def_tds", "fg_made"):
        if col not in team:
            team[col] = 0
    team["td"] = team[["passing_tds", "rushing_tds", "special_teams_tds", "def_tds"]].fillna(0).sum(axis=1)
    team["fg"] = team["fg_made"].fillna(0)
    agg = team.groupby(["season", "week", "team", "game_id"], as_index=False).agg(
        td=("td", "sum"), fg=("fg", "sum"),
        passing_tds=("passing_tds", "sum"), rushing_tds=("rushing_tds", "sum"))

    rows = []
    for row in games.itertuples():
        if any(v != v for v in (row.total_line, row.spread_line)):  # NaN
            continue
        # nflverse spread is POSITIVE when the home team is favoured.
        for team_abbr, implied, points in (
            (row.home_team, (row.total_line + row.spread_line) / 2, row.home_score),
            (row.away_team, (row.total_line - row.spread_line) / 2, row.away_score),
        ):
            rows.append({"game_id": row.game_id, "season": row.season, "week": row.week,
                         "team": team_abbr, "implied": implied, "points": points})
    lines = pd.DataFrame(rows)
    merged = lines.merge(agg, on=["season", "week", "team", "game_id"], how="inner")
    return merged.dropna(subset=["points"])


def fit_event(frame, event: Event) -> dict[str, Any]:
    """Logistic on implied total, or a flat rate when the slope does nothing."""
    import numpy as np
    from sklearn.linear_model import LogisticRegression

    y = event.predicate(frame).astype(int).to_numpy()
    x = frame["implied"].to_numpy().reshape(-1, 1)
    base = float(y.mean())
    lo, hi = float(np.percentile(x, 5)), float(np.percentile(x, 95))

    if y.min() == y.max():
        return {"kind": "flat", "rate": base, "n": int(len(y)), "reason": "no variation"}

    model = LogisticRegression().fit(x, y)
    coef, intercept = float(model.coef_[0][0]), float(model.intercept_[0])
    effect = abs(_logistic(intercept + coef * hi) - _logistic(intercept + coef * lo))
    if effect < MIN_SLOPE_EFFECT:
        return {"kind": "flat", "rate": base, "n": int(len(y)),
                "reason": f"implied total moves this only {effect * 100:.1f}pp across the range",
                "slope_effect": effect}
    return {"kind": "logistic", "coef": coef, "intercept": intercept, "n": int(len(y)),
            "base_rate": base, "slope_effect": effect,
            "range": [lo, hi],
            "at_low": _logistic(intercept + coef * lo), "at_high": _logistic(intercept + coef * hi)}


def probability(fit: dict[str, Any], implied_total: float | None) -> float:
    """Apply a fitted event to one team. Clamped: a model is not a certainty."""
    if fit["kind"] == "flat" or implied_total is None:
        value = fit.get("rate", fit.get("base_rate", 0.0))
    else:
        value = _logistic(fit["intercept"] + fit["coef"] * float(implied_total))
    return min(0.999, max(0.001, float(value)))


def fit_all(seasons: Sequence[int]) -> dict[str, Any]:
    frame = load_frames(seasons)
    fits = {e.key: {**fit_event(frame, e), "label": e.label, "dk_market": e.dk_market}
            for e in EVENTS}
    return {"fit_version": FIT_VERSION, "seasons": list(seasons),
            "team_games": int(len(frame)), "events": fits,
            "independence_note": (
                "Per-team probabilities are multiplied. Checked on 89 real Sunday 1pm "
                "windows: independence predicted 32.9% of slates with every team scoring "
                "a TD against 31.5% observed, 6.6% vs 5.6% for FG, 85.1% vs 79.8% for any "
                "points. Independence is mildly optimistic; the gaps sit inside sampling "
                "noise at n=89, so no correction is fitted."),
            }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seasons", type=int, nargs=2, default=(2021, 2025),
                        metavar=("FIRST", "LAST"))
    args = parser.parse_args(argv)
    seasons = list(range(args.seasons[0], args.seasons[1] + 1))
    try:
        artifact = fit_all(seasons)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(artifact, indent=2) + "\n")
    print(f"{artifact['team_games']:,} team-games, seasons {seasons[0]}-{seasons[-1]}\n")
    for key, fit in artifact["events"].items():
        if fit["kind"] == "flat":
            print(f"  {key:12} flat     {fit.get('rate', 0) * 100:5.1f}%   ({fit.get('reason', '')})")
        else:
            print(f"  {key:12} logistic {fit['at_low'] * 100:5.1f}% -> {fit['at_high'] * 100:5.1f}% "
                  f"across implied {fit['range'][0]:.0f}-{fit['range'][1]:.0f}")
    print(f"\nwrote {ARTIFACT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
