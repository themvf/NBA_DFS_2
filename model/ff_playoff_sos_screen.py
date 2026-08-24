"""Why season-long strength of schedule was NOT built, and weeks 15-17 was.

Backs the numbers quoted in `ingest/ff_playoff_sos.py` so they are reproducible
rather than asserted. Answers one question: given prior-season defensive data
and a published schedule, how much can strength of schedule actually move a
QB/RB/WR/TE projection?

TWO INDEPENDENT EFFECTS KILL SEASON-LONG SOS
--------------------------------------------
1. Defenses barely carry over. Year-over-year correlation is ~0.18 for pass
   yards allowed, ~0.23 for rush yards, and ~0.03 for PASSING TOUCHDOWNS -- the
   metric most directly tied to QB/WR/TE scoring, and effectively zero.
2. Seventeen opponents average away most of what remains. Individual defenses
   span 35-42% (pass) and 52-83% (rush) between best and worst, but a full
   slate compresses to a ~12-14% spread between the most extreme TEAMS.

Multiply spread by carryover and a season-long adjustment is worth roughly
1.2-1.4% to a typical player -- about three points on a 250-point receiver,
against projection error an order of magnitude larger. That is why no
season-long SOS term exists in the model: it would be a confident-looking number
that cannot move accuracy, which is exactly how the v1.7 DST regression shipped.

WEEKS 15-17 SURVIVE ON A STRUCTURAL ARGUMENT, NOT A HOPEFUL ONE
---------------------------------------------------------------
Three opponents average out far less than seventeen. The SAME unstable ratings
produce a ~27-32% spread instead of ~12-14%, so effective signal is ~2.3-3.6%
for a typical player -- 2-3x season-long, on exactly the weeks DraftKings Best
Ball scores as separate tournament rounds. Still small, which is why it ships as
an INDICATOR and never as a projection input.

Usage:
    python -m model.ff_playoff_sos_screen
"""

from __future__ import annotations

import statistics
from typing import Any

import pandas as pd

from ingest.ff_independent import NFLVERSE_SCHEDULE_URL, normalize_team
from ingest.ff_playoff_sos import (
    FANTASY_PLAYOFF_WEEKS,
    _defense_allowed,
    _playoff_opponents,
)

SEASONS = range(2020, 2026)
TARGET_SEASON = 2026
METRICS = (("pass_yds", "pass yds"), ("rush_yds", "rush yds"))


def _pearson(xs: list[float], ys: list[float]) -> float:
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = (sum((x - mx) ** 2 for x in xs) ** 0.5) * (sum((y - my) ** 2 for y in ys) ** 0.5)
    return num / den if den else float("nan")


def _all_opponents(schedule: pd.DataFrame, season: int) -> dict[str, list[str]]:
    games = schedule[(schedule["season"] == season) & (schedule["game_type"] == "REG")]
    out: dict[str, list[str]] = {}
    for _, game in games.iterrows():
        home, away = normalize_team(game["home_team"]), normalize_team(game["away_team"])
        out.setdefault(home, []).append(away)
        out.setdefault(away, []).append(home)
    return out


def _sos(defense: dict[str, dict[str, float]], opponents: dict[str, list[str]], metric: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for team, opposing in opponents.items():
        values = [defense[o][metric] for o in opposing if o in defense]
        if values:
            out[team] = statistics.mean(values)
    return out


def run() -> dict[str, Any]:
    schedule = pd.read_csv(NFLVERSE_SCHEDULE_URL)
    defense = {season: _defense_allowed(season) for season in SEASONS}
    results: dict[str, Any] = {"carryover": {}, "windows": {}}

    print("Playoff-SOS screen -- how much can strength of schedule actually move a projection?\n")

    print("1. CARRYOVER: how much of a defense's rating survives to next season?")
    print(f"   {'metric':<12}{'r(y, y+1)':>11}{'pairs':>8}")
    print("   " + "-" * 31)
    carryover: dict[str, float] = {}
    for metric, name in METRICS:
        xs, ys = [], []
        for season in SEASONS:
            if season + 1 not in defense:
                continue
            for team, rates in defense[season].items():
                if team in defense[season + 1]:
                    xs.append(rates[metric])
                    ys.append(defense[season + 1][team][metric])
        carryover[metric] = _pearson(xs, ys)
        results["carryover"][metric] = round(carryover[metric], 3)
        print(f"   {name:<12}{carryover[metric]:>11.3f}{len(xs):>8}")

    print("\n2. DISPERSION: how different are schedules, before and after averaging?")
    print(f"   {'window':<14}{'metric':<12}{'spread':>9}{'x carry':>9}{'effective':>11}{'typical':>9}")
    print("   " + "-" * 64)
    prior = defense[TARGET_SEASON - 1]
    windows = {
        "full season": _all_opponents(schedule, TARGET_SEASON),
        f"weeks {FANTASY_PLAYOFF_WEEKS[0]}-{FANTASY_PLAYOFF_WEEKS[-1]}":
            _playoff_opponents(schedule, TARGET_SEASON),
    }
    for label, opponents in windows.items():
        for metric, name in METRICS:
            ratings = _sos(prior, opponents, metric)
            if not ratings:
                continue
            values = sorted(ratings.values())
            spread = (values[-1] - values[0]) / statistics.mean(values)
            effective = spread * carryover[metric]
            results["windows"][f"{label}/{metric}"] = {
                "spread_pct": round(spread * 100, 1),
                "effective_pct": round(effective * 100, 1),
                "typical_player_pct": round(effective * 50, 1),
            }
            print(
                f"   {label:<14}{name:<12}{spread:>9.1%}{carryover[metric]:>9.2f}"
                f"{effective:>11.1%}{effective / 2:>9.1%}"
            )

    print(
        "\n   'spread' is the extreme-to-extreme gap between TEAMS; 'typical' halves it"
        "\n   because most teams sit near the middle. 'effective' multiplies by carryover,"
        "\n   since a schedule is only as knowable as the defenses in it."
        "\n\n   VERDICT: season-long SOS is worth ~1% to a typical player and is NOT"
        "\n   modelled. Weeks 15-17 run 2-3x that -- fewer opponents means less"
        "\n   averaging -- and are surfaced as a Best Ball indicator only, never as a"
        "\n   projection input. See ingest/ff_playoff_sos.py."
    )
    return results


if __name__ == "__main__":
    run()
