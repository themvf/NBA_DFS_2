"""Do the books' player lines pick each game's leader in rushing yards, receiving yards and receptions?

Per final game, the closing lines (`nfl_prop_implied_lines`, DraftKings first)
of every listed player are read as calibrated expected values
(`model/nfl_prop_implied.py`, CALIBRATION_V1) and graded two ways:

1. Top choice: the player with the highest expected value is the market's
   pick; credit 1 when he leads the game, 1/k when k players tie for the lead.
   Directly comparable with the game-leaders page's "model first choice" and
   "average baseline" (same credit rule).
2. Leader chances: each listed player's stat is drawn from a normal around his
   expected value (the conversion's own spread), INDEPENDENTLY of his
   teammates, and his share of draws as the game's top value is his chance to
   lead. Graded by the favourite's calibration and by log loss on games whose
   leader was listed. Independence ignores that teammates compete for the same
   touches (which widens the gap between them), so it overstates the
   favourite; so does the conversion's spread, a prior chosen to read a mean,
   not a whole distribution (1 catch for receptions is far too narrow). This
   reading tests the conversion as much as the market; the top choice does not.

The actual leader is read from nflverse weekly stats for both teams (players
the books did not list included). Exploratory.

    python -m research.nfl_leader_lines --season 2026 --weeks 1-5
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import statistics as st
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from config import load_config
from db.database import DatabaseManager
from model import nfl_prop_implied as pim
from model.nfl_dfs_dk_pool_match import normalize_name
from research.nfl_prop_implied import parse_weeks

METRICS = {"rushing_yards": "player_rush_yds", "receiving_yards": "player_reception_yds", "receptions": "player_receptions"}
LABEL = {"rushing_yards": "Most rushing yards", "receiving_yards": "Most receiving yards", "receptions": "Most receptions"}
ALIAS = {"LA": "LAR", "WSH": "WAS", "JAC": "JAX"}
DRAWS = 20000


def team(t):
    return ALIAS.get(t, t) if t else t


def load(db, season: int, weeks: list[int]):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=5)
    lines = collections.defaultdict(list)
    for r in db.execute(
            """SELECT week, event_id, player_key, player_name, team, market, line, over_price, under_price
               FROM nfl_prop_implied_lines WHERE season = %s AND week = ANY(%s) AND capture_role = 'close'
               AND conversion_version = %s AND market = ANY(%s) AND commence_time < %s""",
            (season, weeks, pim.VERSION, list(METRICS.values()), cutoff)):
        lines[(r["week"], r["event_id"])].append(r)
    stats = collections.defaultdict(list)
    for r in db.execute(
            """SELECT s.week, s.team, f.canonical_name, s.source_row FROM ff_player_week_stats s JOIN ff_players f ON f.id = s.player_id
               WHERE s.season = %s AND s.week = ANY(%s)""", (season, weeks)):
        row = r["source_row"] if isinstance(r["source_row"], dict) else json.loads(r["source_row"] or "{}")
        stats[(r["week"], team(r["team"] or row.get("team")))].append((normalize_name(r["canonical_name"]), row))
    return lines, stats


def spread(market: str, mean: float) -> float:
    md = pim.MARKETS[market]
    return max(md.spread, md.spread_ratio * mean)


def study(db, season: int, weeks: list[int]) -> dict:
    lines, stats = load(db, season, weeks)
    rng = np.random.default_rng(20261012)
    results = {m: [] for m in METRICS}
    for (w, event), rows in lines.items():
        teams = {team(r["team"]) for r in rows if r["team"]}
        if len(teams) != 2:
            continue
        players = {}
        for t in teams:
            for key, row in stats.get((w, t), []):
                players.setdefault(key, row)
        if not players:
            continue
        for metric, market in METRICS.items():
            listed = {}
            for r in rows:
                if r["market"] != market:
                    continue
                mean = pim.market_mean(market, r["line"], r["over_price"], r["under_price"])
                if mean is not None:
                    listed[r["player_key"]] = mean * pim.CALIBRATION_V1[market]
            if len(listed) < 2:
                continue
            actual = {k: float(v.get(metric) or 0) for k, v in players.items()}
            top = max(actual.values())
            if top <= 0:
                continue
            leaders = {k for k, v in actual.items() if v == top}
            order = sorted(listed, key=lambda k: -listed[k])
            fav, second = order[0], order[1]
            keys = list(listed)
            means = np.array([listed[k] for k in keys])
            sds = np.array([spread(market, m) for m in means])
            draws = np.maximum(rng.normal(means, sds, size=(DRAWS, len(keys))), 0)
            winners = draws.argmax(axis=1)
            share = np.bincount(winners, minlength=len(keys)) / DRAWS
            chance = dict(zip(keys, share))
            listed_leaders = leaders & set(keys)
            results[metric].append({
                "week": w, "event": event, "n_listed": len(keys),
                "credit": (1 / len(leaders)) if fav in leaders else 0.0,
                "second_credit": (1 / len(leaders)) if second in leaders else 0.0,
                "top2": bool({fav, second} & leaders),
                "fav_chance": float(chance[fav]),
                "leader_listed": bool(listed_leaders),
                "leader_chance": float(sum(chance[k] for k in listed_leaders) / len(leaders)) if listed_leaders else None,
            })
    return results


def report(results: dict, weeks: list[int], evaluation: dict | None) -> None:
    print(f"Game leaders from the books' closing lines, weeks {weeks[0]}-{weeks[-1]} (final games)\n")
    for metric, games in results.items():
        if not games:
            continue
        n = len(games)
        credit = st.mean(g["credit"] for g in games)
        w14 = [g for g in games if g["week"] <= 4]
        listed = [g for g in games if g["leader_listed"]]
        ll = -st.mean(math.log(max(g["leader_chance"], 1e-4)) for g in listed) if listed else float("nan")
        uniform = -st.mean(math.log(1 / g["n_listed"]) for g in listed) if listed else float("nan")
        print(f"== {LABEL[metric]}: {n} games, {st.mean(g['n_listed'] for g in games):.1f} players listed a game")
        w14_text = f" (weeks 1-4: {st.mean(g['credit'] for g in w14):.1%} over {len(w14)} games)" if w14 else ""
        print(f"   books' top choice led: {credit:.1%}{w14_text}")
        miss = [g for g in games if g["credit"] == 0]
        print(f"   second-biggest line led: {st.mean(g['second_credit'] for g in games):.1%}; "
              f"when the favourite missed outright it was {(st.mean(g['second_credit'] for g in miss) if miss else 0):.1%} of {len(miss)}; "
              f"leader was one of the top two in {st.mean(g['top2'] for g in games):.1%}")
        if evaluation and metric in evaluation and set(weeks) <= {1, 2, 3, 4}:
            e = evaluation[metric]
            print(f"   same weeks, game-leaders page: model first choice {e['top_choice_credit']:.1%}, recent-average baseline {e['mean_baseline_credit']:.1%}")
        print(f"   leader was a listed player in {len(listed)}/{n} games; favourite's average chance {st.mean(g['fav_chance'] for g in games):.1%}")
        print(f"   log loss on the actual leader (listed games) {ll:.3f} vs {uniform:.3f} for equal chances")
        print(f"   {'favourite chance':18}{'games':>7}{'avg chance':>12}{'led':>8}")
        for lo, hi in ((0, .35), (.35, .5), (.5, .65), (.65, 1.01)):
            b = [g for g in games if lo <= g["fav_chance"] < hi]
            if b:
                print(f"   {f'{lo:.0%}-{min(hi, 1):.0%}':18}{len(b):>7}{st.mean(g['fav_chance'] for g in b):>12.1%}{st.mean(g['credit'] for g in b):>8.1%}")
        print()


def page_evaluation(season: int) -> dict | None:
    path = Path("web/src/data/game-leaders.json")
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    for e in data.get("evaluation", []):
        if e.get("season") == season:
            return e.get("summary")
    return None


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-5")
    args = parser.parse_args(argv)
    weeks = parse_weeks(args.weeks)
    results = study(DatabaseManager(load_config().database_url, initialize_schema=False), args.season, weeks)
    report(results, weeks, page_evaluation(args.season))


if __name__ == "__main__":
    main()
