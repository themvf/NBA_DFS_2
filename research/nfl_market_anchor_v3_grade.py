"""Grade partial market anchoring (`nfl-market-anchor-v3`) on stored 2026 weeks.

Population: each week's main Classic slate report card (the slate's model
projection and DK points actually scored), QB/RB/WR/TE, not ruled out, model
projection over 0.5. Lines are the latest pregame capture per market, books in
the web's order (DraftKings first), matched by name and kickoff within 30
minutes, the way `attachMarketAnchor` reads them. Players the books gave a full
set are v2 and unchanged; the graded population is the players v3 newly
anchors. On them v2 is the model, so v3 vs v2 is v3 vs the model.

Compared, paired MAE with a game-clustered bootstrap:
  * td_only   - the shipped rule: model + td_only_delta (touchdowns from the anytime-TD line)
  * partial   - every priced stat (rush yards, receptions, receiving yards, TD) from the
                line, the rest the model's (the candidate the grade rejected for volume lines)
  * placebo   - every model TD shrunk by one constant (mean market TD / mean model TD):
                does the line carry information, or does any TD shrink help?

    python -m research.nfl_market_anchor_v3_grade --weeks 1-6
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import statistics as st

from config import load_config
from db.database import DatabaseManager
from model import nfl_prop_implied as pim
from model.nfl_dfs_dk_pool_match import normalize_name
from research.nfl_prop_implied import parse_weeks

BOOKS = ("draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics", "pinnacle")  # attachMarketAnchor's order
MARKETS = tuple(pim.MARKETS) + ("player_anytime_td",)
PARTIAL = {"player_rush_yds": "rushing_yards", "player_receptions": "receptions",
           "player_reception_yds": "receiving_yards", "player_anytime_td": "scrimmage_tds"}
ALIAS = {"LA": "LAR", "WAS": "WSH", "JAC": "JAX", "AZ": "ARI"}


def _a(team):
    return ALIAS.get(team, team)


def load(db, season: int, weeks: list[int]):
    cards, means = {}, {}
    for r in db.execute(
            """SELECT DISTINCT ON (week) week, payload FROM nfl_dfs_slate_report_cards
               WHERE season = %s AND format = 'classic' AND week = ANY(%s)
               ORDER BY week, jsonb_array_length(payload->'rows') DESC, created_at DESC""", (season, weeks)):
        payload = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        cards[r["week"]] = payload["rows"]
        m = {}
        for p in db.execute("SELECT player_name, position, stat_means FROM nfl_dfs_player_projections WHERE run_id = %s",
                            (payload.get("projection_run_id"),)):
            sm = p["stat_means"] if isinstance(p["stat_means"], dict) else json.loads(p["stat_means"] or "{}")
            m.setdefault((normalize_name(p["player_name"]), p["position"]), sm)
        means[r["week"]] = m
    kick = {(g["week"], _a(g["away"]), _a(g["home"])): g["kickoff"] for g in db.execute(
        """SELECT g.week, h.abbreviation AS home, a.abbreviation AS away, g.kickoff FROM nfl_season_games g
           JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
           WHERE g.season = %s AND g.week = ANY(%s)""", (season, weeks))}
    ko = [k for k in kick.values() if k]
    props = db.execute(
        """SELECT DISTINCT ON (event_id, player, market) event_id, player, market, commence_time, books
           FROM prop_odds_history
           WHERE sport = 'nfl' AND market = ANY(%s) AND commence_time BETWEEN %s AND %s AND captured_at < commence_time
           ORDER BY event_id, player, market, captured_at DESC""", (list(MARKETS), min(ko), max(ko)))
    groups = collections.defaultdict(list)
    by_event = {}
    for r in props:
        book = next((b for b in BOOKS if (r["books"] or {}).get(b)), None)
        if not book:
            continue
        g = by_event.get((r["event_id"], r["player"]))
        if g is None:
            g = by_event[(r["event_id"], r["player"])] = {"kickoff": r["commence_time"], "lines": {}}
            groups[normalize_name(r["player"])].append(g)
        q = r["books"][book]
        g["lines"][r["market"]] = {k: q.get(k) for k in ("line", "over", "under", "yes")}
    return cards, means, kick, groups


def model_line(sm: dict) -> dict:
    f = lambda k: float(sm.get(k) or 0.0)
    return {"passing_yards": 0.0, "passing_tds": 0.0, "passing_interceptions": 0.0, "rushing_yards": f("rushing_yards"),
            "receptions": f("receptions"), "receiving_yards": f("receiving_yards"),
            "scrimmage_tds": f("rushing_tds") + f("receiving_tds")}


def _priced(m: str, q: dict | None) -> bool:
    return bool(q) and (q.get("yes") is not None if m == "player_anytime_td"
                        else all(q.get(k) is not None for k in ("line", "over", "under")))


def _calibrated(m: str, q: dict) -> float:
    raw = pim.anytime_td_rate(q["yes"]) if m == "player_anytime_td" else pim.market_mean(m, q["line"], q["over"], q["under"])
    return raw * pim.CALIBRATION_V1[m]


def rows_for(cards, means, kick, groups) -> list[dict]:
    out = []
    for w, card in sorted(cards.items()):
        for r in card:
            pos, proj = r.get("position"), r.get("projected")
            if pos not in ("RB", "WR", "TE") or proj is None or proj <= 0.5 or r.get("cohort") == "out" \
                    or r.get("status") != "scored" or r.get("actual") is None:
                continue
            away, _, home = str(r.get("game_key") or "").partition("@")
            ko = kick.get((w, _a(away), _a(home)))
            cands = [g for g in groups.get(normalize_name(r["name"]), []) if ko and abs((g["kickoff"] - ko).total_seconds()) <= 1800]
            if len(cands) != 1:
                continue
            lines = cands[0]["lines"]
            sm = means[w].get((normalize_name(r["name"]), pos))
            if pim.stat_line(pos, lines, calibration=pim.CALIBRATION_V1) is not None or not sm:
                continue  # v2 anchors him (unchanged in v3), or the model has no stat means
            M = model_line(sm)
            delta = pim.td_only_delta(pos, lines, M["scrimmage_tds"])
            if delta is None:
                continue
            K = dict(M)
            priced = [m for m in PARTIAL if _priced(m, lines.get(m))]
            for m in priced:
                K[PARTIAL[m]] = _calibrated(m, lines[m])
            out.append({"w": w, "pos": pos, "game": (w, r.get("game_key")), "act": r["actual"], "model": proj,
                        "td_only": proj + delta, "partial": proj + pim.dk_points(pos, K) - pim.dk_points(pos, M),
                        "td_model": M["scrimmage_tds"], "td_market": K["scrimmage_tds"],
                        "volume": priced != ["player_anytime_td"]})
    ratio = sum(x["td_market"] for x in out) / sum(x["td_model"] for x in out)
    for x in out:
        x["placebo"] = x["model"] + 6 * (ratio - 1) * x["td_model"]
    return out


def compare(xs: list[dict], a: str, b: str, n: int = 2000, seed: int = 7):
    games = collections.defaultdict(list)
    for x in xs:
        games[x["game"]].append(x)
    d = lambda s: st.mean(abs(x[a] - x["act"]) for x in s) - st.mean(abs(x[b] - x["act"]) for x in s)
    rng, keys = random.Random(seed), list(games)
    boots = sorted(d([x for k in rng.choices(keys, k=len(keys)) for x in games[k]]) for _ in range(n))
    return d(xs), boots[int(0.025 * n)], boots[int(0.975 * n)], len(games)


def report(rows: list[dict]) -> None:
    def line(label, xs, a, b):
        if len(xs) < 3:
            print(f"  {label:44} n={len(xs)}")
            return
        diff, lo, hi, g = compare(xs, a, b)
        print(f"  {label:44} n={len(xs):4} games={g:3}  MAE {b} {st.mean(abs(x[b] - x['act']) for x in xs):5.2f}"
              f"  {a} {st.mean(abs(x[a] - x['act']) for x in xs):5.2f}  diff {diff:+.3f} [{lo:+.3f}, {hi:+.3f}]")

    print(f"Players v3 newly anchors: {len(rows)} (weeks {sorted({x['w'] for x in rows})}); "
          f"mean model TD {st.mean(x['td_model'] for x in rows):.3f}, line TD {st.mean(x['td_market'] for x in rows):.3f}")
    for pos in ("ALL", "RB", "WR", "TE"):
        xs = rows if pos == "ALL" else [x for x in rows if x["pos"] == pos]
        print(f"-- {pos}")
        line("td_only vs model (= v2)", xs, "td_only", "model")
        line("td_only vs model, weeks 5+", [x for x in xs if x["w"] >= 5], "td_only", "model")
        line("td_only vs model, scored > 0", [x for x in xs if x["act"] > 0], "td_only", "model")
        line("td_only vs model, projected 5+", [x for x in xs if x["model"] >= 5], "td_only", "model")
        line("td_only vs constant TD shrink (placebo)", xs, "td_only", "placebo")
        vol = [x for x in xs if x["volume"]]
        line("volume/yards lines: partial vs model", vol, "partial", "model")
        line("volume/yards lines: td_only vs partial", vol, "td_only", "partial")
    print("-- by week")
    for w in sorted({x["w"] for x in rows}):
        line(f"week {w}: td_only vs model", [x for x in rows if x["w"] == w], "td_only", "model")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-6")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report(rows_for(*load(db, args.season, parse_weeks(args.weeks))))


if __name__ == "__main__":
    main()
