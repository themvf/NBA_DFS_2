"""Fit how NFL players' DraftKings scores move together (`nfl-player-correlations-v1`).

The simulated-game lineup scorer (`web/src/lib/nfl-dfs/lineup-draws.ts`) draws
every player on a slate jointly. Until this fit, the dependence was two stated
priors: every pair of teammates correlated 0.40 and every pair of opponents
0.15, whatever their positions, and a DST -0.27 with each opposing player. That
says a QB moves with his RB as much as with his WR1, and that two WRs sharing
one team's targets rise together as strongly as a QB and his receiver.

This script measures the correlation for each pair of ROLES in the same game,
from DraftKings points actually scored (`nfl_dfs_player_week_results`):

  roles     within a game, each team's players at a position are ranked by
            their season-average DK points among those who played: QB1/QB2,
            RB1/RB2/RB3 (third and deeper), WR1/WR2/WR3/WR4 (fourth and
            deeper), TE1/TE2, DST, K. The web ranks a slate the same way, by
            projection.
  scores    each player-season (player, season, team) with at least
            MIN_GAMES games is turned into normal scores (rank within his own
            games -> standard normal quantile). Pearson on normal scores is the
            Gaussian-copula correlation the simulation uses, and is robust to
            the long right tail of fantasy points.
  pairs     every pair of players in one game, keyed by (same team or
            opponents, role, role). Pairs inside one role bucket (two WR4s)
            are their own key.
  shrinkage each key is shrunk toward its coarse class (e.g. same-team
            pass-catcher pairs) with SHRINK_PAIRS pseudo-pairs, so a thin key
            like QB2-K borrows from its neighbours.

Fit on FIT_SEASONS; HOLDOUT_SEASON is scored, never fitted, and the report
compares the fitted table with the old priors on it.

Writes web/src/lib/nfl-dfs/player-correlations.ts (generated; do not edit).

    python -m research.nfl_dfs_player_correlations [--no-write]
"""
from __future__ import annotations

import argparse
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from statistics import NormalDist

import psycopg2
from dotenv import load_dotenv

VERSION = "nfl-player-correlations-v1"
FIT_SEASONS = (2023, 2024, 2025)
FIT_SCORING = "nfl-dk-realized-v2"
HOLDOUT_SEASON = 2026
HOLDOUT_SCORING = "nfl-dk-realized-v4"
MIN_GAMES = 4
SHRINK_PAIRS = 100
ROLE_DEPTH = {"QB": 2, "RB": 3, "WR": 4, "TE": 2, "DST": 1, "K": 1}
OUT = Path(__file__).resolve().parents[1] / "web" / "src" / "lib" / "nfl-dfs" / "player-correlations.ts"

# Old stated priors (nfl-lineup-draws-v1 / captain simulation): game loading
# sqrt(.15), team loading sqrt(.25), DST -sqrt(.3) on the opposing team factor.
OLD_SAME, OLD_OPP, OLD_DST_OPP = 0.40, 0.15, -math.sqrt(0.3) * 0.5


def role_of(position: str, rank: int) -> str:
    depth = ROLE_DEPTH.get(position, 1)
    if depth == 1:
        return position
    return f"{position}{min(rank, depth)}"


def coarse(position: str) -> str:
    if position in ("DST", "K"):
        return position
    if position == "QB":
        return "QB"
    return "SKILL"


def key_of(same: bool, a: str, b: str) -> str:
    x, y = sorted((a, b))
    return f"{'same' if same else 'opp'}:{x}:{y}"


def class_of(same: bool, pa: str, pb: str) -> str:
    x, y = sorted((coarse(pa), coarse(pb)))
    return f"{'same' if same else 'opp'}:{x}:{y}"


def old_prior(same: bool, pa: str, pb: str) -> float:
    if pa == "DST" and pb == "DST":
        return 0.0
    if "DST" in (pa, pb):
        return 0.0 if same else OLD_DST_OPP
    return OLD_SAME if same else OLD_OPP


def load(cur, seasons, scoring):
    cur.execute(
        """SELECT player_id, season, week, game_id, team, opponent, position, actual_dk_fpts
             FROM nfl_dfs_player_week_results
            WHERE season = ANY(%s) AND scoring_version = %s AND scoring_status = 'exact'
              AND game_id IS NOT NULL AND team IS NOT NULL""",
        (list(seasons), scoring),
    )
    return cur.fetchall()


def normal_scores(rows):
    """(player, season, team) -> normal score per row; plus the group mean for ranking."""
    nd = NormalDist()
    groups = defaultdict(list)
    for i, r in enumerate(rows):
        groups[(r[0], r[1], r[4])].append(i)
    z = [None] * len(rows)
    mean = [0.0] * len(rows)
    for idx in groups.values():
        pts = [rows[i][7] for i in idx]
        m = sum(pts) / len(pts)
        for i in idx:
            mean[i] = m
        if len(idx) < MIN_GAMES:
            continue
        order = sorted(range(len(idx)), key=lambda k: pts[k])
        ranks = [0.0] * len(idx)
        k = 0
        while k < len(order):  # average ranks over ties (many zeros)
            j = k
            while j + 1 < len(order) and pts[order[j + 1]] == pts[order[k]]:
                j += 1
            for t in range(k, j + 1):
                ranks[order[t]] = (k + j) / 2 + 1
            k = j + 1
        n = len(idx)
        for k, i in enumerate(idx):
            z[i] = nd.inv_cdf((ranks[k] - 0.5) / n)
    return z, mean


def pair_stats(rows):
    z, mean = normal_scores(rows)
    by_game = defaultdict(list)
    for i, r in enumerate(rows):
        by_game[r[3]].append(i)
    role = [None] * len(rows)
    for idx in by_game.values():
        slots = defaultdict(list)
        for i in idx:
            slots[(rows[i][4], rows[i][6])].append(i)
        for (_, pos), members in slots.items():
            members.sort(key=lambda i: (-mean[i], rows[i][0]))
            for rank, i in enumerate(members, start=1):
                role[i] = role_of(pos, rank)
    acc = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0])  # n, sx, sy, sxx, syy, sxy, both in top tail
    meta = {}
    for idx in by_game.values():
        live = [i for i in idx if z[i] is not None]
        for a in range(len(live)):
            for b in range(a + 1, len(live)):
                i, j = live[a], live[b]
                same = rows[i][4] == rows[j][4]
                ra, rb = role[i], role[j]
                if ra > rb:
                    i, j, ra, rb = j, i, rb, ra
                k = key_of(same, ra, rb)
                meta[k] = (same, rows[i][6], rows[j][6])
                s = acc[k]
                x, y = z[i], z[j]
                s[0] += 1; s[1] += x; s[2] += y; s[3] += x * x; s[4] += y * y; s[5] += x * y
                s[6] += x > TAIL_Z and y > TAIL_Z
    out = {}
    for k, (n, sx, sy, sxx, syy, sxy, both) in acc.items():
        if n < 3:
            continue
        cov = sxy / n - (sx / n) * (sy / n)
        vx, vy = sxx / n - (sx / n) ** 2, syy / n - (sy / n) ** 2
        rho = cov / math.sqrt(vx * vy) if vx > 0 and vy > 0 else 0.0
        out[k] = {"n": n, "rho": rho, "tail": both / n, "tail_rho": tail_rho(both / n),
                  "same": meta[k][0], "pa": meta[k][1], "pb": meta[k][2]}
    return out


TAIL = 0.20  # both players in the top 20% of their own games
TAIL_Z = NormalDist().inv_cdf(1 - TAIL)


def upper_orthant(rho: float, c: float = TAIL_Z) -> float:
    """P(X > c, Y > c) for a standard bivariate normal with correlation rho (Simpson's rule)."""
    nd = NormalDist()
    if abs(rho) >= 0.999:
        return 1 - nd.cdf(c) if rho > 0 else max(0.0, 1 - 2 * nd.cdf(c))
    hi, steps = c + 8, 400
    h = (hi - c) / steps
    s = math.sqrt(1 - rho * rho)
    f = lambda x: nd.pdf(x) * (1 - nd.cdf((c - rho * x) / s))
    total = f(c) + f(hi) + sum((4 if i % 2 else 2) * f(c + i * h) for i in range(1, steps))
    return total * h / 3


def tail_rho(rate: float) -> float:
    """The Gaussian correlation whose joint top-20% rate equals `rate` (bisection)."""
    lo, hi = -0.95, 0.95
    for _ in range(40):
        mid = (lo + hi) / 2
        if upper_orthant(mid) < rate:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def shrink(stats, field="rho"):
    cls = defaultdict(lambda: [0, 0.0])
    for s in stats.values():
        c = class_of(s["same"], s["pa"], s["pb"])
        cls[c][0] += s["n"]; cls[c][1] += s["n"] * s[field]
    class_rho = {c: v[1] / v[0] for c, v in cls.items() if v[0]}
    table = {}
    for k, s in stats.items():
        prior = class_rho[class_of(s["same"], s["pa"], s["pb"])]
        table[k] = (s["n"] * s[field] + SHRINK_PAIRS * prior) / (s["n"] + SHRINK_PAIRS)
    return table, class_rho


def holdout_report(table, class_rho, hold, field="rho"):
    """n-weighted squared error of each table against the holdout season's pair values (`field`)."""
    err_fit = err_old = w = 0.0
    lines = []
    for k, s in sorted(hold.items(), key=lambda kv: -kv[1]["n"]):
        fit = table.get(k, class_rho.get(class_of(s["same"], s["pa"], s["pb"]), 0.0))
        old = old_prior(s["same"], s["pa"], s["pb"])
        se = 1 / math.sqrt(s["n"])
        err_fit += s["n"] * (s[field] - fit) ** 2
        err_old += s["n"] * (s[field] - old) ** 2
        w += s["n"]
        if len(lines) < 30:
            lines.append(f"  {k:<22} n={s['n']:>5}  2026 {s[field]:+.3f} (±{1.96 * se:.3f})  fitted {fit:+.3f}  old {old:+.3f}")
    return err_fit / w, err_old / w, lines


def write_ts(tables, stats, hold_mse):
    """`tables`: {"pearson": (table, classes), "tail": (table, classes)}; `hold_mse`: same keys -> (fitted, old)."""
    rounded = lambda d: {k: round(v, 4) for k, v in sorted(d.items())}
    n = {k: stats[k]["n"] for k in sorted(stats)}
    (pair, cls), (tail_pair, tail_cls) = tables["pearson"], tables["tail"]
    body = (
        "// GENERATED by research/nfl_dfs_player_correlations.py; do not edit by hand.\n"
        f"// Fit on {', '.join(map(str, FIT_SEASONS))} DraftKings points ({FIT_SCORING}); {HOLDOUT_SEASON} held out.\n"
        f"// Holdout n-weighted squared error, fitted vs old priors: overall correlation {hold_mse['pearson'][0]:.5f} vs {hold_mse['pearson'][1]:.5f}; "
        f"joint-top-{round(TAIL * 100)}% correlation {hold_mse['tail'][0]:.5f} vs {hold_mse['tail'][1]:.5f}.\n"
        f"export const PLAYER_CORRELATIONS_VERSION = {json.dumps(VERSION)};\n"
        f"export const ROLE_DEPTH: Readonly<Record<string, number>> = {json.dumps(ROLE_DEPTH)};\n"
        "/** Key `same|opp:<role>:<role>` (roles sorted) -> Gaussian-copula correlation of normal scores (overall co-movement), shrunk. */\n"
        f"export const PAIR_CORRELATION: Readonly<Record<string, number>> = {json.dumps(rounded(pair), indent=2)};\n"
        "/** Fallback by coarse class `same|opp:<QB|SKILL|DST|K>:<...>` for a role pair never seen. */\n"
        f"export const CLASS_CORRELATION: Readonly<Record<string, number>> = {json.dumps(rounded(cls), indent=2)};\n"
        f"/** Same keys: the Gaussian correlation that reproduces how often both players finish in their own top {round(TAIL * 100)}% (joint booms), shrunk. */\n"
        f"export const PAIR_CORRELATION_TAIL: Readonly<Record<string, number>> = {json.dumps(rounded(tail_pair), indent=2)};\n"
        f"export const CLASS_CORRELATION_TAIL: Readonly<Record<string, number>> = {json.dumps(rounded(tail_cls), indent=2)};\n"
        "/** Pairs behind each fitted key (fit seasons). */\n"
        f"export const PAIR_SAMPLE: Readonly<Record<string, number>> = {json.dumps(n)};\n"
    )
    OUT.write_text(body, encoding="utf-8", newline="\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args()
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    fit_rows = load(cur, FIT_SEASONS, FIT_SCORING)
    hold_rows = load(cur, (HOLDOUT_SEASON,), HOLDOUT_SCORING)
    conn.close()
    stats = pair_stats(fit_rows)
    table, class_rho = shrink(stats)
    tail_table, tail_class = shrink(stats, "tail_rho")
    hold = pair_stats(hold_rows)
    mse_fit, mse_old, lines = holdout_report(table, class_rho, hold)
    tail_mse = holdout_report(tail_table, tail_class, hold, "tail_rho")[:2]
    print(f"Holdout {HOLDOUT_SEASON} joint-top-{round(TAIL * 100)}%: n-weighted MSE fitted {tail_mse[0]:.5f} vs old priors {tail_mse[1]:.5f}")
    print(f"{VERSION}: {len(fit_rows)} fit rows, {len(stats)} role pairs; holdout {len(hold_rows)} rows, {len(hold)} pairs")
    print("Largest fitted keys:")
    for k, s in sorted(stats.items(), key=lambda kv: -kv[1]["n"])[:40]:
        print(f"  {k:<22} n={s['n']:>6}  raw {s['rho']:+.3f}  shrunk {table[k]:+.3f}  old {old_prior(s['same'], s['pa'], s['pb']):+.3f}")
    print("Classes:", json.dumps({k: round(v, 3) for k, v in class_rho.items()}))
    print(f"Holdout {HOLDOUT_SEASON}: n-weighted MSE fitted {mse_fit:.5f} vs old priors {mse_old:.5f}")
    print("\n".join(lines))
    if not args.no_write:
        write_ts({"pearson": (table, class_rho), "tail": (tail_table, tail_class)}, stats,
                 {"pearson": (mse_fit, mse_old), "tail": tail_mse})
        print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
