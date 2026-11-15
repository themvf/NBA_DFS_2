"""Multi-book NFL rushing-yards lines against results, 2023-2026 (discovery and holdout).

The archive pull behind `nfl-rushing-under-history-v1` stored six books' quotes for every
game, and that study read DraftKings only. This script builds one row per (season, game,
player) with every book's line and prices at the pregame snapshot and the player's actual
rushing yards, then scores betting rules that use the other books: shopping the under,
pricing a US book against Pinnacle's de-vigged number at the same line, and line outliers.

Discovery is 2023-2024 only. A rule is frozen in docs/nfl-rushing-multibook-study.md
before 2025 or 2026 is scored with it. Nothing here writes to the database.

    python -m research.nfl_rushing_multibook build --out artifacts/nfl_rushing_multibook.json
    python -m research.nfl_rushing_multibook screen --data artifacts/nfl_rushing_multibook.json --seasons 2023,2024
    python -m research.nfl_rushing_multibook test --data artifacts/nfl_rushing_multibook.json --seasons 2025,2026
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable, Mapping

import numpy as np

from research import nfl_prop_under_study as under
from research import nfl_rushing_under as ru

MARKET = "player_rush_yds"
SHARP = "pinnacle"
US_BOOKS = ("draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics")
BOOKS = US_BOOKS + (SHARP,)
MATCH_WINDOW_MINUTES = 120
SEED = 20261114
RESAMPLES = 2000


def decimal(price: float) -> float:
    return under.american_to_decimal(price)


def fair_over(over: float, under_price: float) -> float:
    """De-vigged probability of the over from one book's two-sided quote (proportional)."""
    a, b = 1 / decimal(over), 1 / decimal(under_price)
    return a / (a + b)


def quote(raw: Any) -> dict | None:
    q = ru.complete_quote(raw)
    if q is None:
        return None
    q["updated"] = raw.get("lastUpdate")
    return q


def build_rows(games: list[dict], captures: Iterable[Mapping[str, Any]], names: Mapping, stats: Mapping,
               covered: set[str], season: int) -> list[dict]:
    """Latest pregame capture per (game, player), every book's complete quote, and the result."""
    latest: dict[tuple[str, str], dict] = {}
    for cap in captures:
        game = ru.match_game(cap, games, MATCH_WINDOW_MINUTES)
        if game is None:
            continue
        kickoff, captured = ru._ts(game["kickoff"]), ru._ts(cap["captured_at"])
        if not (captured < kickoff and captured < ru._ts(cap["commence_time"])):
            continue
        books = {b: q for b in BOOKS if (q := quote((cap.get("books") or {}).get(b)))}
        if not books:
            continue
        key = (game["game_id"], ru.normalize_name(cap["player"]))
        if key not in latest or captured > latest[key]["_captured"]:
            latest[key] = {"season": season, "week": game["week"], "game_id": game["game_id"], "player": cap["player"],
                           "key": key[1], "kickoff": kickoff.isoformat(), "captured_at": captured.isoformat(),
                           "_captured": captured, "books": books}
    rows = []
    for row in latest.values():
        row.pop("_captured")
        if row["game_id"] not in covered:
            continue
        ids = names.get((row["game_id"], row["key"]), [])
        stat = stats.get((row["week"], ids[0])) if len(ids) == 1 else None
        if stat is None:
            row["actual"], row["void"] = None, "no_stat_row" if len(ids) != 2 else "ambiguous_name"
        else:
            row["actual"], row["void"] = float(stat.get("rushing_yards") or 0.0), None
        rows.append(row)
    return sorted(rows, key=lambda r: (r["kickoff"], r["game_id"], r["key"]))


def load_season(season: int) -> list[dict]:
    conn = under._connect()
    cur = conn.cursor()
    reg = {**ru.REGISTRATION, "season": season}
    games, _ = ru.load_inputs(cur, reg)
    cur.execute("""SELECT event_id, player, books, captured_at, commence_time, home_team_name, away_team_name
                   FROM prop_odds_history
                   WHERE sport = 'nfl' AND market = %s AND captured_at < commence_time
                     AND commence_time >= %s AND commence_time < %s""",
                (MARKET, f"{season}-08-01", f"{season + 1}-03-01"))
    cols = ("event_id", "player", "books", "captured_at", "commence_time", "home_team_name", "away_team_name")
    captures = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.rollback()
    conn.close()
    weeks = {g["game_id"]: g["week"] for g in games}
    completed = {g["game_id"] for g in games if g["completed"]}
    stats, covered, _ = under.load_stats(season, weeks, completed)
    names, _ = ru.load_names(season)
    return build_rows(games, captures, names, stats, covered, season)


# --- betting rules: each yields bets {side, book, line, price} for one row ---

def settle(side: str, line: float, price: float, actual: float) -> float:
    if actual == line:
        return 0.0
    won = actual < line if side == "under" else actual > line
    return decimal(price) - 1 if won else -1.0


def rule_dk_under(row):
    q = row["books"].get("draftkings")
    return [("under", "draftkings", q["line"], q["under"])] if q else []


def rule_best_price_under_at_dk_line(row):
    """The under at DraftKings' line, at whichever book pays most for that same line."""
    dk = row["books"].get("draftkings")
    if not dk:
        return []
    same = [(q["under"], b) for b, q in row["books"].items() if b != SHARP and q["line"] == dk["line"]]
    price, book = max(same, key=lambda x: decimal(x[0]))
    return [("under", book, dk["line"], price)]


def rule_highest_line_under(row):
    """The under at the US book with the highest line; ties go to the best price."""
    us = [(q["line"], decimal(q["under"]), q["under"], b) for b, q in row["books"].items() if b != SHARP]
    if len(us) < 2:
        return []
    line, _, price, book = max(us)
    return [("under", book, line, price)]


def make_sharp_value(threshold: float, sides=("over", "under")) -> Callable:
    """Any US book's side priced at least `threshold` EV against Pinnacle's fair number, same line.

    One bet per row and side: the best price among qualifying books."""
    def rule(row):
        pin = row["books"].get(SHARP)
        if not pin:
            return []
        p_over = fair_over(pin["over"], pin["under"])
        out = []
        for side in sides:
            p = p_over if side == "over" else 1 - p_over
            cands = [(decimal(q[side]), q[side], b) for b, q in row["books"].items()
                     if b != SHARP and q["line"] == pin["line"] and p * decimal(q[side]) - 1 >= threshold]
            if cands:
                _, price, book = max(cands)
                out.append((side, book, pin["line"], price))
        return out
    return rule


def make_line_gap(k: float, reference: str = SHARP, sides=("over", "under")) -> Callable:
    """A US book's line at least k yards from the reference line: under when above, over when below.

    `reference` is Pinnacle or 'median' (the median line of the other books). One bet per row:
    the farthest line, ties to the best price."""
    def rule(row):
        best = None
        for b, q in row["books"].items():
            if b == SHARP:
                continue
            if reference == SHARP:
                ref = row["books"].get(SHARP)
                ref_line = ref["line"] if ref else None
            else:
                others = [x["line"] for bb, x in row["books"].items() if bb != b]
                ref_line = median(others) if len(others) >= 2 else None
            if ref_line is None:
                continue
            gap = q["line"] - ref_line
            side = "under" if gap >= k else "over" if gap <= -k else None
            if side is None or side not in sides:
                continue
            cand = (abs(gap), decimal(q[side]), side, b, q["line"], q[side])
            if best is None or cand[:2] > best[:2]:
                best = cand
        return [(best[2], best[3], best[4], best[5])] if best else []
    return rule


RULES: dict[str, Callable] = {
    "dk_under": rule_dk_under,
    "best_price_under_at_dk_line": rule_best_price_under_at_dk_line,
    "highest_line_under": rule_highest_line_under,
    **{f"sharp_value_{int(t * 100)}": make_sharp_value(t) for t in (0.02, 0.03, 0.05)},
    **{f"sharp_value_{int(t * 100)}_{s}": make_sharp_value(t, (s,)) for t in (0.02, 0.03, 0.05) for s in ("over", "under")},
    **{f"gap_vs_pinnacle_{k}": make_line_gap(k) for k in (1, 2, 3, 5)},
    **{f"gap_vs_pinnacle_{k}_{s}": make_line_gap(k, SHARP, (s,)) for k in (1, 2, 3, 5) for s in ("over", "under")},
    **{f"gap_vs_median_{k}": make_line_gap(k, "median") for k in (1, 2, 3, 5)},
    **{f"gap_vs_median_{k}_{s}": make_line_gap(k, "median", (s,)) for k in (1, 2, 3, 5) for s in ("over", "under")},
}


def bets_for(rows: Iterable[Mapping[str, Any]], rule: Callable) -> list[dict]:
    out = []
    for row in rows:
        if row["actual"] is None:
            continue
        for side, book, line, price in rule(row):
            out.append({"cluster": row["season"] * 100 + row["week"], "season": row["season"], "game_id": row["game_id"],
                        "player": row["player"], "side": side, "book": book, "line": line, "price": price,
                        "actual": row["actual"], "pnl": settle(side, line, price, row["actual"])})
    return out


def summarize(bets: list[dict]) -> dict:
    if not bets:
        return {"bets": 0}
    pnl = np.array([b["pnl"] for b in bets])
    clusters = sorted({b["cluster"] for b in bets})
    index = {c: i for i, c in enumerate(clusters)}
    s, n = np.zeros(len(clusters)), np.zeros(len(clusters))
    for b in bets:
        s[index[b["cluster"]]] += b["pnl"]
        n[index[b["cluster"]]] += 1
    interval = None
    if len(clusters) >= 4:
        draws = np.random.default_rng(SEED).integers(0, len(clusters), size=(RESAMPLES, len(clusters)))
        roi = s[draws].sum(axis=1) / n[draws].sum(axis=1)
        interval = [round(float(np.quantile(roi, 0.025)), 4), round(float(np.quantile(roi, 0.975)), 4)]
    books = defaultdict(int)
    for b in bets:
        books[b["book"]] += 1
    return {"bets": len(bets), "wins": int((pnl > 0).sum()), "losses": int((pnl < 0).sum()),
            "pushes": int((pnl == 0).sum()), "roi": round(float(pnl.mean()), 4), "interval": interval,
            "game_weeks": len(clusters), "avg_decimal": round(float(np.mean([decimal(b["price"]) for b in bets])), 3),
            "top_book_share": round(max(books.values()) / len(bets), 3), "books": dict(books),
            "by_season": {str(y): {"bets": int(sum(b["season"] == y for b in bets)),
                                   "roi": round(float(np.mean([b["pnl"] for b in bets if b["season"] == y])), 4)}
                          for y in sorted({b["season"] for b in bets})}}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out", type=Path, required=True)
    b.add_argument("--seasons", default="2023,2024,2025,2026")
    for name in ("screen", "test"):
        s = sub.add_parser(name)
        s.add_argument("--data", type=Path, required=True)
        s.add_argument("--seasons", required=True)
        s.add_argument("--rules", default="")
    a = p.parse_args()
    if a.command == "build":
        rows = [r for season in a.seasons.split(",") for r in load_season(int(season))]
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(rows, default=str))
        counts = defaultdict(int)
        for r in rows:
            counts[r["season"]] += 1
        print(json.dumps({"rows": len(rows), "by_season": dict(counts)}))
        return
    seasons = {int(x) for x in a.seasons.split(",")}
    rows = [r for r in json.loads(a.data.read_text()) if r["season"] in seasons]
    chosen = [x for x in a.rules.split(",") if x] or list(RULES)
    print(json.dumps({name: summarize(bets_for(rows, RULES[name])) for name in chosen}, indent=1))


if __name__ == "__main__":
    main()
