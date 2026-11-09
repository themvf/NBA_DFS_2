"""Discovery check: NFL first-TD forecasts re-created for past weeks, scored against DraftKings' prices.

The live model only started publishing on 2026-11-07, so earlier weeks are re-created here from data
available before each game (play-by-play history before the week, the closing spread and total) and
priced at DraftKings' last first-TD price before kickoff. Two versions: the model alone, and the model
with players DraftKings left off its anytime-TD list removed. Our injury reports are NOT used (we only
store their current state), so this is not the live -lines-v2 version. Discovery only: it is not a
forward record and changes nothing.

    python -m research.nfl_first_td_market_check --season 2026 --weeks 6-8 [--json F]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model import nfl_first_td as n
from model import nfl_first_td_market as mk


def run(db: DatabaseManager, season: int, first: int, last: int) -> dict:
    games = n.upcoming(db, "g.season = %s AND g.week BETWEEN %s AND %s AND g.kickoff < NOW()", (season, first, last))
    labelled = {g["game_id"]: g for g in n.load_games(db)}
    games = [g for g in games if g["game_id"] in labelled]
    history, firsts, names = n._inputs(db)
    lines = n.load_lines(db, games)
    ids = [g["game_id"] for g in games]
    opening, positions, every_name = mk.load_opening(db, ids), mk.load_positions(db), mk.all_names(db)
    out = {"season": season, "weeks": [first, last], "games": len(games), "versions": {}}
    for label, filt in (("model_alone", False), ("dk_list_only", True)):
        rows = []
        for f in n.predict(games, n.FITTED, history, firsts, names, lines, filter_lines=filt):
            g = next(x for x in games if x["game_id"] == f["game_id"])
            px = mk.load_prices(db, g["home_odds_name"], g["away_odds_name"], g["commence_time"], g["commence_time"])
            lab = n.label(labelled[f["game_id"]])
            row = mk.grade_game(f["dist"], every_name, px["prices"] if px else {}, lab, positions,
                                opening.get(f["game_id"]), f["home"], f["away"])
            row["game"], row["week"] = f["game"], f["week"]
            rows.append(row)
        summary = mk.summarize(rows)
        by_week = {}
        for w in range(first, last + 1):
            wk = [r for r in rows if r["week"] == w]
            if wk:
                by_week[w] = mk.summarize(wk)["bets"]
        summary["by_week"] = by_week
        summary["biggest_disagreements"] = sorted(
            ({"game": r["game"], "name": b["name"], "prob": round(b["prob"], 3), "american": b["american"],
              "won": b["won"]} for r in rows for b in r["priced"]),
            key=lambda b: -(b["prob"] * mk.american_to_decimal(b["american"])))[:10]
        out["versions"][label] = summary
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--weeks", default="6-8")
    ap.add_argument("--json", type=Path)
    args = ap.parse_args(argv)
    first, last = (int(x) for x in args.weeks.split("-"))
    report = run(DatabaseManager(load_config().database_url, initialize_schema=False), args.season, first, last)
    text = json.dumps(report, indent=2, default=str)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
