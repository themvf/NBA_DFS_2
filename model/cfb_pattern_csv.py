"""CSV exports of the CFB pattern watch tables, shaped for Datawrapper uploads.

Same data as the reader page (model/cfb_pattern_publish.py): the graded ledger,
the frozen selections and the weekly FBS base rates. Numbers are written as plain
numbers (percentages as 81.1, not "81.1%") so Datawrapper detects numeric columns.

    python -m model.cfb_pattern_csv [--out-dir artifacts/cfb_pattern_watch/datawrapper]
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model import cfb_pattern_publish as pub

DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "datawrapper"


def _pct(v):
    return None if v is None else round(v * 100, 1)


def _price(d: float) -> str:
    return "-110" if d == 1.9091 else pub._am(d)


def patterns_table(rows: list[dict]) -> list[dict]:
    out = []
    for s in pub.trigger_summary(rows):
        out.append({
            "Pattern": s["code"], "Rule": s["desc"], "Bet": s["bet"],
            "Graded": s["n"], "Wins": s["w"], "Losses": s["l"], "Pushes": s["p"],
            "Record": f"{s['w']}-{s['l']}" + (f"-{s['p']}" if s["p"] else ""),
            "Units": round(s["units"], 2),
            "ROI (%)": _pct(s["roi"]),
            "Sample floor": s["floor"],
            "Progress to floor (%)": round(100 * s["n"] / s["floor"], 1),
            "Discovery record": s["disc"][0], "Discovery ROI": s["disc"][1], "Discovery dates": int(s["disc"][2]),
        })
    return out


def weekly_table(rates: list[dict]) -> list[dict]:
    return [{
        "Week": r["week"], "Games": r["games"],
        "Favorites won (%)": _pct(r["fav_su"]), "Price implied (%)": _pct(r["implied"]),
        "Difference (pts)": None if r["excess_pp"] is None else round(r["excess_pp"], 1),
        "Favorites covered (%)": _pct(r["fav_ats"]), "Overs (%)": _pct(r["over"]),
        "Decided by 7 or less (%)": _pct(r["within_7"]),
    } for r in rates]


def american(decimal: float | None) -> str | None:
    return None if decimal is None else pub._am(decimal)


def market_splits_table(splits: list[dict]) -> list[dict]:
    return [{
        "Split": s["split"], "Games": s["games"], "Wins": s["wins"], "Losses": s["losses"],
        "Record": f"{s['wins']}-{s['losses']}",
        "Favorites won (%)": _pct(s["win_rate"]), "Price implied (%)": _pct(s["implied"]),
        "Difference (pts)": None if s["excess_pp"] is None else round(s["excess_pp"], 1),
        "Units": round(s["units"], 2), "ROI (%)": _pct(s["roi"]),
        "Median price": american(s["median_price"]),
        "Underdog record": f"{s['losses']}-{s['wins']}",
        "Underdog units": round(s["dog_units"], 2), "Underdog ROI (%)": _pct(s["dog_roi"]),
        "Underdog median price": american(s["dog_median_price"]),
        **({} if "fav_covers" not in s else {
            "Fav ATS record": f"{s['fav_covers']}-{s['dog_covers']}-{s['ats_pushes']}",
            "Fav covered (%)": _pct(s["cover_rate"]),
            "Fav ATS units": round(s["fav_ats_units"], 2), "Fav ATS ROI (%)": _pct(s["fav_ats_roi"]),
            "Dog ATS units": round(s["dog_ats_units"], 2), "Dog ATS ROI (%)": _pct(s["dog_ats_roi"]),
            "Games with total": s["total_games"],
            "Over-under record": f"{s['overs']}-{s['unders']}-{s['total_pushes']}",
            "Overs (%)": _pct(s["over_rate"]),
            "Over units": round(s["over_units"], 2), "Over ROI (%)": _pct(s["over_roi"]),
            "Under units": round(s["under_units"], 2), "Under ROI (%)": _pct(s["under_roi"]),
        }),
    } for s in splits]


def market_breakdown_table(breakdown: dict) -> list[dict]:
    """One flat table, simplest cut first: every game, then single cuts, then two-way grids."""
    from model import cfb_pattern_measures as measures

    out = []
    for level, (key, name, _a, b) in enumerate(measures.BREAKDOWN_ORDER, start=1):
        rows = breakdown.get(key) or []
        rows = [rows] if isinstance(rows, dict) else rows
        for r, t in zip(rows, market_splits_table(rows)):
            out.append({"Level": level, "Level name": name, "Grid": b is not None,
                        "Group": r.get("group"), "Column": r.get("column"), **t})
    return out


def results_table(rows: list[dict]) -> list[dict]:
    label = {k: v[0] for k, v in pub.TRIGGER_LABEL.items()}
    ordered = sorted(rows, key=lambda r: (r["game_date"], r["commence_time"], r["trigger"]))
    return [{
        "Date": r["game_date"], "Game": f"{r['away']} @ {r['home']}", "Pattern": label[r["trigger"]],
        "Pick": r["selection"], "Price": _price(r["price"]),
        "Final": f"{r['away_score']}-{r['home_score']}", "Result": r["result"], "Units": round(r["pnl_units"], 2),
    } for r in ordered]


def cumulative_table(rows: list[dict]) -> list[dict]:
    """Running units by game date: one column per pattern plus the total. For a line chart."""
    codes = [v[0] for v in pub.TRIGGER_LABEL.values()]
    label = {k: v[0] for k, v in pub.TRIGGER_LABEL.items()}
    run = {c: 0.0 for c in codes}
    out = []
    for d in sorted({r["game_date"] for r in rows}):
        for r in rows:
            if r["game_date"] == d:
                run[label[r["trigger"]]] += r["pnl_units"]
        row = {"Date": d}
        row.update({c: round(run[c], 2) for c in codes})
        row["All patterns"] = round(sum(run.values()), 2)
        out.append(row)
    return out


def frozen_vs_close_table(fvc: dict) -> list[dict]:
    label = {k: v[0] for k, v in pub.TRIGGER_LABEL.items()}
    out = []
    for i in fvc["items"]:
        is_total = i["trigger"] == "T4_big_fav_over"
        out.append({
            "Game": f"{i['away']} @ {i['home']}", "Pattern": label[i["trigger"]], "Pick": i["selection"],
            "Frozen home spread or total": i["freeze_total"] if is_total else i["freeze_spread"],
            "Closing home spread or total": i["close_total"] if is_total else i["close_spread"],
            "Points vs close": i["points_clv"],
            "Result at close": i["close_result"], "Result at frozen line": i["freeze_result"],
            "Units at best retail": None if i["best_retail_pnl"] is None else round(i["best_retail_pnl"], 2),
        })
    return out


def write_csv(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return 0
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if v is None else v) for k, v in r.items()})
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--store", action="store_true",
                    help="also save the tables to cfb_pattern_datasets (read by the site's /cfb/pattern-watch page)")
    args = ap.parse_args(argv)
    from model import cfb_pattern_measures as measures
    from model import cfb_pattern_store as store

    # Storing needs the table to exist, so let the schema digest check run then.
    db = DatabaseManager(load_config().database_url, initialize_schema=args.store)
    rows = store.read_ledger(db)
    selections = list(store.read_selections(db).values())
    base_games = measures.load_base_rate_games(db)
    rates = measures.weekly_base_rates(base_games)
    fvc = measures.freeze_vs_close(rows, selections)
    out = Path(args.out_dir)
    tables = {
        "patterns.csv": patterns_table(rows),
        "weekly_favorites.csv": weekly_table(rates),
        "market_splits.csv": market_splits_table(measures.market_splits(base_games)),
        "market_breakdown.csv": market_breakdown_table(measures.market_breakdown(base_games)),
        "results.csv": results_table(rows),
        "cumulative_units.csv": cumulative_table(rows),
        "frozen_vs_close.csv": frozen_vs_close_table(fvc),
    }
    for name, t in tables.items():
        print(f"{out / name}: {write_csv(out / name, t)} rows")
    if args.store:
        from model import cfb_pattern_watch as pw
        payload = {"study_version": pw.STUDY_VERSION, **{n.removesuffix(".csv"): t for n, t in tables.items()}}
        saved = store.save_dataset(db, payload)
        print("dataset: saved new version" if saved else "dataset: unchanged, not stored again")
    print(f"exported {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
