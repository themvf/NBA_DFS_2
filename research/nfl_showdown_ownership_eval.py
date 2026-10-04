"""Score Showdown ownership variants against imported contests.

    python -m research.nfl_showdown_ownership_eval \\
        --contest 195786073=path/to/contest-standings-195786073.csv \\
        --contest 195943238=path/to/contest-standings-195943238.csv

Each contest must already be imported (nfl_dfs_field_contests, attached to a
slate upload). Variants are defined in VARIANTS below; every variant is also
scored with ACTUAL ownership (a perfect forecast) and with none, because the
question is how much of the available gain a forecast captures, not its score
in isolation. Read-only: SELECTs and the standings file.
"""

from __future__ import annotations

import argparse
import collections
import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_field_audit import normalize_name
from model.nfl_dfs_field_structure import parse_lineup
from model.nfl_showdown_ownership_eval import (
    VERSION, fit_metrics, portfolio, prior_showdown, score_portfolio,
)

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

#: name -> prior_showdown keyword arguments. v2 is what shipped before this study.
VARIANTS = {
    "prior v2 (value exponent 1.5)": {"value_exponent": 1.5},
    "candidate v3 (value exponent 0.5; not live)": {"value_exponent": 0.5},
}
LEVERAGE = (0.5, 1.0)
SEEDS = (1, 2, 3)


def load_slate(db: PipelineDatabase, contest_id: str) -> tuple[int, pd.DataFrame]:
    contest = db.execute("SELECT week, slate_upload_id FROM nfl_dfs_field_contests WHERE contest_id = %s", (contest_id,))
    if not contest or not contest[0]["slate_upload_id"]:
        sys.exit(f"contest {contest_id} is not imported or has no slate attached")
    week, upload = contest[0]["week"], contest[0]["slate_upload_id"]
    slate = pd.DataFrame([dict(r) for r in db.execute(
        """SELECT normalized_name, name, position, team, salary, captain_salary, avg_fpts_dk, dk_status, is_out, our_proj
           FROM nfl_dfs_slate_players WHERE upload_id = %s""", (upload,))])
    own = {r["normalized_name"]: r["drafted_by_slot"] or {} for r in db.execute(
        "SELECT normalized_name, drafted_by_slot FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest_id,))}
    slate["flex_act"] = slate["normalized_name"].map(lambda n: float(own.get(n, {}).get("FLEX", 0) or 0))
    slate["cpt_act"] = slate["normalized_name"].map(lambda n: float(own.get(n, {}).get("CPT", 0) or 0))
    slate = slate.drop_duplicates("normalized_name")
    slate = slate[~slate["is_out"].astype(bool)].reset_index(drop=True)
    proj, avg = slate["our_proj"], slate["avg_fpts_dk"]
    # The number the field drafts on: the same 60/40 blend the prior uses.
    pts = np.where(proj.notna() & avg.notna(), 0.6 * proj + 0.4 * avg, proj.fillna(avg)).astype(float)
    slate["pts"] = np.where(np.isnan(pts) | (pts <= 0), 0.0, pts)
    # Portfolio building needs a strictly positive objective for every player it may pick.
    slate["pts"] = slate["pts"].clip(lower=0.0)
    return int(week), slate


def real_lineup_counts(path: Path) -> tuple[collections.Counter, int]:
    counts: collections.Counter = collections.Counter()
    total = 0
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) > 5 and row[5].strip() and row[0].strip().isdigit():
                parsed = parse_lineup(row[5])
                cpt = [normalize_name(n) for s, n in parsed if s == "CPT"]
                flex = sorted(normalize_name(n) for s, n in parsed if s == "FLEX")
                if len(parsed) == 6 and len(cpt) == 1 and len(flex) == 5:
                    counts[(cpt[0], tuple(flex))] += 1
                    total += 1
    return counts, total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contest", action="append", required=True, metavar="ID=PATH")
    args = parser.parse_args()
    db = PipelineDatabase(load_config().database_url)

    fit_rows, frontier_rows = [], []
    for spec in args.contest:
        contest_id, _, path = spec.partition("=")
        week, slate = load_slate(db, contest_id)
        counts, total = real_lineup_counts(Path(path))
        print(f"week {week} showdown {contest_id}: pool {len(slate)}, {total:,} real lineups, "
              f"{len(counts):,} distinct   [{VERSION}]")
        inputs = {name: prior_showdown(slate, **kw) for name, kw in VARIANTS.items()}
        inputs["ACTUAL ownership (perfect forecast)"] = (slate["flex_act"].to_numpy(float), slate["cpt_act"].to_numpy(float))
        zero = np.zeros(len(slate))
        for name, (flex, cap) in inputs.items():
            if not name.startswith("ACTUAL"):
                fit_rows.append({"week": week, "variant": name, **fit_metrics(slate, flex, cap)})
        for leverage in LEVERAGE:
            for name, (flex, cap) in {**inputs, "none": (zero, zero)}.items():
                scored = [score_portfolio(slate, portfolio(slate, flex, cap, leverage, seed), counts) for seed in SEEDS]
                frontier_rows.append({"week": week, "k": leverage, "ownership": name,
                                      **{key: float(np.mean([s[key] for s in scored])) for key in
                                         ("proj_pts", "share_duplicated", "mean_real_copies")}})
    print("\nForecast accuracy (lower MAE better; rho_chalk = rank among players the field used)")
    print(pd.DataFrame(fit_rows).round(3).to_string(index=False))
    print(f"\nPortfolio trade-off, mean of {len(SEEDS)} seeds, 80 lineups each "
          "(compare proj_pts at similar share_duplicated)")
    print(pd.DataFrame(frontier_rows).round(2).to_string(index=False))


if __name__ == "__main__":
    main()
