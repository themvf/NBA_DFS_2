"""Evaluate the first NFL context definition against a frozen PBP release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from model.nfl_context_measures import neutral_snap_interval_feasibility


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbp", type=Path, help="frozen nflverse PBP parquet")
    parser.add_argument("--season-type", default="REG")
    args = parser.parse_args()
    rows = pd.read_parquet(args.pbp)
    if "season_type" in rows and args.season_type:
        rows = rows[rows["season_type"].eq(args.season_type)].copy()
    report = neutral_snap_interval_feasibility(rows)
    report["source"] = str(args.pbp)
    report["seasonType"] = args.season_type
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
