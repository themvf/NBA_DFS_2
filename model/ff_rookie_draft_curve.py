"""Fit a continuous draft-pick -> rookie-value curve from real historical outcomes,
replacing the hardcoded pick-bucket table in ingest/ff_independent.py::_rookie_points().

Read-only against nflverse (public roster CSVs, no auth) and our own DB
(ff_players, ff_player_season_features -- already populated). No writes.

Population: true rookies (roster_weekly nflverse `rookie_year == season`) for
2023, 2024, 2025, matched by gsis_id to our canonical ff_players row, joined to
their real rookie-season fantasy_points_ppr/std in ff_player_season_features.

Curve form (per position): value(pick) = floor + (peak - floor) * exp(-pick / decay)
-- monotonic decay, asymptotes to a floor rather than zero, matching the
"large early drop-off, flatter late" shape documented in the Rookie Super
Model source material.

Validation: leave-one-class-out walk-forward (fit on 2 seasons, predict the
3rd, rotate), never fit and grade on the same season. Reports curve MAE vs.
the current hardcoded bucket table's MAE on the same held-out predictions.

Usage:
    python -m model.ff_rookie_draft_curve
"""

from __future__ import annotations

import hashlib
import io
import statistics
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests
from scipy.optimize import curve_fit

from config import load_config
from db.database import DatabaseManager
from ingest.ff_independent import _rookie_points

NFLVERSE_ROSTER_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/"
    "roster_weekly_{season}.csv"
)
SEASONS = (2023, 2024, 2025)
POSITIONS = ("QB", "RB", "WR", "TE")


@dataclass
class RookieRow:
    season: int
    position: str
    draft_number: int
    player_id: int
    name: str
    actual_ppr: float
    actual_std: float
    games: int


def fetch_rookie_class(season: int) -> pd.DataFrame:
    response = requests.get(NFLVERSE_ROSTER_URL.format(season=season), timeout=60)
    response.raise_for_status()
    df = pd.read_csv(io.BytesIO(response.content))
    rookies = df[(df["rookie_year"] == season) & df["draft_number"].notna()].copy()
    rookies = rookies.drop_duplicates(subset=["gsis_id"], keep="last")
    return rookies[["gsis_id", "position", "draft_number", "full_name"]]


def build_dataset(db: DatabaseManager) -> list[RookieRow]:
    rows: list[RookieRow] = []
    for season in SEASONS:
        rookies = fetch_rookie_class(season)
        for _, r in rookies.iterrows():
            position = str(r["position"])
            if position not in POSITIONS:
                continue
            gsis_id = r["gsis_id"]
            if not isinstance(gsis_id, str) or not gsis_id:
                continue
            player = db.execute_one(
                "SELECT id, canonical_name FROM ff_players WHERE gsis_id=%s AND season=2026",
                (gsis_id,),
            )
            if not player:
                continue
            outcome = db.execute_one(
                "SELECT fantasy_points_ppr, fantasy_points_std, games FROM ff_player_season_features WHERE player_id=%s AND season=%s",
                (int(player["id"]), season),
            )
            if not outcome or outcome["fantasy_points_ppr"] is None:
                continue
            rows.append(RookieRow(
                season=season,
                position=position,
                draft_number=int(r["draft_number"]),
                player_id=int(player["id"]),
                name=str(player["canonical_name"]),
                actual_ppr=float(outcome["fantasy_points_ppr"]),
                actual_std=float(outcome["fantasy_points_std"]),
                games=int(outcome["games"] or 0),
            ))
    return rows


def _decay_curve(pick: Any, floor: float, peak: float, decay: float) -> Any:
    import numpy as np
    return floor + (peak - floor) * np.exp(-pick / decay)


def fit_position_curve(rows: list[RookieRow], scoring: str = "ppr") -> dict[str, float] | None:
    if len(rows) < 6:
        return None
    picks = [r.draft_number for r in rows]
    actuals = [r.actual_ppr if scoring == "ppr" else r.actual_std for r in rows]
    lower, upper = (-50.0, 0.0, 1.0), (200.0, 400.0, 400.0)
    p0 = [
        min(max(min(actuals), lower[0]), upper[0]),
        min(max(max(actuals), lower[1]), upper[1]),
        60.0,
    ]
    try:
        params, _ = curve_fit(
            _decay_curve, picks, actuals,
            p0=p0,
            maxfev=10000,
            bounds=(lower, upper),
        )
    except (RuntimeError, ValueError):
        return None
    return {"floor": float(params[0]), "peak": float(params[1]), "decay": float(params[2])}


def current_bucket_prediction(position: str, pick: int, scoring: str) -> float:
    return _rookie_points(position, pick, scoring.upper() if scoring != "ppr" else "PPR")


def walk_forward(rows: list[RookieRow]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for position in POSITIONS:
        pos_rows = [r for r in rows if r.position == position]
        if len(pos_rows) < 9:
            results[position] = {"status": "insufficient_sample", "n": len(pos_rows)}
            continue
        curve_errors: list[float] = []
        bucket_errors: list[float] = []
        fold_reports = []
        for holdout_season in SEASONS:
            train = [r for r in pos_rows if r.season != holdout_season]
            test = [r for r in pos_rows if r.season == holdout_season]
            if not test or len(train) < 6:
                continue
            fit = fit_position_curve(train, "ppr")
            if not fit:
                continue
            fold_curve_errors = []
            fold_bucket_errors = []
            for r in test:
                curve_pred = _decay_curve(r.draft_number, fit["floor"], fit["peak"], fit["decay"])
                bucket_pred = current_bucket_prediction(position, r.draft_number, "ppr")
                curve_err = abs(curve_pred - r.actual_ppr)
                bucket_err = abs(bucket_pred - r.actual_ppr)
                curve_errors.append(curve_err)
                bucket_errors.append(bucket_err)
                fold_curve_errors.append(curve_err)
                fold_bucket_errors.append(bucket_err)
            fold_reports.append({
                "holdout_season": holdout_season,
                "n_test": len(test),
                "fit_params": fit,
                "curve_mae": round(statistics.mean(fold_curve_errors), 2) if fold_curve_errors else None,
                "bucket_mae": round(statistics.mean(fold_bucket_errors), 2) if fold_bucket_errors else None,
            })
        full_fit = fit_position_curve(pos_rows, "ppr")
        results[position] = {
            "n": len(pos_rows),
            "walk_forward_curve_mae": round(statistics.mean(curve_errors), 2) if curve_errors else None,
            "walk_forward_bucket_mae": round(statistics.mean(bucket_errors), 2) if bucket_errors else None,
            "n_walk_forward_predictions": len(curve_errors),
            "full_sample_fit": full_fit,
            "folds": fold_reports,
        }
    return results


_CACHE_PATH = "C:/Users/joshb/AppData/Local/Temp/claude/ff_rookie_curve_dataset_cache.json"


def _load_or_build_dataset(db: DatabaseManager) -> list[RookieRow]:
    import json
    import os
    if os.path.exists(_CACHE_PATH):
        with open(_CACHE_PATH, "r", encoding="utf-8") as handle:
            cached = json.load(handle)
        return [RookieRow(**item) for item in cached]
    rows = build_dataset(db)
    with open(_CACHE_PATH, "w", encoding="utf-8") as handle:
        json.dump([r.__dict__ for r in rows], handle)
    return rows


def main() -> None:
    config = load_config()
    db = DatabaseManager(config.database_url)
    rows = _load_or_build_dataset(db)
    print(f"Total matched rookie rows across {SEASONS}: {len(rows)}")
    by_pos = {}
    for r in rows:
        by_pos.setdefault(r.position, 0)
        by_pos[r.position] += 1
    print("By position:", by_pos)
    report = walk_forward(rows)
    import json
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
