"""Reconstruct opening NFL spreads from prior scoring and PBP descriptors."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from model.nfl_context_engine import stable_digest
from model.nfl_context_research import team_game_measurements
from model.nfl_market_context_research import evaluate_market_attribution, market_pregame_features


TEAM = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LA", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS",
    "Washington Football Team": "WAS",
}


def opening_lines(db: DatabaseManager) -> pd.DataFrame:
    rows = db.execute("""
        SELECT DISTINCT ON (season, home_team, away_team)
          season, home_team, away_team, home_spread, snapshot_at, lead_minutes, book_count
        FROM nfl_line_snapshots
        WHERE label='open' AND home_spread IS NOT NULL AND lead_minutes > 0
        ORDER BY season, home_team, away_team, lead_minutes ASC, snapshot_at DESC
    """)
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("no opening NFL spread snapshots available")
    frame["home_team"] = frame["home_team"].map(TEAM)
    frame["away_team"] = frame["away_team"].map(TEAM)
    return frame.dropna(subset=["home_team", "away_team", "home_spread"])


def persist(db: DatabaseManager, report: dict[str, object]) -> None:
    db.execute(
        """INSERT INTO nfl_market_context_research_runs(run_id, study_version, report)
           VALUES (%s,%s,%s) ON CONFLICT(run_id) DO NOTHING""",
        (report["runId"], report["studyVersion"], Json(report)),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbp", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url)
    team_games = pd.concat(
        [team_game_measurements(pd.read_parquet(path)) for path in sorted(args.pbp)],
        ignore_index=True,
    )
    features = market_pregame_features(team_games)
    lines = opening_lines(db)
    samples = features.merge(
        lines, on=["season", "home_team", "away_team"], validate="one_to_one"
    )
    report = evaluate_market_attribution(samples)
    report["sampleRows"] = len(samples)
    report["sampleDigest"] = stable_digest(samples.to_dict(orient="records"))
    report["runId"] = stable_digest(report)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    if args.apply:
        persist(db, report)
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
