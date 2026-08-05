"""Compute shrunk weekly teammate correlations for all 32 NFL teams from real
weekly player stats, for use as evidence in the Best Ball AI advisors.

Design mirrors the "Correlation contract" already specified in
docs/nfl-best-ball-model-improvement-spec.md section 7.11 -- position-pair
relationship_type buckets (QB_WR, QB_TE, QB_RB, WR_WR, RB_WR, WR_TE, RB_TE,
RB_RB, TE_TE), shrinkage toward a league-wide prior when a specific pair's
own sample is thin (the NYJ Breece Hall / Tyrod Taylor case -- a 3-4 week
correlation of +0.80 that's mostly noise from a backup-QB stretch, not a
stable relationship).

A pair's correlation reflects real teammate history for the given season.
If a player has changed teams since, this script produces no row for them
with their new teammates -- there is no historical data to shrink from, and
fabricating one would violate this project's evidence discipline.

Usage:
    python -m ingest.ff_teammate_correlation --season 2025
"""

from __future__ import annotations

import argparse
import io
import json
from typing import Any

import numpy as np
import pandas as pd
import requests

from config import load_config
from db.database import DatabaseManager
from ingest.ff_fantasypros import RefreshDatabase

WEEKLY_STATS_URL = "https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{season}.csv"
SKILL_POSITIONS = {"QB", "RB", "WR", "TE"}
POSITION_PRIORITY = {"QB": 0, "RB": 1, "WR": 2, "TE": 3}
MIN_SAMPLE_WEEKS = 2  # below this, a Pearson correlation isn't even computable
SHRINKAGE_K = 6.0  # half-credibility at 6 overlapping games -- a documented, un-tuned choice
MIN_SAMPLE_FOR_PRIOR = 3  # a pair needs at least this many overlapping weeks to inform the league-wide prior


def dk_fantasy_points(row: pd.Series) -> float:
    passing_yards = row.get("passing_yards") or 0.0
    rushing_yards = row.get("rushing_yards") or 0.0
    receiving_yards = row.get("receiving_yards") or 0.0
    fumbles_lost = (row.get("rushing_fumbles_lost") or 0.0) + (row.get("receiving_fumbles_lost") or 0.0) + (row.get("sack_fumbles_lost") or 0.0)
    two_pt = (row.get("rushing_2pt_conversions") or 0.0) + (row.get("receiving_2pt_conversions") or 0.0)
    points = (
        passing_yards / 25.0
        + (row.get("passing_tds") or 0.0) * 4.0
        - (row.get("passing_interceptions") or 0.0) * 1.0
        + (3.0 if passing_yards >= 300 else 0.0)
        + rushing_yards / 10.0
        + (row.get("rushing_tds") or 0.0) * 6.0
        + (3.0 if rushing_yards >= 100 else 0.0)
        + (row.get("receptions") or 0.0) * 1.0
        + receiving_yards / 10.0
        + (row.get("receiving_tds") or 0.0) * 6.0
        + (3.0 if receiving_yards >= 100 else 0.0)
        + two_pt * 2.0
        - fumbles_lost * 1.0
    )
    return round(float(points), 2)


def fetch_weekly_stats(season: int) -> pd.DataFrame:
    response = requests.get(WEEKLY_STATS_URL.format(season=season), timeout=60)
    response.raise_for_status()
    df = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    df = df[
        (df["season"] == season)
        & (df["position"].isin(SKILL_POSITIONS))
        & df["player_id"].notna()
        & df["team"].notna()
    ].copy()
    df["dk_points"] = df.apply(dk_fantasy_points, axis=1)
    return df


def relationship_type(pos_a: str, pos_b: str) -> str:
    if POSITION_PRIORITY[pos_a] <= POSITION_PRIORITY[pos_b]:
        return f"{pos_a}_{pos_b}"
    return f"{pos_b}_{pos_a}"


def compute_pairs(df: pd.DataFrame) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for team, team_df in df.groupby("team"):
        pivot = team_df.pivot_table(index="week", columns="player_id", values="dk_points", aggfunc="sum")
        players = pivot.columns.tolist()
        positions = team_df.drop_duplicates("player_id").set_index("player_id")["position"].to_dict()
        names = team_df.drop_duplicates("player_id").set_index("player_id")["player_display_name"].to_dict()
        for i, a in enumerate(players):
            for b in players[i + 1:]:
                overlap = pivot[[a, b]].dropna()
                sample_weeks = int(len(overlap))
                raw = None
                if sample_weeks >= MIN_SAMPLE_WEEKS:
                    std_a, std_b = overlap[a].std(), overlap[b].std()
                    if std_a > 0 and std_b > 0:
                        raw = float(overlap[a].corr(overlap[b]))
                pairs.append({
                    "team": team,
                    "gsis_a": a,
                    "gsis_b": b,
                    "name_a": names[a],
                    "name_b": names[b],
                    "relationship_type": relationship_type(positions[a], positions[b]),
                    "sample_weeks": sample_weeks,
                    "raw_correlation": raw,
                })
    return pairs


def compute_priors(pairs: list[dict[str, Any]]) -> dict[str, float]:
    by_type: dict[str, list[float]] = {}
    for pair in pairs:
        if pair["raw_correlation"] is not None and pair["sample_weeks"] >= MIN_SAMPLE_FOR_PRIOR:
            by_type.setdefault(pair["relationship_type"], []).append(pair["raw_correlation"])
    return {rel: float(np.mean(values)) for rel, values in by_type.items() if values}


def apply_shrinkage(pairs: list[dict[str, Any]], priors: dict[str, float]) -> None:
    global_prior = float(np.mean(list(priors.values()))) if priors else 0.0
    for pair in pairs:
        prior = priors.get(pair["relationship_type"], global_prior)
        weight = pair["sample_weeks"] / (pair["sample_weeks"] + SHRINKAGE_K)
        if pair["raw_correlation"] is None:
            weight = 0.0
            raw_for_blend = prior
        else:
            raw_for_blend = pair["raw_correlation"]
        pair["prior_correlation"] = round(prior, 4)
        pair["shrinkage_weight"] = round(weight, 4)
        pair["shrunk_correlation"] = round(raw_for_blend * weight + prior * (1 - weight), 4)


def persist(db: RefreshDatabase, season: int, pairs: list[dict[str, Any]]) -> dict[str, int]:
    # Batch-fetch every gsis_id -> id mapping once (one round trip) instead of
    # two SELECTs per pair -- thousands of pairs otherwise means thousands of
    # remote round trips to Neon just for identity lookups.
    id_by_gsis: dict[str, int] = {
        row["gsis_id"]: int(row["id"])
        for row in db.execute("SELECT id, gsis_id FROM ff_players WHERE season=2026 AND gsis_id IS NOT NULL")
    }
    matched = 0
    unmatched = 0
    for pair in pairs:
        id_a_raw = id_by_gsis.get(pair["gsis_a"])
        id_b_raw = id_by_gsis.get(pair["gsis_b"])
        if id_a_raw is None or id_b_raw is None:
            unmatched += 1
            continue
        id_a, id_b = (id_a_raw, id_b_raw) if id_a_raw <= id_b_raw else (id_b_raw, id_a_raw)
        db.execute(
            """INSERT INTO ff_teammate_correlations
               (season,player_a_id,player_b_id,team_abbrev,relationship_type,sample_weeks,
                raw_correlation,prior_correlation,shrunk_correlation,shrinkage_weight)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(season,player_a_id,player_b_id) DO UPDATE SET
                 team_abbrev=EXCLUDED.team_abbrev,relationship_type=EXCLUDED.relationship_type,
                 sample_weeks=EXCLUDED.sample_weeks,raw_correlation=EXCLUDED.raw_correlation,
                 prior_correlation=EXCLUDED.prior_correlation,shrunk_correlation=EXCLUDED.shrunk_correlation,
                 shrinkage_weight=EXCLUDED.shrinkage_weight,computed_at=NOW()""",
            (
                season, id_a, id_b, pair["team"], pair["relationship_type"], pair["sample_weeks"],
                pair["raw_correlation"], pair["prior_correlation"], pair["shrunk_correlation"], pair["shrinkage_weight"],
            ),
        )
        matched += 1
    return {"matched": matched, "unmatched": unmatched}


def run(season: int) -> dict[str, Any]:
    config = load_config()
    DatabaseManager(config.database_url)  # schema initialization and migrations
    db = RefreshDatabase(config.database_url)
    try:
        df = fetch_weekly_stats(season)
        pairs = compute_pairs(df)
        priors = compute_priors(pairs)
        apply_shrinkage(pairs, priors)
        stats = persist(db, season, pairs)
        db.close()
        return {
            "season": season,
            "pairs_computed": len(pairs),
            "priors": {key: round(value, 3) for key, value in sorted(priors.items())},
            **stats,
        }
    except Exception:
        db.close(error=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2025)
    args = parser.parse_args()
    print(json.dumps(run(args.season), indent=2))


if __name__ == "__main__":
    main()
