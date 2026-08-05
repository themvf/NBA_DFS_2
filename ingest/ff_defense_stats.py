"""Opponent-quality-adjusted defense-vs-position ratings.

Built to answer one question a mathematician raised while reviewing the
schedule-strength idea: a defense that plays the league's best offense
twice a season looks worse than it is, and a raw "fantasy points allowed by
position" number doesn't correct for that. This module removes the
confound with a one-pass residual regression (regress points allowed on the
average season-long output of the offenses actually faced; the residual,
re-centered to league mean, is the opponent-adjusted rating).

Validated 2026-08-05 (see model/ff_schedule_strength_backtest.py for the
full walk-forward analysis): the opponent-adjustment is real and helps, but
whether a rating PERSISTS from one season to the next was tested across
2023-2025 and only clears a pre-registered significance bar for RB
(bootstrap 95% CI on the AR(1) year-over-year slope [0.15, 0.54], R^2=0.12).
QB, WR, and TE showed no significant year-over-year signal and are
deliberately not turned into an applied projection factor -- see
SCHEDULE_ADJUSTED_POSITIONS in ingest/ff_independent.py and the
"Fantasy Football Schedule Strength" section of CLAUDE.md.

This module only produces and persists the RATING (raw + opponent-adjusted,
all 4 tracked positions, so QB/WR/TE stay available for future re-testing
once more seasons of data exist). The schedule-strength FACTOR that
actually touches a projection lives in ingest/ff_independent.py and is
applied to RB only.

Usage:
    python -m ingest.ff_defense_stats --seasons 2023 2024 2025
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
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_independent import build_schedule_context, normalize_team

WEEKLY_STATS_URL = "https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{season}.csv"
SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv"
TRACKED_POSITIONS = ("QB", "RB", "WR", "TE")
# Below this many teams with a usable opponent-strength value, the 2-parameter
# OLS residual regression isn't stable -- fall back to the raw (unadjusted)
# number for that position/season rather than fit noise.
MIN_TEAMS_FOR_REGRESSION = 8


def fetch_csv(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=90, headers={"User-Agent": "DFS-Vegas/1.0"})
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content), low_memory=False)


def compute_season_ratings(weekly: pd.DataFrame, schedule_ctx: dict[str, Any], season: int) -> pd.DataFrame:
    """Raw + opponent-adjusted (Option A residual regression) defense-vs-
    position ratings for one season, for all 4 tracked positions.

    `games` -- the denominator for points-per-game -- comes from
    `schedule_ctx["games_played"]` (the schedule file), never from counting
    rows in `weekly`: a team-week with a zero/near-zero stat line (e.g. a
    starter exiting early) is still one game the defense played, and
    deriving "games" from which rows happen to have stats would silently
    undercount it.
    """
    games_played = schedule_ctx["games_played"]
    opponents = schedule_ctx["opponents"]

    work = weekly[
        (weekly["season_type"] == "REG") & (weekly["position"].isin(TRACKED_POSITIONS))
    ].copy()
    work["opponent_team"] = work["opponent_team"].map(normalize_team)
    work["team"] = work["team"].map(normalize_team)

    defense = work.groupby(["opponent_team", "position"]).agg(
        fpts_std=("fantasy_points", "sum"), fpts_ppr=("fantasy_points_ppr", "sum"),
    ).reset_index()
    defense["games"] = defense["opponent_team"].map(games_played)
    defense["fpts_allowed_std_pg"] = defense["fpts_std"] / defense["games"]
    defense["fpts_allowed_ppr_pg"] = defense["fpts_ppr"] / defense["games"]

    # Offense side: each team's OWN season-long output per position,
    # independent of any single defense -- the covariate the residual
    # regression removes from the defense's raw allowed number.
    offense = work.groupby(["team", "position"]).agg(
        fpts_std=("fantasy_points", "sum"), fpts_ppr=("fantasy_points_ppr", "sum"),
    ).reset_index()
    offense["games"] = offense["team"].map(games_played)
    offense["off_std_pg"] = offense["fpts_std"] / offense["games"]
    offense["off_ppr_pg"] = offense["fpts_ppr"] / offense["games"]
    off_lookup_std = offense.set_index(["team", "position"])["off_std_pg"].to_dict()
    off_lookup_ppr = offense.set_index(["team", "position"])["off_ppr_pg"].to_dict()

    rows = []
    for _, r in defense.iterrows():
        team, position = r["opponent_team"], r["position"]
        opp_list = opponents.get(team, [])
        opp_std = [off_lookup_std.get((o, position)) for o in opp_list]
        opp_ppr = [off_lookup_ppr.get((o, position)) for o in opp_list]
        opp_std = [v for v in opp_std if v is not None]
        opp_ppr = [v for v in opp_ppr if v is not None]
        rows.append({
            "season": season, "team_abbrev": team, "position": position,
            "games": int(r["games"]),
            "fpts_allowed_std_pg": float(r["fpts_allowed_std_pg"]),
            "fpts_allowed_ppr_pg": float(r["fpts_allowed_ppr_pg"]),
            "avg_opp_off_std_pg": float(np.mean(opp_std)) if opp_std else None,
            "avg_opp_off_ppr_pg": float(np.mean(opp_ppr)) if opp_ppr else None,
        })
    out = pd.DataFrame(rows)

    for pg_col, opp_col, adj_col in (
        ("fpts_allowed_std_pg", "avg_opp_off_std_pg", "fpts_allowed_std_pg_adj"),
        ("fpts_allowed_ppr_pg", "avg_opp_off_ppr_pg", "fpts_allowed_ppr_pg_adj"),
    ):
        out[adj_col] = out[pg_col]  # default: unadjusted, overwritten per-position below when solvable
        for position in TRACKED_POSITIONS:
            mask = (out["position"] == position) & out[opp_col].notna()
            sub = out.loc[mask]
            if len(sub) < MIN_TEAMS_FOR_REGRESSION:
                continue
            X = np.vstack([np.ones(len(sub)), sub[opp_col].values]).T
            y = sub[pg_col].values
            coef, *_ = np.linalg.lstsq(X, y, rcond=None)
            pred = X @ coef
            resid = y - pred
            out.loc[mask, adj_col] = y.mean() + resid

    for pg_col, rank_col in (
        ("fpts_allowed_std_pg_adj", "rank_std"), ("fpts_allowed_ppr_pg_adj", "rank_ppr"),
    ):
        # Rank 1 = allows the FEWEST points (stingiest defense / hardest
        # matchup) -- decided explicitly, never assumed, to avoid a
        # directionality bug (see CLAUDE.md).
        out[rank_col] = out.groupby("position")[pg_col].rank(method="min", ascending=True).astype(int)
    return out


def save_ratings(db: RefreshDatabase, ratings: pd.DataFrame) -> int:
    saved = 0
    for _, row in ratings.iterrows():
        db.execute(
            """INSERT INTO nfl_defense_vs_position
               (season, team_abbrev, position, games,
                fpts_allowed_std_pg, fpts_allowed_ppr_pg,
                fpts_allowed_std_pg_adj, fpts_allowed_ppr_pg_adj,
                rank_std, rank_ppr, fetched_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
               ON CONFLICT (season, team_abbrev, position) DO UPDATE SET
                 games=EXCLUDED.games,
                 fpts_allowed_std_pg=EXCLUDED.fpts_allowed_std_pg,
                 fpts_allowed_ppr_pg=EXCLUDED.fpts_allowed_ppr_pg,
                 fpts_allowed_std_pg_adj=EXCLUDED.fpts_allowed_std_pg_adj,
                 fpts_allowed_ppr_pg_adj=EXCLUDED.fpts_allowed_ppr_pg_adj,
                 rank_std=EXCLUDED.rank_std, rank_ppr=EXCLUDED.rank_ppr,
                 fetched_at=NOW()""",
            (
                int(row["season"]), row["team_abbrev"], row["position"], int(row["games"]),
                round(float(row["fpts_allowed_std_pg"]), 3), round(float(row["fpts_allowed_ppr_pg"]), 3),
                round(float(row["fpts_allowed_std_pg_adj"]), 3), round(float(row["fpts_allowed_ppr_pg_adj"]), 3),
                int(row["rank_std"]), int(row["rank_ppr"]),
            ),
        )
        saved += 1
    return saved


def _run(seasons: list[int], db: RefreshDatabase) -> dict[str, Any]:
    schedule = fetch_csv(SCHEDULE_URL)
    total_saved = 0
    per_season: dict[int, int] = {}
    for season in seasons:
        weekly = fetch_csv(WEEKLY_STATS_URL.format(season=season))
        if len(weekly) < 5000:
            raise RuntimeError(f"nflverse weekly stats for {season} returned suspiciously few rows")
        ctx = build_schedule_context(schedule, season)
        if len(ctx["games_played"]) != 32:
            raise RuntimeError(f"Expected 32 teams with games for {season}; found {len(ctx['games_played'])}")
        ratings = compute_season_ratings(weekly, ctx, season)
        completeness = ratings.groupby("team_abbrev").size()
        incomplete = completeness[completeness < len(TRACKED_POSITIONS)]
        if len(incomplete):
            raise RuntimeError(f"{season}: teams missing tracked positions: {incomplete.to_dict()}")
        saved = save_ratings(db, ratings)
        per_season[season] = saved
        total_saved += saved
    return {"seasons": seasons, "rows_saved_by_season": per_season, "rows_saved": total_saved}


def run(seasons: list[int]) -> dict[str, Any]:
    config = load_config()
    db = RefreshDatabase(config.database_url)
    try:
        result = _run(seasons, db)
        db.close()
        return result
    except Exception:
        db.close(error=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", type=int, nargs="+", default=[2023, 2024, 2025])
    args = parser.parse_args()
    print(json.dumps(run(args.seasons), indent=2))
