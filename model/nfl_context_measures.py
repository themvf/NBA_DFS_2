"""Versioned NFL context measurements built from canonical play facts."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd

from model.nfl_context_engine import (
    ContextDefinition,
    ContextMeasurement,
    ContextState,
)


NEUTRAL_SNAP_INTERVAL = ContextDefinition(
    key="neutral_offensive_snap_interval_seconds",
    version="v1",
    unit="game_clock_seconds_per_interval",
    description=(
        "Mean game-clock seconds between adjacent eligible offensive snaps in "
        "the same game, possession, drive, and quarter while both pre-play "
        "states are within seven points through quarter three."
    ),
    definition={
        "neutral": "pre-play quarter <= 3 and -7 <= score differential <= 7",
        "eligible_snap": (
            "play_type in {pass,run}; excludes kneels, spikes, two-point attempts, "
            "no-play and administrative records"
        ),
        "interval": (
            "adjacent source records first; retain only pairs whose endpoints are "
            "eligible, same game/team/drive/quarter, and delta is 0..60 seconds"
        ),
        "weighting": "each valid interval has equal weight",
        "meaning": "game-clock consumption, not wall-clock snap-to-snap time",
    },
)


REQUIRED_COLUMNS = {
    "game_id",
    "play_id",
    "posteam",
    "drive",
    "qtr",
    "play_type",
    "qb_kneel",
    "qb_spike",
    "two_point_attempt",
    "score_differential",
    "game_seconds_remaining",
}


def _neutral_snap_intervals_all(rows: pd.DataFrame, *, max_seconds: int) -> pd.DataFrame:
    if max_seconds <= 0:
        raise ValueError("max_seconds must be positive")
    missing = REQUIRED_COLUMNS - set(rows.columns)
    if missing:
        raise ValueError(f"missing neutral interval columns: {sorted(missing)}")

    ordered = rows.sort_values(["game_id", "play_id"]).copy()
    previous = ordered.shift(1)
    official = pd.Series(True, index=ordered.index)
    previous_official = pd.Series(True, index=ordered.index)
    if {"snap_execution", "action_validity"} <= set(ordered.columns):
        official = ordered["snap_execution"].eq("executed") & ordered[
            "action_validity"
        ].eq("counted")
        previous_official = previous["snap_execution"].eq("executed") & previous[
            "action_validity"
        ].eq("counted")
    eligible = (
        official
        & ordered["posteam"].notna()
        & ordered["play_type"].isin(["pass", "run"])
        & ordered["qb_kneel"].ne(1)
        & ordered["qb_spike"].ne(1)
        & ordered["two_point_attempt"].ne(1)
        & ordered["qtr"].le(3)
        & ordered["score_differential"].between(-7, 7)
    )
    previous_eligible = (
        previous_official
        & previous["posteam"].notna()
        & previous["play_type"].isin(["pass", "run"])
        & previous["qb_kneel"].ne(1)
        & previous["qb_spike"].ne(1)
        & previous["two_point_attempt"].ne(1)
        & previous["qtr"].le(3)
        & previous["score_differential"].between(-7, 7)
    )
    seconds = previous["game_seconds_remaining"] - ordered["game_seconds_remaining"]
    valid = (
        eligible
        & previous_eligible
        & ordered["game_id"].eq(previous["game_id"])
        & ordered["posteam"].eq(previous["posteam"])
        & ordered["drive"].notna()
        & ordered["drive"].eq(previous["drive"])
        & ordered["qtr"].eq(previous["qtr"])
        & seconds.between(0, max_seconds)
    )
    return pd.DataFrame(
        {
            "game_id": ordered.loc[valid, "game_id"],
            "posteam": ordered.loc[valid, "posteam"],
            "from_play_id": previous.loc[valid, "play_id"],
            "to_play_id": ordered.loc[valid, "play_id"],
            "seconds": seconds.loc[valid].astype(float),
        }
    ).reset_index(drop=True)


def neutral_snap_intervals(
    rows: pd.DataFrame, *, team: str, max_seconds: int = 60
) -> pd.DataFrame:
    """Return qualifying intervals without bridging an excluded source row."""
    all_intervals = _neutral_snap_intervals_all(rows, max_seconds=max_seconds)
    return all_intervals.loc[
        all_intervals["posteam"].eq(team),
        ["game_id", "from_play_id", "to_play_id", "seconds"],
    ].reset_index(drop=True)


def neutral_snap_interval_feasibility(rows: pd.DataFrame) -> dict[str, object]:
    """Coverage and cap sensitivity gate for the provisional pace measure."""
    missing = REQUIRED_COLUMNS - set(rows.columns)
    if missing:
        raise ValueError(f"missing neutral interval columns: {sorted(missing)}")
    teams = sorted(str(team) for team in rows["posteam"].dropna().unique())
    neutral_snaps = rows[
        rows["posteam"].notna()
        & rows["play_type"].isin(["pass", "run"])
        & rows["qb_kneel"].ne(1)
        & rows["qb_spike"].ne(1)
        & rows["two_point_attempt"].ne(1)
        & rows["qtr"].le(3)
        & rows["score_differential"].between(-7, 7)
    ]
    by_cap: dict[str, dict[str, float | int | None]] = {}
    intervals_at_sixty = pd.DataFrame()
    for cap in (45, 60, 75, 90):
        combined = _neutral_snap_intervals_all(rows, max_seconds=cap)
        if cap == 60:
            intervals_at_sixty = combined
        by_cap[str(cap)] = {
            "intervals": int(len(combined)),
            "meanSeconds": float(combined["seconds"].mean()) if len(combined) else None,
            "medianSeconds": float(combined["seconds"].median()) if len(combined) else None,
        }
    sixty = intervals_at_sixty
    counts = (
        sixty.groupby("game_id").size()
        if len(sixty)
        else pd.Series(dtype="int64")
    )
    # A game contains two team-games. Preserve that distinction by deriving it
    # from each team's interval frame rather than grouping only on game_id.
    team_game_counts = (
        [int(value) for value in sixty.groupby(["game_id", "posteam"]).size()]
        if len(sixty)
        else []
    )
    return {
        "definitionId": NEUTRAL_SNAP_INTERVAL.definition_id,
        "neutralEligibleSnaps": int(len(neutral_snaps)),
        "missingClockSnaps": int(neutral_snaps["game_seconds_remaining"].isna().sum()),
        "missingClockRate": (
            float(neutral_snaps["game_seconds_remaining"].isna().mean())
            if len(neutral_snaps)
            else None
        ),
        "teams": len(teams),
        "gamesWithIntervals": int(counts.index.nunique()),
        "teamGamesWithIntervals": len(team_game_counts),
        "teamGamesMeeting20Intervals": sum(value >= 20 for value in team_game_counts),
        "teamGameMedianIntervals": (
            float(pd.Series(team_game_counts).median()) if team_game_counts else None
        ),
        "capSensitivity": by_cap,
    }


def build_neutral_snap_interval_context(
    rows: pd.DataFrame,
    *,
    team: str,
    target_id: str,
    as_of_at: datetime,
    source_snapshot_ids: Iterable[str],
    fact_release_id: str,
) -> ContextMeasurement:
    intervals = neutral_snap_intervals(rows, team=team)
    games = sorted(intervals["game_id"].astype(str).unique()) if len(intervals) else []
    numerator = float(intervals["seconds"].sum()) if len(intervals) else 0.0
    denominator = float(len(intervals))
    value = numerator / denominator if denominator else None
    all_team_games = sorted(rows.loc[rows["posteam"].eq(team), "game_id"].astype(str).unique())
    return ContextMeasurement(
        subject_type="team",
        subject_id=team,
        target_id=target_id,
        definition_id=NEUTRAL_SNAP_INTERVAL.definition_id,
        as_of_at=as_of_at,
        window={"gameIds": all_team_games},
        numerator=numerator,
        denominator=denominator,
        value=value,
        state=ContextState.OBSERVED,
        coverage={
            "eligibleIntervals": len(intervals),
            "gamesWithIntervals": games,
            "gamesConsidered": all_team_games,
            "minimumCoverageMet": len(intervals) >= 20 and len(games) >= 2,
        },
        source_snapshot_ids=tuple(sorted(source_snapshot_ids)),
        fact_release_id=fact_release_id,
    )
