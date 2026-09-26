"""Leakage-safe retrospective tests for NFL PBP context candidates."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from model.nfl_context_engine import stable_digest
from model.nfl_context_measures import NEUTRAL_SNAP_INTERVAL, _neutral_snap_intervals_all
from model.nfl_play_facts import build_play_facts, facts_frame


STUDY_VERSION = "nfl-context-volume-study-v1"
BASELINE_FEATURES = ("team_prior_plays", "opponent_prior_plays")
CHALLENGER_FEATURES = BASELINE_FEATURES + (
    "team_prior_interval_seconds",
    "opponent_prior_interval_seconds",
)


@dataclass(frozen=True)
class StudySource:
    path: str
    sha256: str
    rows: int
    seasons: tuple[int, ...]


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def team_game_measurements(raw: pd.DataFrame) -> pd.DataFrame:
    """Build one canonical opportunity/pace row per team-game."""
    required = {
        "game_id", "play_id", "desc", "play_type", "season", "week",
        "home_team", "away_team", "posteam", "drive", "qtr", "qb_kneel",
        "qb_spike", "two_point_attempt", "score_differential",
        "game_seconds_remaining",
    }
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"missing context research columns: {sorted(missing)}")
    rows = raw.loc[raw["season_type"].eq("REG")].copy() if "season_type" in raw else raw.copy()
    rows = rows.reset_index(drop=True)
    if rows.empty:
        return pd.DataFrame()
    facts = build_play_facts(rows, source_observation_id="research", fact_release_id="research")
    canonical = facts_frame(facts)
    official = canonical["snap_execution"].eq("executed") & canonical["action_validity"].eq("counted")
    opportunity = canonical.loc[
        official
        & canonical["play_type"].isin(["pass", "run"])
        & canonical["qb_kneel"].ne(1)
        & canonical["qb_spike"].ne(1)
        & canonical["two_point_attempt"].ne(1)
    ].copy()
    for column in ("epa", "success", "yards_gained", "sack", "interception", "fumble_lost"):
        opportunity[column] = (
            pd.to_numeric(rows.loc[opportunity.index, column], errors="coerce")
            if column in rows
            else 0.0
        )
    opportunity["is_pass"] = opportunity["play_type"].eq("pass").astype(float)
    opportunity["explosive"] = (
        (opportunity["play_type"].eq("pass") & opportunity["yards_gained"].ge(20))
        | (opportunity["play_type"].eq("run") & opportunity["yards_gained"].ge(10))
    ).astype(float)
    plays = opportunity.groupby(["game_id", "posteam"]).size().rename("plays")
    descriptors = opportunity.groupby(["game_id", "posteam"]).agg(
        offensive_epa=("epa", "mean"),
        success_rate=("success", "mean"),
        explosive_rate=("explosive", "mean"),
        pass_rate=("is_pass", "mean"),
        sack_rate=("sack", "mean"),
        turnover_rate=("interception", "mean"),
        fumble_lost_rate=("fumble_lost", "mean"),
    )
    intervals = _neutral_snap_intervals_all(canonical, max_seconds=60)
    pace = intervals.groupby(["game_id", "posteam"])["seconds"].agg(
        interval_seconds_sum="sum", interval_count="count"
    )
    game_columns = ["game_id", "season", "week", "home_team", "away_team"]
    for column in ("total_home_score", "total_away_score", "home_score", "away_score"):
        if column in rows:
            game_columns.append(column)
    games = rows.sort_values("play_id").drop_duplicates("game_id", keep="last")[game_columns]
    home = games.assign(team=games["home_team"], opponent=games["away_team"])
    away = games.assign(team=games["away_team"], opponent=games["home_team"])
    result = pd.concat([home, away], ignore_index=True)
    result = result.join(plays, on=["game_id", "team"]).join(pace, on=["game_id", "team"])
    result = result.join(descriptors, on=["game_id", "team"])
    home_score = "total_home_score" if "total_home_score" in result else "home_score"
    away_score = "total_away_score" if "total_away_score" in result else "away_score"
    if home_score in result and away_score in result:
        is_home = result["team"].eq(result["home_team"])
        result["points_for"] = np.where(is_home, result[home_score], result[away_score])
        result["points_against"] = np.where(is_home, result[away_score], result[home_score])
    result["plays"] = result["plays"].fillna(0).astype(int)
    result["interval_count"] = result["interval_count"].fillna(0).astype(int)
    result["interval_seconds_sum"] = result["interval_seconds_sum"].fillna(0.0)
    return result.sort_values(["season", "week", "game_id", "team"]).reset_index(drop=True)


def pregame_features(team_games: pd.DataFrame, *, lookback: int = 4, minimum: int = 3) -> pd.DataFrame:
    """Create features using completed prior games only; the target is the current game."""
    if lookback < minimum or minimum < 1:
        raise ValueError("lookback must be at least the positive minimum")
    ordered = team_games.sort_values(["team", "season", "week", "game_id"]).copy()
    groups = ordered.groupby(["team", "season"], sort=False)
    ordered["team_prior_plays"] = groups["plays"].transform(
        lambda values: values.shift(1).rolling(lookback, min_periods=minimum).mean()
    )
    prior_seconds = groups["interval_seconds_sum"].transform(
        lambda values: values.shift(1).rolling(lookback, min_periods=minimum).sum()
    )
    prior_count = groups["interval_count"].transform(
        lambda values: values.shift(1).rolling(lookback, min_periods=minimum).sum()
    )
    ordered["team_prior_interval_seconds"] = prior_seconds / prior_count.replace(0, np.nan)
    opponent = ordered[
        ["game_id", "team", "team_prior_plays", "team_prior_interval_seconds"]
    ].rename(columns={
        "team": "opponent",
        "team_prior_plays": "opponent_prior_plays",
        "team_prior_interval_seconds": "opponent_prior_interval_seconds",
    })
    featured = ordered.merge(opponent, on=["game_id", "opponent"], how="left", validate="many_to_one")
    required = list(CHALLENGER_FEATURES) + ["plays"]
    return featured.dropna(subset=required).sort_values(
        ["season", "week", "game_id", "team"]
    ).reset_index(drop=True)


def _fit_predict(train: pd.DataFrame, test: pd.DataFrame, features: Iterable[str]) -> np.ndarray:
    columns = list(features)
    x_train = train[columns].to_numpy(dtype=float)
    x_test = test[columns].to_numpy(dtype=float)
    means = x_train.mean(axis=0)
    scales = x_train.std(axis=0)
    scales[scales == 0] = 1.0
    design = np.column_stack([np.ones(len(train)), (x_train - means) / scales])
    coefficients = np.linalg.lstsq(design, train["plays"].to_numpy(dtype=float), rcond=None)[0]
    return np.column_stack([np.ones(len(test)), (x_test - means) / scales]) @ coefficients


def _metrics(actual: np.ndarray, prediction: np.ndarray) -> dict[str, float | int]:
    error = prediction - actual
    return {
        "n": int(len(actual)),
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(error**2))),
        "bias": float(np.mean(error)),
    }


def _clustered_interval(rows: pd.DataFrame, *, seed: int, draws: int) -> dict[str, float]:
    per_game = rows.groupby("game_id", sort=True)["mae_gain"].mean().to_numpy(dtype=float)
    if len(per_game) < 2:
        return {"lower95": float("nan"), "median": float("nan"), "upper95": float("nan")}
    rng = np.random.default_rng(seed)
    sampled = rng.choice(per_game, size=(draws, len(per_game)), replace=True).mean(axis=1)
    lower, median, upper = np.quantile(sampled, [0.025, 0.5, 0.975])
    return {"lower95": float(lower), "median": float(median), "upper95": float(upper)}


def evaluate_volume_candidate(
    samples: pd.DataFrame,
    *,
    minimum_train_seasons: int = 2,
    seed: int = 20260923,
    bootstrap_draws: int = 2000,
) -> dict[str, object]:
    """Use expanding season holdouts and decide whether context may enter shadow."""
    seasons = sorted(int(value) for value in samples["season"].unique())
    folds: list[dict[str, object]] = []
    scored: list[pd.DataFrame] = []
    for index, season in enumerate(seasons):
        train_seasons = seasons[:index]
        if len(train_seasons) < minimum_train_seasons:
            continue
        train = samples[samples["season"].isin(train_seasons)]
        test = samples[samples["season"].eq(season)].copy()
        if train.empty or test.empty:
            continue
        baseline = _fit_predict(train, test, BASELINE_FEATURES)
        challenger = _fit_predict(train, test, CHALLENGER_FEATURES)
        actual = test["plays"].to_numpy(dtype=float)
        test["baseline_prediction"] = baseline
        test["challenger_prediction"] = challenger
        test["mae_gain"] = np.abs(baseline - actual) - np.abs(challenger - actual)
        scored.append(test)
        baseline_metrics = _metrics(actual, baseline)
        challenger_metrics = _metrics(actual, challenger)
        folds.append({
            "season": season,
            "trainSeasons": train_seasons,
            "baseline": baseline_metrics,
            "challenger": challenger_metrics,
            "maeGain": baseline_metrics["mae"] - challenger_metrics["mae"],
        })
    if not scored:
        raise ValueError("at least three seasons are required for expanding holdouts")
    combined = pd.concat(scored, ignore_index=True)
    actual = combined["plays"].to_numpy(dtype=float)
    baseline_metrics = _metrics(actual, combined["baseline_prediction"].to_numpy(dtype=float))
    challenger_metrics = _metrics(actual, combined["challenger_prediction"].to_numpy(dtype=float))
    interval = _clustered_interval(combined, seed=seed, draws=bootstrap_draws)
    improved_folds = sum(float(fold["maeGain"]) > 0 for fold in folds)
    mae_gain = float(baseline_metrics["mae"] - challenger_metrics["mae"])
    gates = {
        "sampleAtLeast1000": len(combined) >= 1000,
        "atLeastThreeHoldoutSeasons": len(folds) >= 3,
        "majorityOfSeasonsImprove": improved_folds > len(folds) / 2,
        "maeGainAtLeastQuarterPlay": mae_gain >= 0.25,
        "clusteredLower95AboveZero": interval["lower95"] > 0,
    }
    eligible = all(gates.values())
    return {
        "studyVersion": STUDY_VERSION,
        "definitionId": NEUTRAL_SNAP_INTERVAL.definition_id,
        "question": "Does prior neutral snap interval improve pregame team offensive-play forecasts?",
        "target": "official non-kneel, non-spike pass/run plays in the target game",
        "baselineFeatures": list(BASELINE_FEATURES),
        "challengerAddedFeatures": list(CHALLENGER_FEATURES[len(BASELINE_FEATURES):]),
        "folds": folds,
        "aggregate": {
            "baseline": baseline_metrics,
            "challenger": challenger_metrics,
            "maeGain": mae_gain,
            "clusteredBootstrap": interval,
        },
        "promotionGate": gates,
        "status": "eligible_for_shadow_only" if eligible else "not_qualified",
        "productionProjectionEffect": "none",
        "optimizerEffect": "none",
        "bettingEffect": "none",
    }


def run_study(paths: Iterable[Path]) -> tuple[dict[str, object], pd.DataFrame]:
    sources: list[StudySource] = []
    frames: list[pd.DataFrame] = []
    for path in sorted(paths):
        raw = pd.read_parquet(path)
        measured = team_game_measurements(raw)
        if not measured.empty:
            frames.append(measured)
        seasons = tuple(sorted(int(value) for value in raw["season"].dropna().unique()))
        sources.append(StudySource(str(path), _file_digest(path), len(raw), seasons))
    if not frames:
        raise ValueError("no regular-season PBP rows found")
    samples = pregame_features(pd.concat(frames, ignore_index=True))
    report = evaluate_volume_candidate(samples)
    report["sources"] = [source.__dict__ for source in sources]
    report["sampleDigest"] = stable_digest(samples.to_dict(orient="records"))
    report["runId"] = stable_digest(report)
    return report, samples
