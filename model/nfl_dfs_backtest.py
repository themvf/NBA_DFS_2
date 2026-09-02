"""Chronological validation harness for the historical NFL DFS model.

The source is the immutable nflverse weekly-stat cache, not
``ff_player_week_stats``.  The matched convenience table is built against the
current player universe and would create survivorship bias in a historical
cohort.  Every target week is predicted from strictly earlier weeks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from model.nfl_dfs_historical import (
    MODEL_CONFIG,
    MODEL_VERSION,
    HistoricalWeek,
    ProjectionContext,
    artifact_digest,
    draftkings_points,
    project_player,
)


HARNESS_VERSION = "nfl-dfs-walk-forward-v1"
POSITIONS = ("QB", "RB", "WR", "TE")


def source_paths(source_root: Path, seasons: Iterable[int]) -> list[Path]:
    metadata_path = source_root / "artifacts" / "ff_v2_historical_context_2020_2025.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    wanted = set(int(value) for value in seasons)
    found: dict[int, Path] = {}
    for source_key, source in metadata.get("sources", {}).items():
        if not source_key.startswith("weekly-stats:"):
            continue
        season = int(source_key.rsplit(":", 1)[1])
        if season in wanted:
            cache_path = source.get("cache_path") or source.get("cachePath")
            if cache_path:
                found[season] = source_root / Path(cache_path)
    missing = wanted - set(found)
    if missing:
        raise RuntimeError(f"Missing immutable weekly-stat snapshots for seasons: {sorted(missing)}")
    return [found[season] for season in sorted(found)]


def load_source_rows(paths: Iterable[Path]) -> tuple[list[HistoricalWeek], list[dict[str, Any]]]:
    weeks: list[HistoricalWeek] = []
    evidence: list[dict[str, Any]] = []
    for path in paths:
        frame = pd.read_parquet(path)
        frame = frame[(frame["season_type"] == "REG") & (frame["position"].isin(POSITIONS))]
        parts = path.parts
        portable = str(Path(*parts[parts.index("data"):])) if "data" in parts else path.name
        evidence.append({"path": portable, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "rows": int(len(frame))})
        for row in frame.to_dict("records"):
            player_key = str(row.get("player_id") or "").strip()
            if not player_key:
                continue
            # Stable local integer for the pure model; the GSIS id remains the
            # auditable identity and is used across seasons.
            local_id = int(artifact_digest({"gsis_id": player_key})[:15], 16)
            weeks.append(HistoricalWeek(
                player_id=local_id,
                player_gsis_id=player_key,
                player_name=str(row.get("player_display_name") or row.get("player_name") or player_key),
                position=str(row["position"]),
                season=int(row["season"]),
                week=int(row["week"]),
                team=str(row.get("team") or "") or None,
                opponent=str(row.get("opponent_team") or "") or None,
                stats=row,
            ))
    return weeks, evidence


def opponent_factor(prior: list[HistoricalWeek], position: str, opponent: str | None) -> float | None:
    if not opponent:
        return None
    allowed = [row.dk_points for row in prior if row.position == position and row.opponent == opponent]
    league = [row.dk_points for row in prior if row.position == position]
    if not allowed or not league:
        return None
    # Sixteen equivalent games of league-average evidence prevents one early
    # outlier from being called a defense signal.
    league_mean = float(np.mean(league))
    shrunk = (sum(allowed) + 16.0 * league_mean) / (len(allowed) + 16.0)
    return None if league_mean <= 0 else float(np.clip(shrunk / league_mean, 0.80, 1.20))


def _spearman(actual: list[float], predicted: list[float]) -> float | None:
    if len(actual) < 2:
        return None
    a = pd.Series(actual).rank(method="average").to_numpy()
    p = pd.Series(predicted).rank(method="average").to_numpy()
    value = float(np.corrcoef(a, p)[0, 1])
    return None if np.isnan(value) else value


def run_backtest(
    rows: list[HistoricalWeek],
    evaluation_season: int,
    *,
    seed: int = 20260902,
    draws: int = 400,
) -> dict[str, Any]:
    targets = [row for row in rows if row.season == evaluation_season and row.position in POSITIONS and row.week >= 5]
    results: list[dict[str, Any]] = []
    config = dict(MODEL_CONFIG)
    config["draws"] = draws

    for target_week in sorted({row.week for row in targets}):
        prior = [row for row in rows if row.season < evaluation_season or (row.season == evaluation_season and row.week < target_week)]
        prior_by_position = {position: [row for row in prior if row.position == position] for position in POSITIONS}
        prior_by_player: dict[int, list[HistoricalWeek]] = defaultdict(list)
        for row in prior:
            prior_by_player[row.player_id].append(row)
        peer_cache: dict[tuple[str, int], list[HistoricalWeek]] = {}
        defense_cache: dict[tuple[str, str | None], float | None] = {}

        for target in (row for row in targets if row.week == target_week):
            own = prior_by_player.get(target.player_id, [])[-int(config["max_player_games"]):]
            if len(own) < int(config["minimum_historical_games"]):
                continue
            mean_bucket = int(round(float(np.mean([row.dk_points for row in own])) * 2))
            peer_key = (target.position, mean_bucket)
            if peer_key not in peer_cache:
                center = mean_bucket / 2.0
                candidates = [row for row in prior_by_position[target.position] if row.player_id != target.player_id]
                by_peer: dict[int, list[HistoricalWeek]] = defaultdict(list)
                for row in candidates:
                    by_peer[row.player_id].append(row)
                peer_ids = sorted(
                    by_peer,
                    key=lambda peer_id: (abs(float(np.mean([row.dk_points for row in by_peer[peer_id]])) - center), peer_id),
                )
                selected: list[HistoricalWeek] = []
                for peer_id in peer_ids:
                    selected.extend(sorted(by_peer[peer_id], key=lambda row: row.chronological_key, reverse=True))
                    if len(selected) >= int(config["max_prior_games"]):
                        break
                peer_cache[peer_key] = selected[:int(config["max_prior_games"])]
            defense_key = (target.position, target.opponent)
            if defense_key not in defense_cache:
                defense_cache[defense_key] = opponent_factor(prior_by_position[target.position], target.position, target.opponent)
            projection = project_player(
                player_id=target.player_id,
                player_gsis_id=target.player_gsis_id,
                player_name=target.player_name,
                position=target.position,
                historical_rows=own + peer_cache[peer_key],
                cutoff_season=evaluation_season,
                cutoff_week=target.week,
                context=ProjectionContext(opponent_factor=defense_cache[defense_key]),
                seed=seed,
                config=config,
            )
            if projection.model_proj_fpts is None or projection.baseline_fpts is None:
                continue
            results.append({
                "position": target.position,
                "season": target.season,
                "week": target.week,
                "player_gsis_id": target.player_gsis_id,
                "actual": target.dk_points,
                "model": projection.model_proj_fpts,
                "baseline": projection.baseline_fpts,
                "p10": projection.floor_fpts,
                "p90": projection.ceiling_fpts,
                "history_games": projection.history_games,
            })

    metrics: dict[str, dict[str, Any]] = {}
    for position in POSITIONS:
        cohort = [row for row in results if row["position"] == position]
        actual = [row["actual"] for row in cohort]
        model = [row["model"] for row in cohort]
        baseline = [row["baseline"] for row in cohort]
        metrics[position] = {
            "n": len(cohort),
            "model_mae": round(float(np.mean(np.abs(np.asarray(model) - actual))), 6) if cohort else None,
            "baseline_mae": round(float(np.mean(np.abs(np.asarray(baseline) - actual))), 6) if cohort else None,
            "mae_delta": round(float(np.mean(np.abs(np.asarray(model) - actual)) - np.mean(np.abs(np.asarray(baseline) - actual))), 6) if cohort else None,
            "model_bias": round(float(np.mean(np.asarray(model) - actual)), 6) if cohort else None,
            "model_spearman": _spearman(actual, model),
            "baseline_spearman": _spearman(actual, baseline),
            "actual_at_or_below_p10": round(float(np.mean([row["actual"] <= row["p10"] for row in cohort])), 6) if cohort else None,
            "actual_at_or_below_p90": round(float(np.mean([row["actual"] <= row["p90"] for row in cohort])), 6) if cohort else None,
        }

    payload = {
        "harness_version": HARNESS_VERSION,
        "model_version": MODEL_VERSION,
        "evaluation_season": evaluation_season,
        "cutoff_rule": "target season/week excluded; only strictly earlier regular-season rows eligible",
        "cohort_rule": "skill-position nflverse rows, weeks 5-18, player must have >=2 prior games",
        "benchmark": "recency-weighted own-history mean; historical DK Avg unavailable until salary archives are stored",
        "seed": seed,
        "config": config,
        "n": len(results),
        "metrics": metrics,
    }
    payload["output_digest"] = artifact_digest(payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--evaluation-season", type=int, default=2025)
    parser.add_argument("--training-start", type=int, default=2020)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--draws", type=int, default=400)
    args = parser.parse_args()
    paths = source_paths(args.source_root, range(args.training_start, args.evaluation_season + 1))
    rows, evidence = load_source_rows(paths)
    result = run_backtest(rows, args.evaluation_season, draws=args.draws)
    result["source_evidence"] = evidence
    result["output_digest"] = artifact_digest({key: value for key, value in result.items() if key != "output_digest"})
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)


if __name__ == "__main__":
    main()
