"""Retrospective DST/K diagnostic; never a pre-lock or promotion verdict.

Archived nflverse quoted lines can be revised or captured after kickoff. This
screen checks direction and scoring mechanics on completed seasons only. The
registered coherent forward study remains the promotion authority.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_projections import _history, _slate_environment
from model.nfl_dfs_historical import BOOM_THRESHOLDS, MODEL_CONFIG, project_player
from model.nfl_special_teams_projection import SpecialTeamsContext, project_special_teams
from model.nfl_team_aliases import normalize_team


def target_rows(db: DatabaseManager, season: int, week: int) -> list[dict]:
    return db.execute(
        """SELECT w.player_id,p.gsis_id,p.canonical_name,p.position,w.team,w.opponent,
                  result.actual_dk_fpts
           FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id
           JOIN LATERAL (
             SELECT r.actual_dk_fpts FROM nfl_dfs_player_week_results r
             WHERE r.player_week_stat_id=w.id AND r.scoring_status='exact'
             ORDER BY r.computed_at DESC,r.id DESC LIMIT 1
           ) result ON TRUE
           WHERE w.season=%s AND w.week=%s AND w.season_type='REG'
             AND p.position IN ('DST','K')
           ORDER BY p.position,w.team,w.player_id""",
        (season, week),
    )


def compare(rows: list[dict]) -> dict:
    def interval_score(actual: float, low: float, high: float) -> float:
        # Central 80% interval, descriptive only. The registered WIS gate also
        # needs P25/P75 and its own week-clustered uncertainty calculation.
        return high - low + 10 * max(low - actual, 0) + 10 * max(actual - high, 0)

    by_position: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_position[row["position"]].append(row)
    result = {}
    for position, games in sorted(by_position.items()):
        paired = [r for r in games if r.get("candidate") is not None and r.get("baseline") is not None]
        reasons = sorted({r.get("reason") for r in games if r.get("reason")})
        distribution_rows = [r for r in paired if all(r.get(f"{source}_{tail}") is not None
                             for source in ("baseline", "candidate")
                             for tail in ("p10", "p50", "p90", "boom"))]
        result[position] = {
            "target_rows": len(games), "paired_rows": len(paired),
            "unavailable_reasons": {reason: sum(r.get("reason") == reason for r in games) for reason in reasons},
            "baseline_mae": round(sum(abs(r["baseline"] - r["actual"]) for r in paired) / len(paired), 4) if paired else None,
            "candidate_mae": round(sum(abs(r["candidate"] - r["actual"]) for r in paired) / len(paired), 4) if paired else None,
            "distribution_rows": len(distribution_rows),
        }
        if paired:
            result[position]["paired_mae_delta"] = round(
                sum(abs(r["candidate"] - r["actual"]) - abs(r["baseline"] - r["actual"])
                    for r in paired) / len(paired), 4)
        if distribution_rows:
            for source in ("baseline", "candidate"):
                result[position][f"{source}_interval_score80"] = round(sum(
                    interval_score(r["actual"], r[f"{source}_p10"], r[f"{source}_p90"])
                    for r in distribution_rows) / len(distribution_rows), 4)
                result[position][f"{source}_boom_brier"] = round(sum(
                    (r[f"{source}_boom"] - float(r["actual"] >= BOOM_THRESHOLDS[position])) ** 2
                    for r in distribution_rows) / len(distribution_rows), 6)
    return result


def run(db: DatabaseManager, season: int, weeks: range, draws: int) -> dict:
    rows = []
    config = {**MODEL_CONFIG, "draws": draws}
    # Load once; both projection functions apply the strict target-week cutoff
    # for each row. Keep only positions needed by this diagnostic.
    history = [row for row in _history(db, season + 1, 1) if row.position in {"DST", "K"}]
    for week in weeks:
        environment = {normalize_team(team): value for team, value in _slate_environment(db, season, week).items()}
        for target in target_rows(db, season, week):
            own = environment.get(normalize_team(target["team"]), {})
            opposing = environment.get(normalize_team(target["opponent"]), {})
            args = dict(player_id=int(target["player_id"]), player_gsis_id=target["gsis_id"],
                        player_name=target["canonical_name"], position=target["position"],
                        historical_rows=history, cutoff_season=season, cutoff_week=week,
                        seed=20260902, config=config)
            baseline = project_player(**args)
            candidate = project_special_teams(**args, context=SpecialTeamsContext(
                team_implied_total=own.get("team_implied_total"),
                opponent_implied_total=opposing.get("team_implied_total"),
                opponent_team=target["opponent"],
            ))
            rows.append({
                "season": season, "week": week, "position": target["position"],
                "player_id": target["player_id"], "team": target["team"],
                "actual": float(target["actual_dk_fpts"]),
                "baseline": baseline.model_proj_fpts, "candidate": candidate.mean,
                "baseline_p10": baseline.floor_fpts, "baseline_p50": baseline.median_fpts,
                "baseline_p90": baseline.ceiling_fpts, "baseline_boom": baseline.boom_rate,
                "candidate_p10": candidate.p10, "candidate_p50": candidate.p50,
                "candidate_p90": candidate.p90, "candidate_boom": candidate.boom,
                "reason": candidate.feature_snapshot.get("reason"),
            })
    return {
        "status": "retrospective_diagnostic_only", "season": season,
        "weeks": [weeks.start, weeks.stop - 1], "draws": draws,
        "market_timing": "archived quote, original pre-lock availability unverified",
        "summary": compare(rows), "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--first-week", type=int, default=1)
    parser.add_argument("--last-week", type=int, default=18)
    parser.add_argument("--draws", type=int, default=300)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.first_week < 1 or args.last_week < args.first_week or args.draws < 100:
        parser.error("Require ordered regular-season weeks and at least 100 draws")
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report = run(db, args.season, range(args.first_week, args.last_week + 1), args.draws)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, indent=2))


if __name__ == "__main__":
    main()
