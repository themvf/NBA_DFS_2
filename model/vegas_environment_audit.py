"""Read-only Phase 0 coverage audit for Vegas-environment DFS research.

The database connection is placed in a read-only transaction. The sole write
is the versioned JSON artifact requested by ``intent.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

from config import load_config


AUDIT_VERSION = "vegas-environment-phase0-v2"
DEFAULT_ARTIFACT_DIR = Path("artifacts")
RECOMMENDATIONS = {
    "GO_NFL_MVP",
    "GO_WITH_LIMITED_SEASONS",
    "BLOCKED_MISSING_MARKETS",
    "BLOCKED_MISSING_PLAYER_LINKAGE",
    "BLOCKED_OTHER",
}
CHECKPOINT_WINDOWS_HOURS: dict[str, tuple[float, float] | None] = {
    "open": None,
    "t_minus_48h": (42.0, 54.0),
    "t_minus_24h": (20.0, 28.0),
    "t_minus_6h": (5.0, 7.0),
    "t_minus_90m": (1.0, 2.0),
    "t_minus_15m": (5.0 / 60.0, 35.0 / 60.0),
    "close": None,
}


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def percent(numerator: int, denominator: int) -> float | None:
    return round(numerator * 100.0 / denominator, 2) if denominator else None


def lead_hours(captured_at: datetime, kickoff: datetime) -> float:
    return (kickoff - captured_at).total_seconds() / 3600.0


def checkpoint_is_satisfied(name: str, leads: Sequence[float]) -> bool:
    pregame = [lead for lead in leads if lead > 0]
    if name in {"open", "close"}:
        return bool(pregame)
    low, high = CHECKPOINT_WINDOWS_HOURS[name] or (0.0, 0.0)
    return any(low <= lead <= high for lead in pregame)


@dataclass(frozen=True)
class RecommendationPolicy:
    minimum_market_games: int = 100
    minimum_market_seasons: int = 2
    minimum_evaluable_player_rows: int = 500
    minimum_evaluable_slates: int = 10


def recommend(
    *,
    pregame_market_games: int,
    market_seasons: int,
    evaluable_player_rows: int,
    evaluable_slates: int,
    policy: RecommendationPolicy = RecommendationPolicy(),
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if pregame_market_games < policy.minimum_market_games:
        reasons.append(
            f"Only {pregame_market_games} completed/past games have eligible pregame markets; "
            f"{policy.minimum_market_games} are required."
        )
        return "BLOCKED_MISSING_MARKETS", reasons
    if (
        evaluable_player_rows < policy.minimum_evaluable_player_rows
        or evaluable_slates < policy.minimum_evaluable_slates
    ):
        reasons.append(
            f"Only {evaluable_player_rows} player rows across {evaluable_slates} slates link "
            "salary, identity, projection, and realized results."
        )
        return "BLOCKED_MISSING_PLAYER_LINKAGE", reasons
    if market_seasons < policy.minimum_market_seasons:
        reasons.append(f"Eligible markets cover {market_seasons} season(s); cross-season stability is unavailable.")
        return "GO_WITH_LIMITED_SEASONS", reasons
    reasons.append("Market and player linkage gates pass for the NFL descriptive MVP.")
    return "GO_NFL_MVP", reasons


class ReadOnlyDb:
    def __init__(self, database_url: str) -> None:
        self.connection = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        self.connection.set_session(readonly=True, autocommit=False)

    def query(self, statement: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute(statement, tuple(params))
            return [dict(row) for row in cursor.fetchall()]

    def table_exists(self, table: str) -> bool:
        return bool(self.query("SELECT to_regclass(%s) IS NOT NULL AS exists", (f"public.{table}",))[0]["exists"])

    def close(self) -> None:
        self.connection.rollback()
        self.connection.close()


def _inventory(db: ReadOnlyDb) -> dict[str, Any]:
    specs = {
        "game_odds_history": ("captured_at", "sport='nfl'"),
        "nfl_matchups": ("commence_time", "TRUE"),
        "nfl_season_games": ("kickoff", "game_type='REG'"),
        "ff_player_week_stats": ("fetched_at", "season_type='REG'"),
        "nfl_dfs_projection_runs": ("as_of_at", "TRUE"),
        "nfl_dfs_player_projections": ("created_at", "TRUE"),
        "nfl_dfs_slate_uploads": ("created_at", "TRUE"),
        "nfl_dfs_slate_players": ("created_at", "TRUE"),
        "nfl_dfs_player_week_results": ("computed_at", "TRUE"),
        "dk_slates": ("created_at", "sport='nfl'"),
        "dk_players": ("id", "EXISTS (SELECT 1 FROM dk_slates s WHERE s.id=dk_players.slate_id AND s.sport='nfl')"),
    }
    result: dict[str, Any] = {}
    for table, (time_column, predicate) in specs.items():
        if not db.table_exists(table):
            result[table] = {"exists": False, "rows": 0, "first": None, "last": None}
            continue
        row = db.query(
            f"SELECT COUNT(*)::int rows, MIN({time_column}) first, MAX({time_column}) last "
            f"FROM {table} WHERE {predicate}"
        )[0]
        result[table] = {"exists": True, **row}
    return result


def _market_audit(db: ReadOnlyDb, now: datetime) -> dict[str, Any]:
    games = db.query(
        """SELECT g.id schedule_game_id,g.season,g.week,g.matchup_id,g.kickoff,
                  COALESCE(g.completed,m.completed,FALSE) completed,
                  COALESCE(g.home_score,m.home_score) home_score,
                  COALESCE(g.away_score,m.away_score) away_score,
                  g.quoted_total_line,g.quoted_spread_line,g.quote_source,
                  m.event_id
           FROM nfl_season_games g
           LEFT JOIN nfl_matchups m ON m.id=g.matchup_id
           WHERE g.game_type='REG' AND g.kickoff IS NOT NULL
           ORDER BY g.season,g.week,g.id""",
    )
    odds = db.query(
        """SELECT h.id,h.matchup_id,h.capture_key,h.captured_at,h.bookmaker_count,
                  h.vegas_total,h.home_spread,h.home_implied,h.away_implied,
                  m.commence_time
           FROM game_odds_history h
           LEFT JOIN nfl_matchups m ON m.id=h.matchup_id
           WHERE h.sport='nfl'"""
    )
    odds_by_matchup: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in odds:
        odds_by_matchup[int(row["matchup_id"])].append(row)

    past_games = [game for game in games if game["kickoff"] < now]
    upcoming_games = [game for game in games if game["kickoff"] >= now]
    checkpoint_counts = {name: 0 for name in CHECKPOINT_WINDOWS_HOURS}
    season_market_games: set[int] = set()
    games_with_pregame = 0
    games_with_four_books = 0
    games_with_team_result = 0
    games_with_historical_closing_reference = 0
    closing_reference_seasons: set[int] = set()
    accepted_rows = 0
    regular_post_start_rows = 0
    invalid_market_rows = 0
    capture_keys: dict[str, int] = defaultdict(int)
    season_rows: dict[int, dict[str, int]] = defaultdict(lambda: {"past_games": 0, "mapped": 0, "pregame_market": 0, "completed_score": 0})
    for game in past_games:
        season = int(game["season"])
        season_rows[season]["past_games"] += 1
        if game["matchup_id"] is not None:
            season_rows[season]["mapped"] += 1
        if game["completed"] and game["home_score"] is not None and game["away_score"] is not None:
            games_with_team_result += 1
            season_rows[season]["completed_score"] += 1
        if game["quoted_total_line"] is not None and game["quoted_spread_line"] is not None:
            games_with_historical_closing_reference += 1
            closing_reference_seasons.add(season)
        rows = odds_by_matchup.get(int(game["matchup_id"]), []) if game["matchup_id"] is not None else []
        leads: list[float] = []
        supported = False
        for row in rows:
            kickoff = game["kickoff"]
            lead = lead_hours(row["captured_at"], kickoff)
            capture_keys[str(row["capture_key"])] += 1
            if lead <= 0:
                regular_post_start_rows += 1
                continue
            if row["vegas_total"] is None or row["home_spread"] is None:
                invalid_market_rows += 1
                continue
            accepted_rows += 1
            leads.append(lead)
            supported = supported or int(row["bookmaker_count"] or 0) >= 4
        if leads:
            games_with_pregame += 1
            season_market_games.add(season)
            season_rows[season]["pregame_market"] += 1
            for name in checkpoint_counts:
                checkpoint_counts[name] += int(checkpoint_is_satisfied(name, leads))
        games_with_four_books += int(bool(leads) and supported)

    total_games = len(past_games)
    checkpoint_coverage = {
        name: {"games": count, "eligible_past_games": total_games, "coverage_pct": percent(count, total_games), "window_hours": CHECKPOINT_WINDOWS_HOURS[name]}
        for name, count in checkpoint_counts.items()
    }
    upcoming_mapped = 0
    upcoming_with_market = 0
    upcoming_with_four_books = 0
    for game in upcoming_games:
        if game["matchup_id"] is None:
            continue
        upcoming_mapped += 1
        leads = []
        supported = False
        for row in odds_by_matchup.get(int(game["matchup_id"]), []):
            lead = lead_hours(row["captured_at"], game["kickoff"])
            if lead > 0 and row["vegas_total"] is not None and row["home_spread"] is not None:
                leads.append(lead)
                supported = supported or int(row["bookmaker_count"] or 0) >= 4
        upcoming_with_market += int(bool(leads))
        upcoming_with_four_books += int(bool(leads) and supported)

    provider_mapped_rows = sum(row["commence_time"] is not None for row in odds)
    provider_complete_pregame_rows = sum(
        row["commence_time"] is not None
        and row["captured_at"] < row["commence_time"]
        and row["vegas_total"] is not None
        and row["home_spread"] is not None
        for row in odds
    )
    provider_post_start_rows = sum(
        row["commence_time"] is not None and row["captured_at"] >= row["commence_time"]
        for row in odds
    )
    return {
        "past_regular_season_games": total_games,
        "mapped_to_nfl_matchups": sum(row["mapped"] for row in season_rows.values()),
        "mapping_pct": percent(sum(row["mapped"] for row in season_rows.values()), total_games),
        "games_with_eligible_pregame_market": games_with_pregame,
        "pregame_market_coverage_pct": percent(games_with_pregame, total_games),
        "games_with_four_plus_books": games_with_four_books,
        "four_book_support_pct_of_pregame": percent(games_with_four_books, games_with_pregame),
        "accepted_pregame_rows": accepted_rows,
        "rejected_post_start_rows": regular_post_start_rows,
        "rejected_incomplete_market_rows": invalid_market_rows,
        "games_with_completed_scores": games_with_team_result,
        "team_result_coverage_pct": percent(games_with_team_result, total_games),
        "eligible_market_seasons": sorted(season_market_games),
        "historical_closing_reference": {
            "games": games_with_historical_closing_reference,
            "coverage_pct": percent(games_with_historical_closing_reference, total_games),
            "seasons": sorted(closing_reference_seasons),
            "source": "nfl_season_games quoted lines (nflverse)",
            "checkpoint_eligible": False,
            "reason": "The source has no per-game pregame availability timestamp; it cannot establish open or checkpoint state.",
        },
        "capture_keys": dict(sorted(capture_keys.items())),
        "checkpoint_coverage": checkpoint_coverage,
        "by_season": {str(season): values for season, values in sorted(season_rows.items())},
        "upcoming_readiness": {
            "games": len(upcoming_games),
            "mapped_to_nfl_matchups": upcoming_mapped,
            "mapping_pct": percent(upcoming_mapped, len(upcoming_games)),
            "games_with_eligible_pregame_market": upcoming_with_market,
            "pregame_market_coverage_pct": percent(upcoming_with_market, len(upcoming_games)),
            "games_with_four_plus_books": upcoming_with_four_books,
        },
        "all_nfl_odds_rows": {
            "rows": len(odds),
            "mapped_to_commence_time": provider_mapped_rows,
            "mapping_pct": percent(provider_mapped_rows, len(odds)),
            "complete_pregame_rows": provider_complete_pregame_rows,
            "post_start_rows": provider_post_start_rows,
            "missing_matchup_time_rows": len(odds) - provider_mapped_rows,
        },
    }


def _player_audit(db: ReadOnlyDb) -> dict[str, Any]:
    weekly = db.query(
        """SELECT COUNT(*)::int rows,
                  COUNT(*) FILTER (WHERE w.fantasy_points_ppr IS NOT NULL)::int ppr_rows,
                  COUNT(*) FILTER (WHERE w.team IS NOT NULL)::int team_rows,
                  COUNT(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM nfl_season_games g
                    JOIN nfl_teams t ON t.team_id IN (g.home_team_id,g.away_team_id)
                    WHERE g.season=w.season AND g.week=w.week AND t.abbreviation=w.team
                  ))::int schedule_link_rows,
                  COUNT(DISTINCT w.season)::int seasons,
                  MIN(w.season)::int first_season,MAX(w.season)::int last_season
           FROM ff_player_week_stats w WHERE w.season_type='REG'"""
    )[0]
    realized = {
        "rows": 0,
        "exact_rows": 0,
        "excluded_rows": 0,
        "game_linked_exact_rows": 0,
        "seasons": 0,
        "scoring_versions": [],
    }
    if db.table_exists("nfl_dfs_player_week_results"):
        realized = db.query(
            """WITH latest AS (
                 SELECT r.*,ROW_NUMBER() OVER (
                   PARTITION BY player_week_stat_id
                   ORDER BY computed_at DESC,id DESC
                 ) revision_rank
                 FROM nfl_dfs_player_week_results r
               )
               SELECT COUNT(*)::int rows,
                      COUNT(*) FILTER (WHERE scoring_status='exact')::int exact_rows,
                      COUNT(*) FILTER (WHERE scoring_status='excluded')::int excluded_rows,
                      COUNT(*) FILTER (WHERE scoring_status='exact' AND game_id IS NOT NULL)::int game_linked_exact_rows,
                      COUNT(DISTINCT season)::int seasons,
                      ARRAY_AGG(DISTINCT scoring_version ORDER BY scoring_version) scoring_versions
               FROM latest WHERE revision_rank=1"""
        )[0]
    new_workspace = {"uploads": 0, "player_rows": 0, "identity_rows": 0, "projection_rows": 0, "salary_rows": 0, "result_link_rows": 0, "projected_ownership_rows": 0, "actual_ownership_rows": 0, "evaluable_slates": 0}
    if db.table_exists("nfl_dfs_slate_players"):
        new_workspace = db.query(
            """SELECT COUNT(DISTINCT u.upload_id)::int uploads,
                      COUNT(*)::int player_rows,
                      COUNT(*) FILTER (WHERE sp.ff_player_id IS NOT NULL)::int identity_rows,
                      COUNT(*) FILTER (WHERE sp.our_proj IS NOT NULL)::int projection_rows,
                      COUNT(*) FILTER (WHERE sp.salary > 0)::int salary_rows,
                      COUNT(*) FILTER (WHERE EXISTS (
                        SELECT 1 FROM ff_player_week_stats w
                        WHERE w.player_id=sp.ff_player_id AND w.season=r.season
                          AND w.week=r.week AND w.season_type='REG'
                      ))::int result_link_rows,
                      COUNT(*) FILTER (WHERE sp.linestar_own_pct IS NOT NULL)::int projected_ownership_rows,
                      0::int actual_ownership_rows,
                      0::int evaluable_slates
               FROM nfl_dfs_slate_uploads u
               JOIN nfl_dfs_slate_players sp ON sp.upload_id=u.upload_id
               LEFT JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id"""
        )[0]
        complete = db.query(
            """SELECT COUNT(*)::int evaluable_slates FROM (
                 SELECT u.upload_id,u.format,
                   COUNT(*) FILTER (WHERE sp.salary>0 AND sp.our_proj IS NOT NULL AND EXISTS (
                     SELECT 1 FROM ff_player_week_stats w
                     WHERE w.player_id=sp.ff_player_id AND w.season=r.season
                       AND w.week=r.week AND w.season_type='REG'
                   )) result_rows
                 FROM nfl_dfs_slate_uploads u
                 JOIN nfl_dfs_slate_players sp ON sp.upload_id=u.upload_id
                 LEFT JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
                 GROUP BY u.upload_id,u.format
               ) samples WHERE result_rows >= CASE WHEN format='showdown' THEN 6 ELSE 9 END"""
        )[0]
        new_workspace["evaluable_slates"] = int(complete["evaluable_slates"] or 0)
    legacy = {"slates": 0, "player_rows": 0, "evaluable_rows": 0, "actual_ownership_rows": 0, "evaluable_slates": 0}
    if db.table_exists("dk_slates") and db.table_exists("dk_players"):
        legacy = db.query(
            """SELECT COUNT(DISTINCT s.id)::int slates,COUNT(p.id)::int player_rows,
                      COUNT(*) FILTER (WHERE p.salary>0 AND p.actual_fpts IS NOT NULL
                        AND COALESCE(p.our_proj,p.live_proj,p.linestar_proj) IS NOT NULL)::int evaluable_rows,
                      COUNT(*) FILTER (WHERE p.actual_own_pct IS NOT NULL)::int actual_ownership_rows,
                      COUNT(DISTINCT s.id) FILTER (WHERE p.actual_fpts IS NOT NULL)::int evaluable_slates
               FROM dk_slates s LEFT JOIN dk_players p ON p.slate_id=s.id WHERE s.sport='nfl'"""
        )[0]
    evaluable_player_rows = int(new_workspace["result_link_rows"] or 0) + int(legacy["evaluable_rows"] or 0)
    evaluable_slates = int(new_workspace["evaluable_slates"] or 0) + int(legacy["evaluable_slates"] or 0)
    return {
        "historical_player_weeks": weekly,
        "realized_dk_scoring": realized,
        "nfl_workspace": new_workspace,
        "legacy_dk_tables": legacy,
        "evaluable_player_rows": evaluable_player_rows,
        "evaluable_slates": evaluable_slates,
        "optimal_lineup_reconstructable_slates": evaluable_slates,
        "ownership_adjusted_rows": int(new_workspace["actual_ownership_rows"] or 0) + int(legacy["actual_ownership_rows"] or 0),
        "notes": [
            "Exact realized DK scoring, including DST, is versioned from ff_player_week_stats source components.",
            "DST scoring evidence retains reusable components so redraft scoring can apply league-specific settings without rewriting source data.",
            "LineStar ownership in nfl_dfs_slate_players is projected ownership, not actual contest ownership.",
            "Optimal-lineup reconstruction requires salary and realized DK points on the same frozen slate.",
        ],
    }


def build_report(db: ReadOnlyDb, *, now: datetime, policy: RecommendationPolicy = RecommendationPolicy()) -> dict[str, Any]:
    inventory = _inventory(db)
    market = _market_audit(db, now)
    players = _player_audit(db)
    recommendation, reasons = recommend(
        pregame_market_games=int(market["games_with_eligible_pregame_market"]),
        market_seasons=len(market["eligible_market_seasons"]),
        evaluable_player_rows=int(players["evaluable_player_rows"]),
        evaluable_slates=int(players["evaluable_slates"]),
        policy=policy,
    )
    report: dict[str, Any] = {
        "artifact_type": "vegas-environment-phase0-coverage-audit",
        "audit_version": AUDIT_VERSION,
        "sport": "nfl",
        "as_of": now.isoformat(),
        "read_only": True,
        "policy": policy.__dict__,
        "inventory": inventory,
        "market_coverage": market,
        "player_linkage": players,
        "recommendation": recommendation,
        "recommendation_reasons": reasons,
        "next_actions": _next_actions(recommendation, market, players),
    }
    report["artifact_digest"] = digest({key: value for key, value in report.items() if key not in {"as_of", "artifact_digest"}})
    return report


def _next_actions(recommendation: str, market: Mapping[str, Any], players: Mapping[str, Any]) -> list[str]:
    actions: list[str] = []
    if recommendation == "BLOCKED_MISSING_MARKETS":
        actions.append("Backfill or accumulate canonical pregame NFL total/spread captures with kickoff-safe timestamps.")
        actions.append("Do not substitute nfl_season_games quoted_total_line for missing checkpoint history unless its availability timestamp is proven.")
    if int(players["evaluable_player_rows"]) < RecommendationPolicy().minimum_evaluable_player_rows:
        actions.append("Persist historical NFL DK salary slates and link each player to ff_player_week_stats results.")
        if int(players["realized_dk_scoring"]["exact_rows"] or 0) == 0:
            actions.append("Compute realized DK scoring from immutable weekly source_row data and verify with golden scoring fixtures.")
    if int(players["ownership_adjusted_rows"]) == 0:
        actions.append("Collect actual contest ownership before attempting ownership-adjusted leverage conclusions.")
    if int(market["rejected_post_start_rows"]) > 0:
        actions.append("Keep post-start odds rows quarantined from every pregame feature and metric.")
    actions.append("Re-run this exact audit after remediation; do not begin predictive fitting until its gates pass.")
    return actions


def write_report(report: Mapping[str, Any], artifact_path: Path) -> None:
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--as-of", help="Offset-aware ISO timestamp; defaults to now")
    args = parser.parse_args()
    now = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("--as-of must include a UTC offset")
    artifact = args.artifact or DEFAULT_ARTIFACT_DIR / f"vegas_environment_phase0_nfl_{now.date().isoformat()}.json"
    config = load_config()
    db = ReadOnlyDb(config.database_url)
    try:
        report = build_report(db, now=now)
    finally:
        db.close()
    if report["recommendation"] not in RECOMMENDATIONS:
        raise RuntimeError("Unknown recommendation")
    write_report(report, artifact)
    print(json.dumps({
        "artifact": str(artifact),
        "artifact_digest": report["artifact_digest"],
        "recommendation": report["recommendation"],
        "recommendation_reasons": report["recommendation_reasons"],
    }, indent=2))


if __name__ == "__main__":
    main()
