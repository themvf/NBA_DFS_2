"""Build immutable pre-kickoff CFB team features for upcoming games."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Iterable, Mapping

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_cfb_team_game_features
from model.cfb_historical_signals import (
    OpponentAdjustment,
    blend_feature,
    opponent_adjusted_ratings,
    season_blend_weights,
)

FEATURE_VERSION = "cfb-team-context-v2"

# Games and history are read with these columns; both team classifications are
# carried so the FBS-versus-FBS population can be enforced in one place.
_HISTORY_COLUMNS = """
    m.id, m.season, m.week, m.commence_time, m.neutral_site,
    m.home_team_id, m.away_team_id, m.home_score, m.away_score,
    ht.classification AS home_classification,
    at.classification AS away_classification
"""
_HISTORY_JOINS = """
    FROM cfb_matchups m
    JOIN cfb_teams ht ON ht.team_id = m.home_team_id
    JOIN cfb_teams at ON at.team_id = m.away_team_id
"""


def _is_fbs(classification: object) -> bool:
    return str(classification or "").strip().casefold() == "fbs"


def partition_fbs_games(rows: Iterable[Mapping[str, object]]) -> dict:
    """Split rows into the FBS-versus-FBS population and its exclusions.

    The canonical population for every CFB cohort in this project is
    FBS-versus-FBS (see the historical-signal spec and
    ``research.cfb_hypotheses``).  An FBS team's September schedule usually
    contains exactly one FCS opponent, so counting a 63-3 tune-up alongside a
    conference game distorts the very weeks the current-season blend is most
    sensitive to.

    A missing classification is excluded but counted separately, so a
    systematic upstream gap surfaces as an unknown count rather than as a
    quietly smaller sample.
    """
    kept: list[Mapping[str, object]] = []
    non_fbs = 0
    unknown = 0
    for row in rows:
        home = row.get("home_classification")
        away = row.get("away_classification")
        if home is None or away is None:
            unknown += 1
        elif _is_fbs(home) and _is_fbs(away):
            kept.append(row)
        else:
            non_fbs += 1
    return {"games": kept, "excluded_non_fbs": non_fbs, "excluded_unknown_classification": unknown}


def summarize_team_games(rows: list[dict], team_id: int) -> dict:
    points_for: list[float] = []
    points_against: list[float] = []
    wins = 0
    for row in rows:
        is_home = int(row["home_team_id"]) == team_id
        scored = float(row["home_score"] if is_home else row["away_score"])
        allowed = float(row["away_score"] if is_home else row["home_score"])
        points_for.append(scored)
        points_against.append(allowed)
        wins += int(scored > allowed)
    games = len(points_for)
    return {
        "games": games,
        "points_for": mean(points_for) if games else None,
        "points_against": mean(points_against) if games else None,
        "margin": mean(a - b for a, b in zip(points_for, points_against)) if games else None,
        "win_rate": wins / games if games else None,
    }


def season_adjustments(rows: Iterable[Mapping[str, object]]) -> dict[int, OpponentAdjustment]:
    """Solve one SRS per season.

    Ratings are never pooled across seasons: a roster turns over far too much
    between years for a multi-season solve to describe either one.
    """
    by_season: dict[int, list[Mapping[str, object]]] = {}
    for row in rows:
        by_season.setdefault(int(row["season"]), []).append(row)
    return {season: opponent_adjusted_ratings(games) for season, games in by_season.items()}


def adjustment_context(
    adjustments: Mapping[int, OpponentAdjustment], team_id: int, season: int,
) -> tuple[dict | None, dict | None]:
    """Return the team's current-season and prior-season rating context.

    The prior value averages the team's rating in each earlier season that has
    one, mirroring how ``prior`` pools raw production across the same window.
    """
    current = adjustments.get(season)
    current_context = None
    if current and team_id in current.ratings:
        current_context = {
            "rating": current.ratings[team_id],
            "strength_of_schedule": current.strength_of_schedule[team_id],
            "home_field_advantage": current.home_field_advantage,
            "population_teams": current.teams,
            "population_games": current.games,
            # Ratings compare only within a component; an early-season graph is
            # fragmented, so this is the caveat on the number above.
            "schedule_components": current.components,
            "converged": current.converged,
        }
    prior_ratings = [
        adjustment.ratings[team_id]
        for prior_season, adjustment in adjustments.items()
        if prior_season < season and team_id in adjustment.ratings
    ]
    prior_sos = [
        adjustment.strength_of_schedule[team_id]
        for prior_season, adjustment in adjustments.items()
        if prior_season < season and team_id in adjustment.strength_of_schedule
    ]
    prior_context = None
    if prior_ratings:
        prior_context = {
            "rating": mean(prior_ratings),
            "strength_of_schedule": mean(prior_sos) if prior_sos else None,
            "seasons": len(prior_ratings),
        }
    return current_context, prior_context


def _history_before(
    db: DatabaseManager, before: datetime, *, min_season: int,
    team_id: int | None = None,
) -> list[dict]:
    filters = [
        "m.completed=TRUE", "m.home_score IS NOT NULL", "m.away_score IS NOT NULL",
        "m.commence_time < %s", "m.season >= %s",
    ]
    params: list[object] = [before, min_season]
    if team_id is not None:
        filters.append("(m.home_team_id=%s OR m.away_team_id=%s)")
        params.extend([team_id, team_id])
    return db.execute(
        f"SELECT {_HISTORY_COLUMNS} {_HISTORY_JOINS} "
        f"WHERE {' AND '.join(filters)} ORDER BY m.commence_time",
        tuple(params),
    )


def build_team_feature(
    db: DatabaseManager, *, game: dict, team_id: int, opponent_team_id: int,
    as_of: datetime,
) -> dict:
    """Build one feature row.

    Delegates to the same composition the scheduled batch uses so the two
    paths cannot drift apart.
    """
    season = int(game["season"])
    rows = _history_before(db, as_of, min_season=season - 3)
    partition = partition_fbs_games(rows)
    eligible = partition["games"]
    team_rows = [
        row for row in eligible
        if team_id in (int(row["home_team_id"]), int(row["away_team_id"]))
    ]
    adjustments = season_adjustments(eligible)
    roster, roster_confidence = _roster_context(db, team_id, season, as_of)
    return compose_team_feature(
        game=game, team_id=team_id, opponent_team_id=opponent_team_id, as_of=as_of,
        current=summarize_team_games([r for r in team_rows if int(r["season"]) == season], team_id),
        prior=summarize_team_games([r for r in team_rows if int(r["season"]) < season], team_id),
        roster=roster, roster_confidence=roster_confidence,
        adjustments=adjustments, population=partition,
    )


def _roster_context(db: DatabaseManager, team_id: int, season: int, as_of: datetime) -> tuple[dict | None, float]:
    row = db.execute_one(
        """
        SELECT summary_json, confidence, captured_at, available_at
        FROM cfb_roster_snapshots
        WHERE team_id=%s AND season=%s AND point_in_time_eligible=TRUE
          AND available_at <= %s
        ORDER BY available_at DESC, id DESC LIMIT 1
        """,
        (team_id, season, as_of),
    )
    if not row:
        return None, 0.0
    return dict(row.get("summary_json") or {}), float(row.get("confidence") or 0)


def _population_summary(population: Mapping[str, object] | None) -> dict:
    """Report the eligible-game counts without embedding the rows themselves."""
    population = population or {}
    return {
        "eligible_games": len(population.get("games", [])),
        "excluded_non_fbs": population.get("excluded_non_fbs", 0),
        "excluded_unknown_classification": population.get("excluded_unknown_classification", 0),
        "definition": "fbs_versus_fbs",
    }


def compose_team_feature(
    *, game: dict, team_id: int, opponent_team_id: int, as_of: datetime,
    current: dict, prior: dict, roster: dict | None,
    roster_confidence: float,
    adjustments: Mapping[int, OpponentAdjustment] | None = None,
    population: Mapping[str, object] | None = None,
) -> dict:
    """Compose a feature row from already point-in-time-filtered inputs."""
    season = int(game["season"])
    effective_games = float(current["games"])
    current_weight, prior_weight = season_blend_weights(effective_games)
    blended = {
        key: blend_feature(current.get(key), prior.get(key), effective_games)
        for key in ("points_for", "points_against", "margin", "win_rate")
    }
    current_adjusted, prior_adjusted = adjustment_context(
        adjustments or {}, team_id, season,
    )
    blended["opponent_adjusted_margin"] = blend_feature(
        (current_adjusted or {}).get("rating"),
        (prior_adjusted or {}).get("rating"),
        effective_games,
    )
    # Source completeness reports what is actually present, so a team with no
    # prior seasons and no games yet cannot score the same as a fully observed
    # one.  The four components are the current season, the prior window, the
    # roster snapshot and the opponent adjustment.
    present = sum((
        current["games"] > 0,
        prior["games"] > 0,
        roster is not None,
        current_adjusted is not None or prior_adjusted is not None,
    ))
    return {
        "game_id": int(game["id"]), "team_id": team_id,
        "opponent_team_id": opponent_team_id, "feature_version": FEATURE_VERSION,
        "as_of_at": as_of, "available_at": as_of,
        "games_played": int(current["games"]), "effective_games": effective_games,
        "current_weight": current_weight, "prior_weight": prior_weight,
        "source_completeness": present / 4,
        "features_json": {
            "current_season": current,
            "preseason_prior": prior,
            "blended": blended,
            "opponent_adjusted": {
                "current_season": current_adjusted,
                "preseason_prior": prior_adjusted,
                "method": "iterative_srs_home_field_estimated_from_population",
            },
            "population": _population_summary(population),
            "roster": roster,
            "roster_confidence": roster_confidence,
            "weight_formula": "effective_games/(effective_games+4)",
            "point_in_time": True,
        },
    }


def build_upcoming_features(db: DatabaseManager, through_date: date | None = None) -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    through_date = through_date or (now.date() + timedelta(days=14))
    games = db.execute(
        """
        SELECT id, season, week, commence_time, home_team_id, away_team_id
        FROM cfb_matchups
        WHERE completed=FALSE AND commence_time > %s AND game_date <= %s
        ORDER BY commence_time
        """,
        (now, through_date),
    )
    if not games:
        return {"feature_version": FEATURE_VERSION, "games": 0, "rows_written": 0, "as_of": now.isoformat()}
    min_season = min(int(game["season"]) for game in games) - 3
    partition = partition_fbs_games(_history_before(db, now, min_season=min_season))
    eligible = partition["games"]
    adjustments = season_adjustments(eligible)
    by_team: dict[int, list[dict]] = {}
    for row in eligible:
        by_team.setdefault(int(row["home_team_id"]), []).append(row)
        by_team.setdefault(int(row["away_team_id"]), []).append(row)
    roster_rows = db.execute(
        """
        SELECT DISTINCT ON (team_id, season) team_id, season, summary_json, confidence
        FROM cfb_roster_snapshots
        WHERE point_in_time_eligible=TRUE AND available_at <= %s
        ORDER BY team_id, season, available_at DESC, id DESC
        """,
        (now,),
    )
    rosters = {
        (int(row["team_id"]), int(row["season"])): (
            dict(row.get("summary_json") or {}), float(row.get("confidence") or 0),
        ) for row in roster_rows
    }
    feature_rows: list[dict] = []
    for game in games:
        for team_id, opponent_id in (
            (int(game["home_team_id"]), int(game["away_team_id"])),
            (int(game["away_team_id"]), int(game["home_team_id"])),
        ):
            season = int(game["season"])
            team_history = by_team.get(team_id, [])
            roster, roster_confidence = rosters.get((team_id, season), (None, 0.0))
            feature_rows.append(compose_team_feature(
                game=game, team_id=team_id, opponent_team_id=opponent_id, as_of=now,
                current=summarize_team_games(
                    [row for row in team_history if int(row["season"]) == season], team_id,
                ),
                prior=summarize_team_games(
                    [row for row in team_history if int(row["season"]) < season], team_id,
                ),
                roster=roster, roster_confidence=roster_confidence,
                adjustments=adjustments, population=partition,
            ))
    written = upsert_cfb_team_game_features(db, feature_rows)
    return {
        "feature_version": FEATURE_VERSION, "games": len(games), "rows_written": written,
        "as_of": now.isoformat(),
        "population_games": len(eligible),
        "excluded_non_fbs": partition["excluded_non_fbs"],
        "excluded_unknown_classification": partition["excluded_unknown_classification"],
        "seasons_rated": sorted(adjustments),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-date", type=date.fromisoformat)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "")
    print(json.dumps(build_upcoming_features(db, args.through_date), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
