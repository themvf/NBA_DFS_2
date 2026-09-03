"""Build immutable pre-kickoff CFB team features for upcoming games."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from statistics import mean

from config import load_config
from db.database import DatabaseManager
from db.queries import upsert_cfb_team_game_feature, upsert_cfb_team_game_features
from model.cfb_historical_signals import blend_feature, season_blend_weights

FEATURE_VERSION = "cfb-team-context-v1"


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


def _games_before(db: DatabaseManager, team_id: int, before: datetime, *, season: int | None = None, min_season: int | None = None) -> list[dict]:
    filters = ["completed=TRUE", "home_score IS NOT NULL", "away_score IS NOT NULL", "commence_time < %s", "(home_team_id=%s OR away_team_id=%s)"]
    params: list[object] = [before, team_id, team_id]
    if season is not None:
        filters.append("season=%s")
        params.append(season)
    if min_season is not None:
        filters.append("season >= %s")
        params.append(min_season)
    return db.execute(
        f"""SELECT id, season, week, commence_time, home_team_id, away_team_id,
                   home_score, away_score
            FROM cfb_matchups WHERE {' AND '.join(filters)}
            ORDER BY commence_time""",
        tuple(params),
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


def build_team_feature(
    db: DatabaseManager, *, game: dict, team_id: int, opponent_team_id: int,
    as_of: datetime,
) -> dict:
    season = int(game["season"])
    current_rows = _games_before(db, team_id, as_of, season=season)
    prior_rows = _games_before(db, team_id, as_of, min_season=max(0, season - 3))
    prior_rows = [row for row in prior_rows if int(row["season"]) < season]
    current = summarize_team_games(current_rows, team_id)
    prior = summarize_team_games(prior_rows, team_id)
    roster, roster_confidence = _roster_context(db, team_id, season, as_of)
    return compose_team_feature(
        game=game, team_id=team_id, opponent_team_id=opponent_team_id,
        as_of=as_of, current=current, prior=prior, roster=roster,
        roster_confidence=roster_confidence,
    )


def compose_team_feature(
    *, game: dict, team_id: int, opponent_team_id: int, as_of: datetime,
    current: dict, prior: dict, roster: dict | None,
    roster_confidence: float,
) -> dict:
    """Compose a feature row from already point-in-time-filtered inputs."""
    effective_games = float(current["games"])
    current_weight, prior_weight = season_blend_weights(effective_games)
    blended = {
        key: blend_feature(current.get(key), prior.get(key), effective_games)
        for key in ("points_for", "points_against", "margin", "win_rate")
    }
    component_count = 2 + int(roster is not None)
    return {
        "game_id": int(game["id"]), "team_id": team_id,
        "opponent_team_id": opponent_team_id, "feature_version": FEATURE_VERSION,
        "as_of_at": as_of, "available_at": as_of,
        "games_played": int(current["games"]), "effective_games": effective_games,
        "current_weight": current_weight, "prior_weight": prior_weight,
        "source_completeness": component_count / 3,
        "features_json": {
            "current_season": current,
            "preseason_prior": prior,
            "blended": blended,
            "roster": roster,
            "roster_confidence": roster_confidence,
            "weight_formula": "effective_games/(effective_games+4)",
            "point_in_time": True,
        },
    }


def build_upcoming_features(db: DatabaseManager, through_date: date | None = None) -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    through_date = through_date or (now.date() + timedelta(days=14))
    params: list[object] = [now]
    date_filter = "AND game_date <= %s"
    params.append(through_date)
    games = db.execute(
        f"""
        SELECT id, season, week, commence_time, home_team_id, away_team_id
        FROM cfb_matchups
        WHERE completed=FALSE AND commence_time > %s {date_filter}
        ORDER BY commence_time
        """,
        tuple(params),
    )
    if not games:
        return {"feature_version": FEATURE_VERSION, "games": 0, "rows_written": 0, "as_of": now.isoformat()}
    min_season = min(int(game["season"]) for game in games) - 3
    history = db.execute(
        """
        SELECT id, season, week, commence_time, home_team_id, away_team_id,
               home_score, away_score
        FROM cfb_matchups
        WHERE completed=TRUE AND home_score IS NOT NULL AND away_score IS NOT NULL
          AND commence_time < %s AND season >= %s
        ORDER BY commence_time
        """,
        (now, min_season),
    )
    by_team: dict[int, list[dict]] = {}
    for row in history:
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
            current = summarize_team_games(
                [row for row in team_history if int(row["season"]) == season], team_id,
            )
            prior = summarize_team_games(
                [row for row in team_history if int(row["season"]) < season], team_id,
            )
            roster, roster_confidence = rosters.get((team_id, season), (None, 0.0))
            feature_rows.append(compose_team_feature(
                game=game, team_id=team_id, opponent_team_id=opponent_id,
                as_of=now, current=current, prior=prior, roster=roster,
                roster_confidence=roster_confidence,
            ))
    written = upsert_cfb_team_game_features(db, feature_rows)
    return {"feature_version": FEATURE_VERSION, "games": len(games), "rows_written": written, "as_of": now.isoformat()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-date", type=date.fromisoformat)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "")
    print(json.dumps(build_upcoming_features(db, args.through_date), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
