"""Read-only CFB/NFL closing moneyline movement backtest.

Uses the same quote validation, matched retail books, thresholds, and
DraftKings hypothetical execution as research.mlb_closing_steam_study.

Usage: python -m research.football_closing_steam_study --out artifacts/football_closing_steam_study_2026-09-25.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from research.mlb_closing_steam_study import analyze


IDENTITY = {"cfb": "cfbd_game_id", "nfl": "event_id"}


def load_games(db: DatabaseManager, sport: str, *, closing_hour: bool,
               season_type: str | None = None) -> list[dict]:
    if sport not in IDENTITY:
        raise ValueError(f"unsupported sport: {sport}")
    matchup_table = f"{sport}_matchups"
    identity = IDENTITY[sport]
    season_sql = "AND m.season_type = %s" if season_type else ""
    params = (sport, season_type) if season_type else (sport,)
    common = f"""
        m.commence_time < NOW() AND m.completed = TRUE
        AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        AND m.home_score <> m.away_score {season_sql}
    """
    if closing_hour:
        return db.execute(f"""
            SELECT m.id AS matchup_id, m.game_date, m.{identity} AS game_id,
                   m.commence_time, m.home_score, m.away_score,
                   h1.id AS last_id, h1.captured_at AS last_at,
                   h1.books AS last_books, h1.home_team_name, h1.away_team_name,
                   h2.id AS prior_id, h2.captured_at AS prior_at,
                   h2.books AS prior_books
            FROM {matchup_table} m
            JOIN LATERAL (
                SELECT h.id, h.captured_at, h.books, h.home_team_name,
                       h.away_team_name
                FROM game_odds_history h
                WHERE h.sport = %s AND h.matchup_id = m.id
                  AND h.game_date = m.game_date AND h.books IS NOT NULL
                  AND h.captured_at < m.commence_time
                ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
            ) h1 ON TRUE
            LEFT JOIN LATERAL (
                SELECT h.id, h.captured_at, h.books
                FROM game_odds_history h
                WHERE h.sport = %s AND h.matchup_id = m.id
                  AND h.game_date = m.game_date AND h.books IS NOT NULL
                  AND h.captured_at BETWEEN m.commence_time - INTERVAL '90 minutes'
                                        AND m.commence_time - INTERVAL '45 minutes'
                ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
            ) h2 ON TRUE
            WHERE {common}
            ORDER BY m.game_date, m.id
        """, (sport, sport, *(params[1:])))
    return db.execute(f"""
        WITH ranked AS (
            SELECT h.id, h.matchup_id, h.game_date, h.captured_at, h.books,
                   h.home_team_name, h.away_team_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY h.matchup_id ORDER BY h.captured_at DESC, h.id DESC
                   ) AS recency
            FROM game_odds_history h
            JOIN {matchup_table} m ON m.id = h.matchup_id
            WHERE h.sport = %s AND h.books IS NOT NULL
              AND h.game_date = m.game_date AND h.captured_at < m.commence_time
              AND {common}
        )
        SELECT m.id AS matchup_id, m.game_date, m.{identity} AS game_id,
               m.commence_time, m.home_score, m.away_score,
               h1.id AS last_id, h1.captured_at AS last_at,
               h1.books AS last_books, h1.home_team_name, h1.away_team_name,
               h2.id AS prior_id, h2.captured_at AS prior_at,
               h2.books AS prior_books
        FROM {matchup_table} m
        JOIN ranked h1 ON h1.matchup_id = m.id AND h1.recency = 1
        LEFT JOIN ranked h2 ON h2.matchup_id = m.id AND h2.recency = 2
        ORDER BY m.game_date, m.id
    """, params)


def study(db: DatabaseManager, sport: str, season_type: str | None = None) -> dict:
    games = load_games(db, sport, closing_hour=False, season_type=season_type)
    hour_games = load_games(db, sport, closing_hour=True, season_type=season_type)
    return {
        "sport": sport,
        "season_type": season_type or "all",
        "first_game_date": str(min(g["game_date"] for g in games)) if games else None,
        "last_game_date": str(max(g["game_date"] for g in games)) if games else None,
        "strict_final_capture": analyze(games, threshold_pp=1.5),
        "strict_sensitivity_1pp": analyze(games, threshold_pp=1.0),
        "closing_hour": analyze(hour_games, threshold_pp=1.5,
                                min_capture_gap_min=30, max_capture_gap_min=90),
        "closing_hour_sensitivity_1pp": analyze(hour_games, threshold_pp=1.0,
                                                min_capture_gap_min=30, max_capture_gap_min=90),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report = {
        "study": "cfb_nfl_closing_moneyline_movement",
        "source": "game_odds_history + cfb_matchups/nfl_matchups",
        "method": "same as research.mlb_closing_steam_study",
        "cfb": study(db, "cfb"),
        "nfl": study(db, "nfl"),
        "nfl_preseason": study(db, "nfl", "preseason"),
        "nfl_regular": study(db, "nfl", "regular"),
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for group in ("cfb", "nfl", "nfl_preseason", "nfl_regular"):
        for name in ("strict_final_capture", "strict_sensitivity_1pp",
                     "closing_hour", "closing_hour_sensitivity_1pp"):
            result = {k: v for k, v in report[group][name].items() if k != "bets"}
            print(group, name, json.dumps(result))


if __name__ == "__main__":
    main()
