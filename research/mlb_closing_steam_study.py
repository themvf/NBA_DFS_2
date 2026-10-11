"""Read-only backtest of the final pregame MLB moneyline move.

Usage: python -m research.mlb_closing_steam_study [--out artifacts/mlb_closing_steam_study.json]

One game can contribute at most one signal. The signal uses only the last two
pregame captures, so it is the final observed move, not the largest move found
after searching the entire game trail. Prices are frozen at the last capture.
"""
from __future__ import annotations

import argparse
import json
import math
import random
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.sportsbook_policy import selected_books


RETAIL = frozenset({"fanduel", "fanatics", "draftkings", "williamhill_us", "betmgm"})
MAX_QUOTE_AGE_MIN = 35
MAX_FINAL_LEAD_MIN = 15
MAX_CAPTURE_GAP_MIN = 40
MIN_SUPPORT_BOOKS = 3


def american_prob(price: object) -> float | None:
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        return None
    if not math.isfinite(price) or abs(price) < 100 or int(price) != price:
        return None
    return 100 / (100 + price) if price > 0 else -price / (100 - price)


def moneyline_quote(book: dict, at: datetime) -> tuple[float, int, int] | None:
    stamp = book.get("h2h_last_update") or book.get("last_update")
    if not stamp:
        return None
    try:
        updated = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        age = (at - updated.astimezone(timezone.utc)).total_seconds() / 60
    except (ValueError, TypeError, AttributeError):
        return None
    if not 0 <= age <= MAX_QUOTE_AGE_MIN:
        return None
    home, away = book.get("ml_home"), book.get("ml_away")
    hp, ap = american_prob(home), american_prob(away)
    if hp is None or ap is None:
        return None
    return hp / (hp + ap), int(home), int(away)


def load_games(db: DatabaseManager) -> list[dict]:
    # The game-date equality excludes old odds after a reschedule. gamePk-based
    # matchup_id keeps doubleheaders separate. The database is never written.
    return db.execute("""
        WITH ranked AS (
            SELECT h.id, h.matchup_id, h.game_date, h.captured_at, h.books,
                   h.home_team_name, h.away_team_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY h.matchup_id ORDER BY h.captured_at DESC, h.id DESC
                   ) AS recency
            FROM game_odds_history h
            JOIN mlb_matchups m ON m.id = h.matchup_id
            WHERE h.sport = 'mlb' AND h.books IS NOT NULL
              AND h.game_date = m.game_date AND h.captured_at < m.commence_time
              AND m.commence_time < NOW() AND m.game_status IN ('Final', 'Game Over')
              AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
              AND m.home_score <> m.away_score
        )
        SELECT m.id AS matchup_id, m.game_date, m.game_id, m.commence_time,
               m.home_score, m.away_score,
               h1.id AS last_id, h1.captured_at AS last_at, h1.books AS last_books,
               h1.home_team_name, h1.away_team_name,
               h2.id AS prior_id, h2.captured_at AS prior_at, h2.books AS prior_books
        FROM mlb_matchups m
        JOIN ranked h1 ON h1.matchup_id = m.id AND h1.recency = 1
        LEFT JOIN ranked h2 ON h2.matchup_id = m.id AND h2.recency = 2
        ORDER BY m.game_date, m.id
    """)


def load_closing_hour_games(db: DatabaseManager) -> list[dict]:
    """Last pregame quote versus the most recent quote 45–90 minutes out."""
    return db.execute("""
        SELECT m.id AS matchup_id, m.game_date, m.game_id, m.commence_time,
               m.home_score, m.away_score,
               h1.id AS last_id, h1.captured_at AS last_at, h1.books AS last_books,
               h1.home_team_name, h1.away_team_name,
               h2.id AS prior_id, h2.captured_at AS prior_at, h2.books AS prior_books
        FROM mlb_matchups m
        JOIN LATERAL (
            SELECT h.id, h.captured_at, h.books, h.home_team_name, h.away_team_name
            FROM game_odds_history h
            WHERE h.sport = 'mlb' AND h.matchup_id = m.id
              AND h.game_date = m.game_date AND h.books IS NOT NULL
              AND h.captured_at < m.commence_time
            ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
        ) h1 ON TRUE
        LEFT JOIN LATERAL (
            SELECT h.id, h.captured_at, h.books FROM game_odds_history h
            WHERE h.sport = 'mlb' AND h.matchup_id = m.id
              AND h.game_date = m.game_date AND h.books IS NOT NULL
              AND h.captured_at BETWEEN m.commence_time - INTERVAL '90 minutes'
                                    AND m.commence_time - INTERVAL '45 minutes'
            ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
        ) h2 ON TRUE
        WHERE m.commence_time < NOW() AND m.game_status IN ('Final', 'Game Over')
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          AND m.home_score <> m.away_score
        ORDER BY m.game_date, m.id
    """)


def analyze(games: list[dict], *, threshold_pp: float,
            max_lead_min: int = MAX_FINAL_LEAD_MIN,
            min_capture_gap_min: int = 0,
            max_capture_gap_min: int = MAX_CAPTURE_GAP_MIN) -> dict:
    coverage = Counter()
    signals = []
    for game in games:
        coverage["final_with_odds"] += 1
        if game["prior_at"] is None:
            continue
        coverage["two_captures"] += 1
        lead = (game["commence_time"] - game["last_at"]).total_seconds() / 60
        gap = (game["last_at"] - game["prior_at"]).total_seconds() / 60
        if not (0 <= lead <= max_lead_min and min_capture_gap_min < gap <= max_capture_gap_min):
            continue
        coverage["timely_pair"] += 1
        prior_books = selected_books(game["prior_books"])
        last_books = selected_books(game["last_books"])
        matched = {}
        for key in RETAIL & prior_books.keys() & last_books.keys():
            old = moneyline_quote(prior_books[key], game["prior_at"])
            new = moneyline_quote(last_books[key], game["last_at"])
            if old and new:
                matched[key] = (old, new)
        if len(matched) < MIN_SUPPORT_BOOKS:
            continue
        coverage["three_fresh_matched_books"] += 1
        up = sorted(k for k, (old, new) in matched.items() if (new[0] - old[0]) * 100 >= threshold_pp - 1e-9)
        down = sorted(k for k, (old, new) in matched.items() if (old[0] - new[0]) * 100 >= threshold_pp - 1e-9)
        side, supporting = ("home", up) if len(up) >= MIN_SUPPORT_BOOKS else ("away", down)
        if len(supporting) < MIN_SUPPORT_BOOKS:
            continue
        coverage["steam"] += 1
        dk = moneyline_quote(last_books.get("draftkings", {}), game["last_at"])
        if dk is None:
            continue
        coverage["steam_with_dk_price"] += 1
        fair = dk[0] if side == "home" else 1 - dk[0]
        price = dk[1] if side == "home" else dk[2]
        won = (game["home_score"] > game["away_score"]) == (side == "home")
        profit = (1 / american_prob(price) - 1) if won else -1.0
        signals.append({
            "date": str(game["game_date"]), "game_id": game["game_id"],
            "team": game["home_team_name"] if side == "home" else game["away_team_name"],
            "opponent": game["away_team_name"] if side == "home" else game["home_team_name"],
            "home_score": game["home_score"], "away_score": game["away_score"],
            "side": side, "won": won, "dk_price": price,
            "fair_win_probability": fair, "profit_units": profit,
            "lead_minutes": round(lead, 2), "capture_gap_minutes": round(gap, 2),
            "support_books": supporting,
            "support_move_pp": round(sum(abs(matched[k][1][0] - matched[k][0][0]) * 100 for k in supporting) / len(supporting), 3),
        })
    n = len(signals)
    wins = sum(s["won"] for s in signals)
    expected = sum(s["fair_win_probability"] for s in signals)
    units = sum(s["profit_units"] for s in signals)
    # Game-level bootstrap; the interval is descriptive, not a guarantee of a
    # repeatable edge. The seed makes the report reproducible.
    rng = random.Random(20260925)
    roi_draws = []
    if n:
        for _ in range(10000):
            draw = [signals[rng.randrange(n)] for _ in range(n)]
            roi_draws.append(sum(s["profit_units"] for s in draw) / n)
        roi_draws.sort()
    return {
        "threshold_pp_per_book": threshold_pp,
        "max_final_lead_minutes": max_lead_min,
        "min_capture_gap_minutes": min_capture_gap_min,
        "max_capture_gap_minutes": max_capture_gap_min,
        "coverage": dict(coverage),
        "n_bets": n, "wins": wins, "losses": n - wins,
        "win_rate": wins / n if n else None,
        "expected_wins_from_dk_no_vig": expected,
        "expected_win_rate_from_dk_no_vig": expected / n if n else None,
        "win_minus_expected_pp": (wins - expected) / n * 100 if n else None,
        "profit_units": units,
        "roi": units / n if n else None,
        "bootstrap_roi_95": [roi_draws[250], roi_draws[9750]] if n else None,
        "home_bets": sum(s["side"] == "home" for s in signals),
        "mean_support_move_pp": sum(s["support_move_pp"] for s in signals) / n if n else None,
        "bets": signals,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    games = load_games(db)
    closing_hour_games = load_closing_hour_games(db)
    report = {
        "study": "final_two_pregame_captures_mlb_moneyline",
        "source": "game_odds_history + mlb_matchups",
        "first_game_date": str(min(g["game_date"] for g in games)),
        "last_game_date": str(max(g["game_date"] for g in games)),
        "definition": {
            "last_capture_within_minutes_of_first_pitch": MAX_FINAL_LEAD_MIN,
            "last_two_captures_at_most_minutes_apart": MAX_CAPTURE_GAP_MIN,
            "matched_retail_books_required": MIN_SUPPORT_BOOKS,
            "maximum_quote_age_minutes": MAX_QUOTE_AGE_MIN,
            "execution_book": "draftkings",
            "stake_units_per_bet": 1,
        },
        "primary": analyze(games, threshold_pp=1.5),
        "sensitivity": [analyze(games, threshold_pp=t) for t in (1.0, 2.0)],
        "wider_close_30m": analyze(games, threshold_pp=1.5, max_lead_min=30),
        "closing_hour": analyze(closing_hour_games, threshold_pp=1.5,
                                min_capture_gap_min=30, max_capture_gap_min=90),
        "closing_hour_sensitivity": [
            analyze(closing_hour_games, threshold_pp=t,
                    min_capture_gap_min=30, max_capture_gap_min=90)
            for t in (1.0, 2.0)
        ],
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for name in ("primary", "sensitivity", "wider_close_30m",
                 "closing_hour", "closing_hour_sensitivity"):
        for result in report[name] if isinstance(report[name], list) else [report[name]]:
            summary = {k: v for k, v in result.items() if k != "bets"}
            print(name, json.dumps(summary, default=str))


if __name__ == "__main__":
    main()
