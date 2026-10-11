"""Read-only NFL closing moneyline movement study using Pinnacle alone.

Usage: python -m research.nfl_pinnacle_closing_study --out artifacts/nfl_pinnacle_closing_study_2026-09-26.json
"""
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from ingest.sportsbook_policy import selected_books
from research.football_closing_steam_study import load_games
from research.mlb_closing_steam_study import RETAIL, american_prob, moneyline_quote


def analyze(games: list[dict], *, threshold_pp: float, closing_hour: bool) -> dict:
    coverage = Counter()
    selections = []
    for game in games:
        coverage["final_with_odds"] += 1
        if game["prior_at"] is None:
            continue
        coverage["two_captures"] += 1
        lead = (game["commence_time"] - game["last_at"]).total_seconds() / 60
        gap = (game["last_at"] - game["prior_at"]).total_seconds() / 60
        gap_ok = 30 < gap <= 90 if closing_hour else 0 < gap <= 40
        if not 0 <= lead <= 15 or not gap_ok:
            continue
        coverage["timely_pair"] += 1
        old_books = selected_books(game["prior_books"])
        new_books = selected_books(game["last_books"])
        old = moneyline_quote(old_books.get("pinnacle", {}), game["prior_at"])
        new = moneyline_quote(new_books.get("pinnacle", {}), game["last_at"])
        if old is None or new is None:
            continue
        coverage["fresh_pinnacle_pair"] += 1
        home_move_pp = (new[0] - old[0]) * 100
        if abs(home_move_pp) < threshold_pp - 1e-9:
            continue
        coverage["pinnacle_move"] += 1
        side = "home" if home_move_pp > 0 else "away"
        pin_price = new[1] if side == "home" else new[2]
        pin_fair = new[0] if side == "home" else 1 - new[0]
        dk = moneyline_quote(new_books.get("draftkings", {}), game["last_at"])
        dk_price = (dk[1] if side == "home" else dk[2]) if dk else None
        if dk_price is not None:
            coverage["pinnacle_move_with_dk_price"] += 1
        retail_support = []
        for key in RETAIL & old_books.keys() & new_books.keys():
            prior = moneyline_quote(old_books[key], game["prior_at"])
            last = moneyline_quote(new_books[key], game["last_at"])
            if prior and last and (last[0] - prior[0]) * (1 if side == "home" else -1) * 100 >= threshold_pp - 1e-9:
                retail_support.append(key)
        won = (game["home_score"] > game["away_score"]) == (side == "home")
        selections.append({
            "date": str(game["game_date"]), "game_id": game["game_id"],
            "team": game["home_team_name"] if side == "home" else game["away_team_name"],
            "opponent": game["away_team_name"] if side == "home" else game["home_team_name"],
            "side": side, "home_score": game["home_score"], "away_score": game["away_score"],
            "won": won, "pinnacle_move_pp": round(abs(home_move_pp), 3),
            "pinnacle_price": pin_price, "pinnacle_fair_probability": pin_fair,
            "pinnacle_profit_units": (1 / american_prob(pin_price) - 1) if won else -1.0,
            "draftkings_price": dk_price,
            "draftkings_profit_units": ((1 / american_prob(dk_price) - 1) if won else -1.0) if dk_price is not None else None,
            "retail_support_books": sorted(retail_support),
            "retail_three_book_confirmation": len(retail_support) >= 3,
            "lead_minutes": round(lead, 2), "capture_gap_minutes": round(gap, 2),
        })
    n = len(selections)
    wins = sum(s["won"] for s in selections)
    pin_units = sum(s["pinnacle_profit_units"] for s in selections)
    dk_bets = [s for s in selections if s["draftkings_profit_units"] is not None]
    dk_units = sum(s["draftkings_profit_units"] for s in dk_bets)
    rng = random.Random(20260926)
    roi_draws = []
    if n:
        for _ in range(10000):
            roi_draws.append(sum(selections[rng.randrange(n)]["pinnacle_profit_units"] for _ in range(n)) / n)
        roi_draws.sort()
    return {
        "threshold_pp": threshold_pp,
        "window": "closing_hour" if closing_hour else "strict_final_capture",
        "coverage": dict(coverage),
        "n": n, "wins": wins, "losses": n - wins,
        "win_rate": wins / n if n else None,
        "expected_wins_pinnacle_no_vig": sum(s["pinnacle_fair_probability"] for s in selections),
        "pinnacle_profit_units": pin_units,
        "pinnacle_roi": pin_units / n if n else None,
        "pinnacle_bootstrap_roi_95": [roi_draws[250], roi_draws[9750]] if n else None,
        "dk_priced_n": len(dk_bets), "dk_profit_units": dk_units,
        "dk_roi": dk_units / len(dk_bets) if dk_bets else None,
        "retail_confirmed_n": sum(s["retail_three_book_confirmation"] for s in selections),
        "bets": selections,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    report = {
        "study": "nfl_pinnacle_only_closing_moneyline_movement",
        "definition": "Fresh Pinnacle two-sided, no-vig home win probability moves >= threshold; same timing as football_closing_steam_study; one selection per game",
    }
    for season in ("preseason", "regular"):
        final_games = load_games(db, "nfl", closing_hour=False, season_type=season)
        hour_games = load_games(db, "nfl", closing_hour=True, season_type=season)
        report[season] = {
            "first_game_date": str(min(g["game_date"] for g in final_games)) if final_games else None,
            "last_game_date": str(max(g["game_date"] for g in final_games)) if final_games else None,
            "strict": analyze(final_games, threshold_pp=1.5, closing_hour=False),
            "closing_hour": analyze(hour_games, threshold_pp=1.5, closing_hour=True),
            "closing_hour_1pp": analyze(hour_games, threshold_pp=1.0, closing_hour=True),
        }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for season in ("preseason", "regular"):
        for name in ("strict", "closing_hour", "closing_hour_1pp"):
            print(season, name, json.dumps({k: v for k, v in report[season][name].items() if k != "bets"}))


if __name__ == "__main__":
    main()
