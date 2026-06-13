"""First-goal-scorer model → star-rated bets in the ledger.

Model (firstscorer-v1), Poisson superposition:
    share_p = lambda_p / sum_players(lambda_p)         (from the anytime market;
              lambda_p = -ln(1 - P_anytime_raw);        per-leg vig ~cancels in
              the share)
    Lambda  = our_total_pred                            (our match model's total xG)
    P(player scores first) = share_p * (1 - e^(-Lambda))

Compared against the vig-removed first-scorer market line → edge, EV, stars.
First-scorer markets carry ~30-40% overround, so real edges exist.

Only near-term fixtures are processed (books post these ~1-2 days out; also keeps
Odds API credit use low).

Usage:
    python -m model.soccer_first_scorer                 # all near-term fixtures
    python -m model.soccer_first_scorer --hours 96
"""

from __future__ import annotations

import argparse
import logging
import math

from config import load_config
from db.database import DatabaseManager
from ingest.soccer_props import fetch_player_markets, norm_player
from model.soccer_bet_rating import new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "firstscorer-v1"
DEFAULT_WINDOW_HOURS = 72
# Ignore ultra-longshot players the books list at 100/1+ — pure noise for a model.
_MIN_ANYTIME_PROB = 0.01


def predict_and_record(db: DatabaseManager, api_key: str, window_hours: int = DEFAULT_WINDOW_HOURS) -> int:
    """Rate first-scorer bets for fixtures kicking off within window_hours.

    Returns the number of bet rows written/updated.
    """
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch first-scorer markets")
        return 0

    fixtures = db.execute(
        """
        SELECT sm.id, sm.game_id, sm.commence_time, sm.our_total_pred,
               h.name AS home, a.name AS away
        FROM soccer_matchups sm
        JOIN soccer_teams h ON h.team_id = sm.home_team_id
        JOIN soccer_teams a ON a.team_id = sm.away_team_id
        WHERE sm.game_id IS NOT NULL
          AND sm.commence_time IS NOT NULL
          AND sm.commence_time >= NOW()
          AND sm.commence_time <= NOW() + (%s || ' hours')::interval
          AND sm.our_total_pred IS NOT NULL
        ORDER BY sm.commence_time ASC
        """,
        (str(window_hours),),
    )
    if not fixtures:
        print("First scorer: no near-term fixtures with predictions to process")
        return 0

    capture_key = new_capture_key()
    written = 0
    for fx in fixtures:
        markets = fetch_player_markets(api_key, fx["game_id"])
        if not markets or not markets["first"] or not markets["anytime"]:
            logger.debug("No first-scorer/anytime market for %s v %s", fx["home"], fx["away"])
            continue

        # lambda_p per player from the anytime market; total for the share denominator.
        lam: dict[str, float] = {}
        for npl, info in markets["anytime"].items():
            p = min(max(info["prob_raw"], 0.0), 0.95)
            if p < _MIN_ANYTIME_PROB:
                continue
            lam[npl] = -math.log(1.0 - p)
        lam_total = sum(lam.values())
        if lam_total <= 0:
            continue

        Lambda = float(fx["our_total_pred"])
        p_at_least_one = 1.0 - math.exp(-Lambda)

        # One connection per fixture for its writes (the API call already happened
        # above, so we never hold a DB connection idle across slow HTTP).
        with db.connect() as conn:
            for npl, fs in markets["first"].items():
                if fs["best_odds"] is None:
                    continue
                lam_p = lam.get(npl)
                if lam_p is None:
                    # First-scorer listed but no anytime line — skip (no strength estimate).
                    continue
                share = lam_p / lam_total
                our_prob = share * p_at_least_one
                if our_prob <= 0:
                    continue

                record_bet(
                    db,
                    model_version=MODEL_VERSION,
                    bet_type="first_scorer",
                    scope=str(fx["game_id"]),
                    selection_label=fs["name"],
                    our_prob=our_prob,
                    capture_key=capture_key,
                    market_odds=fs["best_odds"],
                    market_prob=fs["prob_vigfree"],
                    book=fs["best_book"],
                    matchup_id=fx["id"],
                    event_commence=fx["commence_time"],
                    longshot_odds_cap=True,
                    conn=conn,
                    inputs={
                        "lambda_p": round(lam_p, 4),
                        "lambda_total": round(lam_total, 4),
                        "match_total_xg": round(Lambda, 4),
                        "share": round(share, 4),
                        "p_at_least_one_goal": round(p_at_least_one, 4),
                        "market_prob_vigfree": round(fs["prob_vigfree"], 4),
                        "book_count": fs["book_count"],
                        "fixture": f"{fx['home']} v {fx['away']}",
                    },
                )
                written += 1

    print(f"First scorer: {written} bets rated across {len(fixtures)} fixtures")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="First-scorer bet model")
    parser.add_argument("--hours", type=int, default=DEFAULT_WINDOW_HOURS, help="Look-ahead window")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_record(db, config.odds_api.api_key, args.hours)
