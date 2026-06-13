"""First-goal-scorer model (firstscorer-v2) — favorite-longshot-aware.

v1 deflated favorites: it built player shares from the RAW anytime market, but
books vig longshots far harder than favorites, so summing raw goal rates over
~30 players inflated the denominator and stole probability from the stars.

v2 fixes this with the **power method** (the standard favorite-longshot de-vig):

  1. Anytime → de-vig by solving for exponent k_a so the de-vigged goal rates
     sum to OUR match total xG:  Σ_p −ln(1 − p_anytimeᵏ) = our_total_pred.
     This removes vig AND anchors player strength to our match model at once.
     share_p = λ_p / Λ ,   our_model_first_p = share_p · (1 − e^(−Λ)).
  2. First-scorer market → de-vig by solving for k_f so the mutually-exclusive
     probabilities (players + "no goalscorer") sum to 1:  Σ pᵏ = 1.
  3. Blend:  our_prob = w · our_model_first + (1−w) · market_fair.
     The edge over the market comes from OUR total xG disagreeing with the line.

Reference for edge = the power-de-vigged first-scorer market.  EV uses the best
offered price (line shopping across us/uk/eu).  First-scorer remains a high-vig
market, so most stay 1★ — but the probabilities are now well-calibrated, which is
what the backtest checks.

Usage:
    python -m model.soccer_first_scorer
    python -m model.soccer_first_scorer --hours 96
"""

from __future__ import annotations

import argparse
import logging
import math

from config import load_config
from db.database import DatabaseManager
from ingest.soccer_props import fetch_player_markets
from model.soccer_bet_rating import new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "firstscorer-v2"
DEFAULT_WINDOW_HOURS = 72
_MIN_ANYTIME_PROB = 0.01
_CLAMP_HI = 0.999
# Blend weight on our anytime/match-model estimate vs the de-vigged market.
_W_MODEL = 0.5


def _clamp(p: float, lo: float = 1e-6, hi: float = _CLAMP_HI) -> float:
    return min(max(p, lo), hi)


def power_devig_exclusive(raw_probs: list[float]) -> list[float]:
    """De-vig a mutually-exclusive market: find k with Σ pᵏ = 1, return [pᵏ].

    Σ pᵏ is monotone decreasing in k (p < 1), so bisection converges.
    """
    probs = [_clamp(p) for p in raw_probs if p > 0]
    if not probs:
        return []
    if sum(probs) <= 1.0:
        return probs  # already ≤ 1 (no vig to remove)

    lo, hi = 1.0, 12.0
    for _ in range(60):
        k = (lo + hi) / 2
        s = sum(p ** k for p in probs)
        if s > 1.0:
            lo = k
        else:
            hi = k
    k = (lo + hi) / 2
    return [p ** k for p in probs]


def solve_anytime_k(raw_probs: list[float], target_lambda: float) -> float:
    """Find exponent k so Σ −ln(1 − p_iᵏ) = target_lambda (anchors total to our model)."""
    probs = [_clamp(p) for p in raw_probs if p > 0]
    if not probs or target_lambda <= 0:
        return 1.0

    def total_lambda(k: float) -> float:
        return sum(-math.log(1.0 - _clamp(p ** k)) for p in probs)

    # total_lambda is monotone decreasing in k.
    lo, hi = 0.3, 8.0
    if total_lambda(hi) > target_lambda:
        return hi
    if total_lambda(lo) < target_lambda:
        return lo
    for _ in range(60):
        k = (lo + hi) / 2
        if total_lambda(k) > target_lambda:
            lo = k
        else:
            hi = k
    return (lo + hi) / 2


def predict_and_record(db: DatabaseManager, api_key: str, window_hours: int = DEFAULT_WINDOW_HOURS) -> int:
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch first-scorer markets")
        return 0

    # Clear UNLOCKED pending first-scorer rows (any version) before re-rating, so
    # players that drop out (filtered glitch lines, market changes) don't leave
    # orphans, and superseded versions are retired.  Locked closing lines and
    # settled rows are preserved for the backtest.
    db.execute(
        "DELETE FROM soccer_bets WHERE bet_type = 'first_scorer' "
        "AND status = 'pending' AND locked = FALSE",
    )

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
            continue

        Lambda = float(fx["our_total_pred"])
        p_at_least_one = 1.0 - math.exp(-Lambda)

        # ── Our model estimate: de-vig anytime, anchored to our match total ──
        anytime_players = [(npl, info["prob_raw"]) for npl, info in markets["anytime"].items()
                           if info["prob_raw"] >= _MIN_ANYTIME_PROB]
        if not anytime_players:
            continue
        k_a = solve_anytime_k([p for _, p in anytime_players], Lambda)
        lam = {npl: -math.log(1.0 - _clamp(p ** k_a)) for npl, p in anytime_players}
        lam_total = sum(lam.values()) or 1.0
        our_model_first = {npl: (lp / lam_total) * p_at_least_one for npl, lp in lam.items()}

        # ── Market estimate: power de-vig the first-scorer market ──
        fs_items = [(npl, fs) for npl, fs in markets["first"].items() if fs.get("prob_raw")]
        raw_list = [fs["prob_raw"] for _, fs in fs_items]
        raw_list.append(markets.get("no_scorer_raw", 0.0) or 0.0)   # include no-goalscorer leg
        devigged = power_devig_exclusive(raw_list)
        market_fair = {fs_items[i][0]: devigged[i] for i in range(len(fs_items))} if devigged else {}

        with db.connect() as conn:
            for npl, fs in fs_items:
                if fs["best_odds"] is None:
                    continue
                m_model = our_model_first.get(npl)
                m_market = market_fair.get(npl)
                if m_model is None and m_market is None:
                    continue
                # Blend whichever estimates we have.
                if m_model is not None and m_market is not None:
                    our_prob = _W_MODEL * m_model + (1 - _W_MODEL) * m_market
                else:
                    our_prob = m_model if m_model is not None else m_market
                ref = m_market if m_market is not None else our_prob
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
                    market_prob=ref,
                    book=fs["best_book"],
                    matchup_id=fx["id"],
                    event_commence=fx["commence_time"],
                    longshot_odds_cap=True,
                    conn=conn,
                    inputs={
                        "model_prob": round(m_model, 4) if m_model is not None else None,
                        "market_fair": round(m_market, 4) if m_market is not None else None,
                        "blended": round(our_prob, 4),
                        "match_total_xg": round(Lambda, 4),
                        "anytime_k": round(k_a, 3),
                        "book_count": fs["book_count"],
                        "fixture": f"{fx['home']} v {fx['away']}",
                    },
                )
                written += 1

    print(f"First scorer (v2): {written} bets rated across {len(fixtures)} fixtures")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="First-scorer bet model (v2, power de-vig)")
    parser.add_argument("--hours", type=int, default=DEFAULT_WINDOW_HOURS, help="Look-ahead window")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_record(db, config.odds_api.api_key, args.hours)
