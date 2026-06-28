"""Per-game bet ratings — moneyline (3-way) and totals (O/U).

For every upcoming fixture, rates:
  * **Moneyline** — Home / Draw / Away.  our_prob from the bivariate-Poisson
    match model (our_prob_home/draw/away on soccer_matchups); reference = the
    vig-free 3-way market probs; odds = home_ml/draw_ml/away_ml.
  * **Totals (O/U)** — Over/Under the consensus goal line.  our_prob from a
    Poisson(our_total_pred) on total goals; reference = vig-free O/U from the
    captured over/under prices.

Each bet carries event_commence = kickoff, so the ledger LOCKS the rating at
kickoff — the pre-game number is captured and never edited afterward.

Usage:
    python -m model.soccer_game_bets                 # all upcoming fixtures
    python -m model.soccer_game_bets --date 2026-06-14

gameline-v3 changes vs v2:
  * Global +6% hydration-break uplift on all WC totals (cooling breaks at
    ~30' and ~75' sustain attacking intensity, pushing goals above historical
    Poisson baseline).
  * Lopsided-match uplift bumped 1.12 → 1.14 (compounds with hydration factor:
    lopsided games now see ~1.06 × 1.14 ≈ 1.21× raw prediction).
"""

from __future__ import annotations

import argparse
import logging
import math

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import new_capture_key, record_bet

logger = logging.getLogger(__name__)

MODEL_VERSION = "gameline-v3"

# Moneyline and totals are efficient markets; the independent Poisson has a fat
# upset tail, so anchor our probability toward the vig-free market and surface
# only the disagreement that survives.  (Same philosophy as the match model.)
_W_MONEYLINE_MODEL = 0.35
_W_TOTAL_MODEL = 0.40

# Draw-specific model weights by xG disparity + Pinnacle availability.
# The bivariate Poisson overestimates draws in lopsided matches (game-management
# not modelled). We reduce our model weight aggressively when disparity is high
# or when Pinnacle data (the sharpest draw pricer) is missing.
_W_DRAW_NO_PINNACLE = 0.05   # no Pinnacle: 95% anchored to consensus → max ~2★
_W_DRAW_HIGH_DISP   = 0.10   # lopsided (|xG gap| > 1.5): 90% Pinnacle
_W_DRAW_MED_DISP    = 0.20   # moderate (|xG gap| 0.8–1.5): 80% Pinnacle
_W_DRAW_NORMAL      = 0.35   # evenly matched: normal weight

# Global hydration-break uplift (v3). The 2026 WC uses mandatory cooling breaks
# at ~30' and ~75' in high heat-stress conditions. These reduce second-half
# fatigue, sustaining attacking intensity into the 70-90' window and pushing
# actual totals above what the Poisson (trained on historical internationals)
# predicts. Applied to ALL fixtures — even close games benefit from the breaks.
_HYDRATION_BREAK_UPLIFT = 1.06  # +6% goals across the board

# Additional uplift for lopsided matches on top of the hydration factor. A heavy
# favourite presses relentlessly against a weak side; combined with the break
# recovery, blowouts run ~14% above raw prediction (bumped from 1.12).
_TOTAL_UPLIFT_DISPARITY = 1.5   # |home_xg - away_xg| threshold
_TOTAL_UPLIFT_FACTOR    = 1.14  # multiply our_total_pred by this (was 1.12)


def _anchor(model_p: float, market_p: float | None, w_model: float) -> float:
    """Blend our model probability toward the vig-free market line."""
    if market_p is None:
        return model_p
    return w_model * model_p + (1 - w_model) * market_p


def _draw_model_weight(pinnacle_draw: float | None, xg_disparity: float) -> float:
    """Return the model weight for draw legs based on data quality and match shape."""
    if pinnacle_draw is None:
        return _W_DRAW_NO_PINNACLE
    if xg_disparity > 1.5:
        return _W_DRAW_HIGH_DISP
    if xg_disparity > 0.8:
        return _W_DRAW_MED_DISP
    return _W_DRAW_NORMAL


def _poisson_cdf(k: int, lam: float) -> float:
    """P(N <= k) for N ~ Poisson(lam)."""
    if k < 0:
        return 0.0
    total = 0.0
    term = math.exp(-lam)
    for i in range(k + 1):
        if i > 0:
            term *= lam / i
        total += term
    return min(1.0, total)


def _over_under_probs(line: float, lam: float) -> tuple[float, float]:
    """(P(over), P(under)) for total goals ~ Poisson(lam).

    over wins if N > line, under if N < line (a push at an integer line is the
    residual gap and settles as void, not counted here).
    """
    p_under = _poisson_cdf(math.ceil(line) - 1, lam)        # N <= ceil(line)-1
    p_over = 1.0 - _poisson_cdf(math.floor(line), lam)      # N >= floor(line)+1
    return p_over, p_under


def predict_and_record(db: DatabaseManager, game_date: str | None = None) -> int:
    """Rate moneyline + totals bets for upcoming fixtures.  Returns rows written."""
    where = "sm.game_date = %s" if game_date else "sm.game_date >= CURRENT_DATE"
    params: tuple = (game_date,) if game_date else ()
    fixtures = db.execute(
        f"""
        SELECT sm.id, sm.game_id, sm.commence_time,
               sm.home_team_id, sm.away_team_id,
               h.name AS home, a.name AS away,
               sm.home_ml, sm.draw_ml, sm.away_ml,
               sm.vegas_prob_home, sm.vegas_prob_draw, sm.vegas_prob_away,
               sm.our_prob_home, sm.our_prob_draw, sm.our_prob_away,
               sm.our_home_xg, sm.our_away_xg,
               sm.pinnacle_prob_home, sm.pinnacle_prob_draw, sm.pinnacle_prob_away,
               sm.vegas_total, sm.over_odds, sm.under_odds, sm.our_total_pred,
               sm.dk_dnb_home_ml, sm.dk_dnb_away_ml,
               sm.dnb_home_prob, sm.dnb_away_prob
        FROM soccer_matchups sm
        JOIN soccer_teams h ON h.team_id = sm.home_team_id
        JOIN soccer_teams a ON a.team_id = sm.away_team_id
        WHERE {where} AND sm.game_id IS NOT NULL
        ORDER BY sm.commence_time ASC
        """,
        params,
    )
    if not fixtures:
        print("Game bets: no upcoming fixtures to rate")
        return 0

    from model.soccer_bet_rating import american_to_prob
    capture_key = new_capture_key()
    written = 0
    # One connection for the whole batch — far fewer round-trips to Neon from CI.
    with db.connect() as conn:
        for fx in fixtures:
            fixture_label = f"{fx['home']} v {fx['away']}"
            commence = fx["commence_time"]

            # xG disparity drives both draw weight and total uplift.
            home_xg = float(fx["our_home_xg"] or 0)
            away_xg = float(fx["our_away_xg"] or 0)
            xg_disp = abs(home_xg - away_xg)

            pin_draw = fx["pinnacle_prob_draw"]
            pin_draw_f = float(pin_draw) if pin_draw is not None else None

            # ── Moneyline (3-way) ──
            for side, label, team_id, our_p, mkt_p, ml in [
                ("home", fx["home"], fx["home_team_id"], fx["our_prob_home"], fx["vegas_prob_home"], fx["home_ml"]),
                ("draw", "Draw", None, fx["our_prob_draw"], fx["vegas_prob_draw"], fx["draw_ml"]),
                ("away", fx["away"], fx["away_team_id"], fx["our_prob_away"], fx["vegas_prob_away"], fx["away_ml"]),
            ]:
                if our_p is None or ml is None:
                    continue

                if side == "draw":
                    # Use Pinnacle as the market reference when available —
                    # sharpest draw pricer, corrects for Poisson overestimation
                    # in lopsided matches. Model weight scales down with disparity
                    # and drops to near-zero when Pinnacle data is missing.
                    ref_p = pin_draw_f if pin_draw_f is not None else (float(mkt_p) if mkt_p is not None else None)
                    w = _draw_model_weight(pin_draw_f, xg_disp)
                    anchored = _anchor(float(our_p), ref_p, w)
                    extra_inputs = {"xg_disp": round(xg_disp, 2), "draw_w": w,
                                    "pinnacle_draw": round(pin_draw_f, 4) if pin_draw_f is not None else None}
                else:
                    ref_p = float(mkt_p) if mkt_p is not None else None
                    anchored = _anchor(float(our_p), ref_p, _W_MONEYLINE_MODEL)
                    extra_inputs = {}

                record_bet(
                    db,
                    model_version=MODEL_VERSION,
                    bet_type="moneyline",
                    scope=str(fx["game_id"]),
                    selection_label=label,
                    our_prob=anchored,
                    capture_key=capture_key,
                    market_odds=int(ml),
                    market_prob=ref_p,
                    matchup_id=fx["id"],
                    subject_team_id=team_id,
                    event_commence=commence,
                    longshot_odds_cap=True,
                    conn=conn,
                    inputs={"side": side, "fixture": fixture_label,
                            "model_prob": round(float(our_p), 4),
                            "anchored_prob": round(anchored, 4),
                            "market_prob_vigfree": round(ref_p, 4) if ref_p is not None else None,
                            **extra_inputs},
                )
                written += 1

            # ── Totals (O/U) ──
            # Two uplifts applied in sequence:
            #   1. Global hydration-break uplift (+6%) — all WC fixtures.
            #   2. Lopsided-match uplift (+14%) — when xG disparity > 1.5.
            line = fx["vegas_total"]
            lam_raw = fx["our_total_pred"]
            over_odds, under_odds = fx["over_odds"], fx["under_odds"]
            if line is not None and lam_raw is not None and over_odds is not None and under_odds is not None:
                lam = float(lam_raw) * _HYDRATION_BREAK_UPLIFT
                uplift_applied = xg_disp > _TOTAL_UPLIFT_DISPARITY
                if uplift_applied:
                    lam *= _TOTAL_UPLIFT_FACTOR
                p_over, p_under = _over_under_probs(float(line), lam)
                raw_o, raw_u = american_to_prob(int(over_odds)), american_to_prob(int(under_odds))
                vf_over = raw_o / (raw_o + raw_u) if (raw_o + raw_u) > 0 else None
                vf_under = raw_u / (raw_o + raw_u) if (raw_o + raw_u) > 0 else None
                for label, our_p, mkt_p, odds in [
                    (f"Over {line}", p_over, vf_over, over_odds),
                    (f"Under {line}", p_under, vf_under, under_odds),
                ]:
                    anchored = _anchor(float(our_p), mkt_p, _W_TOTAL_MODEL)
                    record_bet(
                        db,
                        model_version=MODEL_VERSION,
                        bet_type="total",
                        scope=str(fx["game_id"]),
                        selection_label=label,
                        our_prob=anchored,
                        capture_key=capture_key,
                        market_odds=int(odds),
                        market_prob=float(mkt_p) if mkt_p is not None else None,
                        matchup_id=fx["id"],
                        event_commence=commence,
                        longshot_odds_cap=True,
                        conn=conn,
                        inputs={"line": float(line), "lambda": round(lam, 4),
                                "lambda_raw": round(float(lam_raw), 4),
                                "hydration_uplift": _HYDRATION_BREAK_UPLIFT,
                                "xg_disp": round(xg_disp, 2),
                                "uplift_applied": uplift_applied,
                                "model_p": round(float(our_p), 4),
                                "anchored_p": round(anchored, 4),
                                "fixture": fixture_label},
                    )
                    written += 1

            # ── Draw No Bet (knockout rounds) ──
            # A 2-way market: void on 90-min draw, won/lost otherwise.
            # Our prob = our conditional win prob given the match is decided in 90 min.
            # market_odds = DK's actual posted price (the book the user bets at).
            # market_prob  = Pinnacle/consensus vig-free DNB reference (edge signal).
            dk_h = fx["dk_dnb_home_ml"]
            dk_a = fx["dk_dnb_away_ml"]
            ref_h_dnb = fx["dnb_home_prob"]
            ref_a_dnb = fx["dnb_away_prob"]
            if dk_h is not None and dk_a is not None and fx["our_prob_home"] is not None and fx["our_prob_away"] is not None:
                op_h = float(fx["our_prob_home"])
                op_a = float(fx["our_prob_away"])
                denom = op_h + op_a
                if denom > 0:
                    our_h_dnb = op_h / denom
                    our_a_dnb = op_a / denom
                    ref_h = float(ref_h_dnb) if ref_h_dnb is not None else None
                    ref_a = float(ref_a_dnb) if ref_a_dnb is not None else None
                    for label, team_id, our_p, ref_p, ml in [
                        (fx["home"], fx["home_team_id"], our_h_dnb, ref_h, dk_h),
                        (fx["away"], fx["away_team_id"], our_a_dnb, ref_a, dk_a),
                    ]:
                        anchored = _anchor(our_p, ref_p, _W_MONEYLINE_MODEL)
                        record_bet(
                            db,
                            model_version=MODEL_VERSION,
                            bet_type="draw_no_bet",
                            scope=str(fx["game_id"]),
                            selection_label=label,
                            our_prob=anchored,
                            capture_key=capture_key,
                            market_odds=int(ml),
                            market_prob=ref_p,
                            matchup_id=fx["id"],
                            subject_team_id=team_id,
                            event_commence=commence,
                            longshot_odds_cap=True,
                            conn=conn,
                            inputs={"side": "home" if label == fx["home"] else "away",
                                    "fixture": fixture_label,
                                    "model_prob_conditional": round(our_p, 4),
                                    "anchored_prob": round(anchored, 4),
                                    "dnb_ref_prob": round(ref_p, 4) if ref_p is not None else None,
                                    "dk_odds": int(ml)},
                        )
                        written += 1

    print(f"Game bets: {written} moneyline/total/dnb bets rated across {len(fixtures)} fixtures")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Per-game moneyline + totals bet model")
    parser.add_argument("--date", help="Kickoff date YYYY-MM-DD (default: all upcoming)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_record(db, args.date)
