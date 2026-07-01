"""Soccer game predictions — bivariate Poisson goal model (our number vs market).

Reads team strength ratings (soccer_team_ratings) + global params trained by
model/soccer_ratings.py, computes expected goals per side, and derives the full
score distribution → our O/U total, implied goals, and 3-way win/draw/away
probabilities.  Writes the our_* columns on soccer_matchups for comparison
against the Vegas lines (the soccer analog of game_predictions.predict_and_write).

Market anchoring: the raw model number is blended toward the Vegas line so the
value we surface comes from *disagreement* with a sharp market, not from fighting
it blindly — same philosophy as the LineStar delta for NBA DFS.

Goal-model convention (must match model/soccer_ratings.py):
    lambda = exp(mu + attack[scorer] + defense[conceder] + home_adv*is_home)
defense coefficients are NEGATIVE for good defenses, so they are ADDED.

Usage:
    python -m model.soccer_predictions             # predict all upcoming fixtures
    python -m model.soccer_predictions --date 2026-06-14
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import date, datetime

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from db.queries import get_soccer_model_params

logger = logging.getLogger(__name__)

PARAMS_PATH = DATA_DIR / "soccer_model_params.json"

# Defaults if the params file is missing (e.g. ratings not trained yet).
_DEFAULT_MU = math.log(1.35)
_DEFAULT_HOME_ADV = 0.25

# All WC 2026 games are played on neutral sites — no team has a true home
# advantage regardless of host-nation status. The "home" designation in the
# fixture is purely administrative (bracket assignment), not venue-based.
_NEUTRAL_HOME_DAMPEN = 0.0     # neutral site: no home advantage
_HOST_HOME_DAMPEN = 0.0        # host nations also at neutral venues
_HOST_TEAMS = {"USA", "Mexico", "Canada"}

# Market anchor: weight on our independent model vs the Vegas line.  0.40 = trust
# the market 60%, surface the 40% of our disagreement that survives shrinkage.
_W_MODEL_TOTAL = 0.40
_W_MODEL_SUPREMACY = 0.45

# WC 2026 mismatch calibration: regression on 42 completed games shows
# error = 0.37 * sup - 0.07 (blowout potential grows with Elo gap; even games
# tend toward 0-0 caginess). Apply 50% shrinkage for small-sample caution.
# Applies after the market anchor so it lifts our number above a biased Vegas line.
_WC_SUP_BONUS_SLOPE = 0.18    # shrunk from 0.37 fit
_WC_SUP_BONUS_INTERCEPT = -0.04  # even games get a tiny downward nudge

# Score-matrix truncation — P(>=8 goals for one side) is negligible.
_MAX_GOALS = 10

# Group-stage draw correction: the Poisson model systematically underestimates
# draws in evenly-matched group games (conservative "take a point" tactics).
# Applied ONLY when xG disparity is low — in lopsided matches our model already
# overestimates draws relative to sharp markets, so boosting makes it worse.
# Max disparity threshold: if |home_xg - away_xg| >= this, skip the boost.
_GROUP_STAGE_END = date(2026, 6, 30)
_GROUP_STAGE_DRAW_BOOST = 0.07          # restored: WC 2026 showing 32% draws over 42 games
_GROUP_STAGE_BOOST_MAX_DISPARITY = 0.8  # only boost when game is evenly matched


def _load_params(db: DatabaseManager) -> tuple[float, float]:
    """Load trained mu/home_adv: DB first (CI-safe), then local json, then defaults."""
    row = get_soccer_model_params(db)
    if row and row.get("mu") is not None and row.get("home_adv") is not None:
        return float(row["mu"]), float(row["home_adv"])
    try:
        data = json.loads(PARAMS_PATH.read_text())
        return float(data["mu"]), float(data["home_adv"])
    except Exception:
        logger.warning("Trained model params not found (DB or %s) — using defaults", PARAMS_PATH)
        return _DEFAULT_MU, _DEFAULT_HOME_ADV


def _poisson_pmf(k: int, lam: float) -> float:
    return math.exp(-lam) * lam ** k / math.factorial(k)


def score_matrix(lam_home: float, lam_away: float) -> list[list[float]]:
    """Independent-Poisson score matrix P(i home goals, j away goals)."""
    home_pmf = [_poisson_pmf(i, lam_home) for i in range(_MAX_GOALS + 1)]
    away_pmf = [_poisson_pmf(j, lam_away) for j in range(_MAX_GOALS + 1)]
    return [[home_pmf[i] * away_pmf[j] for j in range(_MAX_GOALS + 1)] for i in range(_MAX_GOALS + 1)]


def outcome_probs(lam_home: float, lam_away: float) -> tuple[float, float, float]:
    """Return (P(home win), P(draw), P(away win)) from the score matrix."""
    m = score_matrix(lam_home, lam_away)
    p_home = p_draw = p_away = 0.0
    for i in range(_MAX_GOALS + 1):
        for j in range(_MAX_GOALS + 1):
            p = m[i][j]
            if i > j:
                p_home += p
            elif i == j:
                p_draw += p
            else:
                p_away += p
    total = p_home + p_draw + p_away
    if total <= 0:
        return 0.0, 0.0, 0.0
    return p_home / total, p_draw / total, p_away / total


def predict_and_write(db: DatabaseManager, game_date: str | None = None) -> int:
    """Compute and store our_* predictions for upcoming fixtures.

    Only writes fixtures where both teams have ratings and a Vegas total exists
    (the market anchor needs it).  With game_date, restricts to that matchday;
    otherwise all upcoming fixtures (today onward).  Returns rows updated.
    """
    mu, home_adv_raw = _load_params(db)

    ratings = {
        int(r["team_id"]): (float(r["attack"] or 0.0), float(r["defense"] or 0.0))
        for r in db.execute("SELECT team_id, attack, defense FROM soccer_team_ratings")
    }

    # Host nations get full home advantage; other designated-home sides are damped.
    host_ids = {
        int(r["team_id"])
        for r in db.execute("SELECT team_id FROM soccer_teams WHERE name = ANY(%s)",
                            (list(_HOST_TEAMS),))
    }

    # Matchday-3 motivation / dead-rubber state (the model is otherwise blind to
    # game state — see model/soccer_motivation.py). Adjustment is applied to the
    # raw model number BEFORE market anchoring.
    from model.soccer_motivation import compute_motivation
    motivation = compute_motivation(db, game_date)

    where = "sm.game_date = %s" if game_date else "sm.game_date >= CURRENT_DATE"
    params: tuple = (game_date,) if game_date else ()
    matchups = db.execute(
        f"""
        SELECT sm.id, sm.home_team_id, sm.away_team_id,
               sm.vegas_total, sm.home_implied, sm.away_implied,
               sm.game_date
        FROM soccer_matchups sm
        WHERE {where}
          -- Only fixtures that haven't kicked off: our_* columns are the
          -- pre-match prediction of record (the ledger locks at kickoff), and
          -- re-predicting a live game re-anchors to in-play market data.
          AND (sm.commence_time IS NULL OR sm.commence_time > NOW())
        """,
        params,
    )

    updated = 0
    skipped = 0
    for m in matchups:
        home_id, away_id = m.get("home_team_id"), m.get("away_team_id")
        if home_id not in ratings or away_id not in ratings:
            skipped += 1
            continue

        atk_home, def_home = ratings[home_id]
        atk_away, def_away = ratings[away_id]

        # Host nation at home → full home advantage; otherwise damped (neutral-ish).
        dampen = _HOST_HOME_DAMPEN if home_id in host_ids else _NEUTRAL_HOME_DAMPEN
        home_adv = home_adv_raw * dampen
        lam_home = math.exp(mu + atk_home + def_away + home_adv)
        lam_away = math.exp(mu + atk_away + def_home)
        model_total = lam_home + lam_away
        model_supremacy = lam_home - lam_away

        # MD3 motivation: dampen total for eased-off sides, tilt supremacy toward
        # the more motivated team (positive sup_shift = toward home).
        motiv = motivation.get(m["id"])
        motivation_label = None
        if motiv is not None:
            model_total *= motiv["total_factor"]
            model_supremacy += motiv["sup_shift"]
            motivation_label = motiv["label"]

        # Market anchor (only when a line exists).
        vegas_total = m.get("vegas_total")
        home_impl = m.get("home_implied")
        away_impl = m.get("away_implied")
        if vegas_total is not None:
            our_total = _W_MODEL_TOTAL * model_total + (1 - _W_MODEL_TOTAL) * float(vegas_total)
        else:
            our_total = model_total
        if home_impl is not None and away_impl is not None:
            mkt_sup = float(home_impl) - float(away_impl)
            our_sup = _W_MODEL_SUPREMACY * model_supremacy + (1 - _W_MODEL_SUPREMACY) * mkt_sup
        else:
            our_sup = model_supremacy

        # WC 2026 mismatch calibration: add goal uplift that scales with Elo gap.
        sup_mag = abs(our_sup)
        our_total += _WC_SUP_BONUS_SLOPE * sup_mag + _WC_SUP_BONUS_INTERCEPT

        # Reconstruct anchored per-side expected goals, then re-derive outcome
        # probabilities from THOSE lambdas so totals and 3-way stay consistent.
        our_home_xg = max(0.05, (our_total + our_sup) / 2.0)
        our_away_xg = max(0.05, (our_total - our_sup) / 2.0)
        p_home, p_draw, p_away = outcome_probs(our_home_xg, our_away_xg)

        # Group-stage draw boost: inflate draw probability for evenly-matched games
        # only. In lopsided matches our Poisson already overestimates draws vs
        # sharp markets, so skip the boost when disparity is high.
        raw_date = m.get("game_date")
        xg_disparity = abs(our_home_xg - our_away_xg)
        if raw_date is not None:
            gd = raw_date if isinstance(raw_date, date) else datetime.fromisoformat(str(raw_date)).date()
            if (gd <= _GROUP_STAGE_END
                    and _GROUP_STAGE_DRAW_BOOST > 0
                    and xg_disparity < _GROUP_STAGE_BOOST_MAX_DISPARITY):
                boost = min(_GROUP_STAGE_DRAW_BOOST, 1.0 - p_draw - 0.01)
                p_draw_new = p_draw + boost
                scale = (1.0 - p_draw_new) / max(p_home + p_away, 1e-9)
                p_home = p_home * scale
                p_away = p_away * scale
                p_draw = p_draw_new

        db.execute(
            """
            UPDATE soccer_matchups
            SET our_total_pred = %s,
                our_home_xg    = %s,
                our_away_xg    = %s,
                our_prob_home  = %s,
                our_prob_draw  = %s,
                our_prob_away  = %s,
                motivation     = %s
            WHERE id = %s
            """,
            (
                round(our_total, 2),
                round(our_home_xg, 2),
                round(our_away_xg, 2),
                round(p_home, 4),
                round(p_draw, 4),
                round(p_away, 4),
                motivation_label,
                m["id"],
            ),
        )
        updated += 1

    msg = f"Soccer predictions: {updated} fixtures written"
    if skipped:
        msg += f" ({skipped} skipped — missing ratings)"
    print(msg)
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Soccer game predictions (bivariate Poisson)")
    parser.add_argument("--date", help="Kickoff date YYYY-MM-DD (default: all upcoming)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    predict_and_write(db, args.date)
