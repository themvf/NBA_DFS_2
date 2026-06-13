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
from datetime import date

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from db.queries import get_soccer_model_params

logger = logging.getLogger(__name__)

PARAMS_PATH = DATA_DIR / "soccer_model_params.json"

# Defaults if the params file is missing (e.g. ratings not trained yet).
_DEFAULT_MU = math.log(1.35)
_DEFAULT_HOME_ADV = 0.25

# World Cup is mostly neutral venues (only the 3 hosts truly play at home), but
# the odds feed still designates a home side.  Dampen the historical home-advantage
# term rather than applying it in full.
_NEUTRAL_HOME_DAMPEN = 0.5

# Market anchor: weight on our independent model vs the Vegas line.  0.40 = trust
# the market 60%, surface the 40% of our disagreement that survives shrinkage.
_W_MODEL_TOTAL = 0.40
_W_MODEL_SUPREMACY = 0.45

# Score-matrix truncation — P(>=8 goals for one side) is negligible.
_MAX_GOALS = 10


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
    home_adv = home_adv_raw * _NEUTRAL_HOME_DAMPEN

    ratings = {
        int(r["team_id"]): (float(r["attack"] or 0.0), float(r["defense"] or 0.0))
        for r in db.execute("SELECT team_id, attack, defense FROM soccer_team_ratings")
    }

    where = "sm.game_date = %s" if game_date else "sm.game_date >= CURRENT_DATE"
    params: tuple = (game_date,) if game_date else ()
    matchups = db.execute(
        f"""
        SELECT sm.id, sm.home_team_id, sm.away_team_id,
               sm.vegas_total, sm.home_implied, sm.away_implied
        FROM soccer_matchups sm
        WHERE {where}
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

        lam_home = math.exp(mu + atk_home + def_away + home_adv)
        lam_away = math.exp(mu + atk_away + def_home)
        model_total = lam_home + lam_away
        model_supremacy = lam_home - lam_away

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

        # Reconstruct anchored per-side expected goals, then re-derive outcome
        # probabilities from THOSE lambdas so totals and 3-way stay consistent.
        our_home_xg = max(0.05, (our_total + our_sup) / 2.0)
        our_away_xg = max(0.05, (our_total - our_sup) / 2.0)
        p_home, p_draw, p_away = outcome_probs(our_home_xg, our_away_xg)

        db.execute(
            """
            UPDATE soccer_matchups
            SET our_total_pred = %s,
                our_home_xg    = %s,
                our_away_xg    = %s,
                our_prob_home  = %s,
                our_prob_draw  = %s,
                our_prob_away  = %s
            WHERE id = %s
            """,
            (
                round(our_total, 2),
                round(our_home_xg, 2),
                round(our_away_xg, 2),
                round(p_home, 4),
                round(p_draw, 4),
                round(p_away, 4),
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
