"""Step 2 of cfb-market-weight-v1: fit the weight on v1's forecast versus the line.

Every rule here is fixed in docs/cfb-market-weight-study.md (registered
2026-10-17 before any residual was computed).  v1 (cfb-score-context-v1) is
refitted season by season so every 2023-2025 forecast is out of sample, then
compared with CFBD's canonical historical line:

    forecast = line + w * (model - line)

Only seasons 2022-2025 are loaded; no 2026 game is read.

    python -m research.cfb_market_weight [--output PATH]
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
from scipy.optimize import minimize, minimize_scalar

from config import load_config
from db.database import DatabaseManager
from research.cfb_forecast_v1 import (
    VERSION as V1_VERSION, american_probability, load_rows, predict, replay_games, train,
)

VERSION = "cfb-market-weight-v1"
FIRST_SEASON, LAST_SEASON = 2022, 2025
OOS_SEASONS = (2023, 2024, 2025)
FOLDS = (("A", (2023,), 2024), ("B", (2023, 2024), 2025))
FIXED_ANCHOR = 0.25
BOOTSTRAP_DRAWS = 2000
SEED = 20261017
SIGN_CHECK_MIN_CORR = 0.5
EPS = 1e-4
EASTERN = ZoneInfo("America/New_York")
DEFAULT_OUTPUT = Path("artifacts/cfb-market-weight-v1/fit.json")


# ---------- pure fitting and grading ----------

def weight_through_origin(x, y) -> float | None:
    x, y = np.asarray(x, float), np.asarray(y, float)
    denom = float(np.dot(x, x))
    return float(np.dot(x, y) / denom) if denom > 0 else None


def weight_with_intercept(x, y) -> dict | None:
    x, y = np.asarray(x, float), np.asarray(y, float)
    if len(x) < 3 or float(np.var(x)) == 0:
        return None
    slope, intercept = np.polyfit(x, y, 1)
    return {"slope": float(slope), "intercept": float(intercept)}


def _logit(p):
    p = np.clip(np.asarray(p, float), EPS, 1 - EPS)
    return np.log(p / (1 - p))


def theta_offset(q, p_model, won) -> float | None:
    """Max-likelihood theta in logit(p) = logit(q) + theta*(logit(p_model) - logit(q))."""
    offset, z, y = _logit(q), _logit(p_model) - _logit(q), np.asarray(won, float)
    if len(y) < 3 or not np.any(z):
        return None
    def nll(theta):
        eta = offset + theta * z
        return float(np.sum(np.logaddexp(0, eta) - y * eta))
    return float(minimize_scalar(nll, bounds=(-5, 5), method="bounded").x)


def theta_with_intercept(q, p_model, won) -> dict | None:
    offset, z, y = _logit(q), _logit(p_model) - _logit(q), np.asarray(won, float)
    if len(y) < 3:
        return None
    def nll(params):
        eta = offset + params[0] + params[1] * z
        return float(np.sum(np.logaddexp(0, eta) - y * eta))
    fit = minimize(nll, [0.0, 0.0], method="L-BFGS-B", bounds=[(-5, 5), (-5, 5)])
    return {"intercept": float(fit.x[0]), "theta": float(fit.x[1])}


def anchored_probability(q, p_model, theta):
    eta = _logit(q) + theta * (_logit(p_model) - _logit(q))
    return 1 / (1 + np.exp(-eta))


def cluster_bootstrap(dates, statistic, draws=BOOTSTRAP_DRAWS, seed=SEED):
    """Percentile 95% interval of statistic(row_indices), resampling whole game dates."""
    keys, inverse = np.unique(np.asarray(dates), return_inverse=True)
    if len(keys) == 0:
        return None
    groups = [np.flatnonzero(inverse == k) for k in range(len(keys))]
    rng = np.random.default_rng(seed)
    values = []
    for _ in range(draws):
        idx = np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))])
        value = statistic(idx)
        if value is not None and math.isfinite(value):
            values.append(value)
    if not values:
        return None
    return [round(float(np.quantile(values, 0.025)), 4), round(float(np.quantile(values, 0.975)), 4)]


def _market_arrays(rows, market):
    usable = [r for r in rows if r.get(f"{market}_line") is not None]
    line = np.array([r[f"{market}_line"] for r in usable], float)
    model = np.array([r[f"{market}_model"] for r in usable], float)
    actual = np.array([r[f"{market}_actual"] for r in usable], float)
    return line, model, actual, np.array([r["date"] for r in usable])


def _moneyline_arrays(rows):
    usable = [r for r in rows if r.get("q_home") is not None]
    return (np.array([r["q_home"] for r in usable], float), np.array([r["p_model"] for r in usable], float),
            np.array([r["home_won"] for r in usable], float), np.array([r["date"] for r in usable]))


def fit_market(rows: list[dict], market: str, ci: bool = True) -> dict:
    line, model, actual, dates = _market_arrays(rows, market)
    x, y = model - line, actual - line
    w = weight_through_origin(x, y)
    return {"n": len(x), "dates": len(set(dates.tolist())),
            "w": None if w is None else round(w, 4),
            "ci95": cluster_bootstrap(dates, lambda i: weight_through_origin(x[i], y[i])) if ci else None,
            "sensitivity_with_intercept": weight_with_intercept(x, y)}


def fit_moneyline(rows: list[dict], ci: bool = True) -> dict:
    q, pm, won, dates = _moneyline_arrays(rows)
    theta = theta_offset(q, pm, won)
    return {"n": len(q), "dates": len(set(dates.tolist())),
            "theta": None if theta is None else round(theta, 4),
            "ci95": cluster_bootstrap(dates, lambda i: theta_offset(q[i], pm[i], won[i])) if ci else None,
            "sensitivity_with_intercept": theta_with_intercept(q, pm, won)}


def _paired(diff, dates):
    """Mean of a per-game difference, with a game-date clustered interval."""
    return {"mean": round(float(np.mean(diff)), 4),
            "ci95": cluster_bootstrap(dates, lambda i: float(np.mean(diff[i])))}


def grade_market(rows: list[dict], market: str, w: float) -> dict:
    line, model, actual, dates = _market_arrays(rows, market)
    if len(line) == 0:
        return {"n": 0}
    def err(weight):
        return line + weight * (model - line) - actual
    def mae(weight):
        return round(float(np.mean(np.abs(err(weight)))), 4)
    def rmse(weight):
        return round(float(np.sqrt(np.mean(err(weight) ** 2))), 4)
    weights = {"line": 0.0, "anchored": w, "fixed_0.25": FIXED_ANCHOR, "v1_alone": 1.0}
    return {
        "n": len(line), "dates": len(set(dates.tolist())), "w": round(w, 4),
        "mae": {k: mae(v) for k, v in weights.items()},
        "rmse": {k: rmse(v) for k, v in weights.items()},
        "anchored_minus_line_mae": _paired(np.abs(err(w)) - np.abs(err(0)), dates),
        "fixed_minus_line_mae": _paired(np.abs(err(FIXED_ANCHOR)) - np.abs(err(0)), dates),
    }


def grade_moneyline(rows: list[dict], theta: float) -> dict:
    q, pm, won, dates = _moneyline_arrays(rows)
    if len(q) == 0:
        return {"n": 0}
    def prob(t):
        return anchored_probability(q, pm, t)
    def brier(t):
        return (prob(t) - won) ** 2
    def logloss(t):
        p = np.clip(prob(t), EPS, 1 - EPS)
        return -(won * np.log(p) + (1 - won) * np.log(1 - p))
    return {
        "n": len(q), "theta": round(theta, 4),
        "brier": {"market": round(float(np.mean(brier(0))), 5),
                  "anchored": round(float(np.mean(brier(theta))), 5),
                  "v1_alone": round(float(np.mean((pm - won) ** 2)), 5)},
        "log_loss": {"market": round(float(np.mean(logloss(0))), 5),
                     "anchored": round(float(np.mean(logloss(theta))), 5)},
        "anchored_minus_market_brier": _paired(brier(theta) - brier(0), dates),
    }


def decision(pooled: dict, folds: dict) -> str:
    """The registered decision rule for the margin market."""
    fit = pooled["margin"]
    if fit["ci95"] is None or fit["w"] is None:
        return "NO_ESTIMATE"
    lo, hi = fit["ci95"]
    if lo <= 0 <= hi:
        return "NO_RESIDUAL_SKILL"
    if fit["w"] < 0:
        return "ANTI_INFORMATIVE"
    fold_below = all((f["margin"].get("anchored_minus_line_mae", {}).get("ci95") or [0, 0])[1] < 0
                     for f in folds.values())
    return "RESIDUAL_SKILL" if fold_below else "INCONCLUSIVE"


# ---------- data ----------

def load_lines(db) -> dict:
    out: dict = defaultdict(dict)
    for r in db.execute("""
        SELECT DISTINCT ON (h.game_id, h.market_type)
               h.game_id, h.market_type, h.provider, h.home_value, h.away_value
        FROM cfb_historical_game_lines h JOIN cfb_matchups m ON m.id = h.game_id
        WHERE m.season BETWEEN %s AND %s AND h.is_canonical_reference
          AND h.line_designation = 'historical_reference'
        ORDER BY h.game_id, h.market_type, h.id DESC""", (FIRST_SEASON, LAST_SEASON)):
        out[int(r["game_id"])][r["market_type"]] = dict(r)
    return out


def build_rows(db) -> tuple[list[dict], dict]:
    games, plays, drives, _ = load_rows(db, LAST_SEASON)
    if any(int(g["season"]) > LAST_SEASON for g in games):
        raise SystemExit("a game after 2025 was loaded; 2026 must stay untouched")
    replay = replay_games(games, plays, drives)
    lines = load_lines(db)
    neutral = {int(r["id"]): bool(r["neutral_site"]) for r in db.execute(
        "SELECT id, neutral_site FROM cfb_matchups WHERE season BETWEEN %s AND %s",
        (OOS_SEASONS[0], LAST_SEASON))}
    rows, counts = [], {}
    for season in OOS_SEASONS:
        model, residual_sd, trained = train(replay, FIRST_SEASON, season - 1)
        completed = [g for g in games if int(g["season"]) == season and g["completed"]
                     and g["home_score"] is not None and g["away_score"] is not None]
        forecasted = [r for r in replay if r["season"] == season and r["completed"]
                      and r["home_score"] is not None and r["away_score"] is not None]
        c = Counter(completed_games=len(completed), v1_forecasts=len(forecasted), trained_games=trained)
        providers = Counter()
        for r in forecasted:
            f = predict(r, model, residual_sd)
            game_lines = lines.get(r["id"], {})
            spread, total, ml = game_lines.get("spread"), game_lines.get("total"), game_lines.get("moneyline")
            q_home = None
            if ml is not None:
                hp, ap = american_probability(ml["home_value"]), american_probability(ml["away_value"])
                if hp is not None and ap is not None:
                    q_home = hp / (hp + ap)
            c["with_spread"] += spread is not None
            c["with_total"] += total is not None
            c["with_moneyline_pair"] += q_home is not None
            c["neutral_site"] += neutral.get(r["id"], False)
            if spread is not None:
                providers[spread["provider"]] += 1
            margin = float(r["home_score"] - r["away_score"])
            rows.append({
                "game_id": r["id"], "season": season,
                "date": r["kickoff"].astimezone(EASTERN).date().isoformat(),
                "margin_model": f["home_margin"], "total_model": f["total"], "p_model": f["home_win_probability"],
                "margin_line": -float(spread["home_value"]) if spread is not None else None,
                "total_line": float(total["home_value"]) if total is not None else None,
                "q_home": q_home,
                "margin_actual": margin, "total_actual": float(r["home_score"] + r["away_score"]),
                "home_won": 1.0 if margin > 0 else 0.0,
            })
        counts[season] = {**dict(c), "not_forecast_by_v1": len(completed) - len(forecasted),
                          "spread_providers": dict(providers), "residual_sd": round(residual_sd, 4)}
    return rows, counts


def sign_check(rows) -> float:
    pairs = [(r["margin_line"], r["margin_actual"]) for r in rows if r["margin_line"] is not None]
    corr = float(np.corrcoef(*zip(*pairs))[0, 1])
    if corr <= SIGN_CHECK_MIN_CORR:
        raise SystemExit(f"spread sign check failed: corr(margin line, actual margin) = {corr:.3f}")
    return corr


def run(rows: list[dict]) -> dict:
    pooled = {"margin": fit_market(rows, "margin"), "total": fit_market(rows, "total"),
              "moneyline": fit_moneyline(rows)}
    folds = {}
    for name, fit_seasons, graded in FOLDS:
        fit_rows = [r for r in rows if r["season"] in fit_seasons]
        test_rows = [r for r in rows if r["season"] == graded]
        w_m = fit_market(fit_rows, "margin", ci=False)["w"]
        w_t = fit_market(fit_rows, "total", ci=False)["w"]
        theta = fit_moneyline(fit_rows, ci=False)["theta"]
        folds[name] = {"fit_seasons": list(fit_seasons), "graded_season": graded,
                       "margin": grade_market(test_rows, "margin", w_m),
                       "total": grade_market(test_rows, "total", w_t),
                       "moneyline": grade_moneyline(test_rows, theta)}
    return {"pooled": pooled, "folds": folds, "decision_margin": decision(pooled, folds),
            "decision_total": decision({"margin": pooled["total"]},
                                       {k: {"margin": v["total"]} for k, v in folds.items()})}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    rows, counts = build_rows(db)
    corr = sign_check(rows)
    report = {"version": VERSION, "football_model": V1_VERSION,
              "run_at": datetime.now(timezone.utc).isoformat(),
              "seasons_loaded": [FIRST_SEASON, LAST_SEASON], "spread_sign_corr": round(corr, 4),
              "counts": counts, **run(rows)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
