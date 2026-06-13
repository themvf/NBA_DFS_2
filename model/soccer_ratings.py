"""Soccer team strength ratings from historical international results.

Two complementary, schedule-aware strength signals, both written to
``soccer_team_ratings`` for teams that exist in ``soccer_teams``:

  * **Elo** — World-Football-style Elo over the full match history.  Margin- and
    importance-weighted, with a home-advantage term.  Schedule-adjusted by
    construction (you only gain rating by beating strong opponents).
  * **Attack / defense** — Dixon-Coles style Poisson coefficients (log scale) fit
    over recent history with time decay, via sklearn ``PoissonRegressor`` on a
    sparse team-dummy design matrix (2 rows per match — each side's goals).  These
    feed the bivariate Poisson goal model in ``model/soccer_predictions.py`` (P2).

Global parameters (μ baseline, home_adv) are written to
``data/soccer_model_params.json`` for the prediction step to read.

Usage:
    python -m model.soccer_ratings                 # train + store ratings
    python -m model.soccer_ratings --report        # print top/bottom, no DB write
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import unicodedata
from datetime import date

import numpy as np
import pandas as pd

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from db.queries import (
    build_soccer_team_name_cache,
    upsert_soccer_model_params,
    upsert_soccer_team_rating,
)
from ingest.soccer_results_history import load_history

logger = logging.getLogger(__name__)

PARAMS_PATH = DATA_DIR / "soccer_model_params.json"

# Elo configuration (World Football Elo conventions).
_ELO_INIT = 1500.0
_ELO_HOME_ADV = 100.0          # rating points added to the home side's expectation
_TOURNAMENT_K = {              # importance weight by competition keyword
    "fifa world cup": 60.0,
    "world cup qualification": 40.0,
    "uefa euro": 50.0,
    "copa américa": 50.0,
    "copa america": 50.0,
    "african cup of nations": 50.0,
    "afc asian cup": 50.0,
    "confederations cup": 45.0,
    "nations league": 40.0,
    "friendly": 20.0,
}
_DEFAULT_K = 30.0

# Poisson fit configuration.
_POISSON_SINCE_YEAR = 2006
_POISSON_HALF_LIFE_YEARS = 3.0
_POISSON_MIN_MATCHES = 8        # teams below this fold into an "OTHER" bucket
_OTHER = "__other__"

# Our soccer_teams name → martj42 results.csv name, where they diverge.
# Most nations match after accent/case normalization; only list exceptions.
_NAME_ALIASES: dict[str, str] = {
    "USA": "United States",
    "South Korea": "South Korea",
    "Ivory Coast": "Ivory Coast",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
}


def _normalize(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _tournament_k(tournament: str) -> float:
    t = (tournament or "").lower()
    for keyword, k in _TOURNAMENT_K.items():
        if keyword in t:
            return k
    return _DEFAULT_K


def _goal_diff_multiplier(margin: int) -> float:
    """World Football Elo margin-of-victory multiplier."""
    if margin <= 1:
        return 1.0
    if margin == 2:
        return 1.5
    return (11 + margin) / 8.0


def compute_elo(df: pd.DataFrame) -> tuple[dict[str, float], dict[str, int]]:
    """Iterate matches chronologically and return {norm_name: elo}, {norm_name: matches}."""
    elo: dict[str, float] = {}
    matches: dict[str, int] = {}

    for row in df.itertuples(index=False):
        home = _normalize(row.home_team)
        away = _normalize(row.away_team)
        if not home or not away:
            continue
        rh = elo.get(home, _ELO_INIT)
        ra = elo.get(away, _ELO_INIT)

        ha = 0.0 if getattr(row, "neutral", False) else _ELO_HOME_ADV
        exp_home = 1.0 / (1.0 + 10 ** (-((rh + ha) - ra) / 400.0))

        hs, as_ = int(row.home_score), int(row.away_score)
        result_home = 1.0 if hs > as_ else 0.0 if hs < as_ else 0.5

        k = _tournament_k(getattr(row, "tournament", "")) * _goal_diff_multiplier(abs(hs - as_))
        delta = k * (result_home - exp_home)
        elo[home] = rh + delta
        elo[away] = ra - delta
        matches[home] = matches.get(home, 0) + 1
        matches[away] = matches.get(away, 0) + 1

    return elo, matches


def fit_poisson_strengths(
    df: pd.DataFrame,
    since_year: int = _POISSON_SINCE_YEAR,
    half_life_years: float = _POISSON_HALF_LIFE_YEARS,
    min_matches: int = _POISSON_MIN_MATCHES,
) -> tuple[float, float, dict[str, float], dict[str, float]]:
    """Fit time-decayed Poisson attack/defense coefficients.

    Returns (mu, home_adv, attack{norm_name}, defense{norm_name}).  Rare teams
    collapse into an OTHER bucket and are excluded from the returned dicts.
    Falls back to a schedule-naive ratio estimate if the GLM is unavailable.
    """
    window = df[df["date"].dt.year >= since_year].copy()
    if window.empty:
        return math.log(1.35), 0.2, {}, {}

    # Team frequency → fold rare teams into OTHER.
    counts: dict[str, int] = {}
    for row in window.itertuples(index=False):
        counts[_normalize(row.home_team)] = counts.get(_normalize(row.home_team), 0) + 1
        counts[_normalize(row.away_team)] = counts.get(_normalize(row.away_team), 0) + 1

    def team_key(name: str) -> str:
        k = _normalize(name)
        return k if counts.get(k, 0) >= min_matches else _OTHER

    teams = sorted({team_key(r.home_team) for r in window.itertuples(index=False)} |
                   {team_key(r.away_team) for r in window.itertuples(index=False)})
    atk_idx = {t: i for i, t in enumerate(teams)}
    def_idx = {t: i + len(teams) for i, t in enumerate(teams)}
    n_cols = 2 * len(teams) + 1          # +1 home indicator
    home_col = n_cols - 1

    # Time-decay weights (most recent match → weight 1).
    max_date = window["date"].max()
    decay = math.log(2.0) / max(half_life_years, 0.5)

    try:
        from scipy.sparse import csr_matrix
        from sklearn.linear_model import PoissonRegressor

        rows: list[int] = []
        cols: list[int] = []
        data: list[float] = []
        y: list[int] = []
        w: list[float] = []
        r = 0
        for m in window.itertuples(index=False):
            hk, ak = team_key(m.home_team), team_key(m.away_team)
            age_years = (max_date - m.date).days / 365.25
            weight = math.exp(-decay * age_years)
            # Home side's goals.
            rows += [r, r, r]; cols += [atk_idx[hk], def_idx[ak], home_col]; data += [1.0, 1.0, 1.0]
            y.append(int(m.home_score)); w.append(weight); r += 1
            # Away side's goals.
            rows += [r, r]; cols += [atk_idx[ak], def_idx[hk]]; data += [1.0, 1.0]
            y.append(int(m.away_score)); w.append(weight); r += 1

        X = csr_matrix((data, (rows, cols)), shape=(r, n_cols))
        model = PoissonRegressor(alpha=1e-3, fit_intercept=True, max_iter=400)
        model.fit(X, np.array(y, dtype=float), sample_weight=np.array(w, dtype=float))

        mu = float(model.intercept_)
        home_adv = float(model.coef_[home_col])
        attack = {t: float(model.coef_[atk_idx[t]]) for t in teams if t != _OTHER}
        defense = {t: float(model.coef_[def_idx[t]]) for t in teams if t != _OTHER}
        logger.info("Poisson fit: %d teams, %d obs, mu=%.3f home_adv=%.3f", len(teams), r, mu, home_adv)
        return mu, home_adv, attack, defense

    except Exception as exc:  # pragma: no cover - fallback path
        logger.warning("PoissonRegressor unavailable/failed (%s) — using ratio fallback", exc)
        return _ratio_strengths(window, team_key, decay, max_date)


def _ratio_strengths(window, team_key, decay, max_date):
    """Schedule-naive log attack/defense from weighted goal averages."""
    scored: dict[str, list[float]] = {}
    conceded: dict[str, list[float]] = {}
    wts: dict[str, list[float]] = {}
    tot_goals = 0.0
    tot_w = 0.0
    for m in window.itertuples(index=False):
        weight = math.exp(-decay * (max_date - m.date).days / 365.25)
        for tk, gf, ga in ((team_key(m.home_team), m.home_score, m.away_score),
                           (team_key(m.away_team), m.away_score, m.home_score)):
            scored.setdefault(tk, []).append(gf * weight)
            conceded.setdefault(tk, []).append(ga * weight)
            wts.setdefault(tk, []).append(weight)
            tot_goals += gf * weight
            tot_w += weight
    league_avg = tot_goals / tot_w if tot_w else 1.35
    attack, defense = {}, {}
    for t in scored:
        if t == _OTHER:
            continue
        sw = sum(wts[t]) or 1.0
        atk_ratio = (sum(scored[t]) / sw) / league_avg
        def_ratio = (sum(conceded[t]) / sw) / league_avg
        # Same convention as the GLM: defense is NEGATIVE for good defenses, so
        # the goal model adds it — lambda = exp(mu + attack[scorer] + defense[conceder]).
        attack[t] = math.log(max(atk_ratio, 0.2))
        defense[t] = math.log(max(def_ratio, 0.2))
    return math.log(league_avg), 0.2, attack, defense


def train_and_store(db: DatabaseManager, write: bool = True) -> int:
    """Train Elo + Poisson strengths and upsert ratings for World Cup teams.

    Returns the number of soccer_teams rows updated with ratings.
    """
    df = load_history()
    elo, elo_matches = compute_elo(df)
    mu, home_adv, attack, defense = fit_poisson_strengths(df)

    # Persist global params for the prediction step — to the DB (so CI is
    # self-sufficient) and to a local json cache (for --report / offline use).
    if write:
        today = date.today().isoformat()
        upsert_soccer_model_params(db, mu=mu, home_adv=home_adv, n_matches=int(len(df)), trained_at=today)
        PARAMS_PATH.write_text(json.dumps({
            "mu": mu,
            "home_adv": home_adv,
            "trained_at": today,
            "n_matches": int(len(df)),
        }, indent=2))

    # Map our soccer_teams → history keys and store.
    name_to_id = build_soccer_team_name_cache(db)
    updated = 0
    unmatched: list[str] = []
    for name, team_id in name_to_id.items():
        hist_name = _NAME_ALIASES.get(name, name)
        key = _normalize(hist_name)
        if key not in elo:
            unmatched.append(name)
            continue
        if write:
            upsert_soccer_team_rating(
                db,
                team_id=team_id,
                elo=round(elo[key], 1),
                attack=round(attack.get(key, 0.0), 4),
                defense=round(defense.get(key, 0.0), 4),
                matches=int(elo_matches.get(key, 0)),
                rating_date=date.today().isoformat(),
            )
        updated += 1

    if unmatched:
        logger.warning("No history match for %d teams (add to _NAME_ALIASES): %s",
                       len(unmatched), ", ".join(sorted(unmatched)))
    logger.info("Stored ratings for %d/%d soccer teams", updated, len(name_to_id))
    return updated


def _report(db: DatabaseManager) -> None:
    df = load_history()
    elo, matches = compute_elo(df)
    mu, home_adv, attack, defense = fit_poisson_strengths(df)
    name_to_id = build_soccer_team_name_cache(db)

    ranked = []
    for name in name_to_id:
        key = _normalize(_NAME_ALIASES.get(name, name))
        if key in elo:
            ranked.append((name, elo[key], attack.get(key, 0.0), defense.get(key, 0.0)))
    ranked.sort(key=lambda x: x[1], reverse=True)

    print(f"\nGlobal: mu={mu:.3f} (~{math.exp(mu):.2f} base goals)  home_adv={home_adv:+.3f}")
    print(f"\n{'Team':<18}{'Elo':>8}{'Atk':>8}{'Def':>8}")
    for name, e, a, d in ranked[:12]:
        print(f"{name:<18}{e:>8.0f}{a:>8.2f}{d:>8.2f}")
    print("   ...")
    for name, e, a, d in ranked[-5:]:
        print(f"{name:<18}{e:>8.0f}{a:>8.2f}{d:>8.2f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Train soccer strength ratings")
    parser.add_argument("--report", action="store_true", help="Print ranking without writing to DB")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.report:
        _report(db)
    else:
        n = train_and_store(db)
        print(f"Ratings stored for {n} World Cup teams")
