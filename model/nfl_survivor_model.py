"""Win probabilities for the NFL survivor grid.

THE ONE IDEA THIS MODULE EXISTS TO PROTECT
------------------------------------------
A survivor grid needs a number in all 272 cells, but the market has only
priced about 112 of them. The remaining cells have to be modeled, and the
whole risk of a tool like this is that the modeled cells become
indistinguishable from the quoted ones. So every probability written here
carries a `provenance` value, and modeled probabilities are additionally
widened by the model's own measured forecast error at that horizon.

Nothing here tries to beat the market. The ladder is:

  1. no-vig moneyline for this exact game      -> MARKET
  2. quoted spread for this exact game         -> MARKET
  3. spread implied by market-implied ratings  -> MODEL
  4. nothing usable                            -> BLOCKED (never a silent 50%)

Ratings in step 3 are a ridge fit to the spreads the market HAS posted, so
they are a compression of the market's own opinion propagated forward -- not
an independent view of who is good.

Usage:
    python -m model.nfl_survivor_model
    python -m model.nfl_survivor_model --season 2026 --recalibrate
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict

import numpy as np
import pandas as pd

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_season_schedule import TEAM_ABBREV_OVERRIDES, fetch_schedule

logger = logging.getLogger(__name__)

MODEL_VERSION = "nfl-survivor-v1"

# Ridge penalty on the rating vector (never on the home-field term). Small
# enough not to flatten real separation, large enough to keep an early-season
# fit with few games per team from exploding.
RIDGE_LAMBDA = 1.0

# Seasons used to fit the spread->probability curve and the horizon error
# table. 1999 is where nflverse's closing-spread coverage becomes dependable.
CALIBRATION_SEASONS = (1999, 2025)

# Ratings are anchored at the last week whose lines are essentially fully
# posted; horizon is measured forward from there.
FULL_COVERAGE_THRESHOLD = 0.75

# 401-point standard-normal quadrature for the horizon widening integral.
_Z = np.linspace(-5.0, 5.0, 401)
_W = np.exp(-0.5 * _Z**2)
_W = _W / _W.sum()


# ---------------------------------------------------------------------------
# Historical corpus
# ---------------------------------------------------------------------------

def historical_games(schedule: pd.DataFrame) -> pd.DataFrame:
    low, high = CALIBRATION_SEASONS
    frame = schedule[
        (schedule["game_type"] == "REG")
        & (schedule["season"] >= low)
        & (schedule["season"] <= high)
        & schedule["spread_line"].notna()
        & schedule["result"].notna()
    ].copy()
    frame["home_win"] = (frame["result"] > 0).astype(int)
    frame["tie"] = (frame["result"] == 0).astype(int)
    return frame


def fit_spread_prob(history: pd.DataFrame) -> dict:
    """Logistic fit of home win on closing spread (home perspective).

    Refit on every run rather than hardcoded so drift in the relationship is
    visible in the stored record instead of silently baked into a constant.
    """
    from sklearn.linear_model import LogisticRegression

    features = history[["spread_line"]].to_numpy()
    target = history["home_win"].to_numpy()
    fit = LogisticRegression().fit(features, target)
    intercept = float(fit.intercept_[0])
    slope = float(fit.coef_[0][0])

    # Ties are rare (about 0.2% of games) and concentrated near pick'em. This
    # only matters under a `tie_survives` pool rule, but a silent zero would be
    # wrong in exactly the games a survivor player agonizes over.
    close = history[history["spread_line"].abs() <= 3.0]
    tie_rate_close = float(close["tie"].mean()) if len(close) else 0.0

    return {
        "intercept": intercept,
        "slope": slope,
        "n": int(len(history)),
        "seasons": f"{CALIBRATION_SEASONS[0]}-{CALIBRATION_SEASONS[1]}",
        "tie_rate_close": tie_rate_close,
    }


def spread_to_prob(spread: float, fit: dict) -> float:
    """P(home wins) given the home-perspective spread (positive = home favored)."""
    return float(1.0 / (1.0 + np.exp(-(fit["intercept"] + fit["slope"] * spread))))


def widen(spread: float, sigma: float, fit: dict) -> float:
    """E[p(spread + eps)] for eps ~ N(0, sigma^2).

    A modeled spread is a point estimate with known error; integrating over
    that error pulls the probability toward 0.5. The effect is small -- about
    two points at a ten-week horizon -- but it is free and it is correct.
    """
    if sigma <= 0:
        return spread_to_prob(spread, fit)
    values = 1.0 / (1.0 + np.exp(-(fit["intercept"] + fit["slope"] * (spread + sigma * _Z))))
    return float((_W * values).sum())


# ---------------------------------------------------------------------------
# Market-implied power ratings
# ---------------------------------------------------------------------------

def fit_ratings(
    games: pd.DataFrame,
    teams: list[str],
    ridge_lambda: float = RIDGE_LAMBDA,
) -> tuple[dict[str, float], float, float]:
    """Ridge-fit `spread = r[home] - r[away] + hfa` on quoted spreads.

    Returns (ratings, hfa, rmse). The home-field term is deliberately left
    unpenalized: it is a real constant of the sport, not a per-team parameter
    that needs shrinking toward zero.
    """
    index = {team: i for i, team in enumerate(teams)}
    n = len(teams)
    design = np.zeros((len(games), n + 1))
    target = games["spread_line"].to_numpy(dtype=float)

    for row, (_, game) in enumerate(games.iterrows()):
        design[row, index[game["home_team"]]] = 1.0
        design[row, index[game["away_team"]]] = -1.0
        design[row, n] = 1.0

    normal = design.T @ design + ridge_lambda * np.eye(n + 1)
    normal[n, n] -= ridge_lambda
    solution = np.linalg.solve(normal, design.T @ target)

    residual = design @ solution - target
    rmse = float(np.sqrt((residual**2).mean())) if len(residual) else 0.0
    return {team: float(solution[index[team]]) for team in teams}, float(solution[n]), rmse


def horizon_calibration(schedule: pd.DataFrame) -> list[dict]:
    """Measure how wrong the rating model is h weeks out, and how unstable.

    Fit through week k, predict week k+h, for k = 4..14 across recent seasons.
    Two numbers per horizon, and they say different things:

      rmse            - how far the modeled spread lands from the eventual
                        closing spread. Feeds the widening in `widen`.
      top_pick_*      - whether the model's best future play is the eventual
                        best play. This is the number that governs how much
                        the UI is allowed to imply about far-out columns, and
                        it degrades much faster than the RMSE does.
    """
    frame = schedule[
        (schedule["game_type"] == "REG")
        & (schedule["season"] >= 2010)
        & (schedule["season"] <= 2025)
        & schedule["spread_line"].notna()
    ].copy()

    errors: dict[int, list[float]] = defaultdict(list)
    exact: dict[int, list[int]] = defaultdict(list)
    top5: dict[int, list[int]] = defaultdict(list)

    for _, season_games in frame.groupby("season"):
        teams = sorted(set(season_games["home_team"]) | set(season_games["away_team"]))
        for cutoff in range(4, 15):
            train = season_games[season_games["week"] <= cutoff]
            if len(train) < 60:
                continue
            ratings, hfa, _ = fit_ratings(train, teams)
            future = season_games[season_games["week"] > cutoff]
            for week, week_games in future.groupby("week"):
                horizon = int(week - cutoff)
                sides: list[tuple[str, float, float]] = []
                for _, game in week_games.iterrows():
                    predicted = ratings[game["home_team"]] - ratings[game["away_team"]] + hfa
                    actual = float(game["spread_line"])
                    errors[horizon].append(predicted - actual)
                    sides.append((game["home_team"], predicted, actual))
                    sides.append((game["away_team"], -predicted, -actual))
                if not sides:
                    continue
                sides.sort(key=lambda item: -item[1])
                best_actual = max(sides, key=lambda item: item[2])[0]
                actual_rank = {
                    team: rank
                    for rank, (team, _, _) in enumerate(sorted(sides, key=lambda i: -i[2]))
                }
                exact[horizon].append(int(sides[0][0] == best_actual))
                top5[horizon].append(int(actual_rank[sides[0][0]] < 5))

    rows: list[dict] = []
    for horizon in sorted(errors):
        sample = np.array(errors[horizon])
        if len(sample) < 200:
            continue
        rows.append(
            {
                "horizon": horizon,
                "n": int(len(sample)),
                "rmse": float(np.sqrt((sample**2).mean())),
                "top_pick_exact_rate": float(np.mean(exact[horizon])) if exact[horizon] else None,
                "top_pick_top5_rate": float(np.mean(top5[horizon])) if top5[horizon] else None,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_calibration(db: DatabaseManager, rows: list[dict]) -> None:
    seasons = "2010-2025"
    for row in rows:
        db.execute(
            """
            INSERT INTO nfl_spread_horizon_calibration
                (season_range, horizon, n, rmse, top_pick_exact_rate,
                 top_pick_top5_rate, model_version, fit_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (season_range, horizon, model_version) DO UPDATE SET
                n = EXCLUDED.n, rmse = EXCLUDED.rmse,
                top_pick_exact_rate = EXCLUDED.top_pick_exact_rate,
                top_pick_top5_rate = EXCLUDED.top_pick_top5_rate,
                fit_at = NOW()
            """,
            (
                seasons,
                row["horizon"],
                row["n"],
                row["rmse"],
                row["top_pick_exact_rate"],
                row["top_pick_top5_rate"],
                MODEL_VERSION,
            ),
        )


def load_sigma_table(db: DatabaseManager) -> dict[int, float]:
    rows = db.execute(
        "SELECT horizon, rmse FROM nfl_spread_horizon_calibration WHERE model_version = %s",
        (MODEL_VERSION,),
    )
    return {row["horizon"]: float(row["rmse"]) for row in rows}


def sigma_for(horizon: int, table: dict[int, float]) -> float:
    """Nearest measured horizon, extrapolating flat past the largest one."""
    if not table:
        return 0.0
    if horizon in table:
        return table[horizon]
    largest = max(table)
    if horizon > largest:
        return table[largest]
    return table[min(table, key=lambda h: abs(h - horizon))]


# ---------------------------------------------------------------------------
# The grid
# ---------------------------------------------------------------------------

def _american_pair_to_prob(home_ml: int | None, away_ml: int | None) -> float | None:
    """No-vig home probability from a two-sided American price pair."""
    if home_ml is None or away_ml is None:
        return None
    if -100 < home_ml < 100 or -100 < away_ml < 100:
        return None  # impossible price; the arithmetic-averaging bug's signature

    def raw(odds: int) -> float:
        return -odds / (-odds + 100) if odds < 0 else 100 / (odds + 100)

    home_raw, away_raw = raw(home_ml), raw(away_ml)
    total = home_raw + away_raw
    return None if total <= 0 else home_raw / total


def compute_and_store(db: DatabaseManager, season: int, fit: dict) -> dict:
    games = db.execute(
        """
        SELECT g.id, g.week, g.home_team_id, g.away_team_id,
               g.quoted_spread_line, g.quoted_home_ml, g.quoted_away_ml,
               g.market_home_ml, g.market_away_ml, g.market_spread_line,
               g.market_captured_at,
               m.home_ml AS live_home_ml, m.away_ml AS live_away_ml,
               m.home_spread AS live_spread, m.fetched_at AS live_captured_at,
               ht.abbreviation AS home_abbrev, at.abbreviation AS away_abbrev
        FROM nfl_season_games g
        JOIN nfl_teams ht ON ht.team_id = g.home_team_id
        JOIN nfl_teams at ON at.team_id = g.away_team_id
        LEFT JOIN nfl_matchups m ON m.id = g.matchup_id
        WHERE g.season = %s
        ORDER BY g.week
        """,
        (season,),
    )
    if not games:
        raise RuntimeError(f"no games loaded for season {season}")

    frame = pd.DataFrame(games)

    # Every source of a real posted spread, in preference order. market_* is
    # the full-season Odds API capture (ingest/nfl_survivor_odds.py) and
    # normally covers all 272 games; the others are fallbacks for when that
    # capture has not run or the provider has dropped a game.
    frame["best_spread"] = (
        frame["market_spread_line"]
        .fillna(-frame["live_spread"])
        .fillna(frame["quoted_spread_line"])
    )

    # Anchor week: the last week whose lines are essentially fully posted.
    # Horizon for a modeled game is measured forward from here. When the whole
    # season is priced this lands on the final week and nothing is modeled at
    # all, which is the intended outcome, not a degenerate one.
    coverage = frame.groupby("week").apply(
        lambda part: part["best_spread"].notna().mean(), include_groups=False
    )
    anchor_week = 0
    for week in sorted(coverage.index):
        if coverage[week] >= FULL_COVERAGE_THRESHOLD:
            anchor_week = int(week)
        else:
            break

    # Ratings are fit on every posted spread available. They are only consumed
    # for games with no price at all, but they are still worth fitting: they
    # are what fills a provider gap, and their fit RMSE is a live check that
    # the ladder's sources agree with each other.
    quoted = frame[frame["best_spread"].notna()].copy()
    quoted["spread_line"] = quoted["best_spread"]
    quoted["home_team"] = quoted["home_abbrev"]
    quoted["away_team"] = quoted["away_abbrev"]
    teams = sorted(set(frame["home_abbrev"]) | set(frame["away_abbrev"]))

    ratings: dict[str, float] = {team: 0.0 for team in teams}
    hfa, fit_rmse = 0.0, None
    if len(quoted) >= 16:
        ratings, hfa, fit_rmse = fit_ratings(quoted, teams)

    for team in teams:
        db.execute(
            """
            INSERT INTO nfl_team_ratings
                (season, as_of_week, team_id, rating, hfa, n_games_fit,
                 ridge_lambda, fit_rmse, model_version, as_of_at)
            SELECT %s, %s, team_id, %s, %s, %s, %s, %s, %s, NOW()
            FROM nfl_teams WHERE abbreviation = %s
            ON CONFLICT (season, as_of_week, team_id, model_version) DO UPDATE SET
                rating = EXCLUDED.rating, hfa = EXCLUDED.hfa,
                n_games_fit = EXCLUDED.n_games_fit, fit_rmse = EXCLUDED.fit_rmse,
                as_of_at = NOW()
            """,
            (season, anchor_week, ratings[team], hfa, len(quoted),
             RIDGE_LAMBDA, fit_rmse, MODEL_VERSION, team),
        )

    sigma_table = load_sigma_table(db)
    counts = {"market_ml_novig": 0, "market_spread": 0, "model_spread": 0, "blocked": 0}

    for game in games:
        provenance = "blocked"
        horizon: int | None = None
        sigma: float | None = None
        home_prob: float | None = None

        # The quoted spread is resolved first and independently of which source
        # ends up producing the probability. A moneyline-derived cell still has
        # a real spread to show, and blanking it just because the probability
        # came from the price side would hide a number the user reads first.
        # nfl_matchups stores the home spread book-style (favorite negative);
        # nflverse stores it positive-is-home-favored.
        # Both Odds API captures quote the same books; they differ only in
        # scope and cadence. The full-season pass runs twice a week and covers
        # every game; the date-scoped pass runs daily but only for that day.
        # For the current week the daily one is usually fresher, and a survivor
        # pick is made on the current week -- so order them by capture time
        # rather than by which module wrote them.
        odds_api_sources = sorted(
            (
                (game["market_captured_at"], game["market_home_ml"],
                 game["market_away_ml"],
                 None if game["market_spread_line"] is None
                 else float(game["market_spread_line"])),
                (game["live_captured_at"], game["live_home_ml"], game["live_away_ml"],
                 None if game["live_spread"] is None else -float(game["live_spread"])),
            ),
            key=lambda entry: (entry[0] is not None, entry[0]),
            reverse=True,
        )

        spread: float | None = None
        spread_source: str | None = None
        for _, _, _, candidate in odds_api_sources:
            if candidate is not None:
                spread, spread_source = candidate, "odds_api"
                break
        if spread is None and game["quoted_spread_line"] is not None:
            spread, spread_source = float(game["quoted_spread_line"]), "nflverse"

        # 1. no-vig moneyline, freshest Odds API capture first
        for ml_home, ml_away, label in (
            *[(entry[1], entry[2], "odds_api") for entry in odds_api_sources],
            (game["quoted_home_ml"], game["quoted_away_ml"], "nflverse"),
        ):
            probability = _american_pair_to_prob(ml_home, ml_away)
            if probability is not None:
                home_prob, provenance = probability, "market_ml_novig"
                spread_source = f"{label}_ml"
                break

        # 2. quoted spread
        if home_prob is None and spread is not None:
            home_prob = spread_to_prob(spread, fit)
            provenance = "market_spread"

        # 3. modeled from market-implied ratings, widened by measured error
        if home_prob is None and len(quoted) >= 16:
            spread = ratings[game["home_abbrev"]] - ratings[game["away_abbrev"]] + hfa
            horizon = max(0, int(game["week"]) - anchor_week)
            sigma = sigma_for(horizon, sigma_table)
            home_prob = widen(spread, sigma, fit)
            provenance, spread_source = "model_spread", "market_implied_ratings"

        counts[provenance] = counts.get(provenance, 0) + 1

        tie = fit["tie_rate_close"] if spread is not None and abs(spread) <= 3.0 else 0.0
        for team_id, opponent_id, is_home in (
            (game["home_team_id"], game["away_team_id"], True),
            (game["away_team_id"], game["home_team_id"], False),
        ):
            if home_prob is None:
                p_win = None
            else:
                p_win = home_prob if is_home else 1.0 - home_prob
                # Ties are drawn from the same pool as wins, so hold the
                # three outcomes to one.
                p_win = max(0.0, min(1.0, p_win * (1.0 - tie)))
            db.execute(
                """
                INSERT INTO nfl_game_win_probs
                    (game_id, season, week, team_id, opponent_team_id, is_home,
                     p_win, p_tie, provenance, spread_used, spread_source,
                     horizon_weeks, sigma_h, model_version, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (game_id, team_id, model_version) DO UPDATE SET
                    p_win = EXCLUDED.p_win, p_tie = EXCLUDED.p_tie,
                    provenance = EXCLUDED.provenance,
                    spread_used = EXCLUDED.spread_used,
                    spread_source = EXCLUDED.spread_source,
                    horizon_weeks = EXCLUDED.horizon_weeks,
                    sigma_h = EXCLUDED.sigma_h, computed_at = NOW()
                """,
                (
                    game["id"], season, game["week"], team_id, opponent_id, is_home,
                    p_win, tie, provenance,
                    None if spread is None else (spread if is_home else -spread),
                    spread_source, horizon, sigma, MODEL_VERSION,
                ),
            )

    return {
        "anchor_week": anchor_week,
        "quoted_games": int(len(quoted)),
        "hfa": hfa,
        "fit_rmse": fit_rmse,
        "provenance": counts,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument(
        "--recalibrate",
        action="store_true",
        help="re-measure the horizon error table (slow; it is a constant of the sport)",
    )
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    schedule = fetch_schedule()

    history = historical_games(schedule)
    fit = fit_spread_prob(history)
    print(
        f"spread->prob fit on {fit['n']} games ({fit['seasons']}): "
        f"p = sigmoid({fit['intercept']:.5f} + {fit['slope']:.5f} * spread); "
        f"tie rate within 3 pts = {fit['tie_rate_close']*100:.2f}%"
    )

    if args.recalibrate or not load_sigma_table(db):
        print("measuring horizon error (this takes a minute)...")
        store_calibration(db, horizon_calibration(schedule))
    table = load_sigma_table(db)
    print(f"horizon sigma table: {len(table)} horizons, h=1 {table.get(1, 0):.2f} pts, "
          f"h=10 {table.get(10, 0):.2f} pts")

    summary = compute_and_store(db, args.season, fit)
    print(
        f"ratings anchored at week {summary['anchor_week']} "
        f"from {summary['quoted_games']} quoted games "
        f"(hfa {summary['hfa']:.2f}, fit rmse {summary['fit_rmse']:.2f})"
    )
    for label, count in summary["provenance"].items():
        if count:
            print(f"  {label:20s} {count:4d} games")


if __name__ == "__main__":
    main()
