"""Pre-registered study: does fading the field beat maximizing survival?

REGISTERED BEFORE THE DATA WAS EXAMINED (survivor spec section 13.3).

H: in a pool of >= 100 entries, an EV-optimized path produces a higher
   expected prize share than the SURVIVE path.

Population   2025 season, 18 weeks, survivorgrid's archived P% as the field
             distribution. Simulated pools at 100 / 500 / 2,000 entries.
             Outcomes drawn from the week's market probabilities with
             within-week correlation -- every entry on the same team shares
             one game result, which is the single easiest thing to get wrong.
Metric       mean prize share, paired bootstrap CI over trials.
Floor        one season is one season. n = 1 field-season is BELOW any
             defensible floor and this was stated before the run, not after.
             The result can only ever be directional; EV mode stays research-
             badged until at least three seasons of archived P% exist and the
             effect holds in each independently.
Kill         no positive effect at any pool size => EV mode is not built. No
             re-slicing to a favourable pool size or week range afterwards.

TWO METHOD CORRECTIONS MADE DURING THE BUILD, BOTH RECORDED RATHER THAN
QUIETLY APPLIED
-----------------------------------------------------------------------
1. The field was first modelled as an aggregate surviving mass splitting
   itself by P% each week, with no per-rival use-each-team-once constraint.
   That returned an exactly zero effect, and it was an artifact: unconstrained
   rivals never get boxed in, so the mass of rivals surviving on OTHER teams
   stays large all season and swamps any single team's pick share -- which
   suppresses the very effect being tested. The simplification ran AGAINST the
   hypothesis. Rivals now each carry their own used-team set and are
   eliminated when they run out of legal teams.

2. The EV strategy first used a closed-form leverage term,
   `-log1p(E[rivals surviving])`. That never once displaced the survival-
   optimal pick, and it is Jensen-biased in the direction that matters: the
   contrarian payoff lives in the tail where the field gets wiped out, and
   plugging a mean into the denominator prices that tail away. Killing EV mode
   on a heuristic biased against it would be a wrong verdict reached by a
   plausible-looking shortcut.

   Its replacement -- an unconstrained week-by-week local search on simulated
   prize share -- was worse, and failed for a reason worth keeping. Prize share
   is mostly zero with occasional ones, so its per-trial SD stays near 0.1 even
   after pairing on a common random stream. Resolving an effect around 0.001
   therefore needs tens of thousands of trials PER CANDIDATE, and the search had
   roughly a hundred candidates. It duly "found" improvements that then failed
   to reproduce on a fresh stream: it was fitting its own simulation noise.

   What is tested instead is a ONE-PARAMETER policy family (below), which can
   actually be powered at a feasible trial count.

Usage:
    python -m model.survivor_field_study
    python -m model.survivor_field_study --trials 20000
"""

from __future__ import annotations

import argparse
import math
from functools import lru_cache

import numpy as np

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_season_schedule import fetch_schedule
from model.nfl_survivor_model import fit_spread_prob, historical_games, spread_to_prob
from model.survivor_backtest import solve_assignment

BLOCKED = 1e9
POOL_SIZES = (100, 500, 2000)


def load_season(season: int):
    """Per-week win probability and opponent map for one season."""
    schedule = fetch_schedule()
    fit = fit_spread_prob(historical_games(schedule))
    games = schedule[
        (schedule["season"] == season)
        & (schedule["game_type"] == "REG")
        & schedule["spread_line"].notna()
    ]
    teams = sorted(set(games["home_team"]) | set(games["away_team"]))
    index = {team: i for i, team in enumerate(teams)}

    probs: dict[int, dict[int, float]] = {}
    pairings: dict[int, dict[int, int]] = {}
    for week, week_games in games.groupby("week"):
        week = int(week)
        probs[week], pairings[week] = {}, {}
        for _, game in week_games.iterrows():
            home, away = index[game["home_team"]], index[game["away_team"]]
            home_prob = spread_to_prob(float(game["spread_line"]), fit)
            probs[week][home], probs[week][away] = home_prob, 1.0 - home_prob
            pairings[week][home], pairings[week][away] = away, home
    return teams, probs, pairings


def load_pick_shares(db: DatabaseManager, season: int, teams: list[str]) -> dict[int, dict[int, float]]:
    rows = db.execute(
        """
        SELECT p.week, t.abbreviation, p.pick_pct
        FROM survivor_pick_popularity p
        JOIN nfl_teams t USING (team_id)
        WHERE p.season = %s AND p.pick_pct IS NOT NULL
        """,
        (season,),
    )
    # nflverse writes LA/WAS where nfl_teams writes LAR/WSH.
    to_nflverse = {"LAR": "LA", "WSH": "WAS"}
    index = {team: i for i, team in enumerate(teams)}

    shares: dict[int, dict[int, float]] = {}
    for row in rows:
        abbrev = to_nflverse.get(row["abbreviation"], row["abbreviation"])
        if abbrev in index:
            shares.setdefault(int(row["week"]), {})[index[abbrev]] = float(row["pick_pct"])
    for week, week_shares in shares.items():
        total = sum(week_shares.values())
        if total > 0:
            shares[week] = {team: share / total for team, share in week_shares.items()}
    return shares


class Planner:
    """Assignment-optimal remaining path, cached by (week position, used set)."""

    def __init__(self, weeks, probs, team_count, pairings):
        self.weeks = weeks
        self.probs = probs
        self.team_count = team_count
        self.pairings = pairings
        self._value = lru_cache(maxsize=200_000)(self._value_uncached)

    def _matrix(self, remaining, used):
        matrix = np.full((len(remaining), self.team_count), BLOCKED)
        for row, week in enumerate(remaining):
            for team, probability in self.probs.get(week, {}).items():
                if team not in used:
                    matrix[row, team] = -math.log(max(probability, 1e-6))
        return matrix

    def _value_uncached(self, week_position: int, used: frozenset) -> float:
        remaining = self.weeks[week_position:]
        if not remaining:
            return 0.0
        matrix = self._matrix(remaining, used)
        total = 0.0
        for row, team in enumerate(solve_assignment(matrix)):
            if team < 0 or matrix[row, team] >= BLOCKED:
                return -BLOCKED
            total += matrix[row, team]
        return -total

    def future_value(self, week_position: int, used: frozenset) -> float:
        return self._value(week_position, used)

    def optimal_path(self, start: int = 0, used: frozenset = frozenset()) -> dict[int, int]:
        """The survival-optimal remaining path as {week: team}."""
        remaining = self.weeks[start:]
        matrix = self._matrix(remaining, used)
        path: dict[int, int] = {}
        for row, team in enumerate(solve_assignment(matrix)):
            if team >= 0 and matrix[row, team] < BLOCKED:
                path[remaining[row]] = team
        return path
def simulate_path(
    path: dict[int, int],
    planner: Planner,
    shares_by_week: dict[int, dict[int, float]],
    pool_size: int,
    trials: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Prize share obtained by a fixed pick sequence, per trial.

    Rivals are vectorized across the pool: an (entries x teams) used mask and
    one categorical draw per alive entry per week from P% restricted to the
    teams that entry has left. An entry with no legal team remaining is
    eliminated, which is what actually thins a large pool late in the season.
    """
    weeks = planner.weeks
    team_count = planner.team_count
    rival_count = max(pool_size - 1, 0)
    results = np.zeros(trials)
    share_vectors = {
        week: np.array([shares_by_week.get(week, {}).get(t, 0.0) for t in range(team_count)])
        for week in weeks
    }

    for trial in range(trials):
        alive = True
        my_last_week = 0
        rival_used = np.zeros((rival_count, team_count), dtype=bool)
        rival_alive = np.ones(rival_count, dtype=bool)
        rival_last_week = np.zeros(rival_count, dtype=int)

        for position, week in enumerate(weeks):
            probs = planner.probs.get(week)
            if not probs:
                continue

            # One draw per GAME, shared by every entry on either side.
            outcomes = np.zeros(team_count, dtype=bool)
            decided = np.zeros(team_count, dtype=bool)
            for team, probability in probs.items():
                if decided[team]:
                    continue
                won = rng.random() < probability
                outcomes[team] = won
                decided[team] = True
                opponent = planner.pairings.get(week, {}).get(team)
                if opponent is not None:
                    outcomes[opponent], decided[opponent] = not won, True

            if alive:
                pick = path.get(week)
                if pick is None:
                    alive = False
                elif outcomes[pick]:
                    my_last_week = position + 1
                else:
                    alive = False

            if rival_count and rival_alive.any():
                weights = (~rival_used) * share_vectors[week][None, :]
                totals = weights.sum(axis=1)
                rival_alive &= ~(rival_alive & (totals <= 0))
                active = np.flatnonzero(rival_alive)
                if active.size:
                    cumulative = np.cumsum(weights[active], axis=1)
                    draws = rng.random(active.size) * cumulative[:, -1]
                    picks = np.minimum((cumulative < draws[:, None]).sum(axis=1), team_count - 1)
                    rival_used[active, picks] = True
                    survived = outcomes[picks]
                    rival_last_week[active[survived]] = position + 1
                    rival_alive[active[~survived]] = False

            if not alive and not rival_alive.any():
                break

        if alive:
            my_last_week = len(weeks)
        best_rival = int(rival_last_week.max()) if rival_count else 0
        if my_last_week > best_rival:
            results[trial] = 1.0
        elif my_last_week == best_rival and my_last_week > 0:
            results[trial] = 1.0 / (1.0 + int((rival_last_week == best_rival).sum()))
        else:
            results[trial] = 0.0
    return results


# Pre-specified contrarian policy family. One parameter, fixed before the
# evaluation was run: among the teams whose survival-optimal net score is
# within `tolerance` nats of the best available pick, take the one the field
# is on least. tolerance = 0 reproduces SURVIVE exactly.
#
# The search this replaced, and why it could not work, is in the module
# docstring. Keep this family small: every extra parameter is another
# comparison, and the whole point of a pre-registration is a fixed count.
CONTRARIAN_TOLERANCES = (0.05, 0.10, 0.20)


def contrarian_path(planner, shares_by_week, tolerance):
    """Walk the season taking the least-picked team inside a net-score band."""
    path = {}
    used = frozenset()

    for position, week in enumerate(planner.weeks):
        scored = {
            team: math.log(max(probability, 1e-6))
            + planner.future_value(position + 1, used | {team})
            for team, probability in planner.probs.get(week, {}).items()
            if team not in used
        }
        if not scored:
            continue
        best = max(scored.values())
        band = [team for team, score in scored.items() if score >= best - tolerance]
        shares = shares_by_week.get(week, {})
        # Least-picked inside the band; ties broken toward the stronger pick.
        pick = min(band, key=lambda team: (shares.get(team, 0.0), -scored[team]))
        path[week] = pick
        used = used | {pick}
    return path


def paired_bootstrap_ci(differences: np.ndarray, rng: np.random.Generator, draws: int = 2000):
    means = np.array(
        [rng.choice(differences, size=len(differences), replace=True).mean() for _ in range(draws)]
    )
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--trials", type=int, default=8000)

    parser.add_argument("--seed", type=int, default=20260901)
    args = parser.parse_args()

    db = DatabaseManager(load_config().database_url)
    teams, probs, pairings = load_season(args.season)
    shares = load_pick_shares(db, args.season, teams)
    if not shares:
        raise SystemExit(
            f"no pick popularity for {args.season}; run ingest.survivor_pick_popularity first"
        )

    weeks = sorted(probs)
    planner = Planner(weeks, probs, len(teams), pairings)
    survive_path = planner.optimal_path()

    print(f"field study -- {args.season}, {len(weeks)} weeks, {len(shares)} weeks of pick "
          f"shares, {args.trials} evaluation trials\n")
    print("  survive-optimal path: "
          + " ".join(f"W{w}:{teams[survive_path[w]]}" for w in weeks if w in survive_path))

    policies = {
        "tol=%.2f" % tolerance: contrarian_path(planner, shares, tolerance)
        for tolerance in CONTRARIAN_TOLERANCES
    }
    for label, path in policies.items():
        differ = sum(
            1 for week in weeks
            if week in survive_path and path.get(week) != survive_path[week]
        )
        print("  %s: %d of %d weeks differ from it" % (label, differ, len(survive_path)))

    print()
    print("  %6s%10s%10s%10s%11s%26s"
          % ("pool", "policy", "SURVIVE", "policy", "delta", "95% CI on delta"))

    any_positive = False
    for pool_size in POOL_SIZES:
        stream = args.seed + 7919 + pool_size
        survive = simulate_path(survive_path, planner, shares, pool_size, args.trials,
                                np.random.default_rng(stream))
        for label, path in policies.items():
            # Common random numbers: both paths see the identical outcome
            # sequence, so the paired difference cancels most of the variance
            # that would otherwise swamp an effect this small.
            policy = simulate_path(path, planner, shares, pool_size, args.trials,
                                   np.random.default_rng(stream))
            difference = policy - survive
            low, high = paired_bootstrap_ci(difference, np.random.default_rng(args.seed + 1))
            positive = low > 0
            any_positive = any_positive or positive
            print("  %6d%10s%10.5f%10.5f%+11.5f%26s%s" % (
                pool_size, label, survive.mean(), policy.mean(), difference.mean(),
                "[%+.5f, %+.5f]" % (low, high), "  *" if positive else ""))


    print("\nVERDICT")
    if any_positive:
        print("  At least one pool size shows a positive effect whose CI excludes zero.")
        print("  This is ONE season of field data. Per the pre-registered floor that is")
        print("  DIRECTIONAL ONLY -- EV mode may be built but stays research-badged until")
        print("  three independent seasons of archived P% each show the effect.")
    else:
        print("  No pool size shows a positive effect whose CI excludes zero.")
        print("  Per the pre-registered kill criterion, EV mode is NOT built. No re-slicing")
        print("  to a favourable pool size or week range -- that would be the exact")
        print("  multiple-comparisons drift this registration exists to prevent.")

    print()
    print("  %d policies x %d pool sizes = %d comparisons, so a single starred"
          % (len(CONTRARIAN_TOLERANCES), len(POOL_SIZES),
             len(CONTRARIAN_TOLERANCES) * len(POOL_SIZES)))
    print("  row at a nominal 95% level would be unremarkable on its own.")
    print()
    print("Limitations: P% is NATIONAL pick share, not the distribution inside")
    print("any real pool. Each policy is a path fixed in advance rather than one")
    print("that adapts to how many rivals actually remain, so this tests planned")
    print("contrarian play rather than contrarian play in general.")


if __name__ == "__main__":
    main()
