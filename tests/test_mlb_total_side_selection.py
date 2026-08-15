"""The MLB totals side must come from the predictive distribution, not `mean > line`.

Regression cover for the mean-vs-median category error diagnosed 2026-08-12:
the totals model regresses ``actual_total - vegas_total`` under squared-error
loss, so it predicts the conditional MEAN, while books set the line at the
MEDIAN.  League-wide those differ by half a run (mean miss +0.51, median miss
0.00).  Selecting the side with ``our_total_pred > line`` therefore said Over on
74-87% of games forever; Over hits only 46.5% against a ~52.4% breakeven.
"""

from __future__ import annotations

import numpy as np

from model.mlb_game_bets import frozen_total_distribution
from model.mlb_game_total_model import total_probabilities


def _snapshot(line: float, p_over: float, p_under: float, p_push: float = 0.0) -> dict:
    return {
        "total_distribution": {
            "line": line,
            "p_over": p_over,
            "p_under": p_under,
            "p_push": p_push,
        }
    }


def test_side_follows_probability_not_the_point_estimate() -> None:
    """The exact shape that bled: mean above the line, Under still likelier."""
    line, our_total_pred = 8.5, 9.04  # real snapshot, matchup 5827
    dist = frozen_total_distribution(_snapshot(line, 0.471, 0.529), line)

    assert our_total_pred > line, "precondition: the old rule would have said Over"
    assert dist is not None
    assert dist["p_under"] > dist["p_over"], "distribution favours Under"

    is_over = dist["p_over"] > dist["p_under"]
    assert is_over is False, "side must follow the distribution, not mean > line"


def test_right_skewed_residuals_put_the_median_below_the_mean() -> None:
    """The mechanism itself: a mean half a run over the line is still ~a coin flip.

    Books set the line at the median, so a right-skewed distribution centred a
    half-run above it must NOT report a lopsided P(over).
    """
    rng = np.random.default_rng(7)
    # Right-skewed run totals: mean sits ~0.5 above the median, as in MLB.
    residuals = rng.gamma(shape=2.0, scale=1.8, size=20000)
    residuals -= residuals.mean()  # rolling-origin residuals are mean-zero
    assert np.median(residuals) < -0.3, "fixture must actually be right-skewed"

    line = 8.5
    probs = total_probabilities(mean_total=line + 0.5, line=line, residuals=residuals)

    # A Poisson/normal would read this as a comfortable Over. The empirical
    # distribution correctly does not.
    assert probs["p_over"] < 0.5
    assert probs["p_over"] + probs["p_under"] + probs["p_push"] == 1.0


def test_probability_is_conditional_on_no_push() -> None:
    """Integer lines can push; our_prob must exclude it to match the book quote."""
    line = 9.0
    dist = frozen_total_distribution(_snapshot(line, 0.391, 0.531, p_push=0.078), line)
    assert dist is not None

    side_raw = dist["p_under"] / dist["decided"]
    assert abs(dist["decided"] - (0.391 + 0.531)) < 1e-9
    assert side_raw > dist["p_under"], "conditional prob exceeds the raw prob"
    assert abs(side_raw - 0.531 / 0.922) < 1e-9


def test_fails_closed_rather_than_guessing() -> None:
    """No usable distribution means no bet — never a symmetric fallback."""
    line = 8.5
    assert frozen_total_distribution({}, line) is None
    assert frozen_total_distribution({"total_distribution": None}, line) is None
    # A distribution built at a different line is a different proposition.
    assert frozen_total_distribution(_snapshot(9.5, 0.48, 0.52), line) is None
    # Degenerate distribution carries no decidable side.
    assert frozen_total_distribution(_snapshot(line, 0.0, 0.0, p_push=1.0), line) is None


def test_poisson_is_not_reachable_from_the_mlb_bet_path() -> None:
    """The soccer Poisson import is what converted the offset into a fake edge."""
    import ast

    import model.mlb_game_bets as bets

    tree = ast.parse(open(bets.__file__, encoding="utf-8").read())

    assert not [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "_over_under_probs"
    ], "the soccer Poisson must not be reachable from the MLB bet path"

    # Executable code must never compare the point prediction against the line.
    # (Checked on the AST, so the docstring warning about this doesn't match.)
    offenders = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Name)
        and node.left.id in {"lam", "our_total_pred", "prediction", "mean_total"}
        and any(
            isinstance(cmp, ast.Name) and cmp.id in {"line", "market_line", "vegas_total"}
            for cmp in node.comparators
        )
    ]
    assert not offenders, "totals side must not be picked by a mean-vs-line comparison"
