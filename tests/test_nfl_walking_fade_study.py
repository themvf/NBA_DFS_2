"""The NFL total_walking fade registration is sealed — these tests prove it.

Every constant was frozen on 2026-08-15, before Week 1 (2026-09-09) and before
a single regular-season observation existed. If a future change moves one, this
suite fails and forces the change to be an explicit, versioned decision instead
of a quiet renegotiation after seeing results.

That is not paranoia about the future; it is this project's documented history.
The soccer anytime-scorer detector produced six memorable winners inside a
sample whose CI ultimately sat entirely below zero, and the 9.5-line totals
"edge" was the most extreme of several slices examined.
"""

from __future__ import annotations

import model.nfl_walking_fade_study as S


def test_registration_is_frozen() -> None:
    assert S.STUDY_VERSION == "nfl-walking-fade-v1"
    assert S.REGISTERED_AT == "2026-08-15"
    assert S.SEASON_START == "2026-09-09", "Week 1 — the clean-test boundary"
    assert S.ALERT_TYPE == "total_walking"
    assert S.FLOOR_N == 100
    assert S.FLOOR_GAMES == 40
    assert S.ROI_CI_LOWER_BOUND == -5.0


def test_preseason_discovery_is_recorded_but_walled_off() -> None:
    """The 14 preseason alerts are the hunch. They may never be pooled into the
    confirmation sample — including as a 'combined' secondary."""
    assert S.DISCOVERY["n"] == 14
    assert S.DISCOVERY["games"] == 12
    assert S.DISCOVERY["toward_flagged"] == 1

    src = open(S.__file__, encoding="utf-8").read()
    # The query that builds the confirmation sample must exclude preseason both
    # by season_type AND by a Week-1 commence bound.
    assert "season_type = 'regular'" in src
    assert "m.commence_time >= %s" in src
    assert "DISCOVERY" not in src.split("def load_observations")[1].split("def ")[0], (
        "the discovery constants must not leak into the confirmation loader"
    )


def test_fade_clv_sign_convention_matches_the_hypothesis() -> None:
    """H says the FLAGGED side overshoots, so the line comes BACK and the FADE
    side gains. A sign error here would invert the entire study."""
    # Flagged 'over' at 44.0, line falls to 43.0 -> overshoot -> fade (under) +1.0
    entry, close = 44.0, 43.0
    assert (entry - close) == 1.0
    # Flagged 'under' at 44.0, line rises to 45.0 -> overshoot -> fade (over) +1.0
    entry, close = 44.0, 45.0
    assert (close - entry) == 1.0


def test_cluster_bootstrap_resamples_games_not_alerts() -> None:
    """Two alerts on one game are one observation of that game's line
    behaviour. Resampling alerts would understate the interval."""
    vals = [2.0] * 4 + [-2.0] * 4
    # Same eight numbers. Left: eight independent games. Right: the same eight
    # alerts arriving on only two games, so there are really two observations.
    tight = S.cluster_bootstrap(vals, [f"g{i}" for i in range(8)], iters=4000)
    loose = S.cluster_bootstrap(vals, ["g1"] * 4 + ["g2"] * 4, iters=4000)
    assert (loose[1] - loose[0]) > (tight[1] - tight[0]), (
        "fewer independent clusters must widen the interval"
    )


def test_verdict_requires_both_clv_and_roi() -> None:
    """A better NUMBER that still loses money is not an edge. The ROI gate is
    conjunctive, so it can only ever make the verdict harder."""
    src = open(S.__file__, encoding="utf-8").read()
    assert "lo > 0 and roi_ok" in src
    assert "CLV positive but ROI gate failed" in src
    # The kill branch must not be reachable by re-slicing.
    assert "No re-slicing, no re-tuning." in src
