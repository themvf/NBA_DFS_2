"""Pre-registered MLB prop program: frozen constants and gate arithmetic.

These tests exist to make the registration tamper-evident. Every constant here
was frozen on 2026-08-15 BEFORE the v3 cohort had a single observation. If a
future change moves one, this suite fails and forces the change to be an
explicit, versioned decision rather than a quiet renegotiation after seeing
results -- which is precisely the failure mode that produced the soccer
anytime-scorer mirage.
"""

from __future__ import annotations

import model.mlb_prop_program as P


def test_registration_constants_are_frozen() -> None:
    assert P.PROGRAM_VERSION == "mlb-prop-program-v1"
    assert P.REGISTERED_AT == "2026-08-15"
    assert P.TEST_FAMILY_SIZE == 1, (
        "one pooled cell. Expanding the family needs a new program version -- "
        "20 tests carry a 64% chance of a spurious winner under a pure null."
    )
    assert P.ANCHORED_MARKETS == (
        "pitcher_strikeouts", "batter_total_bases", "pitcher_outs",
        "pitcher_hits_allowed",
    ), "the four markets with a same-line Pinnacle anchor, per the census"
    assert P.MDE_PP == 1.0
    assert P.FLOOR_SETTLED == 30
    assert P.FLOOR_DISTINCT_DATES == 25
    assert P.CONCENTRATION_DISCLOSE == 0.40
    assert P.LOBO_MIN_MAGNITUDE_RETAINED == 0.50
    assert P.VERDICT_TRIGGER_DATE == "2026-10-04"


def test_power_floor_formula_is_the_registered_one() -> None:
    """floor = (2.80 * SD / MDE)^2. Only SD may be re-estimated, once, blind."""
    assert P.floor_n_eff(4.12) == 134
    # A larger observed SD must RAISE the bar, never lower it.
    assert P.floor_n_eff(5.0) > P.floor_n_eff(4.12)
    assert P.floor_n_eff(3.0) < P.floor_n_eff(4.12)


def test_design_effect_penalises_clustered_observations() -> None:
    """Alerts on one slate share a pitcher, park, weather and umpire. Treating
    them as independent is how a CI ends up 30-40% too narrow."""
    spread = [{"date": f"2026-08-{d:02d}", "clv_pp": v, "exec_book": "dk",
               "market": "m", "outcome": "won"}
              for d, v in zip(range(1, 11), [1, -1] * 5)]
    # Same total n, but every observation on ONE date and correlated within it.
    clumped = [{"date": "2026-08-01", "clv_pp": 5.0, "exec_book": "dk",
                "market": "m", "outcome": "won"} for _ in range(5)] + \
              [{"date": "2026-08-02", "clv_pp": -5.0, "exec_book": "dk",
                "market": "m", "outcome": "lost"} for _ in range(5)]
    assert P.design_effect(spread) < P.design_effect(clumped)
    assert P.design_effect(clumped) > 1.0


def test_concentration_detects_a_single_book_finding() -> None:
    obs = [{"date": "2026-08-01", "clv_pp": 1.0, "exec_book": "betrivers",
            "market": "m", "outcome": "won"} for _ in range(7)] + \
          [{"date": "2026-08-01", "clv_pp": 1.0, "exec_book": "draftkings",
            "market": "m", "outcome": "won"} for _ in range(3)]
    top, share, dist = P.concentration(obs)
    assert top == "betrivers"
    assert abs(share - 0.7) < 1e-9
    assert share > P.CONCENTRATION_DISCLOSE, "70% must trip the C1 disclosure"
    assert dist == {"betrivers": 7, "draftkings": 3}


def test_the_control_detector_is_not_the_live_one() -> None:
    """The control must stay a separate, frozen arm. If these ever became the
    same detector the placebo comparison would silently become circular."""
    assert P.CONTROL_DETECTOR != P.LIVE_DETECTOR
    assert P.CONTROL_DETECTOR == "prop_line_gap"
