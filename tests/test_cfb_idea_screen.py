"""Offline checks of the CFB idea screen's statistics (no database)."""
import numpy as np

from research import cfb_idea_screen as s


def test_benjamini_hochberg_matches_hand_values():
    q = s.benjamini_hochberg([0.01, 0.04, 0.03, 0.20])
    assert [round(v, 4) for v in q] == [0.04, 0.0533, 0.0533, 0.2]


def test_record_counts_the_side_the_signal_points_to():
    sig = np.array([1.0, -1.0, 0.0, 2.0, -3.0])
    y = np.array([1.0, 1.0, 1.0, 0.0, -1.0])
    assert s.record(sig, y) == {"n": 4, "w": 2, "l": 1, "p": 1, "rate": 2 / 3}


def test_clustered_slope_finds_a_planted_effect_and_not_noise():
    rng = np.random.default_rng(7)
    n = 4000
    x = rng.normal(size=n)
    clusters = rng.integers(0, 80, size=n)
    noise = np.sign(rng.normal(size=n))
    planted = np.sign(0.25 * x + rng.normal(size=n))
    assert abs(s.clustered(x, noise, clusters)["z"]) < 3
    assert s.clustered(x, planted, clusters)["z"] > 6


def test_constant_signal_is_not_tested():
    assert s.clustered(np.ones(100), np.ones(100), np.arange(100)) is None


def test_gate_constants_are_frozen():
    assert (s.DISCOVERY_THROUGH, s.CONFIRM, s.BH_Q, s.CONFIRM_P, s.MIN_FLAGGED) == (2024, 2025, 0.10, 0.05, 20)
