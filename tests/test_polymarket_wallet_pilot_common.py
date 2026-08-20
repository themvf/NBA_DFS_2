from __future__ import annotations

from ingest.polymarket_wallet_pilot_common import wilson_lower_bound


def test_wilson_lower_bound_penalizes_small_samples_over_large_ones():
    # The whole point of this metric: a small-n wallet at a HIGHER raw win
    # rate must not outrank a large-n wallet at a lower raw win rate, once
    # sample-size confidence is accounted for. Observed live in this
    # project's own pilot output -- raw win% put a 5-for-5 wallet above a
    # 93-for-... wallet at 77%.
    five_for_five = wilson_lower_bound(5, 5)          # raw 100%
    two_hundred_of_280 = wilson_lower_bound(200, 280)  # raw ~71%
    assert five_for_five < two_hundred_of_280


def test_wilson_lower_bound_stays_near_a_coin_flip_at_tiny_n():
    # A short winning streak alone shouldn't look like strong evidence of
    # skill -- the lower bound must remain close to 50%, not near the raw
    # (100%) rate.
    assert 0.5 < wilson_lower_bound(5, 5) < 0.65


def test_wilson_lower_bound_converges_toward_raw_rate_at_large_n():
    # As n grows, confidence grows, so the lower bound should approach the
    # raw win rate rather than staying anchored near 50%.
    raw_rate = 200 / 280
    lb = wilson_lower_bound(200, 280)
    assert raw_rate - lb < 0.10


def test_wilson_lower_bound_handles_zero_markets():
    assert wilson_lower_bound(0, 0) == 0.0


def test_wilson_lower_bound_monotonic_in_win_rate_at_fixed_n():
    assert wilson_lower_bound(9, 10) > wilson_lower_bound(5, 10) > wilson_lower_bound(1, 10)
