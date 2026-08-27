"""Tests for the v2 CLV wallet screen.

Each test pins one property that, if it broke, would let the measurement
produce a confident wrong answer rather than an obvious failure -- the exact
failure mode that made v1's leaderboard meaningless. See
docs/polymarket-wallet-tracker.md for the v1 post-mortem.
"""

from __future__ import annotations

import pytest

from ingest.polymarket_wallet_clv import (
    MIN_BUY_DOMINANCE,
    MIN_CLV_MARKETS,
    MIN_HOLD_RATIO,
    bootstrap_clv_ci,
    closing_price,
    gate,
    in_reference_terms,
    measure_market,
    new_wallet,
    parse_ts,
    rank_by_clv,
)

START = 1_700_000_000
REF = "Alice"
OTHER = "Bob"
MARKET = {
    "condition_id": "0xtest",
    "question": "Alice vs Bob",
    "winner": REF,
    "outcomes": [REF, OTHER],
    "game_start_ts": START,
    "volume": 1.0,
}


def fill(wallet, outcome, side, size, price, ts):
    return {
        "proxyWallet": wallet, "outcome": outcome, "side": side,
        "size": size, "price": price, "timestamp": ts,
    }


def _close_book(price, size=5000):
    """Three market-maker fills inside the final window, so the close is set
    by them alone and the thin-window fallback never reaches back and sweeps
    in the wallet under test."""
    return [fill("mm", REF, "BUY", size, price, START - t) for t in (300, 200, 100)]


# --- the close --------------------------------------------------------------

def test_game_start_time_parses_gammas_space_separated_offset():
    """Gamma serves '2024-11-11 19:30:00+00', not ISO-8601. A parse failure
    here would silently make every market CLV-ineligible."""
    assert parse_ts("2024-11-11 19:30:00+00") == 1731353400
    assert parse_ts("2024-11-11T19:30:00Z") == 1731353400
    assert parse_ts(None) is None
    assert parse_ts("not a date") is None


def test_close_ignores_in_play_trades():
    """The whole design rests on this. A close taken after the match starts
    has absorbed the outcome, and CLV would collapse into the result."""
    fills = [
        fill("w", REF, "BUY", 100, 0.50, START - 600),
        fill("w", REF, "BUY", 100, 0.99, START + 600),  # in-play, must not count
    ]
    assert closing_price(fills, REF, START, [REF, OTHER]) == pytest.approx(0.50)


def test_close_unifies_both_sides_of_a_binary_market():
    """A trade on Bob at 0.30 is a trade on Alice at 0.70. Dropping the other
    side would halve the observations behind the benchmark."""
    fills = [fill("w", OTHER, "BUY", 100, 0.30, START - 600)]
    assert closing_price(fills, REF, START, [REF, OTHER]) == pytest.approx(0.70)


def test_close_is_volume_weighted_not_last_trade():
    """One odd-lot fill at a stale price must not define the benchmark every
    wallet in the market is scored against."""
    fills = [
        fill("w", REF, "BUY", 1000, 0.60, START - 600),
        fill("w", REF, "BUY", 1, 0.10, START - 60),
    ]
    close = closing_price(fills, REF, START, [REF, OTHER])
    assert close == pytest.approx((1000 * 0.60 + 1 * 0.10) / 1001)
    assert close > 0.55  # nowhere near the 0.10 last trade


def test_market_with_no_pregame_trades_is_ineligible_not_defaulted():
    fills = [fill("w", REF, "BUY", 100, 0.99, START + 60)]
    assert closing_price(fills, REF, START, [REF, OTHER]) is None


# --- CLV --------------------------------------------------------------------

def test_reference_terms_flip_for_the_non_reference_side():
    assert in_reference_terms(REF, 0.4, REF) == pytest.approx(0.4)
    assert in_reference_terms(OTHER, 0.4, REF) == pytest.approx(0.6)


def test_buying_a_near_certainty_at_a_near_certain_price_scores_zero_clv():
    """v1's worst artifact: 101 markets, 100% win rate, $7.5M cost, $8,103
    profit -- a wallet that simply paid full price for favourites topped the
    leaderboard. Under CLV it scores ~0, which is correct."""
    fills = [
        fill("sharp", REF, "BUY", 100, 0.40, START - 7200),
        fill("payer", REF, "BUY", 100, 0.99, START - 7200),
    ] + _close_book(0.99)
    measured, close = measure_market(fills, MARKET)
    assert close == pytest.approx(0.99, abs=0.001)
    assert measured["payer"]["clv_market"] == pytest.approx(0.0, abs=0.01)
    assert measured["sharp"]["clv_market"] > 0.5


def test_clv_is_negative_when_the_market_moves_away_from_the_buyer():
    """CLV must be able to be negative. A metric that can only be zero-or-
    positive is not measuring anything."""
    fills = [
        fill("late", REF, "BUY", 100, 0.80, START - 7200),
    ] + _close_book(0.30)
    measured, _ = measure_market(fills, MARKET)
    assert measured["late"]["clv_market"] < 0


def test_clv_scores_pregame_buys_only_never_sells():
    """Scoring a sell would credit a scalper for unwinding into a move it did
    not predict."""
    fills = [
        fill("scalper", REF, "SELL", 100, 0.20, START - 7200),
    ] + _close_book(0.60)
    measured, _ = measure_market(fills, MARKET)
    assert measured["scalper"]["clv_stake"] == 0
    assert measured["scalper"]["clv_market"] is None


def test_in_play_buys_are_excluded_from_clv_but_still_counted_behaviourally():
    fills = [
        fill("w", REF, "BUY", 100, 0.50, START - 7200),
        fill("w", REF, "BUY", 100, 0.10, START + 3600),
    ] + _close_book(0.50)
    measured, _ = measure_market(fills, MARKET)
    entry = measured["w"]
    assert entry["inplay_trades"] == 1
    assert entry["pregame_trades"] == 1
    # only the pregame $50 is scored, not the in-play $10
    assert entry["clv_stake"] == pytest.approx(50.0)


def test_clv_is_dollar_weighted_within_a_market():
    """v1 bug #4 on a different axis: averaging per-trade ratios would let a
    $1 trade count as much as a $10,000 one."""
    fills = [
        fill("w", REF, "BUY", 10000, 0.50, START - 7200),   # $5,000, big CLV
        fill("w", OTHER, "BUY", 1, 0.50, START - 7200),     # $0.50, big negative
    ] + _close_book(0.90)
    measured, _ = measure_market(fills, MARKET)
    # dominated by the $5,000 leg, not dragged to the midpoint by the $0.50 one
    assert measured["w"]["clv_market"] > 0.35


# --- gates ------------------------------------------------------------------

def _wallet(**over):
    wallet = new_wallet()
    wallet.update({
        "clv_markets": MIN_CLV_MARKETS + 5,
        "pregame_buy_cash": 5000.0,
        "gross_size": 1000.0, "net_abs_size": 900.0,
        "buy_cash": 9000.0, "sell_cash": 1000.0,
        "trades": 100, "dust": 0, "same_second": 0,
    })
    wallet.update(over)
    return wallet


def test_a_clean_directional_wallet_passes_every_gate():
    ok, fails = gate(_wallet())
    assert ok and fails == []


def test_scalper_is_rejected_by_hold_ratio():
    """Buy 500 then sell 500 nets to zero: gross 1000, net_abs 0. A round-
    tripper never held a view, and this is the gate v1 never had."""
    ok, fails = gate(_wallet(gross_size=1000.0, net_abs_size=0.0))
    assert not ok and "scalper" in fails


def test_hold_ratio_boundary_matches_the_declared_constant():
    just_under = _wallet(gross_size=1000.0, net_abs_size=MIN_HOLD_RATIO * 1000.0 - 1)
    just_over = _wallet(gross_size=1000.0, net_abs_size=MIN_HOLD_RATIO * 1000.0 + 1)
    assert "scalper" in gate(just_under)[1]
    assert "scalper" not in gate(just_over)[1]


def test_dust_and_same_second_gates_reject_automation():
    ok, fails = gate(_wallet(trades=100, dust=50, same_second=40))
    assert not ok
    assert "dust" in fails and "same_second" in fails


def test_sell_side_wallet_is_rejected_by_buy_dominance():
    ok, fails = gate(_wallet(buy_cash=1000.0, sell_cash=9000.0))
    assert not ok and "sell_side" in fails
    assert 1000.0 / 10000.0 < MIN_BUY_DOMINANCE


def test_small_sample_wallet_is_rejected_before_ranking_not_after():
    """v1's cohort ROI flipped from -2.5% to +14.6% on one 9-market wallet
    because the filter ran after ranking. Here it cannot be ranked at all."""
    ok, fails = gate(_wallet(clv_markets=9))
    assert not ok and "sample" in fails


def test_gate_reports_every_failure_not_just_the_first():
    ok, fails = gate(_wallet(clv_markets=1, pregame_buy_cash=1.0,
                             gross_size=100.0, net_abs_size=0.0))
    assert not ok
    assert {"sample", "stake", "scalper"} <= set(fails)


def test_rank_by_clv_excludes_gated_wallets_and_counts_the_reason():
    wallets = {
        "good": _wallet(clv_num=100.0, clv_stake=1000.0,
                        obs=[(f"m{i}", 100.0, 10.0) for i in range(MIN_CLV_MARKETS + 5)]),
        "tiny": _wallet(clv_markets=2, clv_num=500.0, clv_stake=100.0,
                        obs=[("m1", 50.0, 250.0), ("m2", 50.0, 250.0)]),
    }
    rows, reasons = rank_by_clv(wallets)
    assert [r["wallet"] for r in rows] == ["good"]
    assert reasons["sample"] == 1
    assert reasons["__rejected__"] == 1


# --- uncertainty ------------------------------------------------------------

def test_bootstrap_ci_brackets_the_point_estimate():
    obs = [(f"m{i}", 100.0, 5.0) for i in range(50)]
    lo, hi = bootstrap_clv_ci(obs, rounds=300)
    assert lo <= 0.05 <= hi


def test_bootstrap_ci_is_undefined_below_two_markets():
    """One market is one cluster; a CI from it would be fabricated."""
    lo, hi = bootstrap_clv_ci([("m1", 100.0, 5.0)])
    assert lo != lo and hi != hi  # NaN


def test_bootstrap_ci_widens_when_markets_disagree():
    consistent = [(f"m{i}", 100.0, 5.0) for i in range(40)]
    noisy = [(f"m{i}", 100.0, 50.0 if i % 2 else -40.0) for i in range(40)]
    c_lo, c_hi = bootstrap_clv_ci(consistent, rounds=500)
    n_lo, n_hi = bootstrap_clv_ci(noisy, rounds=500)
    assert (n_hi - n_lo) > (c_hi - c_lo)


def test_thin_final_window_falls_back_to_the_last_pregame_trades():
    """A market with almost no action in the final hour still needs a close.
    The fallback widens to the last few pregame trades rather than returning
    None -- but it does reach back in time, so a wallet can end up partly
    setting the benchmark it is scored against. That is a real limitation of
    thin markets, pinned here so it stays visible rather than surprising."""
    fills = [
        fill("w", REF, "BUY", 100, 0.40, START - 86400),
        fill("mm", REF, "BUY", 100, 0.60, START - 300),
    ]
    close = closing_price(fills, REF, START, [REF, OTHER])
    assert close is not None
    assert close == pytest.approx(0.50)  # both trades, not just the in-window one
