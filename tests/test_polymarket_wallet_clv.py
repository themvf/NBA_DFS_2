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
    measured, close, _fb = measure_market(fills, MARKET)
    assert close == pytest.approx(0.99, abs=0.001)
    assert measured["payer"]["clv_market"] == pytest.approx(0.0, abs=0.01)
    assert measured["sharp"]["clv_market"] > 0.5


def test_clv_is_negative_when_the_market_moves_away_from_the_buyer():
    """CLV must be able to be negative. A metric that can only be zero-or-
    positive is not measuring anything."""
    fills = [
        fill("late", REF, "BUY", 100, 0.80, START - 7200),
    ] + _close_book(0.30)
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["late"]["clv_market"] < 0


def test_clv_scores_pregame_buys_only_never_sells():
    """Scoring a sell would credit a scalper for unwinding into a move it did
    not predict."""
    fills = [
        fill("scalper", REF, "SELL", 100, 0.20, START - 7200),
    ] + _close_book(0.60)
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["scalper"]["clv_shares"] == 0
    assert measured["scalper"]["clv_market"] is None


def test_in_play_buys_are_excluded_from_clv_but_still_counted_behaviourally():
    fills = [
        fill("w", REF, "BUY", 100, 0.50, START - 7200),
        fill("w", REF, "BUY", 100, 0.10, START + 3600),
    ] + _close_book(0.50)
    measured, _, _fb = measure_market(fills, MARKET)
    entry = measured["w"]
    assert entry["inplay_trades"] == 1
    assert entry["pregame_trades"] == 1
    # only the pregame 100 shares are scored, not the in-play 100
    assert entry["clv_shares"] == pytest.approx(100.0)


def test_clv_is_dollar_weighted_within_a_market():
    """v1 bug #4 on a different axis: averaging per-trade ratios would let a
    $1 trade count as much as a $10,000 one."""
    fills = [
        fill("w", REF, "BUY", 10000, 0.50, START - 7200),   # $5,000, big CLV
        fill("w", OTHER, "BUY", 1, 0.50, START - 7200),     # $0.50, big negative
    ] + _close_book(0.90)
    measured, _, _fb = measure_market(fills, MARKET)
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
        "good": _wallet(clv_num=100.0, clv_shares=1000.0,
                        obs=[(f"m{i}", 100.0, 10.0) for i in range(MIN_CLV_MARKETS + 5)]),
        "tiny": _wallet(clv_markets=2, clv_num=500.0, clv_shares=100.0,
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


# --- walk-forward -----------------------------------------------------------

def _wf_wallets(good_holdout, bad_holdout):
    """Four wallets whose dev CLV ranks them 1-4, so the top half is
    well-defined. The two that rank best in dev then diverge: one keeps its
    CLV in the holdout half, the other reverses."""
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    hold = lambda v: [(f"h{i}", 100.0, v) for i in range(20)]
    return {
        "persists": _wallet(obs=dev(40.0) + hold(good_holdout), clv_num=1.0, clv_shares=100.0),
        "fades": _wallet(obs=dev(30.0) + hold(bad_holdout), clv_num=1.0, clv_shares=100.0),
        "rest_a": _wallet(obs=dev(10.0) + hold(0.0), clv_num=1.0, clv_shares=100.0),
        "rest_b": _wallet(obs=dev(5.0) + hold(0.0), clv_num=1.0, clv_shares=100.0),
    }


def test_walk_forward_scores_the_holdout_half_not_the_half_it_selected_on():
    from ingest.polymarket_wallet_clv import walk_forward
    wallets = _wf_wallets(good_holdout=15.0, bad_holdout=-15.0)
    rows, _ = rank_by_clv(wallets)
    dev_ids = {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)}
    holdout_ids = {f"h{i}" for i in range(20)}
    wf = walk_forward(wallets, rows, dev_ids, holdout_ids, top_n=2)
    assert wf["available"]
    assert wf["top_n"] == 2  # 4 // 2, a real comparison group remains
    # both were selected on dev, but only one keeps positive holdout CLV
    assert wf["persisted"] == 1
    # holdout averages the two, landing near zero -- NOT near dev's +0.35
    assert abs(wf["selected_holdout_clv"]) < 0.05


def test_walk_forward_is_unavailable_without_a_chronological_split():
    from ingest.polymarket_wallet_clv import walk_forward
    wallets = _wf_wallets(10.0, 10.0)
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, set(), set())
    assert not wf["available"] and "split" in wf["reason"]


def test_walk_forward_reports_a_selection_gap_against_the_unselected_rest():
    """The absolute holdout level is not the test -- if every eligible wallet
    beats the close, selecting the top 20 proves nothing. The gap is the test."""
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    hold = lambda v: [(f"h{i}", 100.0, v) for i in range(20)]
    wallets = {
        "top": _wallet(obs=dev(30.0) + hold(10.0), clv_num=1.0, clv_shares=100.0),
        "mid": _wallet(obs=dev(20.0) + hold(10.0), clv_num=1.0, clv_shares=100.0),
        "low": _wallet(obs=dev(5.0) + hold(10.0), clv_num=1.0, clv_shares=100.0),
    }
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=1)
    # everyone earns the same holdout CLV, so selection adds nothing
    assert wf["selected_holdout_clv"] == pytest.approx(wf["rest_holdout_clv"])


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    """Stands in for the per-thread requests.Session the fetcher builds."""

    def __init__(self, pager):
        self._pager = pager

    def get(self, url, params=None, timeout=None):
        payload = self._pager(params.get("offset", 0))
        if payload == 400:
            return _FakeResponse(None, status_code=400)
        return _FakeResponse(payload)


# --- fill-tape depth --------------------------------------------------------

def test_tape_ceiling_matches_the_data_apis_actual_limit():
    """Verified live 2026-08-27: offset 10,000 returns a row, 10,500 returns
    HTTP 400. The shared engine caps at 6,000, and because the API only
    serves newest-first, that missing 40% is the OLDEST 40% -- i.e. exactly
    the pregame window this module measures in. Lowering this constant
    silently shrinks CLV coverage rather than failing."""
    from ingest.polymarket_wallet_clv import MAX_TRADES_PER_MARKET
    from ingest.polymarket_wallet_pilot_common import MAX_TRADES_PER_MARKET as SHARED
    assert MAX_TRADES_PER_MARKET == 10000
    assert MAX_TRADES_PER_MARKET > SHARED


def test_fetch_reports_truncation_rather_than_swallowing_it(monkeypatch):
    """A truncated tape may have lost its whole pregame window, and a market
    contributing no CLV must be distinguishable from one where nobody traded
    before the match."""
    import ingest.polymarket_wallet_clv as mod

    page = [{"timestamp": 1, "size": 1, "price": 0.5} for _ in range(mod.TRADES_PAGE)]
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    monkeypatch.setattr(mod, "_session", lambda: _FakeSession(lambda _o: page))
    fills, hit = mod.fetch_market_fills("0x")
    assert hit is True
    assert len(fills) == mod.MAX_TRADES_PER_MARKET

    monkeypatch.setattr(mod, "_session", lambda: _FakeSession(lambda _o: page[:10]))
    fills, hit = mod.fetch_market_fills("0x")
    assert hit is False and len(fills) == 10


def test_http_400_is_the_offset_ceiling_and_keeps_the_partial_tape(monkeypatch):
    """400 is what the API returns past offset 10,000 -- a real ceiling, so
    the partial tape is kept and flagged rather than dropped."""
    import ingest.polymarket_wallet_clv as mod

    calls = {"n": 0}

    def pager(_offset):
        calls["n"] += 1
        if calls["n"] > 2:
            return 400
        return [{"timestamp": 1, "size": 1, "price": 0.5} for _ in range(mod.TRADES_PAGE)]

    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_session", lambda: _FakeSession(pager))
    fills, hit = mod.fetch_market_fills("0x")
    assert hit is True
    assert len(fills) == 2 * mod.TRADES_PAGE


def test_transient_failure_is_retried_then_raised_never_called_truncation(monkeypatch):
    """Two concurrent scans produced enough rate-limit errors that 794 of
    2,400 markets were recorded as truncated, against 1 in the identical
    scan run alone -- 45% of the sample discarded and reported as a property
    of Polymarket. A transient failure must never masquerade as a ceiling."""
    import ingest.polymarket_wallet_clv as mod

    def always_fails(_offset):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_session", lambda: _FakeSession(always_fails))
    with pytest.raises(mod.TapeIncomplete):
        mod.fetch_market_fills("0x")


def test_transient_failure_that_recovers_completes_the_tape(monkeypatch):
    import ingest.polymarket_wallet_clv as mod

    calls = {"n": 0}

    def flaky(_offset):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("timeout")
        return [{"timestamp": 1, "size": 1, "price": 0.5} for _ in range(10)]

    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_session", lambda: _FakeSession(flaky))
    fills, hit = mod.fetch_market_fills("0x")
    assert hit is False and len(fills) == 10


def test_walk_forward_never_selects_the_entire_population():
    """Requesting the top 20 from 7 wallets left no unselected remainder, so
    the selection-gap comparison silently did not run while a confident-
    looking holdout number still printed. Observed live on the first
    complete tennis run."""
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    hold = [(f"h{i}", 100.0, 5.0) for i in range(20)]
    wallets = {
        f"w{k}": _wallet(obs=dev(30.0 - k) + hold, clv_num=1.0, clv_shares=100.0)
        for k in range(7)
    }
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=20)
    assert wf["available"]
    assert wf["top_n"] == 3            # 7 // 2, not 20
    assert wf["top_n_requested"] == 20
    assert wf["rest_holdout_obs"] > 0  # a comparison group actually exists


def test_walk_forward_refuses_to_split_a_single_wallet():
    from ingest.polymarket_wallet_clv import walk_forward
    dev = [(f"d{i}", 100.0, 20.0) for i in range(MIN_CLV_MARKETS + 5)]
    hold = [(f"h{i}", 100.0, 5.0) for i in range(20)]
    wallets = {"only": _wallet(obs=dev + hold, clv_num=1.0, clv_shares=100.0)}
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=20)
    assert not wf["available"] and "too few" in wf["reason"]


def test_bootstrap_clusters_by_market_across_wallets_not_by_row():
    """Two wallets in the same match faced one price path and one close, so
    their CLV shares a common shock. Resampling rows would treat them as
    independent and understate the interval on exactly the pooled numbers a
    conclusion rests on."""
    # 20 markets, each carrying 10 wallets that all moved together.
    clustered = [(f"m{m}", 100.0, 40.0 if m % 2 else -40.0)
                 for m in range(20) for _ in range(10)]
    lo, hi = bootstrap_clv_ci(clustered, rounds=800)
    # 200 rows but only 20 independent clusters: the interval must stay wide
    assert (hi - lo) > 0.10


def test_bootstrap_is_unchanged_for_a_single_wallet():
    """One row per market, so clustering is a no-op -- the change must not
    move any single-wallet interval."""
    obs = [(f"m{i}", 100.0, 5.0 if i % 3 else -2.0) for i in range(40)]
    lo, hi = bootstrap_clv_ci(obs, rounds=800)
    assert lo <= sum(o[2] for o in obs) / sum(o[1] for o in obs) <= hi


def test_bootstrap_needs_two_distinct_markets_not_two_rows():
    """Ten wallets in one match is one cluster, not ten observations."""
    one_market = [("m1", 100.0, 5.0) for _ in range(10)]
    lo, hi = bootstrap_clv_ci(one_market)
    assert lo != lo and hi != hi  # NaN


def test_walk_forward_flags_a_result_carried_by_one_wallet():
    """CLV is dollar-weighted, so a wallet staking most of the group's money
    IS the group's result. This project's MLB underdog spec sets a 25% bar
    on any single participant and requires leave-one-out to survive."""
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    wallets = {
        # one whale carrying a huge positive holdout stake
        "whale": _wallet(obs=dev(30.0) + [(f"h{i}", 10000.0, 500.0) for i in range(20)],
                         clv_num=1.0, clv_shares=100.0),
        "small_a": _wallet(obs=dev(20.0) + [(f"h{i}", 10.0, -0.5) for i in range(20)],
                           clv_num=1.0, clv_shares=100.0),
        "small_b": _wallet(obs=dev(10.0) + [(f"h{i}", 10.0, -0.5) for i in range(20)],
                           clv_num=1.0, clv_shares=100.0),
        "small_c": _wallet(obs=dev(5.0) + [(f"h{i}", 10.0, -0.5) for i in range(20)],
                           clv_num=1.0, clv_shares=100.0),
    }
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=2)
    assert wf["dominant_wallet"] == "whale"
    assert wf["dominant_stake_share"] > 0.25
    # headline is strongly positive only because of the whale
    assert wf["selected_holdout_clv"] > 0
    # removing it reverses the sign -- the finding was the one wallet
    assert wf["leave_one_out_clv"] < 0


def test_favourite_longshot_check_separates_drift_from_skill():
    """If longshots drift down and favourites drift up, a wallet that simply
    backs favourites earns persistent CLV with no predictive skill, and
    selecting on early CLV would pick exactly those wallets. That is a fact
    about the market, not about the wallet -- and you would trade the drift
    directly rather than follow anyone. The report must be able to see it."""
    from ingest.polymarket_wallet_clv import measure_market
    start = START
    fills = [
        fill("fav_backer", REF, "BUY", 100, 0.80, start - 7200),
        fill("dog_backer", OTHER, "BUY", 100, 0.20, start - 7200),
    ] + _close_book(0.85)
    measured, _, _fb = measure_market(fills, MARKET)
    # entry price is recorded in each wallet's own terms, so the two are
    # distinguishable even though they traded the same match
    assert measured["fav_backer"]["pregame_fav_cash"] > 0
    assert measured["dog_backer"]["pregame_fav_cash"] == 0
    fav = measured["fav_backer"]
    assert fav["pregame_buy_cash"] / fav["pregame_buy_size"] == pytest.approx(0.80)


def test_walk_forward_reports_entry_price_for_selected_and_rest():
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    hold = [(f"h{i}", 100.0, 5.0) for i in range(20)]
    wallets = {}
    for k, (dv, entry) in enumerate([(30.0, 0.85), (25.0, 0.82), (5.0, 0.30), (3.0, 0.25)]):
        w = _wallet(obs=dev(dv) + hold, clv_num=1.0, clv_shares=100.0)
        w["pregame_buy_size"] = 20000.0
        w["pregame_buy_cash"] = 20000.0 * entry   # must clear MIN_PREGAME_STAKE
        w["pregame_fav_cash"] = 20000.0 * entry if entry > 0.5 else 0.0
        wallets[f"w{k}"] = w
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=2)
    # the two selected are favourite-backers, the rest are longshot-backers
    assert wf["selected_avg_entry"] > wf["rest_avg_entry"] + 0.03
    assert wf["selected_fav_share"] == pytest.approx(1.0)
    assert wf["rest_fav_share"] == pytest.approx(0.0)


# --- self-impact on the close -----------------------------------------------

def test_a_wallet_is_not_scored_against_a_close_it_helped_set():
    """Buy early, then buy again inside the close window, and that late
    buying lifts the benchmark the early buy is measured against. Removing
    the wallet's own fills from its own close is what stops a large wallet
    manufacturing its own CLV."""
    # Late activity is SELLS so it sets the close without itself being
    # scored, isolating the self-impact effect from ordinary overpaying.
    fills = [
        fill("selfy", REF, "BUY", 100, 0.50, START - 7200),
        fill("selfy", REF, "SELL", 5000, 0.90, START - 300),
        fill("selfy", REF, "SELL", 5000, 0.90, START - 200),
        # an independent quote also in the window
        fill("other", REF, "BUY", 10000, 0.50, START - 100),
    ]
    measured, close, _fb = measure_market(fills, MARKET)
    # the market-wide close is dragged up by selfy's own volume
    assert close > 0.65
    # but selfy is scored against the independent 0.50, so its 0.50 buy earns
    # ~nothing instead of the ~+0.20 the self-inflated close would have paid
    assert measured["selfy"]["clv_market"] == pytest.approx(0.0, abs=0.01)


def test_wallet_that_is_the_entire_close_cannot_be_scored():
    """With no independent trade in the window there is no benchmark left
    once the wallet is removed, so it contributes no observation rather than
    being scored against itself."""
    fills = [
        fill("only", REF, "BUY", 100, 0.40, START - 7200),
        fill("only", REF, "BUY", 100, 0.90, START - 300),
        fill("only", REF, "BUY", 100, 0.90, START - 200),
        fill("only", REF, "BUY", 100, 0.90, START - 100),
    ]
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["only"]["clv_market"] is None


def test_independent_wallets_are_unaffected_by_the_exclusion():
    """A wallet with no fills in the close window is scored against the same
    number as before -- the fix must not move ordinary observations."""
    fills = [fill("early", REF, "BUY", 100, 0.40, START - 7200)] + _close_book(0.60)
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["early"]["clv_market"] == pytest.approx(0.20, abs=0.001)


# --- the gap is the test statistic ------------------------------------------

def test_gap_ci_excludes_zero_when_selection_genuinely_separates():
    from ingest.polymarket_wallet_clv import bootstrap_gap_ci
    sel = [(f"m{i}", 100.0, 2.0) for i in range(60)]
    rest = [(f"m{i}", 100.0, -2.0) for i in range(60)]
    lo, hi = bootstrap_gap_ci(sel, rest, rounds=600)
    assert lo > 0


def test_gap_ci_includes_zero_when_both_groups_do_the_same_thing():
    """A selected group beating the close means nothing if everyone else
    beats it equally -- that is a market fact, not a selection result."""
    from ingest.polymarket_wallet_clv import bootstrap_gap_ci
    sel = [(f"m{i}", 100.0, 5.0) for i in range(60)]
    rest = [(f"m{i}", 100.0, 5.0) for i in range(60)]
    lo, hi = bootstrap_gap_ci(sel, rest, rounds=600)
    assert lo <= 0 <= hi


def test_gap_ci_cancels_the_shared_market_shock():
    """Both groups are drawn on the SAME resampled markets, so a period where
    the whole market drifted must not inflate the gap's interval."""
    from ingest.polymarket_wallet_clv import bootstrap_gap_ci
    # huge market-level swings, but a constant +0.02 difference between groups
    sel, rest = [], []
    for i in range(60):
        shock = 50.0 if i % 2 else -50.0
        sel.append((f"m{i}", 100.0, shock + 2.0))
        rest.append((f"m{i}", 100.0, shock))
    lo, hi = bootstrap_gap_ci(sel, rest, rounds=600)
    assert lo > 0            # the shared shock cancels
    assert (hi - lo) < 0.02  # and the interval stays tight


# --- weighting ---------------------------------------------------------------

def test_offsetting_share_positions_net_to_zero_clv():
    """The bug that invalidated the first MLB result.

    Dollar-weighting a PRICE MOVE drops the 1/p that converts dollars to
    shares, so two wallets holding perfectly offsetting share positions did
    not cancel: true economic return 0.0000, share-weighted CLV 0.0000, but
    the old form reported +0.0400. The residual was a pure function of price
    level, handing free CLV to whoever's dollars sat on favourites -- a
    persistent style, so it survived the walk-forward as fake 'skill'."""
    fills = [
        fill("fav", REF, "BUY", 1000, 0.90, START - 7200),
        fill("dog", OTHER, "BUY", 1000, 0.10, START - 7200),
    ] + _close_book(0.95, size=50000)
    measured, close, _fb = measure_market(fills, MARKET)
    assert close == pytest.approx(0.95, abs=0.001)
    num = sum(measured[w]["clv_num"] for w in ("fav", "dog"))
    shares = sum(measured[w]["clv_shares"] for w in ("fav", "dog"))
    assert num / shares == pytest.approx(0.0, abs=1e-9)


def test_clv_is_antisymmetric_between_the_two_sides():
    """Buying either side of the same binary market at the vig-free price
    must give equal and opposite CLV per share. Without this, CLV is not
    zero-sum and a market maker CAN win it, which is the entire premise."""
    fills = [
        fill("a", REF, "BUY", 500, 0.30, START - 7200),
        fill("b", OTHER, "BUY", 500, 0.70, START - 7200),
    ] + _close_book(0.45, size=40000)
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["a"]["clv_market"] == pytest.approx(-measured["b"]["clv_market"], abs=1e-9)


def test_clv_weight_is_shares_not_dollars():
    """clv_shares must accumulate SIZE. If it ever holds notional again the
    weighting bug is back, silently."""
    fills = [fill("w", REF, "BUY", 250, 0.40, START - 7200)] + _close_book(0.50)
    measured, _, _fb = measure_market(fills, MARKET)
    assert measured["w"]["clv_shares"] == pytest.approx(250.0)
    assert measured["w"]["clv_shares"] != pytest.approx(250.0 * 0.40)
    # and the reported CLV is the price move itself, in probability points
    assert measured["w"]["clv_market"] == pytest.approx(0.10, abs=1e-9)


def test_gates_can_be_scaled_for_a_dev_only_window():
    """Eligibility judged on the combined sample requires the wallet to have
    kept trading into the holdout, which preferentially deletes the false
    positives from the SELECTED group and manufactures a gap from pure
    survivorship. Gates therefore run on dev fills, scaled."""
    from ingest.polymarket_wallet_clv import DEV_GATE_FRACTION
    half = _wallet(clv_markets=int(MIN_CLV_MARKETS * DEV_GATE_FRACTION) + 1,
                   pregame_buy_cash=600.0)
    assert gate(half)[1] != []                       # fails at full scale
    assert gate(half, scale=DEV_GATE_FRACTION)[0]    # passes on a half window


def test_rank_by_clv_judges_eligibility_on_dev_but_scores_full_sample():
    from ingest.polymarket_wallet_clv import rank_by_clv as rank
    full = _wallet(clv_num=100.0, clv_shares=1000.0,
                   obs=[(f"m{i}", 100.0, 10.0) for i in range(40)])
    # dev half is thin but clears the scaled floor
    dev = _wallet(clv_markets=16, pregame_buy_cash=600.0)
    rows, _ = rank({"w": full}, {"w": dev})
    assert [r["wallet"] for r in rows] == ["w"]
    # the SCORE still comes from the full-sample accumulator
    assert rows[0]["clv"] == pytest.approx(0.1)
    # a wallet whose dev half is empty is not admitted on holdout strength
    rows2, reasons = rank({"w": full}, {})
    assert rows2 == [] and reasons["sample"] == 1


def test_jackknife_reports_the_worst_single_deletion_not_just_the_whale():
    """Dropping only the largest wallet asks 'does the whale carry it'. The
    question that matters is whether ANY single wallet carries it."""
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    wallets = {}
    # three modest contributors plus one wallet supplying the entire edge,
    # deliberately NOT the largest by stake
    for k, (dv, hv, stake) in enumerate([
        (40.0, 0.2, 100.0), (30.0, 0.2, 100.0), (20.0, 0.2, 100.0),
        (10.0, 60.0, 90.0),
    ]):
        wallets[f"w{k}"] = _wallet(
            obs=dev(dv) + [(f"h{i}", stake, hv) for i in range(20)],
            clv_num=1.0, clv_shares=100.0)
    for k in range(4, 8):
        wallets[f"c{k}"] = _wallet(
            obs=dev(1.0) + [(f"h{i}", 100.0, 0.2) for i in range(20)],
            clv_num=1.0, clv_shares=100.0)
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=4)
    assert wf["jackknife"], "jackknife must run"
    assert len(wf["jackknife"]) == wf["top_n"]
    # the worst deletion is the wallet actually supplying the edge
    assert wf["jackknife_worst"]["gap"] < max(j["gap"] for j in wf["jackknife"])
    assert "jackknife_survives_all" in wf


def test_control_side_concentration_is_measured():
    """If the gap's DENOMINATOR is one wallet it is equally fragile."""
    from ingest.polymarket_wallet_clv import walk_forward
    dev = lambda v: [(f"d{i}", 100.0, v) for i in range(MIN_CLV_MARKETS + 5)]
    wallets = {
        "s1": _wallet(obs=dev(30.0) + [(f"h{i}", 100.0, 5.0) for i in range(20)],
                      clv_num=1.0, clv_shares=100.0),
        "s2": _wallet(obs=dev(25.0) + [(f"h{i}", 100.0, 5.0) for i in range(20)],
                      clv_num=1.0, clv_shares=100.0),
        "whale": _wallet(obs=dev(2.0) + [(f"h{i}", 90000.0, 1.0) for i in range(20)],
                         clv_num=1.0, clv_shares=100.0),
        "tiny": _wallet(obs=dev(1.0) + [(f"h{i}", 10.0, 1.0) for i in range(20)],
                        clv_num=1.0, clv_shares=100.0),
    }
    rows, _ = rank_by_clv(wallets)
    wf = walk_forward(wallets, rows, {f"d{i}" for i in range(MIN_CLV_MARKETS + 5)},
                      {f"h{i}" for i in range(20)}, top_n=2)
    assert wf["control_dominant_share"] > 0.9


def test_wallet_clustered_interval_is_wider_when_the_effect_is_per_wallet():
    """Market clustering cannot see a persistent per-wallet effect: it is a
    within-wallet correlation across that wallet's own markets. If the whole
    signal lives in a few wallets, resampling markets keeps every wallet in
    every draw and reports a spuriously tight interval."""
    from ingest.polymarket_wallet_clv import bootstrap_gap_ci, bootstrap_gap_ci_by_wallet
    # 8 selected wallets: 2 carry a big effect, 6 carry none. Every wallet
    # trades the same 40 markets, so market resampling barely moves.
    sel_by_wallet, rest_by_wallet = {}, {}
    for k in range(8):
        v = 40.0 if k < 2 else 0.0
        sel_by_wallet[f"s{k}"] = [(f"m{i}", 100.0, v) for i in range(40)]
    for k in range(8):
        rest_by_wallet[f"r{k}"] = [(f"m{i}", 100.0, 0.0) for i in range(40)]
    sel_obs = [o for v in sel_by_wallet.values() for o in v]
    rest_obs = [o for v in rest_by_wallet.values() for o in v]
    m_lo, m_hi = bootstrap_gap_ci(sel_obs, rest_obs, rounds=500)
    w_lo, w_hi = bootstrap_gap_ci_by_wallet(sel_by_wallet, rest_by_wallet, rounds=500)
    assert (w_hi - w_lo) > (m_hi - m_lo) * 2
    # and the wallet-clustered one correctly admits it cannot exclude zero
    assert w_lo <= 0 <= w_hi
    assert m_lo > 0   # the market-clustered one is falsely confident


def test_wallet_clustered_interval_needs_two_wallets_per_side():
    from ingest.polymarket_wallet_clv import bootstrap_gap_ci_by_wallet
    lo, hi = bootstrap_gap_ci_by_wallet({"a": [("m", 1.0, 1.0)]}, {"b": [("m", 1.0, 0.0)]})
    assert lo != lo and hi != hi  # NaN
