from __future__ import annotations

from ingest.polymarket_wallet_pilot_common import (
    _new_agg,
    rank_wallets,
    rank_wallets_by_edge,
    settle_market,
    wilson_lower_bound,
)


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


def test_settle_market_tracks_all_buys_separately_from_winning_buys():
    # Regression test for the 2026-08-19 finding: a "winning buys only"
    # average is a biased proxy that hides what a wallet paid on its
    # LOSING buys. settle_market returns raw (size, cash) totals for both
    # -- the caller sums these across markets before dividing (see
    # test_rank_wallets_weights_entry_price_by_dollars_not_by_market_count
    # for why dividing per-market and then averaging ratios is wrong).
    fills = [
        {"proxyWallet": "w1", "outcome": "A", "side": "BUY", "size": "10", "price": "0.9"},   # wins
        {"proxyWallet": "w1", "outcome": "B", "side": "BUY", "size": "10", "price": "0.5"},   # loses
    ]
    settled = settle_market(fills, winner="A")
    row = settled["w1"]
    assert row["win_buy_size"] == 10 and row["win_buy_cash"] == 9.0   # only the winning buy
    assert row["buy_size"] == 20 and row["buy_cash"] == 14.0          # both buys


def test_rank_wallets_weights_entry_price_by_dollars_not_by_market_count():
    # Regression test for a real bug caught live 2026-08-19: a wallet with
    # ONE $2,000 bet at a normal price and 140 one-dollar longshot bets
    # showed an "average entry price" near the tiny bets' price when
    # per-market ratios were averaged unweighted -- massively understating
    # what the wallet actually, mostly, paid. The correct average sums
    # dollars-in and shares-in across ALL markets first, then divides once.
    agg = _new_agg()
    agg["markets"] = 2
    agg["wins"] = 1
    agg["pnl"] = 0.0
    agg["cost"] = 2001.0
    # Market 1: a real $2,000 position at price 0.50 (1000 shares)
    agg["buy_size"] += 4000  # 2000/0.50
    agg["buy_cash"] += 2000.0
    # Market 2: a $1 longshot bet at price 0.02 (50 shares)
    agg["buy_size"] += 50
    agg["buy_cash"] += 1.0
    wallet_stats = {"w1": agg}
    qualified = rank_wallets(wallet_stats, min_markets=2)
    row = qualified[0]
    # Dollar-weighted average should sit close to 0.50 (the real position),
    # not collapse toward 0.02 the way an unweighted mean of [0.50, 0.02]
    # would (~0.26).
    assert row["avg_entry_price"] > 0.45


def test_rank_wallets_by_edge_penalizes_favorite_only_betting():
    # The core finding this metric exists to fix: a wallet with a very
    # high win rate but an equally high entry price (buying near-certain
    # favorites at near-certain prices) has ~zero real edge, and must not
    # outrank a wallet with a lower win rate but a much cheaper entry price
    # and therefore genuine edge over what it paid.
    favorite_bettor = {
        "wallet": "w1", "name": "FavBettor", "markets": 100, "wins": 99,
        "win_rate": 0.99, "pnl_usd": 10.0, "cost_usd": 100000.0, "roi": 0.0001,
        "avg_entry_price": 0.99, "avg_winner_entry_price": 0.99,
    }
    edge_bettor = {
        "wallet": "w2", "name": "EdgeBettor", "markets": 100, "wins": 65,
        "win_rate": 0.65, "pnl_usd": 5000.0, "cost_usd": 50000.0, "roi": 0.1,
        "avg_entry_price": 0.55, "avg_winner_entry_price": 0.55,
    }
    ranked = rank_wallets_by_edge([favorite_bettor, edge_bettor])
    assert ranked[0]["wallet"] == "w2"
    assert ranked[0]["edge_lower_bound"] > ranked[1]["edge_lower_bound"]
    # The favorite-only bettor's edge should be small (near zero), not negative-huge
    # or falsely large -- it's genuinely close to fairly priced.
    fav_row = next(r for r in ranked if r["wallet"] == "w1")
    assert -0.1 < fav_row["edge_lower_bound"] < 0.1


def test_rank_wallets_by_edge_filters_out_tiny_stake_lottery_tickets():
    # Regression test for a real bug caught live 2026-08-19: without a
    # min_cost floor, the #1 MLB wallet by edge had 51 markets, 96% win
    # rate, and $29.95 total cost -- a deep-longshot lottery-ticket buyer
    # whose tiny average entry price makes any win at all look like a huge
    # numeric edge. Same failure mode the ROI leaderboard's min_cost floor
    # already guards against.
    lottery_ticket_buyer = {
        "wallet": "w1", "name": "TinyStakes", "markets": 51, "wins": 49,
        "win_rate": 0.96, "pnl_usd": 28.13, "cost_usd": 29.95, "roi": 0.94,
        "avg_entry_price": 0.02, "avg_winner_entry_price": 0.02,
    }
    real_wallet = {
        "wallet": "w2", "name": "RealStakes", "markets": 100, "wins": 65,
        "win_rate": 0.65, "pnl_usd": 5000.0, "cost_usd": 50000.0, "roi": 0.1,
        "avg_entry_price": 0.55, "avg_winner_entry_price": 0.55,
    }
    ranked = rank_wallets_by_edge([lottery_ticket_buyer, real_wallet])
    wallets = [r["wallet"] for r in ranked]
    assert "w1" not in wallets
    assert wallets == ["w2"]
