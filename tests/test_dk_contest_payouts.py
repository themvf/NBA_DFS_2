"""DraftKings prize table + complete standings -> score-to-payout curve."""

from model.dk_contest_payouts import curves, paid_rows, payout_table
from model.statapi_contest import payout_for

# The shape DraftKings' /contests/v1/contests/{id} returns (2026-10-21, contest 196786690).
SUMMARY = [
    {"minPosition": 1, "maxPosition": 1, "payoutDescriptions": [{"value": 100.0}]},
    {"minPosition": 2, "maxPosition": 2, "payoutDescriptions": [{"value": 50.0}]},
    {"minPosition": 3, "maxPosition": 5, "payoutDescriptions": [{"value": 20.0}]},
    {"minPosition": 6, "maxPosition": 8, "payoutDescriptions": [{"value": 0}]},
]


def test_payout_table_drops_unpaid_tiers_and_sums_descriptions():
    two = [{"minPosition": 1, "maxPosition": 1,
            "payoutDescriptions": [{"value": 10.0}, {"value": 5.0}]}]
    assert payout_table(two) == [[1, 1, 15.0]]
    assert payout_table(SUMMARY) == [[1, 1, 100.0], [2, 2, 50.0], [3, 5, 20.0]]
    assert payout_table(None) == []


def test_tied_entries_split_the_places_they_occupy():
    table = payout_table(SUMMARY)
    # 1st alone; two tie for places 2-3 -> (50 + 20) / 2 each.
    rows = paid_rows([90, 80, 80, 70, 60, 50], table)
    assert [r["payout"] for r in rows] == [100.0, 35.0, 35.0, 20.0, 20.0, 0.0]


def test_curve_pays_what_draftkings_paid_and_nothing_below_the_cash_line():
    out = curves([90, 80, 80, 70, 60, 50, 40, 30], SUMMARY, 20)
    curve = out["payout_curve"]
    assert out["reason"] is None
    assert curve["entry_fee"] == 20.0 and curve["paid_entries"] == 5
    assert curve["min_cash_score"] == 60
    assert payout_for(curve, 95) == 100.0
    assert payout_for(curve, 85) == 35.0      # between the tied 80s and 1st
    assert payout_for(curve, 65) == 20.0
    assert payout_for(curve, 55) == 0.0
    assert out["score_curve"][0] == [1, 90]


def test_no_prize_table_means_no_payout_curve_with_a_reason():
    out = curves([90, 80], None, 20)
    assert out["payout_curve"] is None
    assert out["reason"] == "no prize table from DraftKings"
    assert out["score_curve"] is not None
