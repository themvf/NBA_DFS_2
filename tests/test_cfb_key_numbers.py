from __future__ import annotations

from collections import Counter

from model.cfb_key_numbers import (
    DETECTOR_KEY_NUMBERS, audit_counts, margin_counts, rank_table,
)


def _rows(*margins):
    return [{"home_score": 20 + m, "away_score": 20} for m in margins]


def test_margin_is_absolute_so_the_road_favorite_counts_the_same() -> None:
    rows = [{"home_score": 17, "away_score": 24}, {"home_score": 24, "away_score": 17}]
    assert margin_counts(rows) == Counter({7: 2})


def test_ties_and_unplayed_games_are_excluded_not_counted_as_zero() -> None:
    rows = [
        {"home_score": 21, "away_score": 21},   # tie: crosses nothing
        {"home_score": None, "away_score": 14},  # unplayed
        {"home_score": 10, "away_score": 3},
    ]
    assert margin_counts(rows) == Counter({7: 1})


def test_a_number_outside_the_set_with_more_mass_is_reported_as_a_defect() -> None:
    """The specific failure this audit exists to catch."""
    counts = Counter({3: 50, 7: 40, 10: 30, 14: 5, 21: 25})
    result = audit_counts(counts)
    assert result["verdict"] == "SET_INCOMPLETE"
    assert [r["margin"] for r in result["outranking_excluded_numbers"]] == [21]


def test_a_set_that_is_the_real_top_four_is_confirmed() -> None:
    counts = Counter({3: 50, 7: 40, 10: 30, 14: 25, 21: 20, 17: 10})
    result = audit_counts(counts)
    assert result["verdict"] == "SET_CONFIRMED"
    assert result["outranking_excluded_numbers"] == []
    assert result["detector_set"]["3"]["rank"] == 1


def test_stability_counts_seasons_where_the_exact_top_four_holds() -> None:
    per_season = {
        2023: Counter({3: 9, 7: 8, 10: 7, 14: 6, 21: 1}),   # exact match
        2024: Counter({3: 9, 7: 8, 21: 7, 14: 6, 10: 1}),   # 21 displaces 10
    }
    pooled = Counter()
    for c in per_season.values():
        pooled.update(c)
    stability = audit_counts(pooled, per_season)["stability"]
    assert stability == {"seasons": 2, "exact_top4_match": 1, "top2_is_3_and_7": 2}


def test_empty_input_says_no_data_rather_than_confirming_the_set() -> None:
    assert audit_counts(Counter())["verdict"] == "NO_DATA"


def test_rank_table_flags_membership_and_shares_sum_sanely() -> None:
    table = rank_table(margin_counts(_rows(3, 3, 7, 21)), top=3)
    assert table[0] == {"rank": 1, "margin": 3, "games": 2, "share_pct": 50.0,
                        "in_detector_set": True}
    assert any(r["margin"] == 21 and not r["in_detector_set"] for r in table)


def test_the_audit_never_mutates_the_live_detector_set() -> None:
    audit_counts(Counter({99: 100}))
    assert DETECTOR_KEY_NUMBERS == (3, 7, 10, 14)
