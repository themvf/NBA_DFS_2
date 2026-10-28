"""The Monday grade's field-vs-field comparison (research/nfl_ownership_three_way.py)."""
import pandas as pd

from research.nfl_ownership_three_way import compare_fields


def frame(rows):
    return pd.DataFrame(rows, columns=["name", "position", "salary", "actual"])


def test_compare_fields_reports_gap_both_ways_and_ignores_players_missing_from_one_field():
    ref = frame([("A", "QB", 7000, 30.0), ("B", "RB", 6000, 10.0), ("C", "WR", 5000, 2.0), ("Z", "TE", 3000, 1.0)])
    other = frame([("A", "QB", 7000, 45.0), ("B", "RB", 6000, 4.0), ("C", "WR", 5000, 2.0)])
    out = compare_fields(ref, other, top=1)
    assert out["n"] == 3
    assert out["chalk_ref"] == 30.0 and out["chalk_other"] == 45.0
    assert out["more_in_other"][0]["name"] == "A" and out["more_in_other"][0]["gap"] == 15.0
    assert out["more_in_reference"][0]["name"] == "B" and out["more_in_reference"][0]["gap"] == -6.0
    assert out["mae"] == round((15 + 6 + 0) / 3, 2)
