"""nfl-dfs-range-split-v1 shipped: the weekly ratios are the study's split_range at m = 1."""
from ingest.nfl_dfs_range_ratios import MIN_PLAYERS, ratios
from research.nfl_dfs_range_split import split_range


def test_ratios_equal_split_range_at_unit_projection():
    assert ratios(11.1683, 4.8326, 22.6431) == (round(4.8326 / 11.1683, 6), round(22.6431 / 11.1683, 6))
    m, m17, p10, p90 = 14.34, 11.17, 4.83, 22.64
    r10, r90 = ratios(m17, p10, p90)
    lo, hi = split_range(m, m17, p10, p90)
    assert abs(m * r10 - lo) < 1e-4 and abs(m * r90 - hi) < 1e-4


def test_non_positive_long_memory_mean_has_no_ratio():
    assert ratios(0.0, -1.0, 3.0) is None
    assert ratios(-2.0, -3.0, 1.0) is None


def test_floor_guards_a_partial_load():
    assert MIN_PLAYERS >= 200
