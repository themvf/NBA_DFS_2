import pytest

from ingest.cfb_rosters import position_group, summarize_roster


def test_position_groups_cover_core_continuity_units() -> None:
    assert position_group("QB") == "QB"
    assert position_group("OT") == "OL"
    assert position_group("WR") == "PASS_CATCHER"
    assert position_group("CB") == "SECONDARY"
    assert position_group(None) is None


def test_roster_summary_tracks_ids_not_names() -> None:
    players = [
        {"id": "qb-1", "position": "QB"},
        {"id": "ol-1", "position": "OT"},
        {"id": "wr-1", "position": "WR"},
    ]
    summary = summarize_roster(
        players, {"qb-1", "ol-1"}, {"percentPPA": 0.7}, 820.5,
        [{"rating": 0.91}], [{"rating": 0.85}],
    )
    assert summary["returning_roster_count"] == 2
    assert summary["roster_continuity_pct"] == pytest.approx(2 / 3)
    assert summary["returning_quarterbacks"] == 1
    assert summary["returning_offensive_line"] == 1
    assert summary["transfer_rating_in"] == 0.91
    assert summary["talent_composite"] == 820.5
    assert summary["availability_source"] == "not_provided_by_cfbd_roster"
