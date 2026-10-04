from __future__ import annotations

import pytest

from research.nfl_dual_depth_audit import compare_depth, parse_fantasypros_depth


HTML = """
<h1>Washington Commanders</h1>
<table class="position-table">
  <caption>Washington Commanders Depth Charts</caption>
  <tbody>
    <tr><td>WR1</td><td><a class="player-name fp-id-101">Stefon Diggs</a></td></tr>
    <tr><td>WR2</td><td><a class="player-name fp-id-102">Antonio Williams</a></td></tr>
    <tr><td>WR3</td><td><a class="player-name fp-id-103">Treylon Burks</a></td></tr>
  </tbody>
</table>
"""


def test_parse_fantasypros_depth_requires_team_and_unique_rank() -> None:
    rows = parse_fantasypros_depth(HTML, "Washington Commanders")
    assert [(row["name"], row["position"], row["rank"], row["fantasypros_player_id"]) for row in rows] == [
        ("Stefon Diggs", "WR", 1, 101),
        ("Antonio Williams", "WR", 2, 102),
        ("Treylon Burks", "WR", 3, 103),
    ]
    with pytest.raises(ValueError, match="requested team"):
        parse_fantasypros_depth(HTML, "Indianapolis Colts")
    with pytest.raises(ValueError, match="duplicate"):
        parse_fantasypros_depth(HTML.replace("WR2", "WR1"), "Washington Commanders")


def test_compare_depth_preserves_disagreement_and_missing_sources() -> None:
    sleeper = [
        {"id": 1, "name": "Stefon Diggs", "position": "WR", "fantasypros_player_id": 101,
         "depth_order": 1, "depth_chart_position": "SWR", "status": "Active", "fetched_at": "2026-10-04T13:22:43Z"},
        {"id": 2, "name": "Treylon Burks", "position": "WR", "fantasypros_player_id": 103,
         "depth_order": 4, "depth_chart_position": "SWR", "status": "Active", "fetched_at": "2026-10-04T13:22:43Z"},
        {"id": 3, "name": "Jaylin Lane", "position": "WR", "fantasypros_player_id": None,
         "depth_order": 7, "depth_chart_position": "SWR", "status": "Active", "fetched_at": "2026-10-04T13:22:43Z"},
    ]
    fp = parse_fantasypros_depth(HTML, "Washington Commanders")
    result = compare_depth(sleeper, fp)
    assert result["counts"] == {"matched": 2, "conflicts": 1, "sleeper_only": 1, "fantasypros_only": 1}
    burks = next(row for row in result["comparisons"] if row["name"] == "Treylon Burks")
    assert burks["sleeper"]["rank"] == 4
    assert burks["sleeper"]["alignment"] == "SWR"
    assert burks["fantasypros"]["rank"] == 3
    assert burks["decision"] == "conflict"
    assert result["fantasypros_only"][0]["name"] == "Antonio Williams"


def test_compare_depth_rejects_ambiguous_name_fallback() -> None:
    sleeper = [
        {"id": 1, "name": "J. Smith", "position": "WR", "fantasypros_player_id": None, "depth_order": 2},
        {"id": 2, "name": "J Smith", "position": "WR", "fantasypros_player_id": None, "depth_order": 3},
    ]
    result = compare_depth(sleeper, [{"name": "J Smith", "position": "WR", "rank": 2,
                                      "fantasypros_player_id": None}])
    assert result["counts"]["matched"] == 0
    assert result["fantasypros_only"][0]["reason"] == "identity_unresolved"


def test_compare_depth_does_not_override_conflicting_known_player_id() -> None:
    sleeper = [{"id": 1, "name": "Treylon Burks", "position": "WR",
                "fantasypros_player_id": 999, "depth_order": 4}]
    result = compare_depth(sleeper, [{"name": "Treylon Burks", "position": "WR", "rank": 3,
                                      "fantasypros_player_id": 103}])
    assert result["counts"]["matched"] == 0
    assert result["counts"]["fantasypros_only"] == 1
