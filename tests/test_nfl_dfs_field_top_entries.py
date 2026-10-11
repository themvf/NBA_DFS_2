"""`--keep-top N`: the top of an export's standings, roster and all, read against the field."""

import csv

import pytest

from model.nfl_dfs_field_structure import keep_top_entries

PLAYERS = {
    "dakprescott": {"drafted_pct": 70.0, "drafted_by_slot": {"CPT": 15.0, "FLEX": 55.0}},
    "ceedeelamb": {"drafted_pct": 50.0, "drafted_by_slot": {"CPT": 20.0, "FLEX": 30.0}},
    "buckyirving": {"drafted_pct": 24.0, "drafted_by_slot": {"FLEX": 24.0}},
    "bijanrobinson": {"drafted_pct": 37.91, "drafted_by_slot": {"RB": 37.91}},
}


def entry(rank, entry_id, name, points, lineup):
    return {"rank": rank, "entry_id": entry_id, "entry_name": name, "points": points, "lineup_text": lineup}


ENTRIES = [
    entry(1, "a", "grinder (13/150)", 136.59, "CPT Dak Prescott FLEX CeeDee Lamb FLEX Bucky Irving"),
    entry(2, "b", "solo", 135.0, "CPT CeeDee Lamb FLEX Dak Prescott FLEX Unknown Guy"),
    entry(2, "c", "tied (1/2)", 135.0, "CPT CeeDee Lamb FLEX Bucky Irving"),
    entry(4, "d", "fourth", 130.0, "CPT Dak Prescott"),
]


def test_slots_are_read_at_their_own_ownership_and_the_sum_is_the_fields_number():
    rows = keep_top_entries(ENTRIES, 1, PLAYERS)
    assert len(rows) == 1
    top = rows[0]
    assert top["username"] == "grinder" and top["user_entries"] == 150
    assert [p["drafted_pct"] for p in top["players"]] == [15.0, 30.0, 24.0]   # CPT Dak, FLEX Lamb, FLEX Irving
    assert top["ownership_sum"] == 69.0


def test_a_tie_at_the_cut_is_kept_whole_and_unknown_players_do_not_count():
    rows = keep_top_entries(ENTRIES, 2, PLAYERS)
    assert [r["entry_id"] for r in rows] == ["a", "b", "c"]
    solo = rows[1]
    assert solo["user_entries"] is None
    assert solo["players"][2]["drafted_pct"] is None
    assert solo["ownership_sum"] == 20.0 + 55.0


def test_classic_slots_fall_back_to_total_ownership():
    rows = keep_top_entries([entry(1, "x", "u", 200.0, "RB Bijan Robinson")], 1, PLAYERS)
    assert rows[0]["players"][0]["drafted_pct"] == 37.91


def test_bad_inputs_fail_loudly():
    with pytest.raises(ValueError):
        keep_top_entries(ENTRIES, 0, PLAYERS)
    with pytest.raises(ValueError, match="no parseable lineup"):
        keep_top_entries([entry(1, "x", "u", 1.0, "")], 1, PLAYERS)


def test_read_top_entries_reads_entry_id_and_points_from_the_export(tmp_path):
    from ingest.nfl_dfs_field_audit import read_top_entries
    path = tmp_path / "contest-standings-1.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Rank", "EntryId", "EntryName", "TimeRemaining", "Points", "Lineup", "",
                         "Player", "Roster Position", "%Drafted", "FPTS"])
        writer.writerow([1, "111", "grinder (13/150)", 0, "136.59", "CPT Dak Prescott FLEX CeeDee Lamb", "",
                         "Dak Prescott", "CPT", "15%", "26.46"])
        writer.writerow([2, "222", "solo", 0, "135.0", "CPT CeeDee Lamb FLEX Dak Prescott", "",
                         "Dak Prescott", "FLEX", "55%", "17.64"])
        writer.writerow([3, "333", "third", 0, "130.0", "CPT Dak Prescott"])
        writer.writerow(["", "", "", "", "", "", "", "Bucky Irving", "FLEX", "24%", "34.5"])   # ownership-only row
    rows = read_top_entries(path, 2)
    assert [(r["rank"], r["entry_id"], r["points"]) for r in rows] == [(1, "111", 136.59), (2, "222", 135.0)]
    assert rows[0]["entry_name"] == "grinder (13/150)"
