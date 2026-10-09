from research.nfl_archived_contests import grade_slots, rank_against_field


def test_missing_actual_is_withheld_not_zero_and_captain_is_not_doubled():
    actuals = {("player", "CPT"): 30., ("player", "FLEX"): 20.}
    assert grade_slots([{"name": "Player", "slot": "CPT"}], actuals, "showdown")["points"] == 30.
    assert grade_slots([{"name": "Missing", "slot": "FLEX1"}], actuals, "showdown")["points"] is None


def test_archived_field_midrank_keeps_ties_and_does_not_claim_prizes():
    rank = rank_against_field(100, {110: 2, 100: 4, 90: 4})
    assert rank["strictly_better_entries"] == 2
    assert rank["tied_entries"] == 4
    assert rank["percentile_midrank"] == 60
