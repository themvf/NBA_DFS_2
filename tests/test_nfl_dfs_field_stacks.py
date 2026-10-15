import pytest

from model.nfl_dfs_field_stacks import VERSION, classic_field_stacks, parse_stack_label, showdown_build_stacks


def test_parse_stack_label_reads_stat_api_vocabulary():
    assert parse_stack_label("QB/2WR/TE/BB") == {"qb": 1, "wr": 2, "te": 1, "rb": 0, "bb": 1}
    assert parse_stack_label("QB/RB/2BB") == {"qb": 1, "wr": 0, "te": 0, "rb": 1, "bb": 2}
    assert parse_stack_label("QB/3WR") == {"qb": 1, "wr": 3, "te": 0, "rb": 0, "bb": 0}
    assert parse_stack_label(None) is None
    assert parse_stack_label("") is None


def test_parse_stack_label_fails_loudly_on_a_changed_vocabulary():
    with pytest.raises(ValueError):
        parse_stack_label("QB/2WR/K")
    with pytest.raises(ValueError):
        parse_stack_label("2WR/TE")


def test_classic_rates_are_field_wide_and_bucketed_by_rank():
    rows = [{"rank": 1, "stack": "QB/2WR/TE/BB"}, {"rank": 2, "stack": "QB/WR"},
            {"rank": 3, "stack": None}, {"rank": 4, "stack": "QB/TE/RB"}]
    s = classic_field_stacks(rows, total_entries=4)
    assert s["version"] == VERSION and s["population"] == "field" and s["complete"] is True
    whole = s["by_rank"]["all"]
    assert whole["entries"] == 4
    assert whole["qb_stack"] == 0.75 and whole["no_qb_stack"] == 0.25
    assert whole["qb_plus_catchers"] == {"1": 0.5, "3+": 0.25}
    assert whole["qb_plus_te"] == 0.5 and whole["qb_plus_rb"] == 0.25
    assert whole["bring_back_any"] == 0.25 and whole["bring_back"] == {"0": 0.5, "1": 0.25}
    assert s["by_rank"]["top_1pct"]["entries"] == 1 and s["by_rank"]["top_1pct"]["qb_plus_catchers"] == {"3+": 1.0}
    assert s["by_rank"]["top_100"]["entries"] == 4
    assert s["labels"]["none"] == 1


def test_classic_partial_standings_are_not_called_the_field():
    s = classic_field_stacks([{"rank": 1, "stack": "QB/WR"}], total_entries=161557)
    assert s["complete"] is False and s["entries"] == 1 and s["total_entries"] == 161557


def roster(split, captain_pos="QB", qb_team="DAL", mates=2, kicker=False):
    teams = ["DAL"] * split[0] + ["TB"] * split[1]
    # Non-stacked same-team slots are the defense, so `mates` is exactly the
    # QB's skill-position teammates (WR/TE/RB all count as a pairing).
    positions = ["QB"] + ["WR"] * mates + ["DST"] * (split[0] - 1 - mates) + ["WR"] * split[1]
    if kicker:
        positions[-1] = "K"
    players = [{"slot": "FLEX", "team": t, "position": p} for t, p in zip(teams, positions)]
    players[0]["team"] = qb_team
    cpt = next(i for i, p in enumerate(players) if p["position"] == captain_pos)
    players[cpt]["slot"] = "CPT"
    return players


def test_showdown_rates_come_from_the_held_lineups_and_say_so():
    lineups = [{"rank": 1, "players": roster((5, 1), mates=3)}, {"rank": 2, "players": roster((3, 3), mates=1, kicker=True)},
               {"rank": 3, "players": roster((4, 2), captain_pos="WR", mates=2)}, {"rank": 9, "players": [{"slot": "CPT"}]}]
    s = showdown_build_stacks(lineups)
    assert s["version"] == VERSION and s["population"] == "top_users"
    assert s["entries"] == 3 and s["unparsed"] == 1
    whole = s["by_rank"]["all"]
    assert whole["team_split"] == {"3-3": 0.3333, "4-2": 0.3333, "5-1": 0.3333}
    assert whole["any_qb"] == 1.0 and whole["two_qbs"] == 0.0
    assert whole["qb_plus_teammates"] == {"1": 0.3333, "2": 0.3333, "3+": 0.3333}
    assert whole["captain_position"] == {"QB": 0.6667, "WR": 0.3333}
    assert whole["kicker"] == 0.3333
    assert s["by_rank"]["top_1pct"]["entries"] == 1


def test_empty_inputs_raise():
    with pytest.raises(ValueError):
        classic_field_stacks([], total_entries=10)
    with pytest.raises(ValueError):
        showdown_build_stacks([{"rank": 1, "players": []}])
