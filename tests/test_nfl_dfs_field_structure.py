import pytest

from model.nfl_dfs_field_structure import analyze, parse_entry_name, parse_lineup


def classic(qb, extra=""):
    return (f"DST Panthers  FLEX Jake Ferguson QB {qb} RB Chuba Hubbard RB Aaron Jones Sr. "
            f"TE Dalton Schultz WR Jaxon Smith-Njigba WR Ja'Marr Chase WR CeeDee Lamb{extra}")


def test_parse_lineup_handles_multiword_names_and_double_space():
    parsed = parse_lineup(classic("Dak Prescott"))
    assert len(parsed) == 9
    assert ("DST", "Panthers") in parsed
    assert ("RB", "Aaron Jones Sr.") in parsed
    assert ("WR", "Ja'Marr Chase") in parsed


def test_parse_entry_name():
    assert parse_entry_name("mortuculus (13/20)") == ("mortuculus", 20)
    assert parse_entry_name("solo") == ("solo", None)


def test_duplication_and_users():
    rows = [(1, "a (1/20)", classic("Dak Prescott")),
            (2, "a (2/20)", classic("Dak Prescott")),
            (3, "a (3/20)", classic("Dak Prescott")),
            (4, "b (1/1)", classic("Brock Purdy")),
            (5, "c (1/3)", classic("Baker Mayfield"))]
    s = analyze(rows, "classic")
    assert s["entries"] == 5
    assert s["duplication"]["unique_lineups"] == 3
    assert s["duplication"]["max_duplicates"] == 3
    assert s["duplication"]["entry_share_in_duplicated_lineup"] == pytest.approx(0.6)
    assert s["duplication"]["entries_by_duplicate_count"]["3-5"] == 3
    assert s["users"]["distinct"] == 3
    assert s["users"]["entries_per_user_max"] == 3


def test_pair_lift_reflects_a_stack():
    # Everyone rosters the same non-QB core; Prescott always appears with Lamb
    # (a constant in classic()), so lift against a rarer pairing must exceed 1
    # only where co-occurrence is genuinely above independence.
    rows = []
    for i in range(50):
        rows.append((i + 1, f"u{i} (1/1)", classic("Dak Prescott")))
    for i in range(50):
        rows.append((100 + i, f"v{i} (1/1)", classic("Brock Purdy").replace("CeeDee Lamb", "Mike Evans")))
    s = analyze(rows, "classic")
    pairs = {(p["a"], p["b"]): p for p in s["pairs"]["rows"]}
    key = ("CeeDee Lamb", "Dak Prescott")
    assert key in pairs
    assert pairs[key]["joint"] == pytest.approx(0.5)
    assert pairs[key]["lift"] == pytest.approx(2.0)      # 0.5 / (0.5 * 0.5)
    # Perfectly independent items (owned by everyone) have lift 1.
    assert pairs[("Chuba Hubbard", "Jake Ferguson")]["lift"] == pytest.approx(1.0)


def test_unparseable_export_fails_loudly_not_partially():
    rows = [(i, f"u{i} (1/1)", "not a lineup") for i in range(10)]
    with pytest.raises(ValueError, match="did not parse"):
        analyze(rows, "classic")


def test_showdown_captain_is_part_of_lineup_identity():
    cpt_a = "CPT Davante Adams FLEX Matthew Stafford FLEX Kyren Williams FLEX Cam Skattebo FLEX Rams  FLEX Terrance Ferguson"
    cpt_b = "CPT Matthew Stafford FLEX Davante Adams FLEX Kyren Williams FLEX Cam Skattebo FLEX Rams  FLEX Terrance Ferguson"
    s = analyze([(1, "a (1/1)", cpt_a), (2, "b (1/1)", cpt_b), (3, "c (1/1)", cpt_a)], "showdown")
    assert s["duplication"]["unique_lineups"] == 2       # same six players, different captain
    assert s["duplication"]["max_duplicates"] == 2


def test_unknown_format_rejected():
    with pytest.raises(ValueError):
        analyze([], "weird")
