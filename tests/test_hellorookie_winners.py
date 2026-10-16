import pytest

from model.hellorookie_winners import VERSION, parse_page, parse_table, summary


def table(rows, own="Draft %"):
    body = "".join(f"<tr><td>{p}</td><td>{n}</td><td>{o}</td><td>{s}</td><td>{pt}</td></tr>" for p, n, o, s, pt in rows)
    return f"<table><tr><th>Pos.</th><th>Player</th><th>{own}</th><th>Salary</th><th>Points</th></tr>{body}</table>"


LINEUP = [("QB", "J. Fields", "1.8%", "$5,500", "29.52"), ("RB", "A. Jones", "10.0%", "$5,000", "10.0"), ("RB", "B. Hall", "20.0%", "$6,000", "15.0"),
          ("WR", "E. Egbuka", "5.0%", "$4,500", "25.0"), ("WR", "A. Brown", "15.0%", "$7,500", "20.0"), ("WR", "C. Lamb", "30.0%", "$8,000", "30.0"),
          ("TE", "J. Johnson", "2.0%", "$3,000", "22.4"), ("FLEX", "D. Smith", "9.0%", "$6,900", "30.0"), ("D/ST", "Steelers", "5.0%", "$3,500", "12.0")]


def page(headings_and_tables, published="2025"):
    body = "".join(f"<h2>{h}</h2>{t}" for h, t in headings_and_tables)
    return f'<html><script>{{"datePublished":"{published}-11-18T08:47:37-05:00"}}</script><h2>Intro</h2><p>x</p>{body}<h2>Keep reading</h2></html>'


def test_parse_table_reads_positions_money_and_percentages():
    rows = parse_table(table(LINEUP))
    assert [r["slot"] for r in rows] == ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"]
    assert rows[0] == {"slot": "QB", "name": "J. Fields", "draft_pct": 1.8, "salary": 5500, "points": 29.52}


def test_seasons_are_inferred_from_the_week_sequence_and_checked_against_explicit_years():
    p = page([("DraftKings NFL Week 2 Winning Lineup", table(LINEUP)), ("DraftKings NFL Week 1 Winning Lineup", table(LINEUP)),
              ("DraftKings NFL Week 18 Winning Lineup", table(LINEUP)), ("DraftKings 2024 NFL Week 1 Millionaire Winning Lineup", table(LINEUP)),
              ("DraftKings 2023 NFL Week 17 Millionaire Winning Lineup", table(LINEUP, own="Own %"))])
    out = parse_page(p)
    assert [(l["season"], l["week"], l["season_explicit"]) for l in out] == [(2025, 2, False), (2025, 1, False), (2024, 18, False), (2024, 1, True), (2023, 17, True)]
    assert out[0]["version"] == VERSION and out[0]["total_points"] == 193.92 and out[0]["total_salary"] == 49900 and out[0]["flags"] == []


def test_a_mismatched_explicit_year_fails_the_parse():
    p = page([("DraftKings NFL Week 2 Winning Lineup", table(LINEUP)), ("DraftKings 2023 NFL Week 1 Winning Lineup", table(LINEUP))])
    with pytest.raises(ValueError, match="layout changed"):
        parse_page(p)


def test_page_arithmetic_errors_are_flagged_not_corrected():
    over = [(p, n, o, "$6,000" if p == "D/ST" else s, pt) for p, n, o, s, pt in LINEUP]
    na = [(p, n, o, "N/A" if p == "QB" else s, "2.0") for p, n, o, s, pt in LINEUP]  # different points: not a duplicate
    out = parse_page(page([("DraftKings NFL Week 2 Winning Lineup", table(over)), ("DraftKings NFL Week 1 Winning Lineup", table(na))]))
    assert out[0]["flags"] == ["salary_over_cap"] and out[0]["total_salary"] == 52400
    assert out[1]["flags"] == ["salary_missing"] and out[1]["total_salary"] is None and out[1]["players"][0]["salary"] is None
    assert "2025 w2 salary_over_cap" in summary(out)


def test_a_table_repeated_under_two_weeks_is_flagged_on_the_second():
    other = [(p, n, o, s, "1.0") for p, n, o, s, pt in LINEUP]
    out = parse_page(page([("DraftKings NFL Week 9 Winning Lineup", table(LINEUP)), ("DraftKings NFL Week 8 Winning Lineup", table(other)),
                           ("DraftKings NFL Week 7 Winning Lineup", table(LINEUP))]))
    assert out[0]["flags"] == [] and out[1]["flags"] == [] and out[2]["flags"] == ["duplicate_of_week_9"]


def test_two_tables_under_one_heading_or_no_published_date_fail():
    with pytest.raises(ValueError, match="one table"):
        parse_page(page([("DraftKings NFL Week 1 Winning Lineup", table(LINEUP) + table(LINEUP))]))
    with pytest.raises(ValueError, match="datePublished"):
        parse_page("<h2>DraftKings NFL Week 1 Winning Lineup</h2>" + table(LINEUP))
