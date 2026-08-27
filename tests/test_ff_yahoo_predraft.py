from ingest.ff_yahoo_predraft import parse_yahoo_predraft_text


def test_parse_yahoo_predraft_preserves_missing_adp_and_normalizes_defense():
    rows = parse_yahoo_predraft_text("""Jahmyr Gibbs
Jahmyr Gibbs
RB
·
Det
·
Bye 6
XRank #1.2
·
ADP 1.4


Rams
Rams
DEF
·
LAR
·
Bye 11
XRank #175.9


Jackson Meeks
Jackson Meeks
WR, TE
·
Det
·
Bye 6
XRank #2052
""")

    assert len(rows) == 3
    assert rows[0].display_name == "Jahmyr Gibbs"
    assert rows[0].team_abbrev == "DET"
    assert rows[0].xrank == 1.2
    assert rows[0].adp == 1.4
    assert rows[1].position == "DST"
    assert rows[1].adp is None
    assert rows[2].position == "WR"
