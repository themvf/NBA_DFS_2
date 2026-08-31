from ingest.ff_yahoo_predraft import (
    YAHOO_NAME_ALIASES,
    _match_player,
    parse_yahoo_predraft_text,
)


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


def test_name_aliases_resolve_in_both_directions() -> None:
    """The alias map used to be one-way rewrites. When nflverse renamed Kenneth
    Gainwell to Kenny, the rewrite pointed at a spelling that no longer existed
    and silently dropped a back the market drafts inside pick 120."""
    for spelling in ("kennygainwell", "kennethgainwell"):
        assert set(YAHOO_NAME_ALIASES[spelling]) == {"kennygainwell", "kennethgainwell"}


def test_match_finds_a_player_stored_under_either_alias_spelling() -> None:
    paste = """Kenny Gainwell
Kenny Gainwell
RB
·
TB
·
Bye 9
XRank #118
ADP #118.4"""
    row = parse_yahoo_predraft_text(paste)[0]
    for stored_as in ("kennygainwell", "kennethgainwell"):
        player = {"id": 7, "normalized_name": stored_as, "position": "RB",
                  "team_abbrev": "TB", "on_current_board": True}
        player_id, method = _match_player(row, {(stored_as, "RB"): [player]}, {})
        assert player_id == 7, f"unmatched when the roster spells him {stored_as}"
        assert method.startswith("normalized_name_position_team")
