"""NFL slate specials, P0: frozen contract, board parsing, identity resolution.

P0 of this programme (docs/nfl-slate-specials-handoff.md §7) builds exactly one
thing -- the DK market-capture tool -- and measures exactly one number, the
per-family overround, for two Sundays before any simulator exists. These tests
cover that scope and nothing beyond it. The simulator's own tests (Layer A KS
tests, the consistency check, PIT calibration) arrive with P0.5 onward.

Constants are asserted literally, in the tamper-evident style of
tests/test_mlb_prop_program.py: a future change to which families exist, or to
what a selection key is, has to be an explicit versioned decision rather than a
quiet edit, because those keys are what ledger rows settle against.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

import model.nfl_slate_specials as S
from ingest.nfl_specials_market import (
    Lookups,
    ParsedRow,
    build_team_aliases,
    canonical_team,
    capture_key_for,
    compute_overround,
    find_duplicate_keys,
    incomplete_board_problem,
    parse_board_text,
    resolve_game,
    resolve_player,
    resolve_selections,
    resolve_team,
    run,
)

# ── fixtures ─────────────────────────────────────────────────────────────────

TEAM_ROWS = [
    {"abbreviation": "KC", "name": "Kansas City Chiefs", "odds_api_name": "Kansas City Chiefs", "city": "Kansas City"},
    {"abbreviation": "BAL", "name": "Baltimore Ravens", "odds_api_name": "Baltimore Ravens", "city": "Baltimore"},
    {"abbreviation": "SF", "name": "San Francisco 49ers", "odds_api_name": "San Francisco 49ers", "city": "San Francisco"},
    {"abbreviation": "PHI", "name": "Philadelphia Eagles", "odds_api_name": "Philadelphia Eagles", "city": "Philadelphia"},
    {"abbreviation": "NYG", "name": "New York Giants", "odds_api_name": "New York Giants", "city": "New York"},
    {"abbreviation": "NYJ", "name": "New York Jets", "odds_api_name": "New York Jets", "city": "New York"},
]

PLAYER_ROWS = [
    {"player_gsis_id": "00-0033280", "player_name": "Christian McCaffrey", "normalized_name": "christianmccaffrey", "team": "SF", "position": "RB"},
    {"player_gsis_id": "00-0036442", "player_name": "Ja'Marr Chase", "normalized_name": "jamarrchase", "team": "CIN", "position": "WR"},
    {"player_gsis_id": "00-0033873", "player_name": "Patrick Mahomes", "normalized_name": "patrickmahomes", "team": "KC", "position": "QB"},
    {"player_gsis_id": "00-0034796", "player_name": "Lamar Jackson", "normalized_name": "lamarjackson", "team": "BAL", "position": "QB"},
    # Two live players who share a surname and an initial: unresolvable on an
    # abbreviated board, and that must stay unresolvable.
    {"player_gsis_id": "00-0000001", "player_name": "Mike Williams", "normalized_name": "mikewilliams", "team": "NYJ", "position": "WR"},
    {"player_gsis_id": "00-0000002", "player_name": "Mason Williams", "normalized_name": "masonwilliams", "team": "NYG", "position": "WR"},
    # No gsis id: the selection key must fall back to normalized_name|team.
    {"player_gsis_id": None, "player_name": "Practice Squadder", "normalized_name": "practicesquadder", "team": "PHI", "position": "WR"},
]


@pytest.fixture()
def lookups() -> Lookups:
    return Lookups(
        team_by_alias=build_team_aliases(TEAM_ROWS),
        games=(("KC", "BAL"), ("SF", "PHI"), ("NYJ", "NYG")),
        players=tuple(PLAYER_ROWS),
    )


def board(*pairs: tuple[str, int]) -> str:
    """A two-column paste. Padded so it clears the >=100% completeness gate."""
    return "\n".join(f"{label}\n{price:+d}" for label, price in pairs) + "\n"


def padded(*pairs: tuple[str, int]) -> str:
    """A board plus filler, for tests about something other than completeness."""
    filler = tuple((f"Filler {i}", 400) for i in range(6))
    return board(*pairs, *filler)


# ── the frozen contract ──────────────────────────────────────────────────────

def test_families_are_frozen() -> None:
    assert S.MODEL_VERSION == "nfl-specials-v1"
    assert S.N_DRAWS == 50_000
    assert S.FAMILIES == (
        "highest_scoring_game", "lowest_scoring_game",
        "highest_scoring_team", "lowest_scoring_team",
        "most_passing_yards", "most_receiving_yards",
        "first_td_scorer", "first_qb_td_pass", "first_qb_int",
    )
    assert S.MAGNITUDE_FAMILIES + S.TIMING_FAMILIES == S.FAMILIES
    assert len(S.MAGNITUDE_FAMILIES) == 6, "magnitude families need Layers A+B only"
    assert len(S.TIMING_FAMILIES) == 3, "timing families need Layer C, which is P3"


def test_every_family_declares_what_its_selections_are() -> None:
    assert set(S.SELECTION_KIND) == set(S.FAMILIES)
    assert set(S.SELECTION_KIND.values()) == {"game", "team", "player"}
    assert S.selection_kind("highest_scoring_game") == "game"
    assert S.selection_kind("first_td_scorer") == "player"
    with pytest.raises(ValueError):
        S.selection_kind("most_rushing_yards")


def test_calibration_only_is_empty_until_p0_measures_it() -> None:
    """P0's gate fills this from two Sundays of captures -- never from a guess."""
    assert S.FAMILIES_CALIBRATION_ONLY == frozenset()


def test_scope_excludes_by_kickoff_and_never_downweights() -> None:
    one_pm = datetime(2026, 9, 20, 13, 0)      # Sunday 1:00pm ET
    late = datetime(2026, 9, 20, 16, 25)       # Sunday 4:25pm ET
    monday = datetime(2026, 9, 21, 20, 15)
    assert S.in_scope("sunday_1pm", one_pm)
    assert S.in_scope("sunday_all", one_pm)
    assert not S.in_scope("sunday_1pm", late), "a 4:25 game is not in the 1pm market at all"
    assert S.in_scope("sunday_all", late)
    assert not S.in_scope("sunday_all", monday)
    with pytest.raises(ValueError):
        S.in_scope("monday_night", monday)


# ── parsing ──────────────────────────────────────────────────────────────────

def test_parses_the_two_column_layout() -> None:
    rows, problems = parse_board_text(padded(("Christian McCaffrey", 650), ("Saquon Barkley", 700)))
    assert problems == []
    assert (rows[0].label, rows[0].american) == ("Christian McCaffrey", 650)
    assert (rows[1].label, rows[1].american) == ("Saquon Barkley", 700)
    assert [row.source_order for row in rows] == list(range(1, len(rows) + 1))


def test_parses_the_row_layout() -> None:
    text = ("Christian McCaffrey  +650\nSaquon Barkley       +700\n"
            + "".join(f"Filler {i}  +400\n" for i in range(6)))
    rows, problems = parse_board_text(text)
    assert problems == []
    assert (rows[0].label, rows[0].american) == ("Christian McCaffrey", 650)
    assert (rows[1].label, rows[1].american) == ("Saquon Barkley", 700)


def test_parses_a_browser_copy_with_unicode_minus_and_thousands_separator() -> None:
    rows, problems = parse_board_text(
        "Patrick Mahomes\n−120\nLamar Jackson\n +1,200\nFiller\n+150\n"
    )
    assert problems == []
    assert [row.american for row in rows] == [-120, 1200, 150]


def test_a_team_line_is_context_not_a_selection() -> None:
    rows, problems = parse_board_text("Saquon Barkley\nPHI\n+700\n"
                                      + "".join(f"Filler {i}\n+300\n" for i in range(5)))
    assert problems == []
    assert rows[0].label == "Saquon Barkley"
    assert rows[0].context == ("PHI",), "the team line must not become its own selection"
    assert len(rows) == 6


def test_bare_rank_numbers_are_decorative_not_selections() -> None:
    rows, problems = parse_board_text(
        "1\nPatrick Mahomes\n+650\n2\nLamar Jackson\n+700\n"
        + "".join(f"{i + 3}\nFiller {i}\n+400\n" for i in range(6))
    )
    assert problems == []
    assert [row.label for row in rows[:2]] == ["Patrick Mahomes", "Lamar Jackson"]


def test_a_team_name_containing_digits_is_not_read_as_a_price() -> None:
    rows, problems = parse_board_text(padded(("San Francisco 49ers", 450)))
    assert problems == []
    assert rows[0].label == "San Francisco 49ers"


# ── refusals: the paste is not recorded when we may have misread it ──────────

def test_refuses_a_price_inside_the_impossible_band() -> None:
    _, problems = parse_board_text(padded(("Christian McCaffrey", 50)))
    assert any("between -100 and +100" in p for p in problems)


def test_refuses_a_truncated_board_that_cannot_be_a_whole_market() -> None:
    """The top two of a 250-way board would otherwise report a generous -77%."""
    _, problems = parse_board_text(board(("Christian McCaffrey", 750), ("Saquon Barkley", 800)))
    assert any("truncated" in p for p in problems)
    assert incomplete_board_problem([ParsedRow(1, "a", (), 750)]) is not None
    assert incomplete_board_problem([ParsedRow(i, "x", (), -110) for i in range(2)]) is None


def test_refuses_a_price_with_no_label_and_a_label_with_no_price() -> None:
    _, orphan_price = parse_board_text("+750\n+800\n")
    assert any("no preceding selection label" in p for p in orphan_price)
    _, orphan_label = parse_board_text(padded(("Christian McCaffrey", 650)) + "Saquon Barkley\n")
    assert any("has no price" in p for p in orphan_label)


def test_refuses_an_empty_paste() -> None:
    _, problems = parse_board_text("\n\n  \n")
    assert any("no selections found" in p for p in problems)


def test_refuses_a_line_carrying_two_prices() -> None:
    _, problems = parse_board_text("Chiefs +150 +200\n" + padded(("a", 400)))
    assert any("carries 2 prices" in p for p in problems)


# ── the overround, which is all P0 measures ──────────────────────────────────

def test_overround_uses_the_shared_odds_helper() -> None:
    """Not a local copy: a second implementation is a second thing to get wrong."""
    from model.soccer_bet_rating import american_to_prob as shared
    from ingest import nfl_specials_market as M
    assert M.american_to_prob is shared


def test_overround_is_the_textbook_value_on_a_two_way_market() -> None:
    both_sides = [ParsedRow(1, "over", (), -110), ParsedRow(2, "under", (), -110)]
    assert compute_overround(both_sides) == pytest.approx(0.04762, abs=1e-5)


def test_overround_spans_the_whole_board_including_unnamed_selections() -> None:
    """Dropping selections we failed to name would understate DK's margin."""
    rows = [ParsedRow(1, "Known", (), 100), ParsedRow(2, "Mystery Man", (), 100)]
    assert compute_overround(rows) == pytest.approx(0.0)
    assert compute_overround(rows + [ParsedRow(3, "Third", (), 100)]) == pytest.approx(0.5)


# ── identity resolution (§3.4) ───────────────────────────────────────────────

def test_team_aliases_accept_nickname_full_name_and_abbreviation(lookups) -> None:
    assert resolve_team("Chiefs", lookups) == "KC"
    assert resolve_team("Kansas City Chiefs", lookups) == "KC"
    assert resolve_team("KC", lookups) == "KC"
    assert resolve_team("49ers", lookups) == "SF"


def test_a_city_shared_by_two_teams_resolves_to_neither(lookups) -> None:
    """An ambiguous alias is dropped, not arbitrated: New York is Giants AND Jets."""
    assert resolve_team("New York", lookups) is None
    assert resolve_team("Giants", lookups) == "NYG"
    assert resolve_team("Jets", lookups) == "NYJ"


def test_canonical_team_applies_the_shared_override_map() -> None:
    """Imported from ingest.nfl_season_schedule -- a third copy is the AZ/ARI bug."""
    from ingest.nfl_season_schedule import TEAM_ABBREV_OVERRIDES
    assert TEAM_ABBREV_OVERRIDES["AZ"] == "ARI"
    assert TEAM_ABBREV_OVERRIDES["JAC"] == "JAX"
    assert canonical_team("az") == "ARI"
    assert canonical_team("LA") == "LAR"
    assert canonical_team(None) is None


def test_game_key_comes_from_the_schedule_not_dk_print_order(lookups) -> None:
    """DK prints either order; the stored key is always canonical away@home."""
    assert resolve_game("Chiefs @ Ravens", lookups) == "KC@BAL"
    assert resolve_game("Ravens vs Chiefs", lookups) == "KC@BAL"
    assert resolve_game("Ravens v Chiefs", lookups) == "KC@BAL"
    assert resolve_game("Chiefs at Ravens", lookups) == "KC@BAL"


def test_a_game_not_on_the_schedule_is_unresolved(lookups) -> None:
    assert resolve_game("Chiefs @ Eagles", lookups) is None, "not a week-3 fixture"
    assert resolve_game("Chiefs @ Chiefs", lookups) is None
    assert resolve_game("Chiefs", lookups) is None


def test_a_quarterback_market_only_considers_quarterbacks(lookups) -> None:
    assert S.FAMILY_POSITIONS["most_passing_yards"] == frozenset({"QB"})
    key, _ = resolve_player("Christian McCaffrey", (), "most_passing_yards", lookups)
    assert key is None, "a running back cannot lead passing yards"
    key, _ = resolve_player("Patrick Mahomes", (), "most_passing_yards", lookups)
    assert key == "00-0033873"


def test_an_abbreviated_name_resolves_by_initial_and_surname(lookups) -> None:
    key, method = resolve_player("J. Chase", (), "first_td_scorer", lookups)
    assert key == "00-0036442"
    assert method == "initial_surname"


def test_a_shared_initial_and_surname_stays_unresolved(lookups) -> None:
    """Mike and Mason Williams are both live: guessing would mis-settle a bet."""
    key, method = resolve_player("M. Williams", (), "first_td_scorer", lookups)
    assert key is None
    assert method == "ambiguous"


def test_a_team_hint_disambiguates_a_shared_surname(lookups) -> None:
    key, _ = resolve_player("M. Williams", ("NYJ",), "first_td_scorer", lookups)
    assert key == "00-0000001"


def test_an_unknown_player_is_unresolved_rather_than_guessed(lookups) -> None:
    key, method = resolve_player("Somebody Nobody", (), "first_td_scorer", lookups)
    assert key is None
    assert method in {"unmatched", "ambiguous"}


def test_a_player_without_a_gsis_id_falls_back_to_name_and_team(lookups) -> None:
    key, _ = resolve_player("Practice Squadder", (), "first_td_scorer", lookups)
    assert key == "practicesquadder|PHI"


def test_unresolved_selections_keep_their_label_and_are_still_recorded(lookups) -> None:
    rows, problems = parse_board_text(padded(("Somebody Nobody", 900)))
    assert problems == []
    resolved = resolve_selections(rows, "first_td_scorer", lookups)
    assert resolved[0].selection_key == "UNRESOLVED:Somebody Nobody"
    assert not resolved[0].resolved
    assert resolved[0].row.american == 900, "the price survives even when the name does not"


def test_resolution_degrades_when_no_projection_run_exists() -> None:
    """A perishable market price must never be lost waiting on a projection."""
    empty = Lookups(team_by_alias={}, games=(), players=())
    key, method = resolve_player("Patrick Mahomes", (), "first_td_scorer", empty)
    assert key is None
    assert method == "no_projection_rows"


# ── duplicates and capture identity ─────────────────────────────────────────

def test_duplicate_selections_are_refused_because_they_inflate_the_overround(lookups) -> None:
    rows, problems = parse_board_text(padded(("Patrick Mahomes", 650), ("Patrick Mahomes", 650)))
    assert problems == []
    resolved = resolve_selections(rows, "first_td_scorer", lookups)
    assert find_duplicate_keys(resolved) == ["00-0033873"]


def test_distinct_unresolved_labels_are_not_duplicates(lookups) -> None:
    rows, _ = parse_board_text(padded(("Nobody One", 900), ("Nobody Two", 900)))
    assert find_duplicate_keys(resolve_selections(rows, "first_td_scorer", lookups)) == []


def test_capture_key_is_content_derived_and_scoped() -> None:
    args = dict(season=2026, week=3, family="first_td_scorer", scope="sunday_1pm")
    same = capture_key_for(**args, raw_text="a\n+750\n")
    assert same == capture_key_for(**args, raw_text="a\n+750\n"), "identical paste is a no-op"
    assert same != capture_key_for(**args, raw_text="a\n+760\n"), "a moved price is a new capture"
    assert same != capture_key_for(**{**args, "week": 4}, raw_text="a\n+750\n")
    assert same != capture_key_for(**{**args, "scope": "sunday_all"}, raw_text="a\n+750\n")
    assert same.startswith("first_td_scorer:2026w3:sunday_1pm:")


# ── the CLI contract ─────────────────────────────────────────────────────────

def test_run_rejects_an_unknown_family_or_scope(tmp_path: Path) -> None:
    paste = tmp_path / "p.txt"
    paste.write_text(padded(("Patrick Mahomes", 650)))
    with pytest.raises(ValueError, match="unknown family"):
        run(season=2026, week=3, family="most_rushing_yards", scope="sunday_1pm", file_path=paste, db=None)
    with pytest.raises(ValueError, match="unknown scope"):
        run(season=2026, week=3, family="first_td_scorer", scope="monday_night", file_path=paste, db=None)


def test_run_without_a_database_still_reports_the_overround(tmp_path: Path) -> None:
    """P0 is a measurement exercise; --dry-run must answer without any DB."""
    paste = tmp_path / "p.txt"
    paste.write_text(board(*[(f"P{i}", -110) for i in range(2)]))
    report = run(season=2026, week=3, family="first_td_scorer", scope="sunday_1pm",
                 file_path=paste, db=None)
    assert report["overround"] == pytest.approx(0.04762, abs=1e-5)
    assert report["written"] == 0
    assert report["resolved"] == 0, "no projection rows means no resolved keys"
    assert len(report["unresolved"]) == 2


def test_run_refuses_a_malformed_paste_rather_than_recording_part_of_it(tmp_path: Path) -> None:
    paste = tmp_path / "p.txt"
    paste.write_text(padded(("Christian McCaffrey", 50)))
    with pytest.raises(ValueError, match="between -100 and"):
        run(season=2026, week=3, family="first_td_scorer", scope="sunday_1pm", file_path=paste, db=None)
