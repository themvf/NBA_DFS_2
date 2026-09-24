"""The live DraftKings pool: what it may say, and what it may never say.

The dangerous failure here is not a missed update. It is applying the WRONG
pool -- DraftKings lists one game under several contest types at once (Captain
Mode, Snake Showdown, Single Stat), and it lists simulated "Madden Stream"
matchups under real team abbreviations. Each of those matches a real slate on
the two keys that look sufficient (teams, format) and would overwrite live
availability with a fiction. Most of what is asserted below is refusal.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ingest.nfl_dfs_dk_pool import (
    CONTEST_TYPE_FORMAT, is_salary_cap_pool, normalize_players, payload_digest,
    in_window, _parse_start,
)
from model.nfl_dfs_dk_pool_match import (
    SALARY_AGREEMENT_FLOOR, is_out_status, match_pool_to_slate, normalize_name,
)

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
UPLOADED = datetime(2026, 9, 22, 9, 0, tzinfo=timezone.utc)


def slate(*rows):
    return [{"normalized_name": normalize_name(n), "name": n, "salary": s} for n, s in rows]


def pool(*rows):
    return [{"normalized_name": normalize_name(n), "name": n, "team": t, "salary": s,
             "status": st, "is_disabled": False} for n, t, s, st in rows]


# ── Identity ────────────────────────────────────────────────────────────────

def test_normalization_matches_the_slate_rule_including_digits():
    # The field audit's normalizer strips digits, which turns the 49ers defense
    # into "ers". This one must not, because it is joined against the slate's
    # own stored `normalized_name`.
    assert normalize_name("49ers") == "49ers"
    assert normalize_name("Michael Penix Jr.") == "michaelpenix"
    assert normalize_name("James Cook III") == "jamescook"
    assert normalize_name("Amon-Ra St. Brown") == "amonrastbrown"
    # A suffix inside a word is not a suffix.
    assert normalize_name("Olivier Rioux") == "olivierrioux"


# ── Refusals ────────────────────────────────────────────────────────────────

def test_a_different_set_of_teams_is_a_different_slate():
    result = match_pool_to_slate(
        slate(("A B", 7000)), "classic", ["ATL", "GB"],
        pool(("A B", "ATL", 7000, None)), "classic", ["ATL", "SF"],
        captured_at=NOW, upload_captured_at=UPLOADED)
    assert not result.applied and "different set of teams" in result.reason


def test_a_different_format_is_a_different_slate():
    result = match_pool_to_slate(
        slate(("A B", 7000)), "showdown", ["ATL", "GB"],
        pool(("A B", "ATL", 7000, None)), "classic", ["ATL", "GB"],
        captured_at=NOW, upload_captured_at=UPLOADED)
    assert not result.applied


def test_the_same_game_in_another_contest_type_is_caught_by_salary():
    """The real collision: Snake Showdown carries the same two teams and ranks
    players 1..N in the salary field. Team set and format both agree."""
    names = [f"Player {chr(65 + i)}" for i in range(12)]
    result = match_pool_to_slate(
        slate(*[(n, 5000 + 100 * i) for i, n in enumerate(names)]), "showdown", ["ATL", "GB"],
        pool(*[(n, "ATL", i + 1, None) for i, n in enumerate(names)]), "showdown", ["ATL", "GB"],
        captured_at=NOW, upload_captured_at=UPLOADED)
    assert not result.applied
    assert "Salaries disagree" in result.reason
    assert result.salary_agreement == 0.0
    assert not result.statuses, "a refused pool contributes nothing at all"


def test_an_observation_older_than_the_upload_is_not_news():
    result = match_pool_to_slate(
        slate(("A B", 7000)), "classic", ["ATL"],
        pool(("A B", "ATL", 7000, "O")), "classic", ["ATL"],
        captured_at=UPLOADED - timedelta(hours=1), upload_captured_at=UPLOADED)
    assert not result.applied and "no newer" in result.reason


def test_a_repeated_name_is_dropped_rather_than_guessed():
    result = match_pool_to_slate(
        slate(("Mike Williams", 5000), ("Mike Williams", 4200), ("Other Guy", 6000)),
        "classic", ["ATL"],
        pool(("Mike Williams", "ATL", 5000, "O"), ("Other Guy", "ATL", 6000, None)),
        "classic", ["ATL"],
        captured_at=NOW, upload_captured_at=UPLOADED)
    assert result.applied
    assert "mikewilliams" not in result.statuses
    assert "mikewilliams" in result.ambiguous_names
    assert set(result.statuses) == {"otherguy"}


# ── What it does say ────────────────────────────────────────────────────────

def test_a_matching_pool_reports_the_current_tag():
    result = match_pool_to_slate(
        slate(("Brock Bowers", 6600), ("Other Guy", 5000)), "classic", ["LV"],
        pool(("Brock Bowers", "LV", 6600, "O"), ("Other Guy", "LV", 5000, None)),
        "classic", ["LV"], captured_at=NOW, upload_captured_at=UPLOADED)
    assert result.applied
    assert result.statuses["brockbowers"].status == "O"
    assert result.statuses["otherguy"].status is None
    assert result.salary_agreement == 1.0


def test_one_late_added_player_does_not_fail_the_whole_pool():
    names = [f"Player {chr(65 + i)}" for i in range(20)]
    slate_rows = slate(*[(n, 5000) for n in names])
    pool_rows = pool(*[(n, "ATL", 5000 if i else 5100, None) for i, n in enumerate(names)])
    result = match_pool_to_slate(slate_rows, "classic", ["ATL"], pool_rows, "classic", ["ATL"],
                                 captured_at=NOW, upload_captured_at=UPLOADED)
    assert result.applied and result.salary_agreement >= SALARY_AGREEMENT_FLOOR


def test_a_tiny_overlap_skips_the_salary_check_rather_than_asserting_on_it():
    # Three matched players cannot distinguish "different slate" from "one
    # correction", so the check does not run -- but it also does not pass.
    result = match_pool_to_slate(
        slate(("A B", 5000), ("C D", 5100), ("E F", 5200)), "showdown", ["ATL"],
        pool(("A B", "ATL", 1, None), ("C D", "ATL", 2, None), ("E F", "ATL", 3, None)),
        "showdown", ["ATL"], captured_at=NOW, upload_captured_at=UPLOADED)
    assert result.applied and result.salary_agreement == 0.0
    assert result.matched < 10


# ── Capture-side guards ─────────────────────────────────────────────────────

def test_rank_as_salary_is_not_a_salary_cap_pool():
    assert not is_salary_cap_pool([{"s": i + 1} for i in range(50)])
    assert not is_salary_cap_pool([{"s": 0} for _ in range(40)])
    # A real pool: DraftKings' $100 grid, with prices repeating.
    assert is_salary_cap_pool([{"s": 5000}, {"s": 5000}, {"s": 7600}, {"s": 3200}])
    # Off-grid prices are not DraftKings'.
    assert not is_salary_cap_pool([{"s": 5050}, {"s": 5050}, {"s": 7600}])


def test_only_the_two_salary_cap_formats_are_polled():
    assert CONTEST_TYPE_FORMAT == {21: "classic", 96: "showdown"}
    # Snake (189), Snake Showdown (192), Best Ball (145), Single Stat (353/354),
    # in-game halves (108/110) and Madden Stream (158/159) are other games.
    for other in (145, 158, 159, 108, 110, 189, 192, 353, 354, 51):
        assert other not in CONTEST_TYPE_FORMAT


def test_the_digest_ignores_news_and_swappability():
    """`swp` flips for every player at kickoff and `news` ticks on any headline.
    Either in the digest would manufacture a slate-wide 'change' that is not a
    change of availability."""
    base = [{"pid": 1, "fn": "A", "ln": "B", "pn": "WR", "tid": 1, "htid": 1,
             "htabbr": "ATL", "atabbr": "GB", "s": 5000, "i": "", "swp": True,
             "news": 0, "IsDisabledFromDrafting": False}]
    changed = [{**base[0], "swp": False, "news": 2}]
    assert payload_digest(normalize_players(base)) == payload_digest(normalize_players(changed))
    ruled_out = [{**base[0], "i": "O"}]
    assert payload_digest(normalize_players(base)) != payload_digest(normalize_players(ruled_out))


def test_an_empty_tag_becomes_null_not_an_empty_string():
    rows = normalize_players([{"pid": 1, "fn": "A", "ln": "B", "pn": "WR", "tid": 1, "htid": 1,
                               "htabbr": "ATL", "atabbr": "GB", "s": 5000, "i": "",
                               "swp": True, "news": 0, "IsDisabledFromDrafting": False}])
    assert rows[0]["status"] is None
    assert rows[0]["team"] == "ATL" and rows[0]["opponent"] == "GB"


def test_the_poll_window_excludes_games_that_have_started():
    started = {"start_date": NOW - timedelta(minutes=1)}
    soon = {"start_date": NOW + timedelta(hours=2)}
    far = {"start_date": NOW + timedelta(days=30)}
    assert not in_window(started, hours=120, now=NOW)
    assert in_window(soon, hours=120, now=NOW)
    assert not in_window(far, hours=120, now=NOW)
    assert not in_window({"start_date": None}, hours=120, now=NOW)


def test_draftkings_seven_digit_fractional_timestamps_parse():
    parsed = _parse_start("2026-09-25T00:15:00.0000000Z")
    assert parsed == datetime(2026, 9, 25, 0, 15, tzinfo=timezone.utc)
    assert _parse_start(None) is None
    assert _parse_start("not a date") is None


def test_out_statuses_exclude_doubtful_and_questionable():
    # Those two are judgement calls the optimizer owns, not facts a feed asserts.
    assert is_out_status("O") and is_out_status("ir") and is_out_status("PUP")
    assert not is_out_status("D") and not is_out_status("Q") and not is_out_status(None)
