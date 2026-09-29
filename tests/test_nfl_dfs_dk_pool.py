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

import pytest
import requests

from ingest import nfl_dfs_dk_pool as dk
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


# ── A failed read turns the run red; a benign empty group does not ──────────


class _Response:
    def __init__(self, payload=None, status=200):
        self.payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Server Error")

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _Session:
    def __init__(self, response):
        self.response = response

    def get(self, *args, **kwargs):
        return self.response


class _Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.statements.append(sql)

    @property
    def connection(self):
        return self.conn


class _Conn:
    def __init__(self, db):
        self.db, self.statements = db, []
        self.encoding = "UTF8"

    def cursor(self):
        return _Cursor(self)


class _Db:
    """Records polls; `connect()` is one transaction that commits on clean exit."""

    def __init__(self, fail_player_insert=False):
        self.polls, self.committed, self.fail = [], [], fail_player_insert

    def execute(self, sql, params=None):
        assert "nfl_dfs_dk_pool_polls" in sql
        self.polls.append(params)

    def execute_one(self, sql, params=None):
        return None

    def connect(self):
        from contextlib import contextmanager

        @contextmanager
        def transaction():
            conn = _Conn(self)
            yield conn
            self.committed.append(conn.statements)
        return transaction()


GROUP = {"draft_group_id": 1, "contest_type_id": 21, "format": "classic",
         "start_date": NOW + timedelta(hours=10), "game_count": 2}
PLAYERS = [{"pid": i, "fn": "P", "ln": str(i), "pn": "WR", "tid": 1, "htid": 1, "htabbr": "ATL",
            "atabbr": "GB", "s": 5000 + 100 * (i % 3), "i": "", "swp": True, "news": 0,
            "IsDisabledFromDrafting": False} for i in range(6)]


def test_the_lobby_raises_on_an_http_error_or_a_non_lobby_body():
    with pytest.raises(requests.HTTPError):
        dk.fetch_draft_groups(_Session(_Response({"DraftGroups": []}, status=503)))
    with pytest.raises(ValueError, match="DraftGroups"):
        dk.fetch_draft_groups(_Session(_Response({"error": "blocked"})))
    assert dk.fetch_draft_groups(_Session(_Response({"DraftGroups": []}))) == []


def test_an_unpopulated_group_is_recorded_and_benign():
    db = _Db()
    result = dk.capture(db, GROUP, session=_Session(_Response({"playerList": []})))
    assert result["state"] == "not_populated" and not result["fatal"]
    assert db.polls[0][1] is False and "empty player list" in db.polls[0][4]


def test_an_http_failure_on_a_pool_is_recorded_and_fatal():
    db = _Db()
    result = dk.capture(db, GROUP, session=_Session(_Response({}, status=403)))
    assert result["state"] == "failed" and result["fatal"]
    assert len(db.polls) == 1 and db.polls[0][1] is False


def test_snapshot_header_and_players_commit_together(monkeypatch):
    written = []
    monkeypatch.setattr(dk, "execute_values", lambda cursor, sql, rows, **kw: written.append(len(rows)))
    db = _Db()
    result = dk.capture(db, GROUP, session=_Session(_Response({"playerList": PLAYERS})))
    assert result["state"] == "captured"
    assert len(db.committed) == 1, "header and players must be one transaction"
    assert "INSERT INTO nfl_dfs_dk_pool_snapshots" in db.committed[0][0]
    assert written == [len(PLAYERS)]
    assert db.polls[-1][1] is True  # the ok poll is recorded only after the commit


def test_a_failed_player_insert_leaves_no_header_behind(monkeypatch):
    def fail(cursor, sql, rows, **kw):
        raise RuntimeError("connection reset")
    monkeypatch.setattr(dk, "execute_values", fail)
    db = _Db()
    with pytest.raises(RuntimeError):
        dk.capture(db, GROUP, session=_Session(_Response({"playerList": PLAYERS})))
    assert db.committed == [] and db.polls == []


def test_no_draft_groups_during_a_game_week_is_red():
    lines, status = dk.summarize([], hours=120, explicit=False, games_soon=3)
    assert status == 1 and "no salary-cap NFL draft group" in lines[0]
    assert dk.summarize([], hours=120, explicit=False, games_soon=0)[1] == 0
    # An explicit --draft-group run says nothing about the lobby.
    assert dk.summarize([], hours=120, explicit=True, games_soon=3)[1] == 0


def test_run_status_red_only_for_failed_reads():
    benign = {"draft_group_id": 1, "ok": False, "changed": False, "fatal": False,
              "state": "not_populated", "detail": "not populated yet: empty player list"}
    ok = {"draft_group_id": 2, "ok": True, "changed": False, "fatal": False, "state": "unchanged"}
    failed = {"draft_group_id": 3, "ok": False, "changed": False, "fatal": True, "state": "failed",
              "detail": "403 Client Error"}
    assert dk.summarize([benign, ok], hours=120, explicit=False, games_soon=5)[1] == 0
    lines, status = dk.summarize([benign, ok, failed], hours=120, explicit=False, games_soon=5)
    assert status == 1 and any("FAILED  403" in line for line in lines)
