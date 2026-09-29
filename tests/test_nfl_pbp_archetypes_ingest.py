"""The self-healing pbp relabel: new seasons, nflverse corrections, and a
download failure that must not read as "nothing to do"."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from ingest import nfl_pbp_archetypes as pbp


def plays(game_id="2026_01_A_B", yards=(5, 7, -2), extra=None):
    frame = pd.DataFrame({"game_id": game_id, "play_id": [1, 2, 3], "yards_gained": list(yards),
                          "desc": ["run", "pass", "sack"]})
    if extra is not None:
        frame["epa"] = extra
    return frame


# -- Digests ------------------------------------------------------------------

def test_digest_ignores_row_and_column_order_and_int_float_dtype():
    base = pbp.game_digests(plays())
    shuffled = plays().iloc[[2, 0, 1]][["desc", "yards_gained", "play_id", "game_id"]]
    as_float = plays().astype({"yards_gained": "float64"})
    assert pbp.game_digests(shuffled) == base
    assert pbp.game_digests(as_float) == base
    assert base["2026_01_A_B"][1] == 3


def test_digest_changes_when_nflverse_corrects_a_play():
    assert pbp.game_digests(plays(yards=(5, 8, -2))) != pbp.game_digests(plays())
    with_epa = pbp.game_digests(plays(extra=[0.1, float("nan"), -1.2]))
    assert with_epa != pbp.game_digests(plays(extra=[0.1, 0.3, -1.2]))


def test_release_changes_classifies_new_corrected_and_baseline():
    current = {"g1": ("d1", 10), "g2": ("d2-new", 10), "g3": ("d3", 10), "g4": ("d4", 10)}
    labelled = {"g1", "g2", "g3", "g9"}
    stored = {"g1": "d1", "g2": "d2-old"}
    changes = pbp.release_changes(labelled, stored, current)
    assert changes == {"new": ["g4"], "corrected": ["g2"], "baseline": ["g3"]}


# -- Scope and failures -------------------------------------------------------

class FakeDb:
    def __init__(self, seasons=(2026,), completed=0):
        self.seasons, self.completed = list(seasons), completed

    def execute(self, sql, params=None):
        if "SELECT DISTINCT season" in sql:
            return [{"season": s} for s in self.seasons]
        if "SELECT DISTINCT game_id FROM nfl_pbp_archetypes" in sql:
            return [{"game_id": "2026_01_A_B"}]
        raise AssertionError(sql)

    def execute_one(self, sql, params=None):
        assert "nfl_season_games" in sql
        return {"n": self.completed}


def test_the_current_season_is_scoped_in_even_before_the_table_has_it():
    september_2027 = datetime(2027, 9, 15, tzinfo=timezone.utc)
    assert pbp.scoped_seasons(FakeDb([2025, 2026]), september_2027) == [2026, 2027]
    assert pbp.scoped_seasons(FakeDb([2026]), datetime(2026, 9, 29, tzinfo=timezone.utc)) == [2026]
    # January belongs to the season that started the previous September.
    assert pbp.scoped_seasons(FakeDb([]), datetime(2027, 1, 10, tzinfo=timezone.utc)) == [2026]


def test_a_download_failure_after_games_are_played_is_raised(monkeypatch):
    def unavailable(season, cache=None):
        raise OSError("HTTP Error 502: Bad Gateway")
    monkeypatch.setattr(pbp, "load_pbp", unavailable)
    with pytest.raises(RuntimeError, match="46 regular-season games are completed"):
        pbp.load_release(FakeDb(completed=46), 2026)
    # Before any game is played, a missing release is simply not published yet.
    assert pbp.load_release(FakeDb(completed=0), 2027) is None


def test_missing_games_relabels_corrections_and_records_a_baseline_without_relabelling(monkeypatch):
    release = pd.concat([plays("2026_01_A_B", yards=(5, 8, -2)), plays("2026_01_C_D")])
    recorded = {}
    monkeypatch.setattr(pbp, "load_pbp", lambda season, cache=None: release)
    monkeypatch.setattr(pbp, "record_digests", lambda db, season, d: recorded.update(d) or len(d))
    now = datetime(2026, 9, 29, tzinfo=timezone.utc)

    # First check: A_B was labelled before digests existed -> baseline only.
    monkeypatch.setattr(pbp, "stored_digests", lambda db, season: {})
    todo, releases = pbp.missing_games(FakeDb(completed=46), now=now)
    assert todo == {2026: ["2026_01_C_D"]}           # only the never-labelled game
    assert set(recorded) == {"2026_01_A_B"} and 2026 in releases

    # Later: nflverse corrects A_B after its digest was recorded -> relabel it.
    monkeypatch.setattr(pbp, "stored_digests",
                        lambda db, season: {"2026_01_A_B": pbp.game_digests(plays("2026_01_A_B"))["2026_01_A_B"][0]})
    todo, _ = pbp.missing_games(FakeDb(completed=46), now=now)
    assert todo == {2026: ["2026_01_A_B", "2026_01_C_D"]}


def test_relabel_stale_exits_non_zero_when_the_release_cannot_be_read(monkeypatch):
    class Db(FakeDb):
        def __init__(self, *args, **kwargs):
            super().__init__(completed=46)

    monkeypatch.setattr(pbp, "DatabaseManager", Db)
    monkeypatch.setattr(pbp, "stale_games", lambda db: {})
    monkeypatch.setattr(pbp, "load_pbp", lambda season, cache=None: (_ for _ in ()).throw(OSError("404")))
    monkeypatch.setattr("sys.argv", ["pbp", "--relabel-stale", "--database-url", "postgresql://unused"])
    with pytest.raises(RuntimeError, match="could not be read"):
        pbp.main()
