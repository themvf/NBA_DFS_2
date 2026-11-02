"""Who took the field: a player DraftKings lists with a suffix our canonical name lacks."""

from __future__ import annotations

from ingest.nfl_dfs_field_audit import scored_names
from model.nfl_dfs_field_audit import normalize_name


class _FakeDb:
    """Results join to ff_players by canonical name ('James Cook'); the slate
    join returns DraftKings' own name ('James Cook III') for the same player id."""

    def __init__(self, canonical, slate_linked):
        self.canonical, self.slate_linked = canonical, slate_linked

    def execute(self, sql, params=None):
        if "FROM nfl_dfs_slate_players" in sql:
            return [{"name": n} for n in self.slate_linked]
        if "FROM nfl_dfs_player_week_results" in sql:
            return [{"name": n} for n in self.canonical]
        raise AssertionError(sql)


WEEK8 = {  # DraftKings name -> ff_players.canonical_name, 2026 week 8
    "James Cook III": "James Cook",
    "Kyle Pitts Sr.": "Kyle Pitts",
    "KC Concepcion Jr.": "KC Concepcion",
    "Aaron Jones Sr.": "Aaron Jones",
    "Anthony Richardson Sr.": "Anthony Richardson",
    "Travis Hunter": "Travis Hunter",
}


def test_suffixed_draftkings_names_read_as_played_through_the_slate_link():
    db = _FakeDb(canonical=list(WEEK8.values()) + ["Atlanta Falcons"], slate_linked=list(WEEK8))
    played = scored_names(db, 2026, 8, "e3aea255-4919-4d1e-94ed-927cd75b2705")
    for dk_name in WEEK8:
        assert normalize_name(dk_name) in played, dk_name
    assert normalize_name("Falcons") in played


def test_without_the_slate_the_canonical_name_alone_misses_them():
    """The defect this guards: the name-only join, kept as the fallback."""
    db = _FakeDb(canonical=list(WEEK8.values()), slate_linked=[])
    played = scored_names(db, 2026, 8)
    assert normalize_name("James Cook III") not in played
    assert normalize_name("Travis Hunter") in played


def test_unknown_week_declines():
    assert scored_names(_FakeDb([], []), None, 8, "u") is None
