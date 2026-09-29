"""Deactivating a FantasyPros-only duplicate needs the same player, not just the same name."""

from __future__ import annotations

from datetime import datetime, timezone

from ingest import ff_dedupe_identities as dedupe


class FakeDb:
    def __init__(self, candidates):
        self.candidates = candidates
        self.updates = []

    def execute(self, sql, params=None):
        assert "a.gsis_id IS NULL AND a.sleeper_player_id IS NULL" in sql
        return self.candidates

    def execute_many(self, sql, params_list):
        self.updates.append((sql, params_list))


def candidate(fp_id, name, team, twin_team, twin_id=500):
    return {"id": fp_id, "canonical_name": name, "position": "WR", "team_abbrev": team,
            "fantasypros_player_id": 1, "twin_id": twin_id, "twin_gsis": "00-1", "twin_team": twin_team}


def test_same_name_on_another_team_is_a_different_player():
    db = FakeDb([candidate(34, "Puka Nacua", "LAR", "LAR"),
                 candidate(40, "Mike Williams", "NYJ", "LAC")])
    rows = dedupe.dedupe(db, 2026)
    assert [r["id"] for r in rows] == [34]
    assert db.updates[0][1] == [(34,)]


def test_team_aliases_match_and_missing_teams_never_do():
    db = FakeDb([candidate(30, "Trevor Lawrence", "JAC", "JAX"),
                 candidate(31, "No Team", None, "JAX"),
                 candidate(32, "Blank Team", "", "")])
    assert [r["id"] for r in dedupe.dedupe(db, 2026)] == [30]


def test_dry_run_writes_nothing():
    db = FakeDb([candidate(34, "Puka Nacua", "LAR", "LAR")])
    assert len(dedupe.dedupe(db, 2026, dry_run=True)) == 1
    assert db.updates == []


def test_season_defaults_to_the_current_nfl_season_not_a_constant(monkeypatch):
    seen = {}

    class Db(FakeDb):
        def __init__(self, *args, **kwargs):
            super().__init__([])

        def execute(self, sql, params=None):
            seen["season"] = params[0]
            return []

    monkeypatch.setattr(dedupe, "DatabaseManager", Db)
    monkeypatch.setattr(dedupe, "load_config", lambda: type("C", (), {"database_url": "x"})())
    monkeypatch.setattr("sys.argv", ["dedupe", "--dry-run"])
    expected = dedupe.target_season(None, datetime.now(timezone.utc))
    dedupe.main()
    assert seen["season"] == expected
    assert dedupe.target_season(None, datetime(2027, 2, 1, tzinfo=timezone.utc)) == 2026
