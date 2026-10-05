"""A rebuild whose output matches the newest run reuses it instead of writing a copy.

2026-10-04: the hourly availability job wrote a full projection run on every
tick (51 runs, 258 MB of rows in one day) although 68 of the preceding 120
runs were numerically identical to the run before them. These tests pin what
"identical" means (everything a consumer reads, nothing a capture renumbers)
and the cases where a run may not be reused.
"""
from datetime import datetime, timedelta, timezone

import ingest.nfl_dfs_projections as proj

NOW = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)


def player(pid=1, **over):
    row = {"player_id": pid, "player_gsis_id": f"00-{pid}", "player_name": "A", "normalized_name": "a",
           "team": "CLE", "opponent": "PIT", "position": "WR", "projection_status": "historical",
           "history_games": 10, "prior_games": 0, "model_proj_fpts": 10.0, "baseline_fpts": 10.0,
           "floor_fpts": 2.0, "median_fpts": 9.0, "ceiling_fpts": 20.0, "boom_rate": 0.1, "confidence": 0.5,
           "stat_means": {"receptions": 4.0}, "event_id": "e1", "game_id": "g1", "commence_time": NOW,
           "availability": {"status": "available", "transferred": [], "version": 2},
           "feature_snapshot": {"seed": 1, "matchup": {"evidence": {"home": "CLE", "ctx": [1, 2]},
                                                       "shadow": {"delta": 0.4, "ledger": ["run-specific"]}}}}
    row.update(over)
    return row


def decision(**over):
    base = {"state": "AVAILABLE", "projection_status": "historical", "reason": "no qualifying status",
            "source": "sleeper", "kickoff": "2026-10-04T17:00:00+00:00", "version": 3,
            "as_of_at": "2026-10-04T17:59:00+00:00", "available_at": "2026-10-04T17:55:00+00:00",
            "observation_id": 501, "source_snapshot_id": 77, "qualifying_observation_ids": [501],
            "display_only_observation_ids": [], "qualifying_source_snapshot_ids": [77],
            "display_only_source_snapshot_ids": []}
    base.update(over)
    return base


def manifest(decisions=None, config=None):
    return {"season": 2026, "week": 5, "model_config": config or {"availability_qb_transfer_enabled": True},
            "availability_decisions": decisions if decisions is not None else {"1": decision()}}


def test_recaptured_identifiers_and_the_shadow_ledger_do_not_change_the_digest():
    first = proj.output_digest([player()], manifest())
    renumbered = manifest({"1": decision(as_of_at="2026-10-04T18:59:00+00:00", available_at="2026-10-04T18:55:00+00:00",
                                         observation_id=777, source_snapshot_id=91, qualifying_observation_ids=[777],
                                         qualifying_source_snapshot_ids=[91])})
    row = player()
    row["feature_snapshot"]["matchup"]["shadow"] = {"delta": -0.2, "ledger": ["a later run"]}
    assert proj.output_digest([row], renumbered) == first


def test_the_digest_covers_everything_a_consumer_reads():
    base = proj.output_digest([player()], manifest())
    assert proj.output_digest([player(model_proj_fpts=10.1)], manifest()) != base, "a projection number"
    assert proj.output_digest([player(projection_status="out")], manifest()) != base, "the status"
    assert proj.output_digest([player(stat_means={"receptions": 4.5})], manifest()) != base, "a stat line"
    assert proj.output_digest([player(ceiling_fpts=21.0)], manifest()) != base, "the distribution"
    assert proj.output_digest([player(availability={"status": "transferred", "version": 2})], manifest()) != base, "the transfer result"
    assert proj.output_digest([player()], manifest({"1": decision(state="OUT", reason="IR")})) != base, "the decision itself"
    assert proj.output_digest([player()], manifest(config={"availability_qb_transfer_enabled": False})) != base, "the configuration"
    changed = player()
    changed["feature_snapshot"]["matchup"]["evidence"] = {"home": "CLE", "ctx": [9]}
    assert proj.output_digest([changed], manifest()) != base, "the frozen matchup evidence"
    assert proj.output_digest([player(), player(2)], manifest()) != base, "the player set"


def test_a_persisted_row_with_an_evidence_digest_matches_the_in_memory_row():
    # persist_week stores the evidence by digest; a row read back carries only the digest.
    evidence = {"home": "CLE", "ctx": [1, 2]}
    stored = player()
    stored["feature_snapshot"] = {"seed": 1, "matchup": {"evidence_digest": proj.artifact_digest(evidence), "shadow": None}}
    assert proj.output_digest([stored], manifest()) == proj.output_digest([player()], manifest())


def test_special_teams_readiness_changes_reuse_identity_even_without_players():
    legacy = manifest()
    current = {**legacy, "special_teams_candidate_version": proj.SPECIAL_TEAMS_VERSION,
               "special_teams_candidate_coverage": {"DST": {"available": 2, "unavailable": 0}}}
    assert proj.output_digest([], current) != proj.output_digest([], legacy)
    assert proj.output_digest([], {**current, "special_teams_candidate_version": "next-version"}) != proj.output_digest([], current)
    assert proj.output_digest([], {**current, "special_teams_candidate_coverage": {"DST": {"available": 1, "unavailable": 1}}}) != proj.output_digest([], current)
    row = player(position="DST")
    row["feature_snapshot"]["special_teams_candidate"] = {"version": proj.SPECIAL_TEAMS_VERSION, "mean": 8}
    assert proj.output_digest([row], current) != proj.output_digest([player(position="DST")], current)


class RunDB:
    def __init__(self, row):
        self.row = row
        self.queries = []
    def execute_one(self, sql, params=None):
        self.queries.append((" ".join(sql.split()), params))
        return self.row


def test_the_newest_identical_recent_run_is_reused():
    db = RunDB({"run_id": "abc", "as_of_at": NOW - timedelta(minutes=50), "output_digest": "d1"})
    reused = proj.reusable_run(db, season=2026, week=5, digest="d1", now=NOW)
    assert reused["run_id"] == "abc" and reused["age_seconds"] == 3000
    sql, params = db.queries[0]
    assert "ORDER BY as_of_at DESC,created_at DESC LIMIT 1" in sql, "only the newest run can stand in"
    assert params == (2026, 5, proj.MODEL_VERSION)


def test_a_different_output_is_persisted_not_reused():
    db = RunDB({"run_id": "abc", "as_of_at": NOW - timedelta(minutes=50), "output_digest": "d1"})
    assert proj.reusable_run(db, season=2026, week=5, digest="d2", now=NOW) is None


def test_an_identical_run_older_than_the_cap_is_not_reused():
    db = RunDB({"run_id": "abc", "as_of_at": NOW - proj.REUSE_MAX_AGE - timedelta(seconds=1), "output_digest": "d1"})
    assert proj.reusable_run(db, season=2026, week=5, digest="d1", now=NOW) is None
    db = RunDB({"run_id": "abc", "as_of_at": NOW - proj.REUSE_MAX_AGE, "output_digest": "d1"})
    assert proj.reusable_run(db, season=2026, week=5, digest="d1", now=NOW) is not None, "exactly at the cap still reuses"


def test_runs_without_a_digest_and_future_runs_are_never_reused():
    assert proj.reusable_run(RunDB(None), season=2026, week=5, digest="d1", now=NOW) is None
    legacy = RunDB({"run_id": "old", "as_of_at": NOW - timedelta(minutes=5), "output_digest": None})
    assert proj.reusable_run(legacy, season=2026, week=5, digest="d1", now=NOW) is None
    future = RunDB({"run_id": "abc", "as_of_at": NOW + timedelta(minutes=5), "output_digest": "d1"})
    assert proj.reusable_run(future, season=2026, week=5, digest="d1", now=NOW) is None


class RecordingConnection:
    def __init__(self):
        self.statements = []
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False
    def cursor(self):
        return self
    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))
    def fetchone(self):
        return {"created_at": NOW}


class PersistDB:
    def __init__(self):
        self.conn = RecordingConnection()
    def connect(self):
        return self.conn


def test_persist_week_records_the_output_digest_on_the_run(monkeypatch):
    monkeypatch.setattr(proj, "execute_values", lambda *a, **kw: None)
    monkeypatch.setattr(proj, "persist_availability_contexts", lambda *a, **kw: {})
    rows = [player()]
    full = {**manifest(), "artifact_digest": "art", "as_of_at": NOW.isoformat(), "source_evidence": [],
            "seed": 1, "availability": {"policy_mode": "v2", "pregame_frozen_games": [], "unresolved": []},
            "availability_health": {"policy": "p"}, "availability_migration_audit": [],
            "special_teams_candidate_version": proj.SPECIAL_TEAMS_VERSION,
            "special_teams_candidate_coverage": {"DST": {"available": 2, "unavailable": 1}}}
    db = PersistDB()
    proj.persist_week(db, rows, full)
    inserted = [params for sql, params in db.conn.statements if sql.startswith("INSERT INTO nfl_dfs_projection_runs")][0]
    assert inserted[10].adapted["output_digest"] == proj.output_digest(rows, full)
    updated = [params for sql, params in db.conn.statements if sql.startswith("UPDATE nfl_dfs_projection_runs SET availability_manifest")][-1]
    assert updated[0].adapted["output_digest"] == proj.output_digest(rows, full)
    for field in ("special_teams_candidate_version", "special_teams_candidate_coverage"):
        assert inserted[10].adapted[field] == updated[0].adapted[field] == full[field]


def run_main(monkeypatch, argv, reused, capsys):
    import ingest.nfl_dfs_weekly as weekly
    calls = {"persist": 0}
    rows = [player()]
    full = {**manifest(), "artifact_digest": "art", "as_of_at": NOW.isoformat(),
            "availability": {"policy_mode": "v2", "unresolved": []},
            "availability_health": {"policy": "p"}, "availability_migration_audit": [], "matchup_health": {}}
    monkeypatch.setattr(proj, "load_config", lambda: type("C", (), {"database_url": "postgres://x"})())
    monkeypatch.setattr(proj, "DatabaseManager", lambda url: object())
    monkeypatch.setattr(weekly, "target_season", lambda season, now: 2026)
    monkeypatch.setattr(proj, "build_week", lambda db, **kw: (rows, full))
    monkeypatch.setattr(proj, "reusable_run", lambda db, **kw: reused)
    def persist(db, projections, m):
        calls["persist"] += 1
        return "new-run"
    monkeypatch.setattr(proj, "persist_week", persist)
    monkeypatch.setattr("sys.argv", ["projections", "--week", "5", *argv])
    proj.main()
    import json
    out = json.loads(capsys.readouterr().out)
    return out, calls


def test_main_reuses_a_recent_identical_run_and_says_so(monkeypatch, capsys):
    out, calls = run_main(monkeypatch, [], {"run_id": "abc", "as_of_at": NOW.isoformat(), "age_seconds": 600}, capsys)
    assert calls["persist"] == 0
    assert out["run_id"] == "abc" and out["persisted"] is False and out["reused_run"]["run_id"] == "abc"


def test_main_persists_when_nothing_matches(monkeypatch, capsys):
    out, calls = run_main(monkeypatch, [], None, capsys)
    assert calls["persist"] == 1 and out["run_id"] == "new-run" and out["persisted"] is True


def test_always_persist_skips_the_reuse_check(monkeypatch, capsys):
    out, calls = run_main(monkeypatch, ["--always-persist"], {"run_id": "abc", "as_of_at": NOW.isoformat(), "age_seconds": 600}, capsys)
    assert calls["persist"] == 1 and out["persisted"] is True and out["reused_run"] is None
