"""Study registration/write safety; no live database writes."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os

import pytest

from research.nfl_pool_rule_study import (STUDIES, STUDY, archive, assert_forward, enrollment_key, existing_pairs,
                                          freeze_gate, match_games, save_pair)

NOW = datetime(2026, 10, 18, 16, tzinfo=timezone.utc)


def test_forward_boundary_and_population():
    assert_forward(2026, 6, NOW - timedelta(hours=1), NOW, NOW + timedelta(hours=1))
    for season, week in [(2026, 5), (2025, 6), (2027, 6)]:
        with pytest.raises(ValueError, match="population"):
            assert_forward(season, week, NOW, NOW, NOW + timedelta(hours=1))
    with pytest.raises(ValueError, match="before"):
        assert_forward(2026, 6, NOW, NOW, NOW)
    with pytest.raises(ValueError, match="before"):
        assert_forward(2026, 6, NOW + timedelta(seconds=1), NOW, NOW + timedelta(hours=1))


def test_canonical_games_require_unique_complete_match():
    rows = [{"id": 1, "away": "BUF", "home": "LA", "kickoff": NOW},
            {"id": 2, "away": "NYJ", "home": "WSH", "kickoff": NOW}]
    assert [g["id"] for g in match_games(["BUF@LAR", "NYJ@WAS"], rows)] == [1, 2]
    with pytest.raises(ValueError, match="ambiguous"):
        match_games(["BUF@LAR", "NYJ@WAS"], rows + [rows[0]])
    with pytest.raises(ValueError, match="ambiguous"):
        match_games(["BUF@LAR", "NYJ@WAS"], rows[:1])


def test_showdown_slate_must_be_one_game():
    rows = [{"id": 9, "away": "SEA", "home": "DEN", "kickoff": NOW}]
    assert [g["id"] for g in match_games(["SEA@DEN"], rows, "single")] == [9]
    with pytest.raises(ValueError, match="single-game"):
        match_games(["SEA@DEN", "BUF@LAR"], rows, "single")
    with pytest.raises(ValueError, match="multi-game"):
        match_games(["SEA@DEN"], rows, "multi")


def test_enrollment_scope_week_for_classic_game_for_showdown():
    classic, showdown = STUDIES["classic"], STUDIES["showdown"]
    assert enrollment_key(classic, 2026, 6, [3, 1]) == enrollment_key(classic, 2026, 6, [7])
    assert enrollment_key(showdown, 2026, 6, [3]) != enrollment_key(showdown, 2026, 6, [7])
    assert classic["version"] != showdown["version"]


def test_portfolio_study_is_registered_with_its_own_version_and_freeze():
    spec = STUDIES["portfolio"]
    assert spec["version"] == "nfl-portfolio-objective-v3" and spec["format"] == "any" and spec["uniquePerGames"]
    assert spec["freeze"].format(format="showdown") == "artifacts/nfl-portfolio-objective-v3/freeze-showdown.json"
    assert len({s["version"] for s in STUDIES.values()}) == len(STUDIES)


def test_freeze_gate_refuses_missing_failed_and_stale(tmp_path, monkeypatch):
    import research.nfl_pool_rule_study as m
    spec = {**STUDIES["portfolio"], "freeze": "freeze.json"}
    monkeypatch.setattr(m, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="No freeze"):
        freeze_gate(spec, "classic")
    module = tmp_path / "mod.ts"
    module.write_text("x", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    good = hashlib.sha256(b"x").hexdigest()
    (tmp_path / "freeze.json").write_text(json.dumps({"frozenAt": "t", "formats": {"classic": {"status": "fail"}, "showdown": {"status": "pass"}},
                                                      "moduleHashes": {"mod.ts": good}}), encoding="utf-8")
    with pytest.raises(ValueError, match="not 'pass'"):
        freeze_gate(spec, "classic")
    assert freeze_gate(spec, "showdown")["status"] == "pass"
    module.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="predates a change"):
        freeze_gate(spec, "showdown")


class RowsReader:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, query, params=()):
        return self.rows


def test_showdown_reroll_is_per_game_and_unscoped_rows_count():
    rows = [{"run_id": "a", "arm": "A", "status": "research_complete", "game_ids": [9]}]
    assert existing_pairs(RowsReader(rows), 2026, 6, STUDIES["showdown"]["version"], [9])
    assert not existing_pairs(RowsReader(rows), 2026, 6, STUDIES["showdown"]["version"], [12])
    legacy = [{"run_id": "b", "arm": "A", "status": "research_complete", "game_ids": None}]
    assert existing_pairs(RowsReader(legacy), 2026, 6, STUDIES["showdown"]["version"], [12])


class FakeDb:
    def __init__(self, *, duplicate=False, fail_second=False, crosses_lock=False, migrated=True):
        self.duplicate, self.fail_second, self.crosses_lock = duplicate, fail_second, crosses_lock
        self.rows, self.pending, self.committed, self.rolled_back, self.clocks = [], [], [], False, 0
        self.migrated = migrated

    @contextmanager
    def connect(self):
        try:
            yield self
            self.committed.extend(self.pending)
        except Exception:
            self.rolled_back = True
            raise
        finally:
            self.pending = []

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, query, params=()):
        self.rows = []
        if "SELECT EXISTS" in query:
            self.rows = [{"ready": self.migrated}]
        elif "SELECT run_id" in query:
            self.rows = [{"run_id": "existing"}] if self.duplicate else []
        elif "SELECT input_digest" in query:
            self.rows = [{"input_digest": "source-digest"}]
        elif "SELECT id,kickoff" in query:
            self.rows = [{"id": 1, "kickoff": NOW + timedelta(minutes=1)}]
        elif "SELECT clock_timestamp" in query:
            self.clocks += 1
            self.rows = [{"now": NOW + timedelta(minutes=2) if self.crosses_lock and self.clocks > 1 else NOW}]
        elif "INSERT INTO nfl_dfs_optimizer_runs" in query:
            if self.fail_second and self.pending:
                raise RuntimeError("second insert failed")
            self.pending.append(params)

    def fetchall(self):
        return self.rows


def pair_fixture():
    captured = {"pairId": "p", "season": 2026, "week": 6, "gameIds": [1],
                "firstKickoff": (NOW + timedelta(minutes=1)).isoformat(), "source": {
                    "run_id": "source", "input_digest": "source-digest", "upload_id": "u", "projection_run_id": "r",
                    "created_at": (NOW - timedelta(hours=1)).isoformat()}}
    pair = {"version": STUDY, "pairId": "p", "arms": [
        {"arm": arm, "optimizerVersion": "v", "settings": {"projectionSource": "our"}, "inputSnapshot": [],
         "inputDigest": "digest", "buildInfo": {}, "requestedLineups": 20, "lineups": [],
         "status": "research_failed", "failureReason": "infeasible"} for arm in ("A", "B")]}
    return captured, pair


def test_dirty_tree_never_saves():
    db = FakeDb()
    captured, pair = pair_fixture()
    captured["implementation"] = {"dirty": True, "dirtyFiles": [" M web/src/app/dfs/nfl/nfl-optimizer.ts"]}
    with pytest.raises(ValueError, match="uncommitted"):
        save_pair(db, captured, pair)
    assert not db.committed


def test_showdown_pair_saves_both_arms():
    db = FakeDb()
    captured, pair = pair_fixture()
    captured["version"] = pair["version"] = STUDIES["showdown"]["version"]
    receipt = save_pair(db, captured, pair)
    assert len(db.committed) == 2 and len(receipt["saved"]) == 2


def test_atomic_pair_preserves_failed_arms():
    db = FakeDb()
    receipt = save_pair(db, *pair_fixture())
    assert len(db.committed) == 2
    assert [r["status"] for r in receipt["saved"]] == ["research_failed", "research_failed"]


@pytest.mark.parametrize("kwargs,match", [
    ({"fail_second": True}, "second insert"), ({"crosses_lock": True}, "before"), ({"duplicate": True}, "reroll"),
    ({"migrated": False}, "migration")])
def test_no_half_pairs_no_postlock_no_rerolls(kwargs, match):
    db = FakeDb(**kwargs)
    with pytest.raises((ValueError, RuntimeError), match=match):
        save_pair(db, *pair_fixture())
    assert not db.committed
    assert db.rolled_back


def test_immutable_checksummed_capture(tmp_path):
    row = archive(tmp_path, "input", {"x": 1})
    data = gzip.decompress((tmp_path / row["file"]).read_bytes())
    assert json.loads(data) == {"x": 1}
    assert hashlib.sha256(data).hexdigest() == row["sha256Uncompressed"]
    with pytest.raises(FileExistsError):
        archive(tmp_path, "input", {"x": 2})


@pytest.mark.skipif(not os.getenv("NFL_STUDY_TEST_DATABASE_URL"), reason="optional PostgreSQL TEMP-table integration")
def test_postgres_migration_and_persistence_in_rolled_back_temp_tables():
    """Exercise real SQL, entirely in pg_temp, then roll back; no public writes."""
    from uuid import uuid4
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from db.schema import MIGRATIONS

    conn = psycopg2.connect(os.environ["NFL_STUDY_TEST_DATABASE_URL"], cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL search_path=pg_temp,public")
            cur.execute("""CREATE TEMP TABLE nfl_dfs_optimizer_runs (
              run_id UUID PRIMARY KEY,upload_id UUID,projection_run_id UUID,optimizer_version TEXT,mode TEXT,
              projection_source TEXT,settings JSONB,input_snapshot JSONB,input_digest TEXT,build_info JSONB,
              requested_lineups INTEGER,generated_lineups INTEGER,status TEXT,failure_reason TEXT,created_at TIMESTAMPTZ,
              CONSTRAINT nfl_dfs_optimizer_runs_status_check CHECK(status IN ('complete','partial','failed')))""")
            cur.execute("""CREATE TEMP TABLE nfl_dfs_lineups (run_id UUID,lineup_number INTEGER,slots JSONB,player_ids JSONB,
              total_salary INTEGER,projected_fpts FLOAT,floor_fpts FLOAT,ceiling_fpts FLOAT,projected_ownership FLOAT,stack_summary JSONB)""")
            cur.execute("CREATE TEMP TABLE nfl_season_games (id INTEGER,kickoff TIMESTAMPTZ)")
            cur.execute("SELECT clock_timestamp() AS now")
            now = cur.fetchone()["now"]
            source_id, upload_id, projection_id = [str(uuid4()) for _ in range(3)]
            cur.execute("INSERT INTO nfl_season_games VALUES (1,%s)", (now + timedelta(hours=1),))
            cur.execute("INSERT INTO nfl_dfs_optimizer_runs (run_id,input_digest,status) VALUES (%s,'source-digest','complete')", (source_id,))
            migration = next(m for m in MIGRATIONS if "ADD CONSTRAINT nfl_dfs_optimizer_runs_status_check" in m)
            cur.execute(migration)
            cur.execute(migration)  # idempotent
        class TransactionHarness:
            @contextmanager
            def connect(self):
                with conn.cursor() as cur:
                    cur.execute("SAVEPOINT pair_test")
                try:
                    yield conn
                except Exception:
                    with conn.cursor() as cur:
                        cur.execute("ROLLBACK TO SAVEPOINT pair_test")
                    raise
                else:
                    with conn.cursor() as cur:
                        cur.execute("RELEASE SAVEPOINT pair_test")
        captured, pair = pair_fixture()
        captured.update(firstKickoff=(now + timedelta(hours=1)).isoformat())
        captured["source"].update(run_id=source_id, upload_id=upload_id, projection_run_id=projection_id,
                                  created_at=(now - timedelta(minutes=1)).isoformat())
        for arm in pair["arms"]:
            arm["settings"]["researchStudy"] = {"version": STUDY, "season": 2026, "week": 6, "arm": arm["arm"]}
            arm["lineups"] = [{"lineupNumber": 1, "slots": [], "playerIds": [], "totalSalary": 49900,
                                "projectedFpts": 150.5, "floorFpts": 80, "ceilingFpts": 200,
                                "projectedOwnership": None, "stackSummary": {}}]
        receipt = save_pair(TransactionHarness(), captured, pair)
        assert len(receipt["saved"]) == 2
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM nfl_dfs_lineups")
            assert cur.fetchone()["n"] == 2
            cur.execute("SELECT count(*) AS n FROM nfl_dfs_optimizer_runs WHERE status IN ('complete','partial') AND generated_lineups>0")
            assert cur.fetchone()["n"] == 0, "normal saved-run selector excludes research arms"
        with pytest.raises(ValueError, match="reroll"):
            save_pair(TransactionHarness(), captured, pair)
    finally:
        conn.rollback()
        conn.close()


def test_grader_curve_rank_and_verdict_rules():
    from research.nfl_pool_rule_grade import curve_at, rank_of, verdict, paired_summary
    curve = [[1, 100.0], [2, 99.0], [10, 90.0], [100, 50.0]]
    assert curve_at(curve, 0.01, 1000) == 90.0 and curve_at(curve, 0.001, 1000) == 100.0
    assert rank_of(curve, 120) == 1 and rank_of(curve, 95) == 3 and rank_of(curve, 10) == 101
    assert verdict("showdown", [])["status"] == "no verdict"
    same = [{"pairId": str(i), "A": {"median": 100, "top1Pct": 1}, "B": {"median": 100, "top1Pct": 1}} for i in range(8)]
    assert verdict("showdown", same)["status"] == "inconclusive"
    better = [{"pairId": str(i), "A": {"median": 100, "top1Pct": 0}, "B": {"median": 103 + i % 2, "top1Pct": 1}} for i in range(8)]
    assert verdict("showdown", better)["status"] == "B supported"
    assert verdict("portfolio", better)["status"] == "B supported"
    worse = [{"pairId": str(i), "A": {"median": 100, "top1Pct": 1}, "B": {"median": 95 - i % 2, "top1Pct": 0}} for i in range(8)]
    assert verdict("showdown", worse)["status"] == "B rejected"
    assert paired_summary(better, "median")["positive"] == 8
    # Only the milestone's slates are looked at, in order; the ninth does not move an 8-slate verdict.
    assert verdict("showdown", better + [{"pairId": "9", "A": {"median": 100, "top1Pct": 0}, "B": {"median": 50, "top1Pct": 0}}])["milestone"] == 8
