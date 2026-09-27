from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from pathlib import Path
from hashlib import sha256

from research.nfl_matchup_scenario_refresh import preflight, run_cycle

NOW = datetime(2026,9,27,15,tzinfo=timezone.utc)


def comparison():
    return {"season":2026,"week":3,"as_of_at":(NOW-timedelta(minutes=5)).isoformat(),
            "upload_id":"slate", "baseline_run_id":"baseline", "baseline_version":"nfl-dfs-historical-v5",
            "players":[{"kickoff":(NOW+timedelta(hours=2)).isoformat()}]}


def test_missing_stale_or_locked_comparison_cannot_generate_new_scenarios():
    assert preflight(None,NOW) == "missing_current_comparison"
    saved = comparison()
    assert preflight(saved,NOW) is None
    assert preflight(saved,NOW,not_before=NOW.isoformat()) == "comparison_not_fresh_for_this_run"
    saved["players"][0]["kickoff"] = NOW.isoformat()
    assert preflight(saved,NOW) == "comparison_contains_started_game"


def test_dry_run_never_calls_export_or_publisher(tmp_path):
    (tmp_path/"slate-comparison.json").write_text(json.dumps(comparison()))
    def unexpected(*args,**kwargs):
        raise AssertionError("dry run executed a subprocess")
    result, code = run_cycle(tmp_path,now=NOW,persist=True,dry_run=True,runner=unexpected)
    assert code == 0 and result["status"] == "dry_run_ready"
    assert len(result["planned_steps"]) == 5
    assert not result["model_refitted"] and not result["production_changed"]


def test_failure_preserves_status_and_logs_and_never_publishes(tmp_path):
    (tmp_path/"slate-comparison.json").write_text(json.dumps(comparison()))
    calls = []
    def failure(command,**kwargs):
        calls.append(command)
        kwargs["stdout"].write("retained failure evidence")
        return SimpleNamespace(returncode=7)
    result, code = run_cycle(tmp_path,now=NOW,persist=True,runner=failure)
    assert code == 7 and result["failed_step"] == "baseline-marginals"
    assert len(calls) == 1
    snapshot=Path(result["snapshot_dir"])
    assert (snapshot/"baseline-marginals.log").read_text() == "retained failure evidence"
    assert json.loads((tmp_path/"scenario-refresh-status.json").read_text())["status"] == "failed"


def test_zero_exit_without_new_artifacts_cannot_reuse_a_stale_bank(tmp_path):
    (tmp_path/"slate-comparison.json").write_text(json.dumps(comparison()))
    (tmp_path/"coherent-scenario-input.json").write_text('{"old":true}')
    calls = []
    def incomplete(command,**kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)
    result, code = run_cycle(tmp_path,now=NOW,persist=True,runner=incomplete)
    assert code == 1 and result["reason"] == "missing_or_stale_stage_artifact"
    assert len(calls) == 1


def test_distinct_reruns_retain_previous_frozen_bytes_and_hash_manifests(tmp_path):
    (tmp_path/"slate-comparison.json").write_text(json.dumps(comparison()))
    def failure(command,**kwargs):
        kwargs["stdout"].write("bank generation stopped")
        return SimpleNamespace(returncode=2)
    first,_=run_cycle(tmp_path,now=NOW,runner=failure)
    first_dir=Path(first["snapshot_dir"])
    retained=(first_dir/"slate-comparison.json").read_bytes()
    changed=comparison(); changed["baseline_run_id"]="second-baseline"
    (tmp_path/"slate-comparison.json").write_text(json.dumps(changed))
    second,_=run_cycle(tmp_path,now=NOW,runner=failure)
    assert first["snapshot_dir"]!=second["snapshot_dir"]
    assert (first_dir/"slate-comparison.json").read_bytes()==retained
    archive=json.loads((first_dir/"archive-manifest.json").read_text())
    assert archive["files"]["slate-comparison.json"]["sha256"]==sha256(retained).hexdigest()


def test_no_showdown_capture_cannot_reuse_a_previous_saved_file(tmp_path):
    (tmp_path/"coherent-scenario-input.json").write_text('{"old":true}')
    calls=[]
    def no_slate(command,**kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)
    result,code=run_cycle(tmp_path,now=NOW,runner=no_slate)
    assert code==0 and result["status"]=="no_current_pregame_slate"
    assert len(calls)==1 and "research.nfl_showdown_matchup_capture" in calls[0]
