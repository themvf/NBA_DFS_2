"""Repeat the pregame research scenario/report cycle without fitting any model."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from hashlib import sha256
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from model.nfl_dfs_context_variant_study import timestamp

ROOT = Path(__file__).resolve().parents[1]


def preflight(comparison, now, *, not_before=None, season=None, week=None):
    """An older saved artifact cannot stand in for this run's missing slate."""
    if comparison is None:
        return "missing_current_comparison"
    if season is not None and comparison.get("season") != season:
        return "comparison_season_differs"
    if week is not None and comparison.get("week") != week:
        return "comparison_week_differs"
    captured = timestamp(comparison["as_of_at"])
    lower = max(now-timedelta(hours=2), timestamp(not_before)) if not_before else now-timedelta(hours=2)
    if not lower <= captured <= now:
        return "comparison_not_fresh_for_this_run"
    if not comparison.get("players"):
        return "no_supported_salary_players"
    if any(timestamp(player["kickoff"]) <= now for player in comparison["players"]):
        return "comparison_contains_started_game"
    if comparison.get("baseline_version") != "nfl-dfs-historical-v5":
        raise ValueError("coherent study requires the registered v5 baseline")
    return None


def run_cycle(output_dir, *, persist=False, dry_run=False, not_before=None,
              season=None, week=None, draws=300, now=None, runner=subprocess.run):
    clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
    now = clock()
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    comparison_path = output_dir/"slate-comparison.json"
    run_dir = output_dir/"scenario-runs"/(now.strftime("%Y%m%dT%H%M%S%fZ")+"-"+uuid4().hex[:12])
    status_path = output_dir/("scenario-refresh-dry-run.json" if dry_run else "scenario-refresh-status.json")
    report = {"version":"nfl-matchup-scenario-refresh-v1", "checked_at":now.isoformat(),
              "production_changed":False, "model_refitted":False, "persist_requested":persist,
              "dry_run":dry_run, "steps":[], "snapshot_dir":str(run_dir)}

    def save():
        status_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        if not dry_run and run_dir.exists():
            (run_dir/"refresh-status.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
            if report.get("status") not in ("running","ready"):
                hashes={p.name:{"sha256":sha256(p.read_bytes()).hexdigest(),"bytes":p.stat().st_size}
                        for p in run_dir.iterdir() if p.is_file() and p.name!="archive-manifest.json"}
                (run_dir/"archive-manifest.json").write_text(json.dumps({"snapshot_dir":str(run_dir),"files":hashes},indent=2),encoding="utf-8")

    try:
        comparison_bytes = comparison_path.read_bytes() if comparison_path.exists() else None
        comparison = json.loads(comparison_bytes) if comparison_bytes else None
        reason = preflight(comparison, now, not_before=not_before, season=season, week=week)
        if reason and not dry_run:
            # This is a new path on every run: a no-slate result cannot expose
            # a previous Showdown capture as if it were current.
            run_dir.mkdir(parents=True,exist_ok=False)
            comparison_path=run_dir/"slate-comparison.json"
            command=[sys.executable,"-m","research.nfl_showdown_matchup_capture","--upcoming","--output",str(comparison_path)]
            for flag,value in (("--season",season),("--week",week)):
                if value is not None:
                    command.extend([flag,str(value)])
            report.update(status="running",primary_comparison_status=reason)
            entry={"name":"showdown-capture","started_at":clock().isoformat(),"log_file":str(run_dir/"showdown-capture.log")}
            report["steps"].append(entry)
            save()
            with Path(entry["log_file"]).open("w",encoding="utf-8") as log:
                completed=runner(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,timeout=300,check=False)
            entry["returncode"]=completed.returncode
            if completed.returncode:
                report.update(status="failed",failed_step="showdown-capture")
                save()
                return report,completed.returncode
            comparison_bytes=comparison_path.read_bytes() if comparison_path.exists() else None
            comparison=json.loads(comparison_bytes) if comparison_bytes else None
            reason=preflight(comparison,clock(),not_before=entry["started_at"],season=season,week=week)
        if reason:
            report.update(status="no_current_pregame_slate", reason=reason)
            save()
            return report, 0
        if not dry_run and not run_dir.exists():
            run_dir.mkdir(parents=True,exist_ok=False)
            comparison_path=run_dir/"slate-comparison.json"
            comparison_path.write_bytes(comparison_bytes)
        elif dry_run:
            comparison_path=run_dir/"slate-comparison.json"
        report.update(status="ready", season=comparison["season"], week=comparison["week"],
                      upload_id=comparison["upload_id"], baseline_run_id=comparison["baseline_run_id"],
                      comparison_as_of_at=comparison["as_of_at"])
        bank = run_dir/"coherent-scenario-input.json"
        marginals = run_dir/"marginal-scenario-input.json"
        summary = run_dir/"coherent-scenario-summary.json"
        portfolios = run_dir/"portfolio-coherent-comparison.json"
        commands = [
            ("baseline-marginals", ROOT, [sys.executable,"-m","research.nfl_matchup_scenario_export",
                "--upload",str(comparison["upload_id"]),"--run",str(comparison["baseline_run_id"]),
                "--comparison",str(comparison_path),"--output",str(marginals)]),
            ("scenario-export", ROOT, [sys.executable,"-m","research.nfl_coherent_scenario_export",
                "--comparison",str(comparison_path),"--baseline-marginals",str(marginals),"--output",str(bank),"--draws",str(draws)]),
            ("coherent-verification", ROOT/"web", ["node","-r","./scripts/server-only-stub.cjs","--import","tsx",
                "./scripts/verify-nfl-coherent-bank.ts",str(bank),str(run_dir/"coherent-bank-verification.json")]),
            ("portfolio-comparison", ROOT/"web", ["node","-r","./scripts/server-only-stub.cjs","--import","tsx",
                "./scripts/compare-nfl-matchup-portfolios.ts",str(bank),str(comparison_path),str(portfolios)]),
            ("compact-report", ROOT, [sys.executable,"-m","research.nfl_matchup_report_publish",
                "--comparison",str(comparison_path),"--coherent",str(summary),"--portfolios",str(portfolios),
                "--output",str(run_dir/"compact-matchup-report.json")]+(["--apply"] if persist else [])),
        ]
        outputs = {
            "baseline-marginals":[marginals],
            "scenario-export":[bank,summary,run_dir/"coherent-scenario-input-selection-ledger.json",run_dir/"coherent-scenario-input-evaluation-ledger.json"],
            "coherent-verification":[run_dir/"coherent-bank-verification.json"],
            "portfolio-comparison":[portfolios,portfolios.with_suffix(".md")],
            "compact-report":[run_dir/"compact-matchup-report.json"],
        }
        if dry_run:
            report.update(status="dry_run_ready", planned_steps=[{"name":name,"cwd":str(cwd),"command":command} for name,cwd,command in commands])
            save()
            return report, 0
        report["status"] = "running"
        save()
        for name, cwd, command in commands:
            if any(timestamp(player["kickoff"]) <= clock() for player in comparison["players"]):
                report.update(status="no_current_pregame_slate",reason="kickoff_reached_during_refresh",skipped_step=name)
                save()
                return report, 0
            entry = {"name":name,"started_at":datetime.now(timezone.utc).isoformat(),
                     "log_file":str(run_dir/(name+".log"))}
            report["steps"].append(entry)
            save()
            with Path(entry["log_file"]).open("w",encoding="utf-8") as log:
                completed = runner(command,cwd=cwd,stdout=log,stderr=subprocess.STDOUT,timeout=900,check=False)
            entry.update(returncode=completed.returncode,completed_at=datetime.now(timezone.utc).isoformat())
            if completed.returncode:
                report.update(status="failed",failed_step=name)
                save()
                return report, completed.returncode
            stale = [str(path) for path in outputs[name] if not path.exists() or path.stat().st_mtime < timestamp(entry["started_at"]).timestamp()]
            if stale:
                report.update(status="failed",failed_step=name,reason="missing_or_stale_stage_artifact",invalid_artifacts=stale)
                save()
                return report, 1
            save()
        report.update(status="completed",persisted=persist,completed_at=datetime.now(timezone.utc).isoformat())
        save()
        return report, 0
    except Exception as exc:
        report.update(status="failed",error_type=type(exc).__name__)
        save()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    today = datetime.now(timezone.utc)
    parser.add_argument("--output-dir",type=Path,default=Path("artifacts/nfl-matchup-implementation")/today.date().isoformat())
    parser.add_argument("--season",type=int)
    parser.add_argument("--week",type=int)
    parser.add_argument("--not-before",help="Earliest eligible comparison capture, normally this workflow's start")
    parser.add_argument("--draws",type=int,default=300)
    parser.add_argument("--persist",action="store_true",help="Publish only the validated compact research report")
    parser.add_argument("--dry-run",action="store_true",help="Validate pregame inputs and print planned steps without exporting or writing the database")
    args = parser.parse_args(argv)
    if args.draws < 100:
        parser.error("at least 100 research draws are required")
    report, code = run_cycle(args.output_dir,persist=args.persist,dry_run=args.dry_run,not_before=args.not_before,
                            season=args.season,week=args.week,draws=args.draws)
    print(json.dumps(report,indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
