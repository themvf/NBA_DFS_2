"""Capture opponent (defensive) adjustments for a salary upload when it is made.

The twice-daily projection workflow captures only uploads that already exist
when it runs, so a slate uploaded after its last pregame pass -- PIT@CLE on
2026-10-01, uploaded at 6:00 PM ET after the 5:35 PM run -- never got one. And
moving a slate onto newer projections makes a new upload, which loses the
capture until the next scheduled run.

The web app now writes one request per (upload, projection run, profile) to
`nfl_dfs_defensive_capture_requests` when an upload is created, and dispatches
`capture_nfl_defensive_on_upload.yml`, which runs this worker. The 15-minute
Vercel cron dispatches it again whenever a request is still pending, so a
failed dispatch does not strand one.

Each run drains every claimable request. A claim takes a lease, so two runs
never work the same request. Before capturing, the upload must still exist,
hold every claimed salary row, be bound to the run the request recorded, and
every game must still be ahead -- otherwise the request ends `ineligible` with
the reason. Profiles run independently through the existing capture code
(`research.nfl_matchup_implementation.capture_week`), and a request is only
`captured` once its forecast run and player rows are verified in the database.

Spec: docs/nfl-dfs-freshness-capture-on-upload-spec.md. Captures stay
experimental evidence; nothing here changes an optimizer default.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

PFR, VOLUME = "pfr-efficiency", "allowed-rushing-volume"
PROFILES = (PFR, VOLUME)
HISTORICAL_V5 = "nfl-dfs-historical-v5"
MODEL_VERSIONS = {PFR: "nfl-matchup-shadow-v1", VOLUME: "nfl-allowed-rushing-volume-v1"}
LEASE = timedelta(minutes=20)
MAX_ATTEMPTS = 3

from db.schema import NFL_DEFENSIVE_CAPTURE_REQUESTS_DDL as REQUESTS_DDL


def _now() -> datetime:
    return datetime.now(timezone.utc)


def preflight_reason(request: dict, upload: dict | None, stored_players: int, run: dict | None,
                     upload_teams: set[str], pregame: set[str]) -> str | None:
    """Why this request must not be captured, or None when it may proceed. Pure."""
    if upload is None:
        return "upload_missing"
    if stored_players <= 0 or stored_players < int(upload.get("player_count") or 0):
        return f"incomplete_upload ({stored_players} of {upload.get('player_count')} salary rows)"
    if str(upload.get("projection_run_id")) != str(request["projection_run_id"]):
        return "upload_bound_to_a_different_run"
    if run is None:
        return "projection_run_missing"
    if run.get("model_version") != HISTORICAL_V5:
        return f"unsupported_baseline ({run.get('model_version')})"
    if not upload_teams:
        return "upload_has_no_teams"
    unknown = sorted(team for team in upload_teams if team not in pregame)
    if unknown:
        return f"slate_started_or_unscheduled ({', '.join(unknown)})"
    return None


def outcome_from_report(profile: str, report: dict) -> dict:
    """Turn capture_week's report for one upload and one profile into a request outcome. Pure."""
    failed = [f for f in report.get("failed", []) if f.get("profile") == profile]
    if failed:
        return {"state": "failed", "error": failed[0].get("error") or "capture failed"}
    entries = report.get("uploads") or []
    if not entries:
        return {"state": "failed", "error": f"upload not selectable for capture ({report.get('status')})"}
    detail = entries[0].get(profile)
    if not isinstance(detail, dict):
        return {"state": "failed", "error": "capture returned no result for this profile"}
    if profile == VOLUME and detail.get("status") == "skipped":
        return {"state": "ineligible", "error": detail.get("reason") or "skipped"}
    if profile == VOLUME and detail.get("status") != "captured":
        return {"state": "failed", "error": detail.get("error") or f"status {detail.get('status')}"}
    persisted = detail.get("persisted")
    if not isinstance(persisted, dict) or not persisted.get("run_id"):
        return {"state": "failed", "error": "capture was not persisted"}
    return {"state": "verify", "capture_run_id": str(persisted["run_id"]), "players": int(persisted.get("players") or 0)}


def verify_capture(db, request: dict, capture_run_id: str, expected_players: int) -> str | None:
    """None when the persisted forecast run matches the request; otherwise why not."""
    row = db.execute_one("""SELECT upload_id, baseline_run_id, model_version,
        (SELECT COUNT(*) FROM nfl_matchup_player_forecasts p WHERE p.run_id=r.run_id) AS players
        FROM nfl_matchup_forecast_runs r WHERE r.run_id=%s""", (capture_run_id,))
    if row is None:
        return "forecast run not found after capture"
    if str(row["upload_id"]) != str(request["upload_id"]):
        return "forecast run belongs to a different upload"
    if str(row["baseline_run_id"]) != str(request["projection_run_id"]):
        return "forecast run pinned to a different projection run"
    if row["model_version"] != MODEL_VERSIONS[request["profile"]]:
        return f"forecast run has model {row['model_version']}"
    if int(row["players"]) != expected_players:
        return f"forecast run holds {row['players']} player rows, expected {expected_players}"
    return None


def give_up_expired(db, now: datetime) -> int:
    """Requests whose lease expired after the last allowed attempt end as failed."""
    rows = db.execute("""UPDATE nfl_dfs_defensive_capture_requests
        SET state='failed', finished_at=%s, updated_at=%s, lease_until=NULL,
            last_error=COALESCE(last_error,'') || ' gave up after ' || attempts || ' attempts (worker lease expired)'
        WHERE state='running' AND lease_until < %s AND attempts >= %s RETURNING request_id""",
        (now, now, now, MAX_ATTEMPTS))
    return len(rows)


def claim(db, now: datetime, run_url: str | None) -> list[dict]:
    """Take every claimable request under a lease. SKIP LOCKED keeps concurrent workers apart.

    A claim proves a worker started, so an earlier dispatch failure is cleared here: the
    cron's retry starts this worker without passing through the web app's recordDispatch.
    """
    return [dict(r) for r in db.execute("""UPDATE nfl_dfs_defensive_capture_requests q
        SET state='running', attempts=q.attempts+1, lease_until=%s, updated_at=%s, worker_run_url=%s,
            dispatch_error=NULL
        WHERE q.request_id IN (
          SELECT request_id FROM nfl_dfs_defensive_capture_requests
          WHERE (state='pending' OR (state='running' AND lease_until < %s)) AND attempts < %s
          ORDER BY requested_at FOR UPDATE SKIP LOCKED)
        RETURNING q.*""", (now + LEASE, now, run_url, now, MAX_ATTEMPTS))]


def finish(db, request_id, state: str, *, error: str | None = None, capture_run_id: str | None = None,
           players: int | None = None) -> None:
    now = _now()
    db.execute("""UPDATE nfl_dfs_defensive_capture_requests
        SET state=%s, last_error=%s, capture_run_id=%s, captured_players=%s,
            lease_until=NULL, finished_at=%s, updated_at=%s
        WHERE request_id=%s AND state='running'""",
        (state, error, capture_run_id, players, now, now, str(request_id)))


def _upload_context(db, request: dict, now: datetime):
    from model.nfl_pfr_supplement import team_code
    from research.nfl_saved_upload_selection import pregame_teams
    upload = db.execute_one("SELECT * FROM nfl_dfs_slate_uploads WHERE upload_id=%s", (str(request["upload_id"]),))
    stored = db.execute_one("SELECT COUNT(*) AS n FROM nfl_dfs_slate_players WHERE upload_id=%s", (str(request["upload_id"]),))
    run = db.execute_one("SELECT run_id, model_version, season, week FROM nfl_dfs_projection_runs WHERE run_id=%s",
                         (str(request["projection_run_id"]),))
    teams = {team_code(r["team"]) for r in db.execute(
        "SELECT DISTINCT team FROM nfl_dfs_slate_players WHERE upload_id=%s", (str(request["upload_id"]),))}
    pregame = pregame_teams(db, season=run["season"], week=run["week"], as_of=now) if run else set()
    return (dict(upload) if upload else None), int(stored["n"] if stored else 0), (dict(run) if run else None), teams, pregame


def process(db, request: dict, *, fitted: dict, output_dir: Path | None) -> dict:
    """Capture one claimed request. Never raises; the request always ends in a terminal state."""
    from research.nfl_matchup_implementation import capture_week
    profile = request["profile"]
    try:
        now = _now()
        upload, stored, run, teams, pregame = _upload_context(db, request, now)
        reason = preflight_reason(request, upload, stored, run, teams, pregame)
        if reason:
            finish(db, request["request_id"], "ineligible", error=reason)
            return {"request_id": str(request["request_id"]), "profile": profile, "state": "ineligible", "reason": reason}
        report = capture_week(db, season=run["season"], week=run["week"], as_of=now, fitted=fitted,
                              profiles={profile}, upload_id=str(request["upload_id"]),
                              # PFR pins the baseline explicitly; allowed-volume reads the upload's own run.
                              baseline_run_id=str(request["projection_run_id"]) if profile == PFR else None,
                              persist=True, output_dir=output_dir)
        outcome = outcome_from_report(profile, report)
        if outcome["state"] == "verify":
            problem = verify_capture(db, request, outcome["capture_run_id"], outcome["players"])
            if problem:
                outcome = {"state": "failed", "error": problem}
            else:
                finish(db, request["request_id"], "captured", capture_run_id=outcome["capture_run_id"], players=outcome["players"])
                return {"request_id": str(request["request_id"]), "profile": profile, "state": "captured",
                        "capture_run_id": outcome["capture_run_id"], "players": outcome["players"]}
        finish(db, request["request_id"], outcome["state"], error=outcome.get("error"))
        return {"request_id": str(request["request_id"]), "profile": profile, "state": outcome["state"], "error": outcome.get("error")}
    except Exception as exc:  # one request must not starve the others
        error = f"{type(exc).__name__}: {exc}"
        try:
            finish(db, request["request_id"], "failed", error=error)
        except Exception:
            pass  # the lease expires and the next run retries it
        return {"request_id": str(request["request_id"]), "profile": profile, "state": "failed", "error": error}


def drain(db, *, fitted: dict, output_dir: Path | None, run_url: str | None) -> dict:
    db.execute(REQUESTS_DDL)
    now = _now()
    gave_up = give_up_expired(db, now)
    claimed = claim(db, now, run_url)
    results = [process(db, request, fitted=fitted, output_dir=output_dir) for request in claimed]
    return {"claimed": len(claimed), "gave_up_expired": gave_up, "results": results,
            "failed": [r for r in results if r["state"] == "failed"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upload-id", help="Logged only: every run drains all claimable requests.")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("artifacts/nfl-defensive-capture-requests") / _now().date().isoformat())
    args = parser.parse_args()
    from config import load_config
    from db.database import DatabaseManager
    from research.nfl_matchup_implementation import MODEL_PATH
    server, repo, run_id = (os.environ.get(k) for k in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID"))
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if server and repo and run_id else None
    fitted = json.loads(MODEL_PATH.read_text())
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        report = drain(db, fitted=fitted, output_dir=args.output_dir, run_url=run_url)
    report["requested_upload_id"] = args.upload_id
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "drain-summary.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, default=str))
    # Red when a capture failed: the page shows it too, but a failing job must be visible on /health.
    if report["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
