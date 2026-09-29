"""Game-week NFL availability capture, monitoring, and pre-lock freezing.

Sleeper capture is authoritative and independent of the optional FantasyPros
job.  The pre-lock writer never resolves football state; it freezes already
published Phase 2 contexts for kickoff waves within the configured horizon.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any

import requests
from psycopg2.extras import Json

from config import load_config
from ingest.ff_fantasypros import RefreshDatabase
from ingest.ff_injuries import persist_injury_observation
from ingest.ff_independent import normalize_team
from ingest.ff_source_contracts import SnapshotProvenance, persist_source_snapshot
from ingest.nfl_dfs_weekly import target_season
from ingest.nfl_target_week import SeasonComplete, target_week
from model.nfl_context_engine import stable_digest


SLEEPER_URL = "https://api.sleeper.app/v1/players/nfl?active=true"
VERSION = "nfl-availability-operations-v1"
# Every capture this module writes is named ``players-live-<season>-<YYYYMMDDHH>``.
# The fantasy-football refresh (``ingest.ff_independent``) ALSO writes
# ``source='sleeper'`` snapshots, as dataset ``players`` with matched_count=0
# (it records the raw roster, not canonical identity matches). Monitoring
# "the latest Sleeper snapshot" without this filter picked that row three times
# a day and raised a false critical (``implausible_rows``) -- and, worse, could
# hide a stale live feed behind a fresh unrelated snapshot.
LIVE_DATASET_PREFIX = "players-live-"


def should_capture(now: datetime, kickoffs: list[datetime]) -> bool:
    """Hourly near kickoff, every two hours otherwise."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    near = any(tz is not None and timedelta(0) <= tz - now <= timedelta(hours=6) for tz in kickoffs)
    return near or now.hour % 2 == 0


def capture_sleeper(db: Any, *, season: int, week: int, now: datetime) -> dict[str, Any]:
    response = requests.get(SLEEPER_URL, timeout=45, headers={"User-Agent": "DFS-Vegas/1.0"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or len(payload) < 1000:
        raise ValueError("Sleeper player response is incomplete; no clearing writes performed")
    players = db.execute(
        """SELECT id,sleeper_player_id FROM ff_players
           WHERE season=%s AND sleeper_player_id IS NOT NULL""",
        (season,),
    )
    by_sleeper = {str(row["sleeper_player_id"]): int(row["id"]) for row in players}
    matched = [(by_sleeper[key], key, raw) for key, raw in payload.items()
               if key in by_sleeper and isinstance(raw, dict)]
    if len(matched) < 500:
        raise ValueError("Sleeper identity coverage is implausibly low; no writes performed")
    digest = hashlib.sha256(response.content).hexdigest()
    dataset = f"{LIVE_DATASET_PREFIX}{season}-{now:%Y%m%d%H}"
    snapshot_id = persist_source_snapshot(db, SnapshotProvenance(
        source="sleeper", dataset=dataset, season=season, week=week,
        request_params={"url": SLEEPER_URL, "captureWindowUtc": now.strftime("%Y-%m-%dT%H:00Z")},
        fetched_at=now, response_hash=digest, row_count=len(payload),
        matched_count=len(matched), unmatched_count=len(payload) - len(matched),
        # Unmatched rows are non-canonical external roster entries, not a
        # missing source segment. Keep the count in first-class audit columns.
        missingness={}, fallback_tier="A",
        model_eligible=True, eligibility_reason="complete live Sleeper roster/injury/depth capture",
    ))
    observations = events = 0
    for player_id, sleeper_id, raw in matched:
        canonical_team = normalize_team(raw.get("team")) or None
        db.execute(
            """UPDATE ff_players SET
                 team_abbrev=COALESCE(%s,team_abbrev),
                 injury_status=%s,
                 metadata=jsonb_set(metadata,'{sleeper}',%s::jsonb,TRUE),
                 fetched_at=%s
               WHERE id=%s""",
            (canonical_team, raw.get("injury_status"), json.dumps({**raw, "player_id": sleeper_id}), now, player_id),
        )
        result = persist_injury_observation(
            db, player_id=player_id, season=season, source="sleeper",
            source_snapshot_id=snapshot_id, row={**raw, "player_id": sleeper_id},
        )
        observations += int(not result["duplicate"])
        events += int(result["event"] is not None)
    return {
        "snapshotId": snapshot_id, "rowCount": len(payload), "matched": len(matched),
        "unmatched": len(payload) - len(matched), "observations": observations,
        "events": events, "capturedAt": now.isoformat(),
    }


def availability_health(db: Any, *, season: int, week: int, now: datetime,
                        persist: bool = True) -> dict[str, Any]:
    """Evaluate capture freshness and context coverage for one game week.

    ``persist=False`` evaluates without recording an operation run (read-only
    diagnostics against production).
    """
    games = db.execute(
        """SELECT nflverse_game_id game_id,kickoff FROM nfl_season_games
           WHERE season=%s AND week=%s AND game_type='REG' ORDER BY kickoff""",
        (season, week),
    )
    live_pattern = f"{LIVE_DATASET_PREFIX}%"
    # Bounded by ``now`` so an evaluation can be replayed as of a past moment.
    latest = db.execute_one(
        """SELECT id,dataset,fetched_at,row_count,matched_count,unmatched_count,status
           FROM ff_source_snapshots WHERE source='sleeper' AND season=%s
             AND dataset LIKE %s AND fetched_at<=%s
           ORDER BY fetched_at DESC,id DESC LIMIT 1""",
        (season, live_pattern, now),
    )
    prior = db.execute_one(
        """SELECT id,dataset,fetched_at,row_count,matched_count,unmatched_count,status
           FROM ff_source_snapshots WHERE source='sleeper' AND season=%s
             AND dataset LIKE %s AND fetched_at<=%s AND id<>COALESCE(%s,-1)
           ORDER BY fetched_at DESC,id DESC LIMIT 1""",
        (season, live_pattern, now, latest["id"] if latest else None),
    )
    game_ids = [str(row["game_id"]) for row in games]
    official = db.execute_one(
        """SELECT COUNT(*)::int observations,
                  COUNT(DISTINCT raw_payload->>'gameId')::int games
           FROM ff_player_injury_observations o
           JOIN ff_source_snapshots s ON s.id=o.source_snapshot_id
           WHERE o.source='nfl_official' AND o.season=%s AND s.week=%s""",
        (season, week),
    )
    rows = db.execute(
        """SELECT definition_id,target_id,payload
           FROM nfl_context_snapshots
           WHERE publication_status='current' AND target_id=ANY(%s)
             AND definition_id IN ('player_game_availability@v1','team_qb_state@v1')""",
        (game_ids,),
    ) if game_ids else []
    latest_run = db.execute_one(
        """SELECT run_id,availability_manifest->'unresolved' unresolved
           FROM nfl_dfs_projection_runs
           WHERE season=%s AND week=%s AND as_of_at<=%s
           ORDER BY as_of_at DESC,created_at DESC LIMIT 1""",
        (season, week, now),
    )
    unresolved_starters = [
        row for row in ((latest_run or {}).get("unresolved") or [])
        if isinstance(row, dict) and row.get("starter_evidence")
    ]
    player_rows = [row for row in rows if row["definition_id"] == "player_game_availability@v1"]
    qb_rows = [row for row in rows if row["definition_id"] == "team_qb_state@v1"]
    states = Counter(str(row["payload"].get("resolved_availability_state") or "UNKNOWN") for row in player_rows)
    teams = {(str(row["target_id"]), str(row["payload"].get("team"))) for row in player_rows}
    alerts: list[dict[str, str]] = []
    age_hours = None
    if not latest:
        alerts.append({"severity": "critical", "code": "missing_sleeper", "message": "No Sleeper capture exists."})
    else:
        age_hours = (now - latest["fetched_at"]).total_seconds() / 3600
        if latest["row_count"] < 1000 or latest["matched_count"] < 500:
            alerts.append({"severity": "critical", "code": "implausible_rows", "message": "Latest Sleeper row or identity count is below its hard floor."})
        if prior and prior["row_count"]:
            row_change = abs(latest["row_count"] - prior["row_count"]) / prior["row_count"]
            if row_change > 0.20:
                alerts.append({"severity": "critical", "code": "row_count_jump", "message": f"Sleeper row count changed {row_change:.0%} from the prior capture."})
        if prior and prior["matched_count"] and latest["matched_count"] < prior["matched_count"] * 0.90:
            alerts.append({"severity": "warning", "code": "identity_coverage_drop", "message": "Canonical Sleeper matches fell by more than 10%."})
        if age_hours > 6:
            alerts.append({"severity": "critical", "code": "sleeper_stale", "message": f"Latest Sleeper capture is {age_hours:.1f} hours old."})
        elif age_hours > 3:
            alerts.append({"severity": "warning", "code": "sleeper_aging", "message": f"Latest Sleeper capture is {age_hours:.1f} hours old."})
    if len(teams) < len(game_ids) * 2:
        alerts.append({"severity": "warning", "code": "missing_team_context", "message": f"Context covers {len(teams)} of {len(game_ids) * 2} team-games."})
    if len(qb_rows) < len(game_ids) * 2:
        alerts.append({"severity": "warning", "code": "missing_qb_context", "message": f"QB context covers {len(qb_rows)} of {len(game_ids) * 2} team-games."})
    if player_rows and states.get("STALE", 0) / len(player_rows) > 0.10:
        alerts.append({"severity": "warning", "code": "stale_context_rate", "message": "More than 10% of player contexts are stale."})
    if unresolved_starters:
        names = ", ".join(f"{row.get('player')} ({row.get('team')})" for row in unresolved_starters[:6])
        alerts.append({"severity": "warning", "code": "starter_promotion_unresolved",
                       "message": f"{len(unresolved_starters)} ruled-out starter(s) have no promoted replacement: {names}."})
    prelock_games = [row for row in games if timedelta(0) < row["kickoff"] - now <= timedelta(minutes=90)]
    if prelock_games and int(official["games"] or 0) < len(prelock_games):
        alerts.append({"severity": "warning", "code": "official_inactives_incomplete", "message": f"Official inactive coverage is {int(official['games'] or 0)} of {len(prelock_games)} games inside the pre-lock window."})
    critical = any(row["severity"] == "critical" for row in alerts)
    status = "critical" if critical else "warning" if alerts else "healthy"
    report = {
        "version": VERSION, "season": season, "week": week, "evaluatedAt": now.isoformat(),
        "status": status, "games": len(game_ids), "teamContexts": len(teams),
        "qbContexts": len(qb_rows), "playerContexts": len(player_rows),
        "officialInactiveObservations": int(official["observations"] or 0),
        "officialInactiveGames": int(official["games"] or 0),
        "states": dict(sorted(states.items())), "latestSleeperSnapshotId": latest["id"] if latest else None,
        "latestSleeperDataset": latest.get("dataset") if latest else None,
        "latestProjectionRunId": str(latest_run["run_id"]) if latest_run else None,
        "unresolvedStarters": [{key: row.get(key) for key in ("player_id", "player", "team", "position", "reason")}
                               for row in unresolved_starters],
        "latestSleeperAgeHours": age_hours, "alerts": alerts,
    }
    run_id = stable_digest(report)
    if persist:
        db.execute(
            """INSERT INTO nfl_availability_operation_runs
                 (run_id,season,week,evaluated_at,status,report)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT(run_id) DO NOTHING""",
            (run_id, season, week, now, status, Json(report)),
        )
    return {**report, "runId": run_id}


def freeze_prelock(db: Any, *, season: int, week: int, now: datetime,
                   horizon_minutes: int = 90) -> list[dict[str, Any]]:
    games = db.execute(
        """SELECT nflverse_game_id game_id,kickoff FROM nfl_season_games
           WHERE season=%s AND week=%s AND game_type='REG'
             AND kickoff>%s AND kickoff<=%s ORDER BY kickoff,game_id""",
        (season, week, now, now + timedelta(minutes=horizon_minutes)),
    )
    waves: dict[datetime, list[str]] = {}
    for row in games:
        waves.setdefault(row["kickoff"], []).append(str(row["game_id"]))
    saved = []
    for kickoff, game_ids in waves.items():
        contexts = db.execute(
            """SELECT snapshot_id,source_snapshot_ids FROM nfl_context_snapshots
               WHERE publication_status='current' AND target_id=ANY(%s)
                 AND as_of_at<=%s AND available_at<=%s
                 AND definition_id IN ('player_game_availability@v1','team_qb_state@v1')
               ORDER BY snapshot_id""",
            (game_ids, now, now),
        )
        run = db.execute_one(
            """SELECT run_id FROM nfl_dfs_projection_runs
               WHERE season=%s AND week=%s AND as_of_at<=%s
               ORDER BY as_of_at DESC,created_at DESC LIMIT 1""",
            (season, week, now),
        )
        context_ids = sorted(str(row["snapshot_id"]) for row in contexts)
        source_ids = sorted({str(value) for row in contexts for value in (row["source_snapshot_ids"] or [])})
        coverage = {"games": len(game_ids), "contexts": len(context_ids), "hasProjectionRun": bool(run)}
        body = {
            "season": season, "week": week, "slateKey": f"{season}-{week}-{kickoff.isoformat()}",
            "decisionAt": now.isoformat(), "kickoffAt": kickoff.isoformat(), "gameIds": sorted(game_ids),
            "projectionRunId": str(run["run_id"]) if run else None,
            "contextSnapshotIds": context_ids, "sourceSnapshotIds": source_ids, "coverage": coverage,
        }
        digest = stable_digest(body); manifest_id = stable_digest({"type": "prelock", **body})
        db.execute(
            """INSERT INTO nfl_availability_prelock_manifests
                 (manifest_id,season,week,slate_key,decision_at,kickoff_at,projection_run_id,
                  game_ids,context_snapshot_ids,source_snapshot_ids,coverage,payload_digest)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(manifest_id) DO NOTHING""",
            (manifest_id, season, week, body["slateKey"], now, kickoff, body["projectionRunId"],
             Json(body["gameIds"]), Json(context_ids), Json(source_ids), Json(coverage), digest),
        )
        saved.append({"manifestId": manifest_id, **body})
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--mode", choices=("capture", "monitor", "freeze", "all"), default="all")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    now = datetime.now(timezone.utc); season = target_season(args.season, now)
    db = RefreshDatabase(load_config().database_url); failed = False
    try:
        try:
            week = args.week or target_week(db, season, now)
        except SeasonComplete as exc:
            # After the final regular-season game there is no week to capture,
            # monitor or freeze. Report it plainly instead of running with
            # week=None (which used to write week-less snapshots and a vacuous
            # "healthy" report).
            print(json.dumps({"season": season, "mode": args.mode, "status": "season_complete",
                              "reason": str(exc)}, indent=2))
            return 0
        kickoffs = [row["kickoff"] for row in db.execute(
            "SELECT kickoff FROM nfl_season_games WHERE season=%s AND week=%s AND game_type='REG'",
            (season, week),
        )]
        result: dict[str, Any] = {"season": season, "week": week, "mode": args.mode}
        if args.mode in {"capture", "all"}:
            result["sleeper"] = capture_sleeper(db, season=season, week=week, now=now) if args.force or should_capture(now, kickoffs) else {"skipped": "cadence"}
        if args.mode in {"monitor", "all"}:
            result["health"] = availability_health(db, season=season, week=week, now=now)
        if args.mode in {"freeze", "all"}:
            result["prelock"] = freeze_prelock(db, season=season, week=week, now=now)
        print(json.dumps(result, indent=2, default=str))
        return 2 if result.get("health", {}).get("status") == "critical" else 0
    except Exception:
        failed = True
        raise
    finally:
        db.close(error=failed)


if __name__ == "__main__":
    raise SystemExit(main())
