"""Persist slate-scoped report cards for every completed DK slate upload.

Companion to `ingest/nfl_dfs_reportcard.py`. That stream grades the canonical
roster and treats a missing stat row as unknown; this one grades exactly the
players DraftKings listed and treats a missing stat row in a completed,
results-bearing game as the 0 DraftKings paid. The two are persisted under
different version strings and must never be pooled.

Usage:
    python -m ingest.nfl_dfs_slate_reportcard            # every completed upload
    python -m ingest.nfl_dfs_slate_reportcard --season 2026 --week 2
    python -m ingest.nfl_dfs_slate_reportcard --pooled   # weeks-clustered CIs
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import Json

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase, target_season
from model.nfl_dfs_historical import artifact_digest
from model.nfl_dfs_slate_reportcard import VERSION, build_slate_report, pooled_summary


def completed_uploads(db, season: int, week: int | None = None) -> list[dict]:
    rows = db.execute(
        """
        SELECT u.upload_id, u.slate_signature, u.format, u.created_at, u.projection_run_id,
               r.season, r.week, r.model_version
        FROM nfl_dfs_slate_uploads u
        JOIN nfl_dfs_projection_runs r ON r.run_id = u.projection_run_id
        WHERE r.season = %s AND (%s::int IS NULL OR r.week = %s)
        ORDER BY r.week, u.created_at
        """,
        (season, week, week),
    )
    return [dict(r) for r in rows]


def inputs(db, upload: dict) -> dict:
    players = [dict(r) for r in db.execute(
        """
        SELECT ff_player_id, name, position, team, opponent, game_key, projection_status,
               history_games, is_out, our_proj, floor_fpts, ceiling_fpts, salary
        FROM nfl_dfs_slate_players WHERE upload_id = %s
        """,
        (upload["upload_id"],),
    )]
    games = [dict(r) for r in db.execute(
        """
        SELECT g.id, g.kickoff, g.completed, a.abbreviation || '@' || h.abbreviation AS game_key
        FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id = g.home_team_id
        JOIN nfl_teams a ON a.team_id = g.away_team_id
        WHERE g.season = %s AND g.week = %s AND g.game_type = 'REG'
        """,
        (upload["season"], upload["week"]),
    )]
    results = [dict(r) for r in db.execute(
        """
        SELECT id, player_id, game_id, actual_dk_fpts, scoring_status, computed_at
        FROM nfl_dfs_player_week_results WHERE season = %s AND week = %s
        """,
        (upload["season"], upload["week"]),
    )]
    return dict(players=players, games=games, results=results)


def persist(db, report: dict) -> str:
    digest = artifact_digest(report)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO nfl_dfs_slate_report_cards
                    (report_digest, upload_id, season, week, format, version, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (digest, report["upload_id"], report["season"], report["week"], report["format"],
                 report["version"], Json(report, dumps=lambda x: json.dumps(x, default=str))),
            )
    return digest


def latest_reports(db, season: int) -> list[dict]:
    rows = db.execute(
        """
        SELECT DISTINCT ON (upload_id) payload, created_at
        FROM nfl_dfs_slate_report_cards WHERE season = %s AND version = %s
        ORDER BY upload_id, created_at DESC
        """,
        (season, VERSION),
    )
    out = []
    for r in rows:
        payload = r["payload"]
        payload["upload_created_at"] = str(payload.get("upload_created_at") or "")
        out.append(payload)
    return out


def _print_summary(report: dict) -> None:
    s = report["summary"]
    line = [f"{report['week']:>2}w {report['format']:<8} {report['upload_id'][:8]} {report['model_version']}"]
    for pos in ("QB", "RB", "WR", "TE"):
        h0, h6 = s[pos]["hist_0"], s[pos]["hist_6_plus"]
        line.append(f"{pos} hist0 n={h0['scored']} bias={h0['bias_actual_minus_projected']:+.1f}"
                    if h0["scored"] else f"{pos} hist0 n=0")
        line.append(f"hist6+ n={h6['scored']} mae={h6['mae']:.2f}" if h6["scored"] else "hist6+ n=0")
    print("  ".join(line))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--pooled", action="store_true", help="print weeks-clustered CIs over persisted reports")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    db = PipelineDatabase(load_config().database_url)
    implementation = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                      for p in ("model/nfl_dfs_slate_reportcard.py", "ingest/nfl_dfs_slate_reportcard.py")}
    for upload in completed_uploads(db, season, args.week):
        report = build_slate_report(upload=upload, now=now, **inputs(db, upload))
        report["upload_created_at"] = upload["created_at"].isoformat()
        report["implementation"] = implementation
        if report["scorable_games"] == 0:
            print(f"{upload['week']:>2}w {upload['format']:<8} {upload['upload_id'][:8]} not scorable yet "
                  f"({report['statuses']})")
            continue
        digest = persist(db, report)
        _print_summary(report)
    if args.pooled:
        pooled = pooled_summary(latest_reports(db, season))
        print(json.dumps({"weeks": pooled["weeks"],
                          "cells": {k: v for k, v in pooled["cells"].items() if v.get("n")}}, default=str))


if __name__ == "__main__":
    main()
