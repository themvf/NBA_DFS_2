"""Operational health checks for point-in-time MLB decision inputs."""

from __future__ import annotations

import argparse
import json
from datetime import date

from config import load_config
from db.database import DatabaseManager


def collect_mlb_data_health(db: DatabaseManager, target_date: str) -> dict:
    stats = db.execute_one(
        """
        SELECT
          (SELECT COUNT(DISTINCT team_id) FROM mlb_team_stats_history) AS team_entities,
          (SELECT COUNT(*) FROM mlb_team_stats_history) AS team_captures,
          (SELECT COUNT(*) FROM mlb_pitcher_stats_history) AS pitcher_captures,
          (SELECT COUNT(*) FROM mlb_team_stats_history
             WHERE source IS NULL OR available_at IS NULL OR stats_through_at IS NULL
                OR sample_size IS NULL OR transformation_version IS NULL OR raw_checksum IS NULL) AS team_missing_provenance,
          (SELECT COUNT(*) FROM mlb_pitcher_stats_history
             WHERE source IS NULL OR available_at IS NULL OR stats_through_at IS NULL
                OR sample_size IS NULL OR transformation_version IS NULL OR raw_checksum IS NULL) AS pitcher_missing_provenance,
          (SELECT COUNT(*) FROM mlb_team_stats_history WHERE stats_through_at > available_at) AS team_leakage,
          (SELECT COUNT(*) FROM mlb_pitcher_stats_history WHERE stats_through_at > available_at) AS pitcher_leakage,
          (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(available_at))) / 3600.0 FROM mlb_team_stats_history) AS team_age_hours,
          (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(available_at))) / 3600.0 FROM mlb_pitcher_stats_history) AS pitcher_age_hours
        """
    ) or {}
    schedule = db.execute_one(
        """
        SELECT
          COUNT(*) AS games,
          COUNT(m.commence_time) AS starts,
          COUNT(r.id) AS revisions,
          COUNT(*) FILTER (WHERE r.id IS NOT NULL AND r.source_available_at >= m.commence_time) AS post_start_revisions,
          COUNT(*) FILTER (WHERE r.id IS NOT NULL AND (r.source IS NULL OR r.raw_json IS NULL)) AS revision_missing_provenance
        FROM mlb_matchups m
        LEFT JOIN LATERAL (
          SELECT sr.id, sr.source, sr.source_available_at, sr.raw_json
          FROM mlb_schedule_revisions sr
          WHERE sr.matchup_id = m.id
          ORDER BY sr.captured_at DESC, sr.id DESC
          LIMIT 1
        ) r ON TRUE
        WHERE m.game_date = %s AND m.game_id IS NOT NULL
        """,
        (target_date,),
    ) or {}
    bullpen = db.execute_one(
        """
        SELECT
          (SELECT COUNT(*) FROM mlb_relief_appearances) AS relief_appearances,
          (SELECT COUNT(*) FROM mlb_relief_appearances
             WHERE source IS NULL OR source_available_at IS NULL OR raw_checksum IS NULL OR raw_json IS NULL) AS relief_missing_provenance,
          -- COVERAGE, not row count. mlb_bullpen_snapshots is append-only and
          -- UNIQUE(matchup_id, team_id, raw_checksum), so one team-game legitimately
          -- accumulates a new row every time the underlying relief data revises.
          -- COUNT(b.id) therefore grew past games*2 on any date whose bullpen was
          -- re-ingested, and the `== expected` gate below hard-failed the whole MLB
          -- refresh -- which SKIPS prop capture and the alert scan, silently starving
          -- the prop board. The sibling schedule and weather checks already collapse
          -- to one row per game via LEFT JOIN LATERAL ... LIMIT 1; this one did not.
          -- Counting distinct team-games asks the question the label always claimed.
          COUNT(DISTINCT (b.matchup_id, b.team_id)) FILTER (WHERE b.id IS NOT NULL)
            AS bullpen_team_games,
          COUNT(*) FILTER (WHERE b.id IS NOT NULL AND b.quality_outs <= 0) AS empty_quality,
          COUNT(*) FILTER (WHERE b.id IS NOT NULL AND b.available_at >= m.commence_time) AS post_start_snapshots
        FROM mlb_matchups m
        LEFT JOIN mlb_bullpen_snapshots b ON b.matchup_id = m.id
        WHERE m.game_date = %s AND m.game_id IS NOT NULL
        """,
        (target_date,),
    ) or {}
    weather = db.execute_one(
        """
        SELECT COUNT(w.id) AS forecasts,
          COUNT(*) FILTER (WHERE w.id IS NOT NULL AND (
            w.available_at >= m.commence_time OR w.source_status <> 'complete'
            OR w.provider_issued_at IS NULL OR w.valid_at IS NULL
          )) AS invalid_forecasts
        FROM mlb_matchups m
        LEFT JOIN LATERAL (
          SELECT f.* FROM mlb_weather_forecast_snapshots f
          WHERE f.matchup_id = m.id ORDER BY f.available_at DESC, f.id DESC LIMIT 1
        ) w ON TRUE
        WHERE m.game_date = %s AND m.game_id IS NOT NULL
        """,
        (target_date,),
    ) or {}

    def number(row: dict, key: str) -> float:
        value = row.get(key)
        return float(value) if value is not None else 0.0

    checks = []

    def add(key: str, passed: bool, detail: str, remedy: str, severity: str = "error") -> None:
        checks.append({
            "key": key,
            "status": "pass" if passed else "fail",
            "severity": "ok" if passed else severity,
            "detail": detail,
            "remedy": None if passed else remedy,
        })

    team_entities = int(number(stats, "team_entities"))
    team_captures = int(number(stats, "team_captures"))
    pitcher_captures = int(number(stats, "pitcher_captures"))
    add(
        "team_history_population", team_entities == 30 and team_captures >= 30,
        f"{team_captures} captures across {team_entities}/30 teams",
        "Run python -m ingest.mlb_stats for the active season and investigate missing team identities.",
    )
    add(
        "pitcher_history_population", pitcher_captures > 0,
        f"{pitcher_captures} pitcher captures",
        "Run the official MLB pitcher fallback and verify the active-season response.",
    )
    missing_provenance = int(number(stats, "team_missing_provenance") + number(stats, "pitcher_missing_provenance"))
    add(
        "stats_provenance", missing_provenance == 0,
        f"{missing_provenance} stat captures missing required provenance",
        "Re-capture from a named source with availability, cutoff, sample size, version and checksum.",
    )
    leakage = int(number(stats, "team_leakage") + number(stats, "pitcher_leakage"))
    add(
        "stats_cutoff", leakage == 0,
        f"{leakage} captures have stats-through time after availability time",
        "Reject and re-capture rows whose source cutoff is later than their availability timestamp.",
    )
    games = int(number(schedule, "games"))
    starts = int(number(schedule, "starts"))
    revisions = int(number(schedule, "revisions"))
    add(
        "schedule_starts", games == starts,
        f"{starts}/{games} games have a start time on {target_date}",
        f"Refresh the official MLB schedule for {target_date}; do not predict games with missing commence time.",
    )
    add(
        "schedule_revisions", games == revisions,
        f"{revisions}/{games} games have immutable schedule revisions on {target_date}",
        f"Refresh the official MLB schedule for {target_date} and verify revision writes.",
    )
    invalid_revisions = int(number(schedule, "post_start_revisions") + number(schedule, "revision_missing_provenance"))
    add(
        "schedule_provenance", invalid_revisions == 0,
        f"{invalid_revisions} latest revisions are post-start or missing source/raw provenance",
        "Exclude post-start revisions from pregame use and re-capture missing official source payloads.",
    )
    relief_appearances = int(number(bullpen, "relief_appearances"))
    add(
        "reliever_appearances", relief_appearances > 0,
        f"{relief_appearances} official reliever-only appearances available",
        "Backfill official MLB boxscores before constructing bullpen quality or workload.",
    )
    expected_bullpen = games * 2
    bullpen_team_games = int(number(bullpen, "bullpen_team_games"))
    # Coverage is the question: does every team-game have a snapshot? Extra
    # revisions of an existing team-game are correct behaviour, not a defect --
    # any BAD row is still caught by the bullpen_provenance check below, which
    # deliberately keeps scanning every row rather than only the latest.
    add(
        "bullpen_snapshots", bullpen_team_games == expected_bullpen,
        f"{bullpen_team_games}/{expected_bullpen} team-games have a bullpen snapshot on {target_date}",
        f"Run python -m ingest.mlb_bullpen through the latest completed date for {target_date}.",
    )
    invalid_bullpen = int(
        number(bullpen, "relief_missing_provenance")
        + number(bullpen, "empty_quality")
        + number(bullpen, "post_start_snapshots")
    )
    add(
        "bullpen_provenance", invalid_bullpen == 0,
        f"{invalid_bullpen} relief/snapshot rows have missing provenance, empty quality, or post-start capture",
        "Reject invalid bullpen rows and re-capture from official pregame-available boxscores.",
    )
    forecasts = int(number(weather, "forecasts"))
    add(
        "weather_forecasts", forecasts == games,
        f"{forecasts}/{games} games have immutable pregame forecast snapshots on {target_date}",
        f"Refresh schedule weather sources for {target_date}; do not substitute observed/postgame conditions.",
    )
    invalid_weather = int(
        number(weather, "invalid_forecasts")
    )
    add(
        "weather_provenance", invalid_weather == 0,
        f"{invalid_weather} latest forecasts are incomplete, post-start, or missing issue/valid time",
        "Use an issued official forecast for the venue or show weather as unavailable until one is captured.",
    )

    return {
        "target_date": target_date,
        "status": "pass" if all(check["status"] == "pass" for check in checks) else "fail",
        "checks": checks,
        "observed": {
            "team_age_hours": number(stats, "team_age_hours"),
            "pitcher_age_hours": number(stats, "pitcher_age_hours"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit point-in-time MLB data health")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    config = load_config()
    report = collect_mlb_data_health(DatabaseManager(config.database_url), args.date)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
