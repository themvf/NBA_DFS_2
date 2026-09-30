"""Operational health checks for point-in-time MLB decision inputs."""

from __future__ import annotations

import argparse
import json
from datetime import date

from config import load_config
from db.database import DatabaseManager

# Age budget for the point-in-time team/pitcher histories the game-line models
# read. `refresh_mlb_stats.yml` writes them once a day (13:00 UTC), so 48h
# allows one missed run. The pitcher history went 79 days without a row
# (2026-07-12 -> 2026-09-29) while every refresh reported success and this
# module only ever *observed* the age; the gate applies on dates that have
# games, since a stale table matters only when there is a decision to make.
STATS_HISTORY_MAX_AGE_HOURS = 48.0


def _format_team_game(row: dict) -> str:
    commence = row.get("commence_time")
    when = f"{commence:%Y-%m-%d %H:%M}Z" if commence is not None else "no start time"
    return f"{row['away']}@{row['home']} {when} [{row['team']}]"


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
          -- Games with a known start that HAVE a usable pregame revision.
          COUNT(*) FILTER (WHERE m.commence_time IS NOT NULL AND r.id IS NOT NULL) AS revisions,
          COUNT(*) FILTER (WHERE r.id IS NOT NULL AND (r.source IS NULL OR r.raw_json IS NULL)) AS revision_missing_provenance
        FROM mlb_matchups m
        -- Latest revision available BEFORE THIS GAME'S OWN first pitch, not the
        -- latest revision full stop. The evening refresh re-captures schedule
        -- for every game on the date, including ones already in progress, so
        -- the globally-latest revision for those is legitimately post-start.
        -- Judging on it failed the whole run -- and skipped prop capture for the
        -- games that had NOT started. A post-start revision existing is not a
        -- defect; a decision built on one would be, and this cannot select one.
        LEFT JOIN LATERAL (
          SELECT sr.id, sr.source, sr.source_available_at, sr.raw_json
          FROM mlb_schedule_revisions sr
          WHERE sr.matchup_id = m.id
            AND m.commence_time IS NOT NULL
            AND sr.source_available_at < m.commence_time
          ORDER BY sr.source_available_at DESC, sr.id DESC
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
    # The team-games with NO snapshot at all, named, and whether each game has
    # already started. "6/8" said nothing about which game or why; on
    # 2026-09-29 it was PHI@ATL 18:00Z, whose first pitch came before the day's
    # first refresh fired (the 13:10 UTC schedule ran at 18:36 UTC), so no
    # pregame snapshot could ever be built and the count failed all day.
    missing_team_games = [dict(row) for row in db.execute(
        """
        SELECT m.id AS matchup_id, m.commence_time, sides.team_id,
               t.abbreviation AS team, ht.abbreviation AS home, at.abbreviation AS away,
               (m.commence_time IS NOT NULL AND m.commence_time <= NOW()) AS started
        FROM mlb_matchups m
        JOIN LATERAL (VALUES (m.home_team_id), (m.away_team_id)) AS sides(team_id) ON TRUE
        JOIN mlb_teams t ON t.team_id = sides.team_id
        JOIN mlb_teams ht ON ht.team_id = m.home_team_id
        JOIN mlb_teams at ON at.team_id = m.away_team_id
        WHERE m.game_date = %s AND m.game_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM mlb_bullpen_snapshots b
            WHERE b.matchup_id = m.id AND b.team_id = sides.team_id
          )
        ORDER BY m.commence_time NULLS LAST, m.id, sides.team_id
        """,
        (target_date,),
    ) or []]
    weather = db.execute_one(
        """
        SELECT
          COUNT(*) FILTER (WHERE m.commence_time IS NOT NULL AND w.id IS NOT NULL) AS forecasts,
          -- The post-start clause is gone: the LATERAL below cannot return one.
          -- What remains is the real question -- is the PREGAME forecast usable?
          COUNT(*) FILTER (WHERE w.id IS NOT NULL AND (
            w.source_status <> 'complete'
            OR w.provider_issued_at IS NULL OR w.valid_at IS NULL
          )) AS invalid_forecasts
        FROM mlb_matchups m
        -- Same rule as schedule revisions above: latest forecast issued before
        -- THIS GAME'S first pitch. A forecast captured for a game already in
        -- progress says nothing about a pregame decision either way.
        LEFT JOIN LATERAL (
          SELECT f.* FROM mlb_weather_forecast_snapshots f
          WHERE f.matchup_id = m.id
            AND m.commence_time IS NOT NULL
            AND f.available_at < m.commence_time
          ORDER BY f.available_at DESC, f.id DESC LIMIT 1
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

    def warn(key: str, passed: bool, detail: str, note: str) -> None:
        """A named, visible defect that nothing later in the day can repair.

        It is reported with status 'warn' and does not fail the day: failing
        would skip prop capture and the alert scan for every game that has
        NOT started, which is the incident this module's history records.
        """
        checks.append({
            "key": key,
            "status": "pass" if passed else "warn",
            "severity": "ok" if passed else "warning",
            "detail": detail,
            "remedy": None if passed else note,
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
    # Freshness of the histories, gated only when the date has games to decide.
    for label, key in (("team", "team_age_hours"), ("pitcher", "pitcher_age_hours")):
        age = stats.get(key)
        age_hours = float(age) if age is not None else None
        fresh = age_hours is not None and age_hours <= STATS_HISTORY_MAX_AGE_HOURS
        detail = (
            f"latest {label} history capture is {age_hours / 24:.1f} days old"
            if age_hours is not None else f"no {label} history captures at all"
        ) + f" (budget {STATS_HISTORY_MAX_AGE_HOURS:g}h)"
        add(
            f"{label}_history_freshness", games == 0 or fresh, detail,
            f"Run python -m ingest.mlb_stats for the active season; refresh_mlb_stats.yml has "
            f"not written a {label} history row inside the budget, whatever its run status says.",
        )
    add(
        "schedule_starts", games == starts,
        f"{starts}/{games} games have a start time on {target_date}",
        f"Refresh the official MLB schedule for {target_date}; do not predict games with missing commence time.",
    )
    # Denominator is `starts`, not `games`: a game with no commence time cannot
    # be judged pregame at all, and is already owned by schedule_starts above.
    # Counting it here too would report one defect as two.
    add(
        "schedule_revisions", revisions == starts,
        f"{revisions}/{starts} games with a known start have a pregame schedule revision on {target_date}",
        f"Refresh the official MLB schedule for {target_date} and verify revision writes land before first pitch.",
    )
    invalid_revisions = int(number(schedule, "revision_missing_provenance"))
    add(
        "schedule_provenance", invalid_revisions == 0,
        f"{invalid_revisions} pregame revisions are missing source/raw provenance",
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
    #
    # Two different defects hide in one count. A team-game whose game has NOT
    # started and has no snapshot is repairable now, so it fails the day. A
    # team-game whose game started before any refresh built a pregame
    # snapshot cannot be repaired by anything that runs later, so it is named
    # and warned about instead of failing every later refresh of the day
    # (which skips prop capture and the alert scan for the games still to come).
    missing_upcoming = [row for row in missing_team_games if not row.get("started")]
    missing_started = [row for row in missing_team_games if row.get("started")]
    coverage = f"{bullpen_team_games}/{expected_bullpen} team-games have a bullpen snapshot on {target_date}"
    # The count stays authoritative: a shortfall the listing cannot name is
    # still a shortfall, never a pass.
    unnamed_shortfall = bullpen_team_games < expected_bullpen and not missing_team_games
    add(
        "bullpen_snapshots", not missing_upcoming and not unnamed_shortfall,
        coverage + (
            "; missing before first pitch: " + ", ".join(_format_team_game(r) for r in missing_upcoming)
            if missing_upcoming else ""
        ),
        f"Run python -m ingest.mlb_bullpen through the latest completed date for {target_date}; "
        "the named team-games still have no pregame snapshot.",
    )
    warn(
        "bullpen_pregame_missed", not missing_started,
        (
            f"{len(missing_started)} team-game(s) started with no pregame bullpen snapshot: "
            + ", ".join(_format_team_game(r) for r in missing_started)
            if missing_started else "every started game had a pregame bullpen snapshot"
        ),
        "No refresh ran between the game's publication and its first pitch (the scheduled "
        "run fired late), so the snapshot was never built; it cannot be repaired after first "
        "pitch. Check the refresh_mlb_vegas.yml run times for that morning.",
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
        "weather_forecasts", forecasts == starts,
        f"{forecasts}/{starts} games with a known start have a pregame forecast snapshot on {target_date}",
        f"Refresh schedule weather sources for {target_date}; do not substitute observed/postgame conditions.",
    )
    invalid_weather = int(
        number(weather, "invalid_forecasts")
    )
    add(
        "weather_provenance", invalid_weather == 0,
        f"{invalid_weather} pregame forecasts are incomplete or missing issue/valid time",
        "Use an issued official forecast for the venue or show weather as unavailable until one is captured.",
    )

    return {
        "target_date": target_date,
        # 'warn' checks are visible in `checks` but do not fail the day.
        "status": "pass" if all(check["status"] != "fail" for check in checks) else "fail",
        "checks": checks,
        "observed": {
            "team_age_hours": number(stats, "team_age_hours"),
            "pitcher_age_hours": number(stats, "pitcher_age_hours"),
            "bullpen_missing_team_games": [
                {**_row_summary(row)} for row in missing_team_games
            ],
        },
    }


def _row_summary(row: dict) -> dict:
    return {
        "matchup_id": row.get("matchup_id"),
        "team": row.get("team"),
        "game": f"{row.get('away')}@{row.get('home')}",
        "commence_time": str(row.get("commence_time")) if row.get("commence_time") is not None else None,
        "started": bool(row.get("started")),
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
