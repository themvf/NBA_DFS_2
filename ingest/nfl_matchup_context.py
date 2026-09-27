"""Publish matchup context through the shared evidence/qualification boundary."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from model.nfl_context_engine import stable_digest
from model.nfl_matchup_features import DEFINITIONS, build_matchup, contexts, participant_manifests


def load_matchups(db, season, week, as_of):
    games = [dict(g) for g in db.execute("""SELECT g.nflverse_game_id game_id,g.season,g.week,g.kickoff,g.completed,
        h.abbreviation home,a.abbreviation away FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
        WHERE g.season=%s AND g.game_type='REG' ORDER BY g.kickoff""", (season,))]
    snapshots = [dict(s) for s in db.execute("""SELECT DISTINCT ON(game_id)
        snapshot_id,game_id,captured_at,recorded_at,payload FROM nfl_pfr_game_snapshots
        WHERE season=%s AND GREATEST(captured_at,recorded_at)<=%s
        ORDER BY game_id,captured_at DESC,snapshot_id DESC""", (season, as_of))]
    participants = participant_manifests([dict(r) for r in db.execute("""SELECT id,team,source,source_row,fetched_at
        FROM ff_player_week_stats WHERE season=%s AND season_type='REG' AND source='nflverse'
          AND fetched_at<=%s ORDER BY id""", (season, as_of))], games, as_of)
    for snapshot in snapshots:
        snapshot["participant_manifest"] = participants.get(snapshot["game_id"])
    return {g["game_id"]: build_matchup(game=g, prior_games=games, snapshots=snapshots, as_of=as_of)
            for g in games if g["week"] == week and g["kickoff"] > as_of}


def persist_matchups(db, matchups):
    """Descriptive publication only. No numerical consumer can self-approve."""
    now = datetime.now(timezone.utc)
    count = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for definition in DEFINITIONS.values():
            cur.execute("""INSERT INTO nfl_context_definitions
                (definition_id,context_key,version,unit,description,definition,freshness_seconds)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
                (definition.definition_id, definition.key, definition.version, definition.unit,
                 definition.description, json.dumps(definition.definition), definition.freshness_seconds))
        for matchup in matchups.values():
            if datetime.fromisoformat(matchup["kickoff"]) <= now:
                continue
            ids = []
            for source in matchup.get("participant_sources", []):
                oid = "matchup-participants:" + source["manifest_hash"]
                ids.append(oid)
                cur.execute("""INSERT INTO nfl_evidence_observations
                    (observation_id,source,source_record_key,system_observed_at,raw_payload,payload_digest,idempotency_key)
                    VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s) ON CONFLICT DO NOTHING""",
                    (oid, source["source_kind"], source["game_id"], now, json.dumps(source), stable_digest(source), oid))
            for source in matchup["sources"]:
                oid = "pfr:" + source["snapshot_id"]
                ids.append(oid)
                cur.execute("""INSERT INTO nfl_evidence_observations
                    (observation_id,source,source_record_key,system_observed_at,raw_payload,payload_digest,idempotency_key)
                    VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s) ON CONFLICT DO NOTHING""",
                    (oid, source["source_provider"], source["source_url"] or oid, source["recorded_at"],
                     json.dumps(source), stable_digest(source), oid))
            cur.execute("""INSERT INTO nfl_fact_releases
                (release_id,dataset_key,fact_schema_version,source_observation_ids,payload_digest,published_at)
                VALUES (%s,'nfl_matchup','nfl-matchup-features-v1',%s::jsonb,%s,%s) ON CONFLICT DO NOTHING""",
                (matchup["manifest_hash"], json.dumps(ids), stable_digest(matchup), now))
            for c in contexts(matchup, now):
                cur.execute("""INSERT INTO nfl_context_snapshots
                    (snapshot_id,definition_id,subject_type,subject_id,target_id,as_of_at,available_at,
                     measurement_window,numerator,denominator,value,value_state,coverage,source_snapshot_ids,fact_release_id,payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING""", (c.snapshot_id,c.definition_id,c.subject_type,c.subject_id,c.target_id,
                        c.as_of_at,c.available_at,json.dumps(c.window),c.numerator,c.denominator,c.value,c.state.value,
                        json.dumps(c.coverage),json.dumps(list(c.source_snapshot_ids)),c.fact_release_id,json.dumps(c.payload)))
                count += cur.rowcount
    return count


SHADOW_DDL = """
CREATE TABLE IF NOT EXISTS nfl_matchup_forecast_runs (
 run_id TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 season INT NOT NULL, week INT NOT NULL, as_of_at TIMESTAMPTZ NOT NULL,
 baseline_run_id UUID, upload_id UUID, model_version TEXT NOT NULL,
 manifest JSONB NOT NULL, artifact_digest TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS nfl_matchup_player_forecasts (
 run_id TEXT REFERENCES nfl_matchup_forecast_runs(run_id), player_id BIGINT NOT NULL,
 game_id TEXT NOT NULL, kickoff TIMESTAMPTZ NOT NULL, projection JSONB NOT NULL,
 PRIMARY KEY(run_id,player_id)
);
CREATE INDEX IF NOT EXISTS nfl_matchup_player_forecast_lookup
 ON nfl_matchup_player_forecasts(player_id,game_id);
"""


def persist_forecasts(db, artifact):
    """Append shadow rows; never update production projections or saved salary rows."""
    from psycopg2.extras import Json, execute_values
    now = datetime.now(timezone.utc)
    eligible = [p for p in artifact["players"] if datetime.fromisoformat(p["kickoff"]) > now]
    digest = stable_digest(artifact)
    run_id = digest
    db.execute(SHADOW_DDL)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO nfl_matchup_forecast_runs
            (run_id,season,week,as_of_at,baseline_run_id,upload_id,model_version,manifest,artifact_digest)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (run_id,artifact["season"],artifact["week"],artifact["as_of_at"],artifact.get("baseline_run_id"),
             artifact.get("upload_id"),artifact["version"],Json({k:v for k,v in artifact.items() if k != "players"}),digest))
        if eligible:
            execute_values(cur,"""INSERT INTO nfl_matchup_player_forecasts
                (run_id,player_id,game_id,kickoff,projection) VALUES %s ON CONFLICT DO NOTHING""",
                [(run_id,p["player_id"],p["game_id"],p["kickoff"],Json(p)) for p in eligible])
    return {"run_id":run_id,"players":len(eligible),"skipped_locked":len(artifact["players"])-len(eligible)}
