"""Append-only registration, context captures and first-decision receipts."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from psycopg2.extras import Json

ROOT=Path(__file__).resolve().parents[1]
PROTOCOL=ROOT/"docs/cfb-drive-market-v4-1-protocol.json"
VERSION="cfb-drive-market-v4.1"


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),default=str).encode()).hexdigest()


def ensure_schema(db):
    with db.connect() as conn:
        cur=conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS cfb_v4_protocols (
            version TEXT PRIMARY KEY, registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            digest TEXT NOT NULL, git_sha TEXT NOT NULL, protocol_json JSONB NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS cfb_v4_context (
            id BIGSERIAL PRIMARY KEY, game_id INTEGER NOT NULL REFERENCES cfb_matchups(id),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            kind TEXT NOT NULL CHECK(kind IN ('weather','team')), team_id INTEGER REFERENCES cfb_teams(team_id),
            source_url TEXT NOT NULL, source_published_at TIMESTAMPTZ,
            payload_digest TEXT NOT NULL, payload_json JSONB NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS cfb_v4_runs (
            id BIGSERIAL PRIMARY KEY, protocol_version TEXT NOT NULL REFERENCES cfb_v4_protocols(version),
            generated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), report_json JSONB NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS cfb_v4_snapshots (
            id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL REFERENCES cfb_v4_runs(id),
            game_id INTEGER NOT NULL REFERENCES cfb_matchups(id), kickoff TIMESTAMPTZ NOT NULL,
            frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            odds_history_id BIGINT REFERENCES game_odds_history(id), evidence_json JSONB NOT NULL,
            UNIQUE(run_id,game_id))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS cfb_v4_decisions (
            id BIGSERIAL PRIMARY KEY, protocol_version TEXT NOT NULL REFERENCES cfb_v4_protocols(version),
            game_id INTEGER NOT NULL REFERENCES cfb_matchups(id), kickoff TIMESTAMPTZ NOT NULL,
            snapshot_id BIGINT NOT NULL REFERENCES cfb_v4_snapshots(id),
            odds_history_id BIGINT NOT NULL REFERENCES game_odds_history(id),
            frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), evidence_json JSONB NOT NULL,
            UNIQUE(protocol_version,game_id))""")
        cur.execute("""CREATE OR REPLACE FUNCTION cfb_v4_no_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'CFB v4 evidence is append-only'; END $$""")
        for table in ('cfb_v4_protocols','cfb_v4_context','cfb_v4_runs','cfb_v4_snapshots','cfb_v4_decisions'):
            cur.execute(f"""DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='{table}_immutable') THEN
                CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION cfb_v4_no_mutation(); END IF; END $$""")


def registration(db,sha=None):
    spec=json.loads(PROTOCOL.read_text(encoding="utf-8"));hashed=digest(spec)
    if sha:
        with db.connect() as conn:
            cur=conn.cursor()
            cur.execute("""INSERT INTO cfb_v4_protocols(version,digest,git_sha,protocol_json)
                VALUES(%s,%s,%s,%s) ON CONFLICT(version) DO NOTHING""",(VERSION,hashed,sha,Json(spec)))
    row=db.execute_one("SELECT * FROM cfb_v4_protocols WHERE version=%s",(VERSION,))
    if row and row["digest"]!=hashed:
        raise ValueError("Registered protocol differs from code: create a new version, never overwrite registration")
    return dict(row) if row else None


def save_context(db,game_id,kind,team_id,url,payload,published=None):
    with db.connect() as conn:
        cur=conn.cursor()
        cur.execute("""INSERT INTO cfb_v4_context(game_id,kind,team_id,source_url,source_published_at,payload_digest,payload_json)
            VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",(game_id,kind,team_id,url,published,digest(payload),Json(payload)))
        return cur.fetchone()["id"]
