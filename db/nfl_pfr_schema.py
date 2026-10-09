"""Separate, revisioned game supplements; never update nfl_pbp_archetypes."""
DDL = """
CREATE TABLE IF NOT EXISTS nfl_pfr_game_snapshots (
 snapshot_id BIGSERIAL PRIMARY KEY,
 game_id TEXT NOT NULL,
 pfr_game_id TEXT NOT NULL,
 season INTEGER NOT NULL,
 week INTEGER NOT NULL,
 captured_at TIMESTAMPTZ NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 source_sha256 TEXT NOT NULL,
 parser_version TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('complete','partial')),
 payload JSONB NOT NULL,
 UNIQUE(game_id, source_sha256, parser_version, captured_at)
);
CREATE INDEX IF NOT EXISTS nfl_pfr_game_capture_idx
 ON nfl_pfr_game_snapshots(game_id,captured_at DESC,snapshot_id DESC);
CREATE OR REPLACE VIEW nfl_pfr_game_latest AS
 SELECT DISTINCT ON (game_id) * FROM nfl_pfr_game_snapshots
 ORDER BY game_id,captured_at DESC,snapshot_id DESC;
CREATE OR REPLACE VIEW nfl_pfr_player_game_latest AS
 SELECT g.game_id,g.season,g.week,g.captured_at,g.status,g.snapshot_id,
        r->>'section' AS section,r->>'pfr_player_id' AS pfr_player_id,
        r->>'player_name' AS player_name,r->>'team' AS team,
        r->'stats' AS stats,r->'raw' AS raw
 FROM nfl_pfr_game_latest g CROSS JOIN LATERAL jsonb_array_elements(g.payload->'rows') r;
"""


def freeze_identity(db, payload: dict) -> dict:
    """Resolve once at collection; a read must never consult a mutable crosswalk."""
    from copy import deepcopy
    from datetime import datetime, timezone
    from model.nfl_context_engine import stable_digest
    payload = deepcopy(payload)
    if payload.get("identity_manifest"):
        return payload
    exists = db.execute_one("SELECT to_regclass('nfl_player_identity_crosswalk') AS relation")
    identities = db.execute("""SELECT x.external_id,x.gsis_id,x.status,
        p.position FROM nfl_player_identity_crosswalk x
        LEFT JOIN LATERAL (SELECT position FROM ff_players
          WHERE gsis_id=x.gsis_id AND season=%s ORDER BY id LIMIT 1) p ON TRUE
        WHERE x.namespace='pfr'""", (payload["season"],)) if exists and exists["relation"] else []
    by_id = {r["external_id"]: r for r in identities}
    mappings = []
    for player in payload["rows"]:
        identity = by_id.get(player["pfr_player_id"], {})
        resolved = identity.get("status") == "resolved"
        player["gsis_id"] = identity.get("gsis_id") if resolved else None
        player["position"] = identity.get("position") if resolved else None
        player["identity_status"] = identity.get("status", "unresolved")
        mappings.append({k: player.get(k) for k in ("pfr_player_id", "gsis_id", "position", "identity_status")})
    mappings = sorted({stable_digest(m): m for m in mappings}.values(), key=lambda m: str(m["pfr_player_id"]))
    payload["identity_manifest"] = {"version": "pfr-identity-v1", "captured_at": datetime.now(timezone.utc).isoformat(),
                                    "digest": stable_digest(mappings), "mappings": mappings}
    return payload


def save_snapshot(db, payload: dict) -> dict:
    from psycopg2.extras import Json
    payload = freeze_identity(db, payload)
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO nfl_pfr_game_snapshots
                (game_id,pfr_game_id,season,week,captured_at,source_sha256,parser_version,status,payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                tuple(payload[k] for k in ("game_id", "pfr_game_id", "season", "week", "captured_at",
                                          "source_sha256", "parser_version", "status")) + (Json(payload),))
    return payload


def read_game_supplement(db, game_id: str, *, as_of=None) -> dict | None:
    """One game object; avoid multiplying PBP rows by joining player sections."""
    row = db.execute_one("""SELECT snapshot_id,captured_at,recorded_at,parser_version,source_sha256,payload FROM nfl_pfr_game_snapshots
        WHERE game_id=%s AND (%s::timestamptz IS NULL OR GREATEST(captured_at,recorded_at)<=%s::timestamptz)
        ORDER BY captured_at DESC,snapshot_id DESC LIMIT 1""", (game_id, as_of, as_of))
    if not row:
        return None
    payload = row["payload"]
    payload.update({k: str(row[k]) for k in ("snapshot_id", "captured_at", "recorded_at", "parser_version", "source_sha256")})
    for player in payload["rows"]:
        if not payload.get("identity_manifest"):
            player["gsis_id"] = None
            player["identity_status"] = "unresolved_at_capture"
    return payload
