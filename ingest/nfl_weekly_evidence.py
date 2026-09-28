"""Append complete weekly participant evidence independently of fantasy eligibility."""
from __future__ import annotations

import argparse
import json
from psycopg2.extras import Json

from model.nfl_context_engine import stable_digest

DDL = """CREATE TABLE IF NOT EXISTS nfl_weekly_participant_evidence (
 id TEXT PRIMARY KEY, season INT NOT NULL, week INT NOT NULL, game_id TEXT NOT NULL,
 gsis_id TEXT NOT NULL, team TEXT NOT NULL, source TEXT NOT NULL,
 source_row JSONB NOT NULL, fetched_at TIMESTAMPTZ NOT NULL DEFAULT now());
 CREATE INDEX IF NOT EXISTS nfl_weekly_participant_evidence_lookup
 ON nfl_weekly_participant_evidence(season,game_id,gsis_id,fetched_at DESC);
"""


def save_participant_evidence(db, season, frame):
    from ingest.ff_independent import _clean
    db.execute(DDL)
    rows = []
    for _, series in frame.iterrows():
        raw = _clean(series.to_dict())
        if raw.get("season_type", "REG") != "REG" or int(raw["season"]) != season:
            continue
        if not raw.get("player_id") or not raw.get("game_id") or not raw.get("team"):
            raise ValueError("Weekly participant evidence requires player, game and team identity")
        identity = stable_digest({"source": "nflverse", "row": raw})
        rows.append((identity, season, int(raw["week"]), raw["game_id"], raw["player_id"], raw["team"], Json(raw)))
    statement = """INSERT INTO nfl_weekly_participant_evidence
        (id,season,week,game_id,gsis_id,team,source,source_row)
        VALUES (%s,%s,%s,%s,%s,%s,'nflverse',%s) ON CONFLICT DO NOTHING"""
    if hasattr(db, "connect") and rows:
        from psycopg2.extras import execute_batch
        with db.connect() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, statement, rows, page_size=500)
    else:
        for row in rows:
            db.execute(statement, row)
    return len(rows)


def refresh_participant_evidence(db, season):
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    from ingest.nfl_dfs_weekly import fetch_partial
    frame, digest = fetch_partial(NFLVERSE_WEEKLY_STATS_URL.format(season=season), season, team=False)
    # Source revisions are append-only. Identical refreshes retain first observation time.
    return {"season": season, "source_digest": digest,
            "participants": save_participant_evidence(db, season, frame)}


def main():
    from config import load_config
    from db.database import DatabaseManager
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        print(json.dumps(refresh_participant_evidence(db, args.season)))


if __name__ == "__main__":
    main()
