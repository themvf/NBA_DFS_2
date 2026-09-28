"""Rebuild opponent forecasts after a new stored pregame odds capture; no paid requests."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from research.nfl_pickem_matchup import MODEL_PATH, current_quotes, freeze_forecasts, DDL


def refresh_changed_quotes(db):
    now = datetime.now(timezone.utc)
    db.execute(DDL)
    weeks = db.execute("""SELECT DISTINCT season,week FROM nfl_season_games
        WHERE game_type='REG' AND kickoff>%s AND kickoff<%s + INTERVAL '7 days'
        ORDER BY season,week""", (now, now))
    refreshed = []
    model = None
    for week in weeks:
        quotes = current_quotes(db, week["season"], week["week"], now)
        stored = {r["game_id"]: r["quote_at"] for r in db.execute("""SELECT DISTINCT ON(game_id)
            game_id,payload->'input'->'baseline'->>'marketCapturedAt' quote_at
            FROM nfl_pickem_matchup_forecasts WHERE game_id=ANY(%s)
            ORDER BY game_id,available_at DESC,forecast_id DESC""", ([r["game_id"] for r in quotes],))}
        changed = any(q.get("captured_at") and
                      (not stored.get(q["game_id"]) or q["captured_at"] > datetime.fromisoformat(stored[q["game_id"]]))
                      for q in quotes)
        if changed:
            model = model or json.loads(MODEL_PATH.read_text(encoding="utf-8"))
            artifact = freeze_forecasts(db, model, week["season"], week["week"], persist=True)
            refreshed.append({"season": week["season"], "week": week["week"],
                              "forecasts": len(artifact["forecasts"]),
                              "adjusted": sum(f["covered"] for f in artifact["forecasts"])})
    return {"refreshed": refreshed, "paid_requests": 0}


def main():
    from config import load_config
    from db.database import DatabaseManager
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.reuse_connection():
        print(json.dumps(refresh_changed_quotes(db)))


if __name__ == "__main__":
    main()
