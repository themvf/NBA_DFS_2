import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv
_paths.load_env()
from config import load_config
from db.database import DatabaseManager, fetch_rows
db = DatabaseManager(load_config().database_url, initialize_schema=False)
with db.connect() as conn:
    cur = conn.cursor()
    cur.execute("""SELECT game_date, player, player_key, team, projection_id, play_probability, minutes_mean, pts, pts_p10, pts_p50, pts_p90
                   FROM statapi_nba_player_projections""")
    rows = [dict(r) for r in fetch_rows(cur)]; conn.rollback()
iso = lambda x: x.isoformat() if isinstance(x, (datetime, date)) else x
(_paths.DATA_ROOT / "proj" / "statapi.json").write_text(json.dumps(rows, default=iso), encoding="utf-8")
from collections import Counter
print(len(rows), min(r["game_date"] for r in rows), max(r["game_date"] for r in rows), Counter(r["projection_id"] for r in rows), rows[0])
