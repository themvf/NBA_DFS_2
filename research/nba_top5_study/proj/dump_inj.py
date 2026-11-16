import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv
_paths.load_env()
from config import load_config
from db.database import DatabaseManager, fetch_rows
OUT = _paths.DATA_ROOT / "proj"
db = DatabaseManager(load_config().database_url, initialize_schema=False)
def q(sql, args=()):
    with db.connect() as conn:
        cur = conn.cursor(); cur.execute(sql, args); rows = fetch_rows(cur); conn.rollback()
    return [dict(r) for r in rows]
iso = lambda x: x.isoformat() if isinstance(x, (datetime, date)) else x
print(q("select column_name, data_type from information_schema.columns where table_name='nba_injury_observations'"))
print(q("select count(*), min(observed_at), max(observed_at), sum(pg_column_size(states)) as st from nba_injury_observations"))
rows = q("select published_at, observed_at, states from nba_injury_observations order by observed_at")
(OUT / "injuries.json").write_text(json.dumps(rows, default=iso), encoding="utf-8")
print(len(rows)); s = rows[-1]["states"]; s = s if isinstance(s, list) else json.loads(s); print(len(s), s[:2])
box = q("""SELECT p.game_date, p.player, p.team, p.is_home, p.starter, p.played, p.dnp_reason, p.minutes, p.points,
       g.home_team, g.away_team, g.completed, g.espn_event_id
       FROM nba_box_score_players p JOIN nba_box_score_games g ON g.espn_event_id = p.espn_event_id ORDER BY p.game_date""")
(OUT / "box2.json").write_text(json.dumps(box, default=iso), encoding="utf-8")
print(len(box), max(b["game_date"] for b in box))
