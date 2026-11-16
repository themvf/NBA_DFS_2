"""One read-only dump of every NBA points-line capture (main + ladder, per book, outcomes list dropped) and box scores."""
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
caps = q("""SELECT event_id, game_date, commence_time, home_team_name, away_team_name, market, player, captured_at,
       (SELECT jsonb_object_agg(k, v - 'outcomes' - 'bookmakerTitle') FROM jsonb_each(books) AS t(k, v)) AS books
       FROM prop_odds_history WHERE sport = 'nba' AND market IN ('player_points','player_points_alternate')
       ORDER BY game_date, event_id, captured_at""")
(OUT / "captures.json").write_text(json.dumps(caps, default=iso), encoding="utf-8")
box = q("""SELECT p.game_date, p.player, p.player_key, p.team, p.team_abbrev, p.is_home, p.starter, p.played, p.minutes, p.points,
       g.home_team, g.away_team, g.completed, g.espn_event_id
       FROM nba_box_score_players p JOIN nba_box_score_games g ON g.espn_event_id = p.espn_event_id ORDER BY p.game_date""")
(OUT / "box.json").write_text(json.dumps(box, default=iso), encoding="utf-8")
print(len(caps), len(box), (OUT / "captures.json").stat().st_size // 1_000_000, "MB")
