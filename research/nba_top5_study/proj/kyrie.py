import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json
from pathlib import Path
from dotenv import load_dotenv
_paths.load_env()
from config import load_config
from db.database import DatabaseManager, fetch_rows
from model.nba_points_ladder import read_player
db = DatabaseManager(load_config().database_url, initialize_schema=False)
def q(sql, args=()):
    with db.connect() as conn:
        cur = conn.cursor(); cur.execute(sql, args); rows = fetch_rows(cur); conn.rollback()
    return [dict(r) for r in rows]
rows = q("""SELECT market, game_date, commence_time, home_team_name, away_team_name, captured_at, event_id,
   (SELECT jsonb_object_agg(k, v - 'outcomes') FROM jsonb_each(books) t(k,v)) books
   FROM prop_odds_history WHERE sport='nba' AND player ILIKE 'Kyrie Irving%%' AND game_date >= '2026-11-12' ORDER BY captured_at""")
alt = {(r["event_id"], r["captured_at"]): r["books"] for r in rows if r["market"] == "player_points_alternate"}
for r in rows:
    if r["market"] != "player_points": continue
    rd = read_player(r["books"], alt.get((r["event_id"], r["captured_at"])))
    d = rd.distribution
    print(r["game_date"], r["away_team_name"], "@", r["home_team_name"], "tip", r["commence_time"], "| read", r["captured_at"],
          "| line", rd.line, "p_over", rd.p_over and round(rd.p_over,3), {b.book: b.line for b in rd.lines},
          "| floor", d and d.floor, "ceil", d and d.ceiling, "30+", d and d.at_least.get(30), "35+", d and d.at_least.get(35))
print(q("SELECT game_date, player, team_abbrev, played, minutes, points FROM nba_box_score_players WHERE player ILIKE 'Kyrie Irving%%' ORDER BY game_date DESC LIMIT 6"))
print(q("SELECT game_date, status, line, p_over, floor, ceiling, actual_points FROM nba_points_calibration WHERE player ILIKE 'Kyrie Irving%%' ORDER BY game_date DESC LIMIT 6"))
