"""One row per (player-game, capture): consensus line, per-book lines, ladder mean, actual points. Local files only."""
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[1])); import _paths
import sys, json, math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
REPO = str(_paths.REPO)
sys.path.insert(0, REPO)
from model.nba_points_ladder import read_player, player_key, team_key, PLAYER_ALIASES
from research.nba_wta_trios import survival_knots
P = _paths.DATA_ROOT / "proj"
caps = json.loads((P / "captures.json").read_text(encoding="utf-8"))
box = json.loads((P / "box.json").read_text(encoding="utf-8"))
def tk(n):
    k = team_key(n or ""); return "losangelesclippers" if k == "laclippers" else k
def pk(n):
    k = player_key(n); return PLAYER_ALIASES.get(k, k)
# box index: (home, away, date) -> {player_key: row}
games = defaultdict(dict)
for r in box:
    if not r["completed"]: continue
    games[(tk(r["home_team"]), tk(r["away_team"]), r["game_date"][:10])].setdefault(pk(r["player"]), []).append(r)
def curve_mean(curve):
    kn = survival_knots([(c.point, c.p) for c in curve])
    if kn is None: return None
    s, x = kn                      # survival ascending, points descending
    xs, ss = x[::-1], s[::-1]      # points ascending, survival descending
    # mean = integral of survival over points; extend the exponential tail analytically is already in knots
    return float(np.trapezoid(ss, xs)) if hasattr(np, "trapezoid") else float(np.trapz(ss, xs))
main = defaultdict(dict); alt = defaultdict(dict); meta = {}
for c in caps:
    key = (c["event_id"], c["captured_at"])
    (main if c["market"] == "player_points" else alt)[key][c["player"]] = c["books"]
    meta[c["event_id"]] = (c["game_date"], c["commence_time"], c["home_team_name"], c["away_team_name"])
rows = []
for (ev, cap), players in main.items():
    gd, tip, home, away = meta[ev]
    tip_dt = datetime.fromisoformat(tip); cap_dt = datetime.fromisoformat(cap)
    if cap_dt >= tip_dt: continue
    g = None
    for d in (gd, (datetime.fromisoformat(gd) + timedelta(days=1)).date().isoformat(), (datetime.fromisoformat(gd) - timedelta(days=1)).date().isoformat()):
        g = games.get((tk(home), tk(away), d))
        if g: break
    if not g: continue
    for name, books in players.items():
        rd = read_player(books, alt.get((ev, cap), {}).get(name))
        if rd is None: continue
        b = g.get(pk(name))
        if not b or len(b) != 1: continue
        b = b[0]
        dist = rd.distribution
        cm = curve_mean(dist.curve) if dist and dist.curve else None
        rows.append(dict(date=gd, event=ev, player=pk(name), name=name, tip=tip, cap=cap, lead_h=(tip_dt - cap_dt).total_seconds() / 3600,
                         line=rd.line, p_over=rd.p_over, nbooks=len(rd.lines),
                         book_lines={x.book: x.line for x in rd.lines}, book_pover={x.book: x.p_over for x in rd.lines},
                         curve_mean=cm, ladder_books=dist.ladder_books if dist else 0,
                         floor=dist.floor if dist else None, ceiling=dist.ceiling if dist else None,
                         played=bool(b["played"]), actual=b["points"], minutes=b["minutes"]))
(P / "table.json").write_text(json.dumps(rows), encoding="utf-8")
print("rows", len(rows), "player-games", len({(r["event"], r["player"]) for r in rows}), "nights", len({r["date"] for r in rows}))
