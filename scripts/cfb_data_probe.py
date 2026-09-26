"""Read-only probe: what can CFBD and FantasyPros give a college DFS model?

Prints endpoint status, structure and counts only -- never keys, never full
payloads. Run by .github/workflows/cfb_data_probe.yml, where the keys live.

    python scripts/cfb_data_probe.py --team Indiana --season 2026
"""
from __future__ import annotations

import argparse
import os
import time

import requests

CFBD = "https://api.collegefootballdata.com"
FANTASYPROS = "https://api.fantasypros.com/public/v2/json"


def shape(value, depth=0):
    """A compact description of a JSON value: keys and lengths, not contents."""
    if isinstance(value, dict):
        if depth >= 2:
            return f"dict({len(value)})"
        return "{" + ", ".join(f"{k}: {shape(v, depth + 1)}" for k, v in list(value.items())[:12]) + "}"
    if isinstance(value, list):
        return f"list[{len(value)}]" + (f" of {shape(value[0], depth + 1)}" if value else "")
    return type(value).__name__


def cfbd(path, params):
    key = os.environ.get("CFBD_API_KEY")
    r = requests.get(f"{CFBD}/{path}", params=params, headers={"Authorization": f"Bearer {key}"}, timeout=45)
    remaining = r.headers.get("X-CallLimit-Remaining")
    print(f"\nCFBD /{path} {params} -> {r.status_code} (calls remaining: {remaining})")
    return r.json() if r.ok else None


def fantasypros(path, params):
    key = os.environ.get("FANTASYPROS_API_KEY")
    r = requests.get(f"{FANTASYPROS}/{path}", params=params, headers={"x-api-key": key, "Accept": "application/json"}, timeout=45)
    print(f"\nFantasyPros /{path} {params} -> {r.status_code}")
    if not r.ok:
        print("  body:", r.text[:160].replace("\n", " "))
        return None
    data = r.json()
    print("  shape:", shape(data))
    # Which sport did the answer actually come from? A sample of names settles it.
    rows = next((data[k] for k in ("injuries", "items", "players") if isinstance(data.get(k), list) and data[k]), [])
    sample = [{k: row.get(k) for k in ("sport", "player_name", "name", "title", "team_id", "team", "school", "position_id", "injury_status") if row.get(k)} for row in rows[:3]]
    print("  sport:", data.get("sport"), "| sample:", sample)
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--team", default="Indiana")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--week", type=int, default=5)
    parser.add_argument("--fantasypros-lineups", action="store_true",
                        help="only probe FantasyPros college paths that could carry injuries, news or depth charts")
    args = parser.parse_args()
    if args.fantasypros_lineups:
        # One request every 4 seconds: FantasyPros rate-limited a burst on 2026-09-25.
        for path, params in [("cfb/injuries", {}), ("cfb/news", {"limit": 5}), ("cfb/depth-charts", {}),
                             ("cfb/players", {"status": "injured"}), (f"cfb/{args.season}/rankings", {"week": args.week}),
                             (f"cfb/{args.season}/consensus-rankings", {"week": args.week}), ("nfl/injuries", {})]:
            fantasypros(path, params)
            time.sleep(4)
        return
    print("CFBD key present:", bool(os.environ.get("CFBD_API_KEY")),
          "| FantasyPros key present:", bool(os.environ.get("FANTASYPROS_API_KEY")))

    games = cfbd("games", {"year": args.season, "team": args.team}) or []
    done = [g for g in games if g.get("completed")]
    print("  games:", [(g.get("week"), g.get("awayTeam"), g.get("homeTeam"), g.get("startDate", "")[:10], g.get("completed")) for g in games][:8])
    last_week = max((g["week"] for g in done), default=None)

    if last_week:
        box = cfbd("games/players", {"year": args.season, "week": last_week, "team": args.team}) or []
        print("  shape:", shape(box))
        for team in (box[0]["teams"] if box else []):
            cats = {c["name"]: [f'{t["name"]}({len(t["athletes"])})' for t in c["types"]] for c in team["categories"]}
            print(f"  {team.get('team')}: {cats}")
            if team["categories"] and team["categories"][0]["types"]:
                print("  athlete fields:", shape(team["categories"][0]["types"][0]["athletes"][:1]))

    for path in ("stats/player/season", "roster", "player/usage"):
        data = cfbd(path, {"year": args.season, "team": args.team}) or []
        print("  shape:", shape(data))

    # FantasyPros: the NFL path is the control that proves the key works.
    fantasypros(f"nfl/{args.season}/projections", {"week": 3, "position": "QB", "scoring": "PPR"})
    for sport in ("ncaaf", "cfb", "ncaa-football", "cfootball"):
        fantasypros(f"{sport}/{args.season}/projections", {"week": args.week, "position": "QB"})
        fantasypros(f"{sport}/players", {})


if __name__ == "__main__":
    main()
