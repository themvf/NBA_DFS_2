"""Season stats for the First TD pages: the model's record, how first touchdowns happen, and leaders.

Descriptive only; nothing here changes a forecast. Each sport's ``stats`` command builds one JSON
document from its own labelled games and graded forecasts and stores it in ``first_td_stats``,
skipping the write when nothing changed since the last stored copy. The page reads the newest row.

A labelled game (built by each sport's module) is a dict with:
    season, home, away, home_implied, away_implied,
    outcome ('offense' | 'non_offense' | 'no_td'), side ('home' | 'away' | None),
    kind ('rush' | 'rec' | None), pid, name, qb (scorer threw a pass in that game; None if unknown)
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict

STATS_VERSION = "first-td-stats-v1"
CALIBRATION_BINS = (0.0, 0.05, 0.10, 0.15, 0.20)
LEADERS = 10

DDL = [
    """CREATE TABLE IF NOT EXISTS first_td_stats (
        id BIGSERIAL PRIMARY KEY, sport TEXT NOT NULL, version TEXT NOT NULL,
        computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), digest TEXT NOT NULL, stats JSONB NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_first_td_stats_sport ON first_td_stats (sport, computed_at DESC)",
]


def _rate(hits: int, n: int) -> float | None:
    return hits / n if n else None


def base_rates(games: list[dict], seasons: list[int]) -> list[dict]:
    """Per season and all seasons together: how the first touchdown was scored and by whom."""
    out = []
    for label, keep in [(str(s), {s}) for s in sorted(seasons, reverse=True)] + [("All", set(seasons))]:
        rows = [g for g in games if g["season"] in keep and g["outcome"] in ("offense", "non_offense")]
        known = [g for g in rows if g["outcome"] == "non_offense" or g.get("kind")]
        rush = [g for g in known if g.get("kind") == "rush"]
        qb_known = [g for g in rush if g.get("qb") is not None]
        sided = [g for g in rows if g.get("side") in ("home", "away")]
        fav = [g for g in sided if g.get("home_implied") is not None and g.get("away_implied") is not None
               and float(g["home_implied"]) != float(g["away_implied"])]
        fav_hits = sum((float(g["home_implied"]) > float(g["away_implied"])) == (g["side"] == "home") for g in fav)
        out.append({
            "season": label, "games": len(rows),
            "rush": _rate(len(rush), len(known)),
            "rec": _rate(sum(g.get("kind") == "rec" for g in known), len(known)),
            "non_offense": _rate(sum(g["outcome"] == "non_offense" for g in known), len(known)),
            "qb_rush": _rate(sum(bool(g["qb"]) for g in qb_known), len(qb_known)) if qb_known else None,
            "qb_known": len(qb_known),
            "favourite_first": _rate(fav_hits, len(fav)), "favourite_games": len(fav),
            "home_first": _rate(sum(g["side"] == "home" for g in sided), len(sided)),
        })
    return out


def leaders(games: list[dict], season: int) -> dict:
    """This season: players with the most first touchdowns, teams that scored first most often."""
    rows = [g for g in games if g["season"] == season and g["outcome"] in ("offense", "non_offense")]
    players: dict[str, dict] = {}
    for g in rows:
        if g["outcome"] == "offense" and g.get("pid"):
            p = players.setdefault(g["pid"], {"name": g.get("name") or g["pid"], "team": g[g["side"]] if g.get("side") else "",
                                              "firsts": 0})
            p["firsts"] += 1
    teams: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for g in rows:
        if g.get("side") not in ("home", "away"):
            continue
        for side in ("home", "away"):
            teams[g[side]][1] += 1
            teams[g[side]][0] += g["side"] == side
    top_players = sorted(players.values(), key=lambda p: (-p["firsts"], p["name"]))
    cut = max(top_players[LEADERS - 1]["firsts"] if len(top_players) >= LEADERS else 0, 2)
    shown = [p for p in top_players if p["firsts"] >= cut]
    tied = None
    if len(shown) > LEADERS * 2:  # a long tie at the cut is summarised, not cut off alphabetically
        tied = {"firsts": cut, "players": sum(p["firsts"] == cut for p in shown)}
        shown = [p for p in shown if p["firsts"] > cut]
    return {
        "season": season,
        "players": shown,
        "tied": tied,
        "teams": [{"team": t, "firsts": f, "games": n}
                  for t, (f, n) in sorted(teams.items(), key=lambda kv: (-kv[1][0], kv[1][1], kv[0]))[:LEADERS]],
    }


def _player_probs(dist: dict) -> dict[str, float]:
    return {k: float(v) for k, v in dist.items() if not k.startswith("other") and k != "non_offense"}


def track_record(rows: list[dict]) -> dict | None:
    """Graded pregame forecasts: rows of {dist, scorer, prob}. ``scorer`` is the player id or None."""
    if not rows:
        return None
    hits, stated, n_top = 0, 0.0, 0
    bins = [[0, 0.0, 0] for _ in CALIBRATION_BINS]  # forecasts, summed chance, hits
    for r in rows:
        players = _player_probs(r["dist"])
        if players:
            best = max(players, key=players.get)
            n_top += 1
            stated += players[best]
            hits += best == r["scorer"]
        for pid, p in players.items():
            i = max(j for j, lo in enumerate(CALIBRATION_BINS) if p >= lo)
            bins[i][0] += 1
            bins[i][1] += p
            bins[i][2] += pid == r["scorer"]
    edges = list(CALIBRATION_BINS) + [None]
    return {
        "games": len(rows),
        "log_loss": sum(-math.log(max(float(r["prob"]), 1e-9)) for r in rows) / len(rows),
        "top_pick_hit": _rate(hits, n_top), "top_pick_stated": stated / n_top if n_top else None,
        "calibration": [{"from": edges[i], "to": edges[i + 1], "forecasts": b[0],
                         "stated": b[1] / b[0] if b[0] else None, "happened": _rate(b[2], b[0])}
                        for i, b in enumerate(bins)],
    }


def build(games: list[dict], seasons: list[int], current: int, graded: list[dict], version: str,
          market: dict | None = None) -> dict:
    """``market`` (NFL only): the summary against DraftKings' first-TD prices, model/nfl_first_td_market."""
    out = {"version": STATS_VERSION, "graded_version": version, "record": track_record(graded),
           "base_rates": base_rates(games, seasons), "leaders": leaders(games, current)}
    if market is not None:
        out["market"] = market
    return out


def save(db, sport: str, stats: dict) -> bool:
    """Append the stats unless they match the newest stored copy for the sport."""
    text = json.dumps(stats, sort_keys=True, default=str)
    digest = hashlib.sha256(text.encode()).hexdigest()
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
        cur.execute("SELECT digest FROM first_td_stats WHERE sport = %s ORDER BY computed_at DESC LIMIT 1", (sport,))
        last = cur.fetchone()
        if last and last["digest"] == digest:
            return False
        cur.execute("INSERT INTO first_td_stats (sport, version, digest, stats) VALUES (%s, %s, %s, %s)",
                    (sport, STATS_VERSION, digest, text))
    return True
