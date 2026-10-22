"""Advice pick log: record each betting pick suggested in a session, settle it, grade the advice over a season.

Tables (append-only, ``db/schema.py``): ``nfl_advice_picks`` (frozen when logged) and
``nfl_advice_pick_results`` (one row per settlement; a correction appends a new row and the latest wins).

  add      log a pick before kickoff (a pick logged after kickoff is stored with ``late = true`` and left out
           of the graded record)
  settle   settle every finished pick it can from nflverse weekly stats; ``--id N --result R`` settles one
           by hand (any market, e.g. one the auto-settler does not know)
  report   the record, units at the logged price (1 unit a pick), and the logged fair chance against the
           price's implied chance

Markets the auto-settler grades:

* over/under props: ``player_rush_yds``, ``player_reception_yds``, ``player_receptions``, ``player_pass_yds``,
  ``player_rush_reception_yds``, ``player_pass_tds``; yes/no ``player_anytime_td``
* game leaders (selection ``yes``): ``leader_rushing_yards``, ``leader_receiving_yards``, ``leader_receptions``,
  ``leader_total_yards``; a tie for the lead is a dead heat (stake split, paid at full odds)

A player with no stat row in a finished game is not settled automatically (books void props for a player who
does not play, and leader-market rules vary); settle him by hand.

    python -m research.nfl_advice_log add --week 6 --game KC@LV --player "Xavier Worthy" \
        --market player_reception_yds --selection under --line 45.5 --price -115 --book draftkings \
        --fair 0.55 --reason "weak form"
    python -m research.nfl_advice_log settle
    python -m research.nfl_advice_log report
"""

from __future__ import annotations

import argparse
import io
import re
import unicodedata
from datetime import datetime, timezone

SEASON = 2026
TEAM_ALIAS = {"LAR": "LA", "WSH": "WAS", "JAC": "JAX", "AZ": "ARI"}
PROP_STATS = {"player_rush_yds": ("rushing_yards",), "player_reception_yds": ("receiving_yards",),
              "player_receptions": ("receptions",), "player_pass_yds": ("passing_yards",),
              "player_rush_reception_yds": ("rushing_yards", "receiving_yards"), "player_pass_tds": ("passing_tds",),
              "player_anytime_td": ("rushing_tds", "receiving_tds")}
LEADER_STATS = {"leader_rushing_yards": ("rushing_yards",), "leader_receiving_yards": ("receiving_yards",),
                "leader_receptions": ("receptions",), "leader_total_yards": ("rushing_yards", "receiving_yards")}


# ── pure helpers ─────────────────────────────────────────────────────────────
def norm(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    return re.sub(r"[^a-z]", "", s)


def game_id(season: int, week: int, game: str) -> str:
    """'KC@LV' (away@home, any common team codes) -> nflverse '2026_06_KC_LV'."""
    away, home = (TEAM_ALIAS.get(t.strip().upper(), t.strip().upper()) for t in game.split("@"))
    return f"{season}_{week:02d}_{away}_{home}"


def decimal(american: int) -> float:
    return 1 + (100 / -american if american < 0 else american / 100)


def implied(american: int) -> float:
    return 1 / decimal(american)


def profit(result: str, american: int, share: float = 1.0) -> float:
    """Units won on a 1-unit stake. A dead heat splits the stake by ``share`` and pays full odds on that part."""
    if result == "won":
        return decimal(american) - 1
    if result == "lost":
        return -1.0
    if result == "dead_heat":
        return decimal(american) * share - 1
    return 0.0


def grade_prop(selection: str, line: float | None, actual: float, market: str) -> str:
    if market == "player_anytime_td":
        scored = actual > 0
        return "won" if scored == (selection == "yes") else "lost"
    if actual == line:
        return "push"
    went_over = actual > line
    return "won" if went_over == (selection == "over") else "lost"


def grade_leader(player_value: float, values: list[float]) -> tuple[str, float]:
    """(result, share) for a 'yes' on a player to lead, given every player's value in the game."""
    best = max(values)
    if player_value < best:
        return "lost", 1.0
    k = sum(1 for v in values if v == best)
    return ("won", 1.0) if k == 1 else ("dead_heat", 1.0 / k)


def stat_value(row, stats: tuple[str, ...]) -> float:
    total = 0.0
    for s in stats:
        v = row.get(s)
        total += 0.0 if v is None or v != v else float(v)
    return total


def settle_pick(pick: dict, box: list[dict]) -> dict | None:
    """Auto-settle one pick against its game's box (rows of nflverse weekly stats for both teams)."""
    market = pick["market"]
    mine = [r for r in box if norm(r.get("player_display_name") or r.get("player_name")) == norm(pick["player"])]
    if len(mine) != 1:
        return None
    me = mine[0]
    if market in PROP_STATS:
        actual = stat_value(me, PROP_STATS[market])
        if market != "player_anytime_td" and pick["line"] is None:
            return None
        return {"result": grade_prop(pick["selection"], pick["line"], actual, market), "share": 1.0,
                "actual": actual}
    if market in LEADER_STATS and pick["selection"] == "yes":
        values = [stat_value(r, LEADER_STATS[market]) for r in box]
        actual = stat_value(me, LEADER_STATS[market])
        result, share = grade_leader(actual, values)
        return {"result": result, "share": share, "actual": actual}
    return None


def report_rows(picks: list[dict]) -> dict:
    """Record and units over settled, on-time picks; fair-vs-implied over every on-time pick with a fair chance."""
    graded = [p for p in picks if not p["late"] and p.get("result") not in (None, "void")]
    units = sum(profit(p["result"], p["price_american"], p.get("share") or 1.0) for p in graded)
    wins = sum(1 for p in graded if p["result"] == "won") + sum(p.get("share") or 0 for p in graded
                                                                  if p["result"] == "dead_heat")
    losses = sum(1 for p in graded if p["result"] == "lost")
    with_fair = [p for p in picks if not p["late"] and p.get("fair_prob") is not None]
    return {"picks": len(picks), "late": sum(1 for p in picks if p["late"]), "graded": len(graded),
            "wins": wins, "losses": losses, "pushes": sum(1 for p in graded if p["result"] == "push"),
            "units": units, "roi": units / len(graded) if graded else None,
            "avg_fair": sum(p["fair_prob"] for p in with_fair) / len(with_fair) if with_fair else None,
            "avg_implied": sum(implied(p["price_american"]) for p in with_fair) / len(with_fair) if with_fair else None}


# ── database ─────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url)


def add(db, args) -> int:
    gid = game_id(args.season, args.week, args.game)
    rows = db.execute("SELECT kickoff FROM nfl_season_games WHERE nflverse_game_id = %s", (gid,))
    if not rows:
        raise SystemExit(f"no game {gid} in nfl_season_games (check --week and --game AWAY@HOME)")
    kickoff = rows[0]["kickoff"]
    if args.selection in ("over", "under") and args.line is None:
        raise SystemExit("an over/under pick needs --line")
    late = kickoff is not None and datetime.now(timezone.utc) >= kickoff
    out = db.execute(
        """INSERT INTO nfl_advice_picks (season, week, game_id, kickoff, player, market, selection, line,
               price_american, book, fair_prob, reason, late)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (args.season, args.week, gid, kickoff, args.player, args.market, args.selection, args.line, args.price,
         args.book, args.fair, args.reason, late))
    pid = out[0]["id"]
    print(f"logged pick {pid}: {args.player} {args.market} {args.selection} {args.line or ''} {args.price:+d} "
          f"({args.book}) in {gid}{'  LATE: after kickoff, not graded' if late else ''}")
    return pid


def latest_results(db, season: int) -> dict[int, dict]:
    return {r["pick_id"]: dict(r) for r in db.execute(
        """SELECT DISTINCT ON (r.pick_id) r.pick_id, r.result, r.share, r.actual, r.method, r.settled_at
           FROM nfl_advice_pick_results r JOIN nfl_advice_picks p ON p.id = r.pick_id
           WHERE p.season = %s ORDER BY r.pick_id, r.settled_at DESC, r.id DESC""", (season,))}


def load_weekly(season: int):
    import pandas as pd
    import requests

    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    content = requests.get(NFLVERSE_WEEKLY_STATS_URL.format(season=season), timeout=120).content
    frame = pd.read_csv(io.BytesIO(content), low_memory=False)
    return frame[(frame["season"] == season) & (frame["season_type"] == "REG")]


def settle(db, season: int) -> None:
    done = latest_results(db, season)
    open_picks = [dict(p) for p in db.execute(
        """SELECT p.* FROM nfl_advice_picks p JOIN nfl_season_games g ON g.nflverse_game_id = p.game_id
           WHERE p.season = %s AND g.completed""", (season,)) if p["id"] not in done]
    if not open_picks:
        print("nothing to settle")
        return
    frame = load_weekly(season)
    for p in open_picks:
        away, home = p["game_id"].split("_")[2:4]
        box = frame[(frame["week"] == p["week"]) & (frame["team"].isin([away, home]))].to_dict("records")
        if {r["team"] for r in box} != {away, home}:
            print(f"pick {p['id']}: stats for {p['game_id']} not loaded yet")
            continue
        got = settle_pick(p, box)
        if got is None:
            print(f"pick {p['id']}: {p['player']} {p['market']} needs a manual settle (no single stat row, or an "
                  f"unknown market)")
            continue
        db.execute("""INSERT INTO nfl_advice_pick_results (pick_id, result, share, actual, method)
                      VALUES (%s,%s,%s,%s,'auto')""", (p["id"], got["result"], got["share"], got["actual"]))
        print(f"pick {p['id']}: {p['player']} {p['market']} {p['selection']} -> {got['result']} ({got['actual']:g})")


def settle_manual(db, pick_id: int, result: str, actual: float | None, share: float, note: str | None) -> None:
    db.execute("""INSERT INTO nfl_advice_pick_results (pick_id, result, share, actual, method, note)
                  VALUES (%s,%s,%s,%s,'manual',%s)""", (pick_id, result, share, actual, note))
    print(f"pick {pick_id}: {result}")


def report(db, season: int) -> None:
    results = latest_results(db, season)
    picks = [dict(p) | (results.get(p["id"]) or {}) for p in db.execute(
        "SELECT * FROM nfl_advice_picks WHERE season = %s ORDER BY week, id", (season,))]
    if not picks:
        print("no picks logged")
        return
    for p in picks:
        res = p.get("result") or "open"
        pl = profit(res, p["price_american"], p.get("share") or 1.0) if res not in ("open",) else 0.0
        fair = f"{p['fair_prob']:.0%}" if p.get("fair_prob") is not None else "-"
        print(f"{p['id']:>4} wk{p['week']:<2} {p['game_id'][8:]:<9} {p['player'][:22]:<22} {p['market'][:24]:<24} "
              f"{p['selection']:<5} {'' if p['line'] is None else p['line']:<6} {p['price_american']:>+5d} "
              f"fair {fair:>4} implied {implied(p['price_american']):.0%}  {res:<9} {pl:+.2f}"
              f"{'  LATE' if p['late'] else ''}")
    s = report_rows(picks)
    rec = f"{s['wins']:g}-{s['losses']}" + (f"-{s['pushes']}" if s["pushes"] else "")
    roi = f"{s['roi']:+.1%}" if s["roi"] is not None else "n/a"
    print(f"\n{s['picks']} picks ({s['late']} late, not graded), {s['graded']} graded: {rec}, {s['units']:+.2f} units, "
          f"ROI {roi}")
    if s["avg_fair"] is not None:
        print(f"average logged fair chance {s['avg_fair']:.1%} against the price's implied {s['avg_implied']:.1%}")
    print("A season is a few hundred picks at most: read the record as descriptive until then.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=SEASON)
    sub = ap.add_subparsers(dest="command", required=True)
    a = sub.add_parser("add")
    a.add_argument("--week", type=int, required=True)
    a.add_argument("--game", required=True, help="AWAY@HOME, e.g. KC@LV")
    a.add_argument("--player", required=True)
    a.add_argument("--market", required=True,
                   choices=sorted(PROP_STATS) + sorted(LEADER_STATS) + ["other"])
    a.add_argument("--selection", required=True, choices=["over", "under", "yes", "no"])
    a.add_argument("--line", type=float)
    a.add_argument("--price", type=int, required=True, help="American odds, e.g. -115 or 156")
    a.add_argument("--book", default="draftkings")
    a.add_argument("--fair", type=float, help="our fair chance for this selection, 0-1")
    a.add_argument("--reason")
    s = sub.add_parser("settle")
    s.add_argument("--id", type=int)
    s.add_argument("--result", choices=["won", "lost", "push", "void", "dead_heat"])
    s.add_argument("--actual", type=float)
    s.add_argument("--share", type=float, default=1.0)
    s.add_argument("--note")
    sub.add_parser("report")
    args = ap.parse_args(argv)
    db = _db()
    if args.command == "add":
        add(db, args)
    elif args.command == "settle":
        if args.id is not None:
            if not args.result:
                ap.error("--id needs --result")
            settle_manual(db, args.id, args.result, args.actual, args.share, args.note)
        else:
            settle(db, args.season)
    else:
        report(db, args.season)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
