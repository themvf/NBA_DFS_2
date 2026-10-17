"""Run and grade the market-anchored game-leaders test (docs/nfl-leaders-anchored-test.md).

    python -m research.nfl_game_leaders_anchored refresh [--within-hours 6] [--write-db]
    python -m research.nfl_game_leaders_anchored backfill --page-ref GITREF [--write-db]   # discovery games
    python -m research.nfl_game_leaders_anchored grade [--write-db] [--out FILE] [--discovery]

Imports the published model and the DK pricing helpers; changes neither.
"""
from __future__ import annotations

import argparse
import io
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from model.nfl_game_leaders_anchored import (MARKETS, VERSION, anchor_players, anchors, grouped_shares,
                                             leader_log_loss, leader_shares)

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION = json.loads((ROOT / "docs" / "nfl-leaders-anchored-test.json").read_text())
METRICS = tuple(MARKETS)


def bank(history, snapshot, request):
    """Our saved forecast with draws: players, our (published-style) shares for all four markets, decision time."""
    from model.nfl_game_leaders import forecast
    from model.nfl_game_leaders_calibration import production_settings
    from research.nfl_game_leaders import expected_history
    cfg = production_settings()
    request = {**request, "expected_prior_game_ids": expected_history(snapshot, request["game"], request["decision_at"], cfg.recent_games)}
    result = forecast(history, request, cfg, include_draws=True)
    players = [{"identity": p["identity"], "name": p["name"], "team": p["team"], "residual": p.get("residual", False),
                "draws": {f: p["draws"][f] for f in ("rushYds", "recYds", "receptions")}} for p in result["shared_draws"]["players"]]
    ours = {m: {p["identity"]: p["win_share"] for p in result["metrics"][m]["players"]} for m in METRICS}
    return players, ours, result["decision_at"]


def dk_quotes(game_props, name_key):
    """Anchors per (metric, player) from DraftKings main lines and ladders."""
    by = defaultdict(dict)
    for r in game_props:
        dk = (r["books"] or {}).get("draftkings")
        if dk:
            by[(r["market"], name_key(r["player"]))] = dk
    quotes = {}
    for metric, (main_key, alt_key, _) in MARKETS.items():
        for (market, name), book in list(by.items()):
            if market != main_key:
                continue
            pts = anchors(book, by.get((alt_key, name)))
            if pts:
                quotes[(metric, name)] = pts
    return quotes


def price(players, ours, game_props):
    """Rows for one game: our share, the existing DK-implied share and the anchored share, per market and player."""
    from research.nfl_game_leaders_market import norm, price_game
    quotes = dk_quotes(game_props, norm)
    arrays, diagnostics = anchor_players(players, quotes, norm)
    implied = price_game(players, game_props)
    rows = []
    for metric in METRICS:
        anchored = grouped_shares(players, leader_shares(arrays[metric]))
        dk_implied = {r["identity"]: r["market_leader_share"] for r in implied[metric]["rows"]} if metric in implied else {}
        for p in players:
            key = f"OTHER:{p['team']}" if p.get("residual") else p["identity"]
            if any(r["identity"] == key for r in rows if r["metric"] == metric):
                continue
            rows.append({"metric": metric, "identity": key, "player": key if p.get("residual") else p["name"], "team": p["team"],
                         "our_share": ours[metric].get(key, 0.0), "dk_implied_share": dk_implied.get(key),
                         "anchored_share": anchored.get(key, 0.0),
                         "anchors": diagnostics.get(p["identity"], {}).get(metric) if not p.get("residual") else None})
    return rows


def _connect():
    import os
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def ensure_tables() -> None:
    import os
    from db.database import DatabaseManager
    DatabaseManager(os.environ["DATABASE_URL"])


def run_games(published: dict, games: list, write_db: bool, origin: str, check_published: bool, log=print) -> list[str]:
    from model.nfl_game_leaders import prepare
    from research.nfl_game_leaders import read
    from research.nfl_game_leaders_market import check_matches_published, load_props, match_event
    # Short-lived connections only: a simulation takes minutes and the pooled
    # endpoint drops an idle connection held across it.
    conn = _connect()
    props = load_props(conn.cursor())
    conn.close()
    if write_db:
        ensure_tables()
    loaded, histories, written = {}, {}, []
    for g in games:
        gid = g["game"]["game_id"]
        src = g.get("sources") or published.get("sources")
        if not src:
            log(json.dumps({"skipped": gid, "reason": "published game names no sources"}), flush=True)
            continue
        key = (src["snapshot"], src["requests"])
        if key not in loaded:
            loaded[key] = (read(ROOT / src["snapshot"]), read(ROOT / src["requests"]))
        snapshot, requests = loaded[key]
        request = requests.get(gid)
        if not request or "players" not in request:
            log(json.dumps({"skipped": gid, "reason": "no request"}), flush=True)
            continue
        hkey = (id(snapshot), request["decision_at"])
        if hkey not in histories:
            histories[hkey] = prepare(snapshot, request["decision_at"])[0]
        players, ours, decision_at = bank(histories[hkey], snapshot, request)
        if check_published:
            check_matches_published(gid, {m: ours[m] for m in ("rushing_yards", "receiving_yards", "receptions")}, g)
        event_id, game_props = match_event(players, props)
        if not event_id:
            log(json.dumps({"skipped": gid, "reason": "no captured sportsbook event matched"}), flush=True)
            continue
        captured = max(r["captured_at"] for r in game_props)
        if write_db:
            conn = _connect()
            cur = conn.cursor()
            cur.execute("SELECT max(prop_captured_at) FROM nfl_game_leader_anchored WHERE game_id=%s AND forecast_decision_at=%s AND origin=%s",
                        (gid, decision_at, origin))
            previous = cur.fetchone()[0]
            conn.rollback()
            if previous is not None and previous >= captured:
                conn.close()
                log(json.dumps({"skipped": gid, "reason": "already priced against this capture"}), flush=True)
                continue
        rows = price(players, ours, game_props)
        computed = datetime.now(timezone.utc)
        if write_db:
            cur.executemany("""INSERT INTO nfl_game_leader_anchored (game_id, metric, identity, player, team, our_share,
                    dk_implied_share, anchored_share, anchors, prop_captured_at, forecast_decision_at, computed_at, origin, version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [(gid, r["metric"], r["identity"], r["player"], r["team"], r["our_share"], r["dk_implied_share"], r["anchored_share"],
                  json.dumps(r["anchors"]) if r["anchors"] else None, captured, decision_at, computed, origin, VERSION) for r in rows])
            conn.commit()
            conn.close()
        written.append(gid)
        top = {m: max((r for r in rows if r["metric"] == m), key=lambda r: r["anchored_share"]) for m in METRICS}
        log(json.dumps({"priced": gid, "origin": origin, "written": write_db,
                        "anchored_leaders": {m: [r["player"], round(r["anchored_share"], 3), round(r["our_share"], 3)] for m, r in top.items()}}), flush=True)
    return written


# --- grading ---------------------------------------------------------------------------------------

def winners_for(game_id: str, week: int, rows: list[dict], frame, metric: str) -> dict[str, float] | None:
    """Tie-split credit per identity (unlisted leaders credit OTHER:<team>), or None when the game is not in the file."""
    away, home = game_id.split("_")[2:4]
    box = frame[(frame["week"] == week) & (frame["team"].isin([away, home]))]
    if set(box["team"]) != {away, home}:
        return None
    values = (box["rushing_yards"].fillna(0) + box["receiving_yards"].fillna(0)) if metric == "total_yards" else box[metric].fillna(0)
    best = values.max()
    leaders = box[values == best]
    known = {r["identity"] for r in rows}
    credit: dict[str, float] = defaultdict(float)
    for pid, team in zip(leaders["player_id"], leaders["team"]):
        credit[pid if pid in known else f"OTHER:{team}"] += 1 / len(leaders)
    return dict(credit)


def bootstrap(diffs: list[float], resamples: int, seed: int) -> list[float] | None:
    if len(diffs) < 2:
        return None
    rng = np.random.default_rng(seed)
    a = np.asarray(diffs)
    means = a[rng.integers(0, len(a), size=(resamples, len(a)))].mean(axis=1)
    return [round(float(np.quantile(means, .025)), 4), round(float(np.quantile(means, .975)), 4)]


def grade(stored: list[dict], kickoffs: Mapping[str, datetime], weeks: Mapping[str, int], frame, origin: str,
          registration: Mapping[str, Any] = REGISTRATION) -> dict:
    start = datetime.fromisoformat(registration["registered_at"])
    by_game = defaultdict(list)
    for r in stored:
        k = kickoffs.get(r["game_id"])
        if r["origin"] != origin or r["version"] != registration["model_version"] or k is None:
            continue
        # Discovery rows are re-created after the game by design; only their lines
        # must be pregame. A prospective row must have been priced before kickoff.
        if r["prop_captured_at"] >= k or (origin == "prospective" and r["computed_at"] >= k):
            continue
        if origin == "prospective" and (r["prop_captured_at"] < start or k <= start):
            continue
        by_game[r["game_id"]].append(r)
    per_metric = {m: [] for m in METRICS}
    for gid, rows in by_game.items():
        last = max(r["computed_at"] for r in rows)
        rows = [r for r in rows if r["computed_at"] == last]
        for metric in METRICS:
            mrows = [r for r in rows if r["metric"] == metric]
            if not mrows:
                continue
            credit = winners_for(gid, weeks[gid], mrows, frame, metric)
            if credit is None:
                continue
            shares = {name: {r["identity"]: r[col] for r in mrows if r[col] is not None}
                      for name, col in (("ours", "our_share"), ("anchored", "anchored_share"), ("dk_implied", "dk_implied_share"))}
            losses = {name: leader_log_loss(s, credit) for name, s in shares.items() if s}
            per_metric[metric].append({"game_id": gid, "week": weeks[gid], "winners": credit, **{f"log_loss_{k}": round(v, 4) for k, v in losses.items()}})
    boot = registration["bootstrap"]
    floor = registration["floors"]["games_per_market"]
    report = {"test": registration["test"], "origin": origin, "markets": {}}
    for metric, games in per_metric.items():
        diffs = [g["log_loss_anchored"] - g["log_loss_ours"] for g in games]
        interval = bootstrap(diffs, boot["resamples"], boot["seed"])
        n = len(games)
        verdict = None
        if origin == "prospective" and n >= floor and interval:
            verdict = "PASS" if interval[1] < 0 else "FAIL"
        dk = [g["log_loss_dk_implied"] for g in games if "log_loss_dk_implied" in g]
        report["markets"][metric] = {
            "games": n, "floor": floor,
            "mean_log_loss": {"ours": round(float(np.mean([g["log_loss_ours"] for g in games])), 4) if n else None,
                              "anchored": round(float(np.mean([g["log_loss_anchored"] for g in games])), 4) if n else None,
                              "dk_implied": round(float(np.mean(dk)), 4) if dk else None},
            "anchored_minus_ours": round(float(np.mean(diffs)), 4) if n else None, "interval": interval,
            "anchored_better_in": sum(d < 0 for d in diffs), "verdict_if_first": verdict,
            "status": "floor met" if n >= floor else "descriptive until the floor is met"}
        report["markets"][metric]["games_detail"] = games
    return report


def load_grade_inputs(cur, season: int):
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    cur.execute("""SELECT nflverse_game_id, week, kickoff FROM nfl_season_games
                   WHERE season=%s AND game_type='REG' AND nflverse_game_id IS NOT NULL AND completed""", (season,))
    games = cur.fetchall()
    kickoffs, weeks = {g[0]: g[2] for g in games}, {g[0]: int(g[1]) for g in games}
    cur.execute("""SELECT game_id, metric, identity, our_share, dk_implied_share, anchored_share, prop_captured_at,
                          computed_at, origin, version FROM nfl_game_leader_anchored WHERE game_id LIKE %s""", (f"{season}_%",))
    cols = ("game_id", "metric", "identity", "our_share", "dk_implied_share", "anchored_share", "prop_captured_at", "computed_at", "origin", "version")
    stored = [dict(zip(cols, r)) for r in cur.fetchall()]
    content = requests.get(NFLVERSE_WEEKLY_STATS_URL.format(season=season), timeout=120).content
    frame = pd.read_csv(io.BytesIO(content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    return stored, kickoffs, weeks, frame


def write_grade(conn, report: dict) -> None:
    ensure_tables()
    cur = conn.cursor()
    cur.execute("INSERT INTO nfl_leaders_anchored_test_runs (test, evaluated_at, report) VALUES (%s, now(), %s) RETURNING id",
                (report["test"], json.dumps(report, default=str)))
    run_id = cur.fetchone()[0]
    for metric, m in report["markets"].items():
        if m["verdict_if_first"]:
            cur.execute("""INSERT INTO nfl_leaders_anchored_test_verdicts (test, metric, verdict, run_id, decided_at)
                           VALUES (%s,%s,%s,%s,now()) ON CONFLICT (test, metric) DO NOTHING""",
                        (report["test"], metric, m["verdict_if_first"], run_id))
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("refresh")
    r.add_argument("--within-hours", type=float, default=0.)
    r.add_argument("--write-db", action="store_true")
    b = sub.add_parser("backfill")
    b.add_argument("--page-ref", required=True, help="git ref whose web/src/data/game-leaders.json lists the games")
    b.add_argument("--game", action="append")
    b.add_argument("--write-db", action="store_true")
    g = sub.add_parser("grade")
    g.add_argument("--discovery", action="store_true")
    g.add_argument("--write-db", action="store_true")
    g.add_argument("--out", type=Path)
    args = parser.parse_args()
    from model.nfl_longest_touchdown import timestamp
    if args.command == "refresh":
        published = json.loads((ROOT / "web/src/data/game-leaders.json").read_text())
        now = datetime.now(timezone.utc)
        games = [x for x in published["games"] if timestamp(x["game"]["kickoff"]) > now and (
            not args.within_hours or timestamp(x["game"]["kickoff"]) <= now + timedelta(hours=args.within_hours))]
        if not games:
            print(json.dumps({"priced": 0, "reason": "no upcoming published game in the window"}))
            return
        run_games(published, games, args.write_db, "prospective", check_published=True)
    elif args.command == "backfill":
        text = subprocess.run(["git", "show", f"{args.page_ref}:web/src/data/game-leaders.json"], capture_output=True, text=True,
                              check=True, cwd=ROOT, encoding="utf-8").stdout
        published = json.loads(text)
        games = [x for x in published["games"] if not args.game or x["game"]["game_id"] in args.game]
        # Discovery games were published under v3 or before this test existed; their saved
        # forecasts are re-created with the current model, so the published shares are not checked.
        run_games(published, games, args.write_db, "discovery", check_published=False)
    else:
        conn = _connect()
        cur = conn.cursor()
        stored, kickoffs, weeks, frame = load_grade_inputs(cur, REGISTRATION["season"])
        conn.rollback()
        report = grade(stored, kickoffs, weeks, frame, "discovery" if args.discovery else "prospective")
        report["evaluated_at"] = datetime.now(timezone.utc).isoformat()
        if args.write_db and not args.discovery:
            write_grade(conn, report)
        conn.close()
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(report, indent=1, default=str))
        print(json.dumps({m: {k: v for k, v in d.items() if k != "games_detail"} for m, d in report["markets"].items()}, indent=1, default=str))


if __name__ == "__main__":
    main()
