"""Build the shared, read-only dataset for the top-5-lineups study.

Every graded NBA night (Oct 20 onward, box scores complete) is replayed as it stood one
minute before the night's first tip (the contest lock): each game's latest line capture
before the lock, read with the same ladder reader the page uses, the 60 biggest lines kept.
Actual points follow the tracked grade's rule (no box-score row or did not play = 0;
ambiguous or unfinished = None). Also dumped: this season's box scores, last season's
player and team game logs, the nightly line grade rows, the injury report at the lock,
spreads/totals at the lock, and the tracked test's frozen entries.

Read-only: SELECTs only, no session settings, every connection rolled back.
Run from the repo root:  python <this file> <out_dir>
"""
from __future__ import annotations
import sys as _s, pathlib as _p; _s.path.insert(0, str(_p.Path(__file__).resolve().parents[0])); import _paths

import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


from dotenv import load_dotenv  # noqa: E402

_paths.load_env()

from config import load_config  # noqa: E402
from db.database import DatabaseManager, fetch_rows  # noqa: E402
from model.nba_points_ladder import player_key, team_key  # noqa: E402
import research.nba_wta_trios as W  # noqa: E402

OUT = Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)
DEEP = 60


def tk(name: str) -> str:
    k = team_key(name or "")
    return "losangelesclippers" if k == "laclippers" else k


def iso(x):
    return x.isoformat() if isinstance(x, (datetime, date)) else x


def q(db, sql, args=()):
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        rows = fetch_rows(cur)
        conn.rollback()
    return rows


def main() -> None:
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    W.POOL_SIZE = DEEP  # load_pool reads the module global at call time

    dates = [r["d"] for r in q(db, """SELECT DISTINCT game_date AS d FROM prop_odds_history
                                       WHERE sport = 'nba' AND market = 'player_points' ORDER BY 1""")]
    finished = {r["game_date"]: (r["n"], r["done"]) for r in q(db, """SELECT game_date, count(*) AS n,
                 count(*) FILTER (WHERE completed) AS done FROM nba_box_score_games GROUP BY 1""")}

    nights = []
    for d in dates:
        head = q(db, """SELECT min(commence_time) AS first_tip FROM prop_odds_history
                        WHERE sport = 'nba' AND game_date = %s AND market = 'player_points' AND commence_time IS NOT NULL""", (d,))[0]
        if head["first_tip"] is None:
            continue
        n, done = finished.get(d, (0, 0))
        events = q(db, """SELECT DISTINCT event_id, commence_time, home_team_name, away_team_name FROM prop_odds_history
                          WHERE sport = 'nba' AND game_date = %s AND market = 'player_points'""", (d,))
        complete = n > 0 and n == done and n >= len({e["event_id"] for e in events})
        lock = head["first_tip"] - timedelta(minutes=1)
        pool, meta = W.load_pool(db, d, lock)
        caps = {r["event_id"]: r["captured_at"] for r in q(db, """SELECT event_id, max(captured_at) AS captured_at
                     FROM prop_odds_history WHERE sport = 'nba' AND game_date = %s AND market = 'player_points'
                       AND captured_at <= %s GROUP BY 1""", (d, lock))}
        commence = {str(e["event_id"]): e["commence_time"] for e in events}

        # injury report observed before the lock that covers the date
        rep = q(db, """SELECT published_at, observed_at, states, report->'team_submissions' AS submissions
                       FROM nba_injury_observations
                       WHERE observed_at < %s AND published_at <= observed_at
                         AND EXISTS (SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date' = %s)
                       ORDER BY published_at DESC, observed_at DESC LIMIT 1""", (head["first_tip"], d.isoformat()))
        status, submitted, report_meta = {}, {}, None
        if rep:
            r0 = rep[0]
            report_meta = {"publishedAt": iso(r0["published_at"]), "observedAt": iso(r0["observed_at"]),
                           "leadHours": (head["first_tip"] - r0["observed_at"]).total_seconds() / 3600.0}
            states = r0["states"] if isinstance(r0["states"], list) else json.loads(r0["states"])
            subs = r0["submissions"] if isinstance(r0["submissions"], list) else json.loads(r0["submissions"] or "[]")
            for s in states:
                if s.get("game_date") == d.isoformat() and s.get("player_key") and s.get("status"):
                    status[(s["player_key"], s.get("home_key", ""), s.get("away_key", ""))] = s["status"]
            for s in subs:
                if s.get("game_date") == d.isoformat():
                    submitted[tk(str(s.get("team", "")))] = s.get("submission") == "submitted"

        # spreads / totals at the lock
        glines = {r["event_id"]: r for r in q(db, """SELECT DISTINCT ON (event_id) event_id, home_spread, total, captured_at, book_count
                     FROM nba_game_lines WHERE game_date = %s AND captured_at < %s ORDER BY event_id, captured_at DESC""",
                                              (d, head["first_tip"]))}

        # box scores for the night
        box = q(db, """SELECT g.espn_event_id, g.home_team, g.away_team, g.home_score, g.away_score, g.completed,
                              p.player, p.player_key, p.team, p.is_home, p.starter, p.played, p.dnp_reason, p.minutes,
                              p.points, p.fga, p.fta, p.fg3a
                       FROM nba_box_score_games g JOIN nba_box_score_players p ON p.espn_event_id = g.espn_event_id
                       WHERE g.game_date = %s""", (d,))
        by_game = defaultdict(list)
        for b in box:
            by_game[(tk(b["home_team"]), tk(b["away_team"]))].append(b)

        players = []
        for rank, p in enumerate(pool, start=1):
            rows = by_game.get((tk(p.home_team), tk(p.away_team)), [])
            hits = [b for b in rows if player_key(b["player"]) == p.key]
            final = bool(rows) and all(b["completed"] for b in rows)
            if not final or len(hits) > 1:
                actual, minutes, played, starter = None, None, None, None
            elif not hits or not hits[0]["played"]:
                actual, minutes, played, starter = 0.0, 0.0, False, bool(hits[0]["starter"]) if hits else None
            else:
                h = hits[0]
                actual, minutes, played, starter = float(h["points"] or 0.0), h["minutes"], True, bool(h["starter"])
            home_k, away_k = tk(p.home_team), tk(p.away_team)
            st = status.get((p.key, home_k, away_k)) if rep else None
            both = (submitted.get(home_k) is True and submitted.get(away_k) is True) if rep else False
            tip = commence.get(p.event_id)
            cap = caps.get(p.event_id)
            lead = (tip - cap).total_seconds() / 3600.0 if (tip and cap) else None
            gl = glines.get(p.event_id)
            players.append({
                "key": p.key, "name": p.player, "eventId": p.event_id, "home": p.home_team, "away": p.away_team,
                "team": p.team, "isHome": (p.team == p.home_team) if p.team else None, "lineRank": rank,
                "line": p.line, "pOver": p.p_over, "books": p.books, "ladderBooks": p.ladder_books, "curve": p.curve,
                "floor": p.floor, "ceiling": p.ceiling, "p30": p.p30, "p40": p.p40,
                "commence": iso(tip), "capturedAt": iso(cap), "leadHours": lead,
                "injuryStatus": st, "teamsSubmitted": both,
                "sitProbBooks": p.dnp_prob, "sitProbLead": W.sit_prob(p.books, lead),
                "sitProbInjury": W.sit_prob_injury(st, p.books, lead, report_meta["leadHours"] if report_meta else None, both),
                "homeSpread": gl["home_spread"] if gl else None, "gameTotal": gl["total"] if gl else None,
                "actual": actual, "minutes": minutes, "played": played, "starter": starter,
            })
        slate = []
        for (hk, ak), rows in by_game.items():
            for b in rows:
                if b["played"] and b["points"] is not None:
                    slate.append({"key": player_key(b["player"]), "name": b["player"], "team": b["team"],
                                  "home": rows[0]["home_team"], "away": rows[0]["away_team"],
                                  "points": float(b["points"]), "minutes": b["minutes"], "starter": bool(b["starter"]),
                                  "fga": b["fga"], "fta": b["fta"]})
        games = []
        for e in events:
            eid = str(e["event_id"])
            gl = glines.get(eid)
            bx = by_game.get((tk(e["home_team_name"]), tk(e["away_team_name"])), [])
            games.append({"eventId": eid, "home": e["home_team_name"], "away": e["away_team_name"], "commence": iso(e["commence_time"]),
                          "capturedAt": iso(caps.get(eid)), "homeSpread": gl["home_spread"] if gl else None,
                          "total": gl["total"] if gl else None,
                          "homeScore": bx[0]["home_score"] if bx else None, "awayScore": bx[0]["away_score"] if bx else None})
        v1 = q(db, """SELECT payload, created_at FROM nba_wta_portfolios WHERE version = 'nba-wta-trios-v1' AND game_date = %s
                      AND created_at < %s ORDER BY created_at DESC LIMIT 1""", (d, head["first_tip"]))
        v1e = None
        if v1:
            pl = v1[0]["payload"] if isinstance(v1[0]["payload"], dict) else json.loads(v1[0]["payload"])
            v1e = {"createdAt": iso(v1[0]["created_at"]), "entries": [e["players"] for e in pl["entries"]],
                   "baseline": [e["players"] for e in pl["baseline"]]}
        nights.append({"date": d.isoformat(), "firstTip": iso(head["first_tip"]), "lock": iso(lock), "complete": complete,
                       "gamesCount": len(events), "quoted": meta.get("quoted"), "duplicatesDropped": meta.get("duplicatesDropped"),
                       "injuryReport": report_meta, "games": games, "players": players, "slate": slate, "v1": v1e})
        unresolved = sum(1 for p in players[:20] if p["actual"] is None)
        print(d, "complete" if complete else "INCOMPLETE", "pool", len(players), "unresolved top20", unresolved,
              "report" if rep else "no-report", "lines" if glines else "no-lines", "v1" if v1e else "")

    (OUT / "nights.json").write_text(json.dumps(nights, default=iso), encoding="utf-8")

    box_all = q(db, """SELECT p.game_date, g.espn_event_id, g.home_team, g.away_team, g.start_time, p.player, p.player_key, p.team,
                             p.is_home, p.starter, p.played, p.dnp_reason, p.minutes, p.points, p.fgm, p.fga, p.fg3m, p.fg3a,
                             p.ftm, p.fta, p.rebounds, p.assists, p.turnovers, p.position, g.home_score, g.away_score
                      FROM nba_box_score_players p JOIN nba_box_score_games g ON g.espn_event_id = p.espn_event_id
                      ORDER BY p.game_date, g.espn_event_id""")
    (OUT / "box_2026_27.json").write_text(json.dumps(box_all, default=iso), encoding="utf-8")

    logs = q(db, """SELECT season_type, player_id, name, team_abbreviation, opponent_abbreviation, game_id, game_date, is_home,
                           win_loss, minutes, points, fgm, fga, fg3m, fg3a, ftm, fta, rebounds, assists, plus_minus
                    FROM nba_player_game_logs WHERE season = '2025-26' ORDER BY game_date, game_id""")
    (OUT / "player_logs_2025_26.json").write_text(json.dumps(logs, default=iso), encoding="utf-8")
    tlogs = q(db, """SELECT season_type, team_abbreviation, opponent_abbreviation, game_id, game_date, is_home, win_loss, pts, opp_pts,
                            fga, fta, oreb, tov, opp_fga, opp_fta, opp_oreb, opp_tov
                     FROM nba_team_game_logs WHERE season = '2025-26' ORDER BY game_date, game_id""")
    (OUT / "team_logs_2025_26.json").write_text(json.dumps(tlogs, default=iso), encoding="utf-8")

    cal = q(db, """SELECT game_date, event_id, player, player_key, team, commence_time, captured_at, minutes_before_tip, line, p_over,
                          books, floor, ceiling, p20, p25, p30, p35, p40, ladder_books, status, actual_points, minutes
                   FROM nba_points_calibration ORDER BY game_date""")
    (OUT / "calibration_rows.json").write_text(json.dumps(cal, default=iso), encoding="utf-8")
    print("box rows", len(box_all), "logs", len(logs), "team logs", len(tlogs), "calibration rows", len(cal))


if __name__ == "__main__":
    main()
