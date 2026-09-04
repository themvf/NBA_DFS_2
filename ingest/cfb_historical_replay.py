"""Bounded, cached 2026 missing-game historical replay. Never writes live ledgers."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone

from ingest.cfb_schedule import CFB_BOOKMAKERS, CFB_MARKETS, _team_cache, _normal_name
from ingest.game_odds_market import extract_game_markets, parse_iso, require_pregame_capture
from ingest.cfb_capture_audit import quote_issues
from ingest.cfb_movements import movements
from model.line_alerts import (_cfb_market_signals, _cfb_market_snapshot,
                              freeze_execution_price, _nfl_line_outcome, _nfl_line_clv)

VERSION = "2026-missing-6h-v1"


def resolve_candidates(candidates):
    """Collapse same-event/equal-price duplicates conservatively, never conflicts."""
    from copy import deepcopy
    if not candidates:
        return None
    first = candidates[0]
    def prices(event):
        return {b: {k:v for k,v in q.items() if k != "last_update"}
                for b,q in extract_game_markets(event)["books"].items()}
    for event in candidates[1:]:
        if any(event.get(k) != first.get(k) for k in
               ("id", "sport_key", "home_team", "away_team", "commence_time")) or prices(event) != prices(first):
            raise ValueError("Conflicting archive event identity or quote values")
    selected = deepcopy(first)
    # Never make a quote appear fresher by choosing the later duplicate timestamp.
    for book in selected.get("bookmakers", []):
        stamps = [b.get("last_update") for e in candidates for b in e.get("bookmakers", []) if b["key"] == book["key"]]
        book["last_update"] = min(stamps, key=parse_iso) if stamps and all(stamps) else None
    return selected


def request_times(game):
    kickoff = game["commence_time"]
    ticks = {datetime.fromtimestamp(int(kickoff.timestamp() // 300) * 300, timezone.utc)
             - timedelta(minutes=n) for n in range(5, 361, 5)}
    ticks.update(kickoff - timedelta(hours=n) for n in (24, 48))
    ticks.add(kickoff - timedelta(seconds=1))
    return ticks


def replay(game, history):
    history = sorted(history, key=lambda r: (r["captured_at"], r["history_id"]))
    result = {"matchup_id": game["id"], "game": game["label"], "origin": "historical_backtest",
              "version": VERSION, "captures": len(history), "signals": [], "close": None}
    if not history:
        result["missing_reason"] = "No identity-verified pregame archive observations"
        return result
    close = history[-1]
    lead = (game["commence_time"] - close["captured_at"]).total_seconds()
    close_ok = 0 < lead <= 600
    result["close"] = {"snapshot_at": str(close["captured_at"]), "lead_seconds": lead,
                       "quality": "near_close_proxy" if close_ok else "stale",
                       "spread": ( _cfb_market_snapshot(close["books"], "spread") or {}).get("line"),
                       "total": (_cfb_market_snapshot(close["books"], "total") or {}).get("line")}
    result["field_transitions"] = len(list(movements(history)))
    seen = set()
    for end in range(2, len(history) + 1):
        current = history[end-1]
        for signal in _cfb_market_signals(history[:end]):
            key = (signal["alert_type"], signal["side"])
            if key in seen:
                continue
            seen.add(key)
            details = {**signal["details"], "origin": "historical_backtest"}
            market, side = details["market"], signal["side"]
            price = freeze_execution_price(current["books"], market=market, side=side)
            entry = price.get("exec_line")
            # Grading helper expects a HOME spread even for an away selection.
            home_entry = -entry if entry is not None and market == "spread" and side == "away" else entry
            outcome = None
            if home_entry is not None and game["completed"] and game["home_score"] is not None and game["away_score"] is not None:
                outcome = _nfl_line_outcome(market, side, home_entry, game["home_score"], game["away_score"])
            book = price.get("exec_book")
            closing_quote = close["books"].get(book, {})
            closing_line = closing_quote.get("spread_home" if market == "spread" else "total_line")
            updated = parse_iso(closing_quote["last_update"]) if closing_quote.get("last_update") else None
            fresh_quote = updated is not None and 0 < (game["commence_time"]-updated).total_seconds() <= 900
            clv = _nfl_line_clv(market, side, home_entry, closing_line) if (
                close_ok and fresh_quote and home_entry is not None and closing_line is not None) else None
            result["signals"].append({"type": key[0], "side": side, "details": details,
                "price": price, "outcome": outcome, "same_book_line_clv_points": clv,
                "pnl_units": (price["exec_decimal"]-1 if outcome == "won" else -1 if outcome == "lost" else 0)
                    if outcome is not None and price.get("exec_decimal") else None})
    return result


def run(db, api_key, *, execute=False, max_credits=7500):
    from psycopg2.extras import Json
    import requests
    games = db.execute("""SELECT m.*,at.name||' @ '||ht.name AS label FROM cfb_matchups m
        JOIN cfb_teams ht ON ht.team_id=m.home_team_id JOIN cfb_teams at ON at.team_id=m.away_team_id
        WHERE m.season=2026 AND m.commence_time<NOW() AND NOT m.start_time_tbd
        AND NOT EXISTS (SELECT 1 FROM game_odds_history h WHERE h.sport='cfb' AND h.matchup_id=m.id)
        ORDER BY m.commence_time,m.id""")
    plan = {g["id"]: request_times(g) for g in games}
    stamps = sorted(set().union(*plan.values())) if games else []
    cached = {r["requested_at"]: r for r in db.execute("SELECT * FROM cfb_historical_archive WHERE profile=%s", (VERSION,))}
    spent = sum(r["credits"] for r in cached.values())
    uncertain = db.execute_one("""SELECT COALESCE(SUM(requests_last),0) AS n FROM odds_api_usage
        WHERE purpose='historical_backfill_attempt' AND response_status IS DISTINCT FROM 200
        AND metadata->>'profile'=%s""", (VERSION,))["n"]
    spent += uncertain
    todo = [s for s in stamps if s not in cached]
    print(json.dumps({"games": len(games), "requests": len(stamps), "uncached_requests": len(todo),
                      "estimated_additional_credits": len(todo)*30, "already_spent": spent}), flush=True)
    if not execute:
        return
    if spent + len(todo)*30 > max_credits:
        raise ValueError("Planned cost exceeds pilot credit cap; no requests made")
    aliases = _team_cache(db)
    session = requests.Session()
    for index, stamp in enumerate(todo):
        # No automatic retries: a timed-out request may already have consumed credits.
        attempt = db.execute_insert("""INSERT INTO odds_api_usage(sport,purpose,endpoint,event_count,
            markets,bookmakers,requests_last,metadata) VALUES ('cfb','historical_backfill_attempt',
            'historical/sports/americanfootball_ncaaf/odds',0,%s,%s,30,%s) RETURNING id""",
            (CFB_MARKETS, ",".join(CFB_BOOKMAKERS), Json({"profile": VERSION, "requested_at": str(stamp)})))
        try:
            response = session.get("https://api.the-odds-api.com/v4/historical/sports/americanfootball_ncaaf/odds",
                params={"apiKey": api_key, "bookmakers": ",".join(CFB_BOOKMAKERS), "markets": CFB_MARKETS,
                        "oddsFormat": "american", "date": stamp.strftime('%Y-%m-%dT%H:%M:%SZ')}, timeout=45)
        except requests.RequestException:
            raise RuntimeError("Historical request transport failure; stopped without retry; charge may be unknown") from None
        if response.status_code != 200:
            db.execute("UPDATE odds_api_usage SET response_status=%s WHERE id=%s", (response.status_code, attempt))
            raise RuntimeError(f"Historical request failed HTTP {response.status_code}; stopped without retry")
        payload = response.json()
        cost = int(response.headers.get("x-requests-last", 30))
        remaining = int(response.headers.get("x-requests-remaining", 0))
        db.execute("""INSERT INTO cfb_historical_archive(profile,requested_at,credits,remaining,payload)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT(profile,requested_at) DO NOTHING""",
            (VERSION, stamp, cost, remaining, Json(payload)))
        cached[stamp] = {"payload": payload}
        spent += cost
        db.execute("""UPDATE odds_api_usage SET event_count=%s,requests_last=%s,
            requests_remaining=%s,response_status=200 WHERE id=%s""",
            (len(payload.get("data", [])), cost, remaining, attempt))
        if index % 10 == 0 or index == len(todo)-1:
            print(json.dumps({"completed_requests": index+1, "of": len(todo), "credits_spent": spent,
                              "remaining": remaining}), flush=True)
        if spent + 30 > max_credits or remaining < 5000:
            raise RuntimeError("Credit safety limit reached; responses cached for safe resume")
    summaries = []
    for game in games:
        observations = {}
        event_ids = set()
        for stamp in sorted(plan[game["id"]]):
            payload = cached[stamp]["payload"]
            captured = parse_iso(payload["timestamp"])
            if captured > stamp:
                raise ValueError("Archive returned a snapshot after requested time")
            candidates = [e for e in payload.get("data", [])
                if aliases.get(_normal_name(e.get("home_team"))) == game["home_team_id"]
                and aliases.get(_normal_name(e.get("away_team"))) == game["away_team_id"]
                and abs((parse_iso(e["commence_time"])-game["commence_time"]).total_seconds()) <= 300]
            if not candidates:
                continue
            event = resolve_candidates(candidates)
            require_pregame_capture(event_commence=parse_iso(event["commence_time"]),
                                   stored_commence=game["commence_time"], captured_at=captured)
            books = extract_game_markets(event)["books"]
            if quote_issues(books, captured):
                raise ValueError("Invalid archived quote evidence")
            event_ids.add(event["id"])
            observations[captured] = {"history_id": captured.isoformat(), "capture_key": captured.isoformat(),
                                      "captured_at": captured, "books": books}
        if len(event_ids) > 1:
            raise ValueError("Provider event identity changed within replay")
        summary = replay(game, list(observations.values()))
        summary["event_ids"] = sorted(event_ids)
        db.execute("""INSERT INTO cfb_historical_replays(profile,matchup_id,result)
            VALUES (%s,%s,%s) ON CONFLICT(profile,matchup_id) DO UPDATE SET result=EXCLUDED.result,
            evaluated_at=NOW()""", (VERSION, game["id"], Json(summary)))
        summaries.append(summary)
    counts = Counter((s["type"], s["outcome"] or "ungraded") for g in summaries for s in g["signals"])
    print(json.dumps({"credits_spent": spent, "games": [{k:v for k,v in g.items() if k!='signals'} for g in summaries],
        "signals": [{"type": k[0], "outcome": k[1], "n": v} for k,v in sorted(counts.items())]}, indent=2), flush=True)


if __name__ == "__main__":
    from config import load_config
    from db.database import DatabaseManager
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-credits", type=int, default=7500)
    args = parser.parse_args()
    config = load_config()
    database = DatabaseManager(config.database_url)
    with database.reuse_connection():
        run(database, config.odds_api.api_key, execute=args.execute, max_credits=args.max_credits)
