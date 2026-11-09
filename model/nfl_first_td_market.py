"""NFL first touchdown scorer against DraftKings' first-TD prices (``nfl-first-td-market-v1``).

Descriptive; nothing here changes a forecast. For every graded pregame forecast (model/nfl_first_td.py)
it stores, in ``nfl_first_td_market_grades``:
  - each of our players DraftKings priced, with our chance and DraftKings' last price before kickoff
    (``player_1st_td``, captured by capture_nfl_props.yml), so a flat one-unit bet on every player we
    price above DraftKings can be scored at any edge threshold;
  - where the actual scorer sat in our full distribution (rank and chance), and his position group;
  - which team received the opening kickoff, whether its opening drive scored a touchdown, and
    whether it scored the game's first touchdown.

The page reads the summary (``summarize``) and flags a player whose chance is more than
MARKET_FLAG_RATIO x DraftKings' implied chance: in week 9 our biggest disagreements were players
whose role the book knew was gone (Emanuel Wilson 14.9% vs +5000), so a large gap is shown as a
warning, not as value.

    python -m model.nfl_first_td_market grade
    python -m model.nfl_first_td_market report
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import timedelta

from config import load_config
from db.database import DatabaseManager
from model import cfb_first_td as cf

MARKET_VERSION = "nfl-first-td-market-v1"
MARKET = "player_1st_td"
BOOK = "draftkings"
MARKET_FLAG_RATIO = 3.0
EDGE_THRESHOLDS = (0.0, 0.10, 0.20)
PRICE_WINDOW_HOURS = 48  # captures older than this before kickoff are not "the" pregame price
POSITION_GROUP = {"RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE", "QB": "QB"}

DDL = [
    """CREATE TABLE IF NOT EXISTS nfl_first_td_market_grades (
        run_id BIGINT NOT NULL, game_id TEXT NOT NULL, version TEXT NOT NULL,
        market_version TEXT NOT NULL, season INTEGER NOT NULL, week INTEGER, kickoff TIMESTAMPTZ NOT NULL,
        dk_captured_at TIMESTAMPTZ, dk_players INTEGER NOT NULL, priced JSONB NOT NULL,
        outcome TEXT NOT NULL, scorer TEXT, scorer_rank INTEGER, scorer_prob DOUBLE PRECISION,
        scorer_group TEXT, receiving_team TEXT, opening_drive_td BOOLEAN, receiving_scored_first BOOLEAN,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (run_id, game_id))""",
]


# ── pure ─────────────────────────────────────────────────────────────────────
def american_to_decimal(price: float) -> float:
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def match_prices(names: dict[str, str], prices: dict[str, float]) -> dict[str, float]:
    """Our player ids -> DraftKings price, by (first initial, surname); a surname alone when unique on
    both sides (the same rule as the availability filter)."""
    by_key: dict[tuple[str, str], float] = {}
    surnames: dict[str, list[float]] = defaultdict(list)
    for raw, price in prices.items():
        key = cf.name_key(raw)
        if key:
            by_key[key] = price
            surnames[key[1]].append(price)
    ours: dict[str, int] = defaultdict(int)
    for name in names.values():
        key = cf.name_key(name)
        if key:
            ours[key[1]] += 1
    out = {}
    for pid, name in names.items():
        key = cf.name_key(name)
        if not key:
            continue
        if key in by_key:
            out[pid] = by_key[key]
        elif len(surnames.get(key[1], [])) == 1 and ours[key[1]] == 1:
            out[pid] = surnames[key[1]][0]
    return out


def player_probs(dist: dict) -> dict[str, float]:
    return {k: float(v) for k, v in dist.items() if not k.startswith("other") and k != "non_offense"}


def grade_game(dist: dict, names: dict[str, str], prices: dict[str, float], lab: dict,
               positions: dict[str, str], opening: dict | None, home: str, away: str) -> dict:
    """One game's market grade from a stored distribution, DraftKings' prices and what happened."""
    probs = player_probs(dist)
    matched = match_prices({pid: names.get(pid, pid) for pid in probs}, prices)
    scorer = lab.get("pid") if lab["outcome"] == "offense" else None
    priced = sorted(({"pid": pid, "name": names.get(pid, pid), "prob": probs[pid], "american": matched[pid],
                      "won": pid == scorer} for pid in matched), key=lambda r: -r["prob"])
    ranked = sorted(probs, key=lambda pid: -probs[pid])
    rank = ranked.index(scorer) + 1 if scorer in probs else None
    if lab["outcome"] == "non_offense":
        group = "return_or_defense"
    elif scorer:
        group = POSITION_GROUP.get(positions.get(scorer, ""), "other")
    else:
        group = None
    receiving = opening["receiving"] if opening else None
    side_team = {"home": home, "away": away}.get(lab.get("side"))
    return {
        "priced": priced, "outcome": lab["outcome"], "scorer": scorer, "scorer_rank": rank,
        "scorer_prob": probs.get(scorer) if scorer in probs else None, "scorer_group": group,
        "receiving_team": receiving, "opening_drive_td": opening["td"] if opening else None,
        "receiving_scored_first": (side_team == receiving) if receiving and side_team else None,
    }


def bets(priced: list[dict], threshold: float) -> list[dict]:
    return [p for p in priced if p["prob"] * american_to_decimal(p["american"]) - 1 > threshold]


def summarize(rows: list[dict]) -> dict:
    """Rows of grade_game output (with dk_players): the page's market and tracking summary."""
    out: dict = {"version": MARKET_VERSION, "games": len(rows),
                 "priced_games": sum(bool(r["priced"]) for r in rows), "bets": []}
    for t in EDGE_THRESHOLDS:
        picks = [b for r in rows for b in bets(r["priced"], t)]
        wins = [b for b in picks if b["won"]]
        units = sum(american_to_decimal(b["american"]) for b in wins) - len(picks)
        out["bets"].append({"edge_over": t, "bets": len(picks), "wins": len(wins), "units": round(units, 2),
                            "roi": round(units / len(picks), 4) if picks else None})
    ranks = [r["scorer_rank"] for r in rows if r["outcome"] == "offense"]
    out["scorer_rank"] = {"games": len(ranks), "top1": sum(x == 1 for x in ranks if x),
                          "top3": sum(bool(x and x <= 3) for x in ranks),
                          "top10": sum(bool(x and x <= 10) for x in ranks),
                          "outside": sum(x is None or x > 10 for x in ranks)}
    groups: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["scorer_group"]:
            groups[r["scorer_group"]] += 1
    out["scorer_groups"] = dict(sorted(groups.items(), key=lambda kv: -kv[1]))
    known = [r for r in rows if r["receiving_scored_first"] is not None]
    drive = [r for r in known if r["opening_drive_td"] is not None]
    out["opening"] = {"games": len(known), "receiving_scored_first": sum(r["receiving_scored_first"] for r in known),
                      "opening_drive_td": sum(bool(r["opening_drive_td"]) for r in drive),
                      "receiving_first_without_opening_td": sum(r["receiving_scored_first"] for r in drive
                                                                if not r["opening_drive_td"]),
                      "games_without_opening_td": sum(not r["opening_drive_td"] for r in drive)}
    return out


def market_chip(prob: float, american: float | None) -> dict | None:
    """The page warning: we rate a player far above DraftKings, which usually means the book knows something."""
    if american is None:
        return None
    implied = 1 / american_to_decimal(american)
    if prob <= MARKET_FLAG_RATIO * implied:
        return None
    price = f"+{int(american)}" if american > 0 else str(int(american))
    return {"code": "MARKET_FAR_LOWER", "label": f"DK {price} ({implied:.0%}): book rates him far lower"}


# ── data ─────────────────────────────────────────────────────────────────────
def load_prices(db: DatabaseManager, home_name: str, away_name: str, kickoff, before) -> dict | None:
    """DraftKings' latest first-TD prices for one game captured before ``before`` (and before kickoff)."""
    cutoff = min(before, kickoff)
    rows = db.execute("""
        WITH ev AS (
            SELECT event_id, max(capture_key) AS capture_key FROM prop_odds_history
            WHERE sport = 'nfl' AND market = %s AND home_team_name = %s AND away_team_name = %s
              AND commence_time BETWEEN %s - INTERVAL '6 hours' AND %s + INTERVAL '6 hours'
              AND captured_at < %s AND captured_at >= %s AND books ? %s
            GROUP BY event_id ORDER BY max(capture_key) DESC LIMIT 1)
        SELECT p.player, p.captured_at, (p.books -> %s ->> 'yes')::float AS price FROM prop_odds_history p
        JOIN ev ON ev.event_id = p.event_id AND ev.capture_key = p.capture_key
        WHERE p.sport = 'nfl' AND p.market = %s AND p.books ? %s""",
        (MARKET, home_name, away_name, kickoff, kickoff, cutoff, kickoff - timedelta(hours=PRICE_WINDOW_HOURS),
         BOOK, BOOK, MARKET, BOOK))
    prices = {r["player"]: r["price"] for r in rows if r["price"] is not None}
    if not prices:
        return None
    return {"captured_at": rows[0]["captured_at"], "prices": prices}


def load_opening(db: DatabaseManager, game_ids: list[str]) -> dict[str, dict]:
    """Opening kickoff: the receiving team (the kickoff row's posteam) and whether that drive ended in a TD."""
    if not game_ids:
        return {}
    rows = db.execute("""
        SELECT DISTINCT ON (game_id) game_id, posteam, drive_result FROM nfl_pbp_archetypes
        WHERE play_type = 'kickoff' AND quarter = 1 AND game_id = ANY(%s)
        ORDER BY game_id, game_seconds_remaining DESC, play_id""", (game_ids,))
    return {r["game_id"]: {"receiving": r["posteam"], "td": r["drive_result"] == "Touchdown"} for r in rows}


def load_positions(db: DatabaseManager) -> dict[str, str]:
    return {r["gsis_id"]: r["position"] for r in db.execute("""
        SELECT DISTINCT ON (gsis_id) gsis_id, position FROM ff_players
        WHERE gsis_id IS NOT NULL ORDER BY gsis_id, season DESC""")}


def odds_names(db: DatabaseManager, game_ids: list[str]) -> dict[str, tuple[str, str]]:
    rows = db.execute("""
        SELECT g.nflverse_game_id AS game_id, ht.odds_api_name AS home, at.odds_api_name AS away
        FROM nfl_season_games g JOIN nfl_teams ht ON ht.team_id = g.home_team_id
        JOIN nfl_teams at ON at.team_id = g.away_team_id WHERE g.nflverse_game_id = ANY(%s)""", (game_ids,))
    return {r["game_id"]: (r["home"], r["away"]) for r in rows}


def all_names(db: DatabaseManager) -> dict[str, str]:
    from model import nfl_first_td as n
    names = {r["player_id"]: r["name"] for r in db.execute("""
        SELECT player_id, max(player_name) AS name FROM nfl_pbp_play_participants
        WHERE player_id IS NOT NULL AND role IN ('rusher', 'receiver', 'passer') GROUP BY player_id""")}
    names.update(n.display_names(db))
    return names


def grade(db: DatabaseManager) -> int:
    """Market-grade every graded forecast that has no market row yet (append-only)."""
    from model import nfl_first_td as n
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
    pending = db.execute("""
        SELECT g.run_id, g.game_id, g.version, g.season, g.week, g.kickoff, p.dist, p.home, p.away
        FROM nfl_first_td_grades g JOIN nfl_first_td_predictions p ON p.run_id = g.run_id AND p.game_id = g.game_id
        WHERE NOT EXISTS (SELECT 1 FROM nfl_first_td_market_grades m WHERE m.run_id = g.run_id AND m.game_id = g.game_id)""")
    if not pending:
        return 0
    ids = sorted({r["game_id"] for r in pending})
    games = {g["game_id"]: g for g in n.load_games(db) if g["game_id"] in ids}
    teams, opening, positions, names = odds_names(db, ids), load_opening(db, ids), load_positions(db), all_names(db)
    written = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for r in pending:
            g = games.get(r["game_id"])
            if not g:
                continue
            home_name, away_name = teams.get(r["game_id"], (None, None))
            px = load_prices(db, home_name, away_name, r["kickoff"], r["kickoff"]) if home_name else None
            row = grade_game(r["dist"], names, px["prices"] if px else {}, n.label(g), positions,
                             opening.get(r["game_id"]), r["home"], r["away"])
            cur.execute("""
                INSERT INTO nfl_first_td_market_grades (run_id, game_id, version, market_version, season, week, kickoff,
                    dk_captured_at, dk_players, priced, outcome, scorer, scorer_rank, scorer_prob, scorer_group,
                    receiving_team, opening_drive_td, receiving_scored_first)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                (r["run_id"], r["game_id"], r["version"], MARKET_VERSION, r["season"], r["week"], r["kickoff"],
                 px["captured_at"] if px else None, len(px["prices"]) if px else 0, json.dumps(row["priced"]),
                 row["outcome"], row["scorer"], row["scorer_rank"], row["scorer_prob"], row["scorer_group"],
                 row["receiving_team"], row["opening_drive_td"], row["receiving_scored_first"]))
            written += 1
    return written


def stored_rows(db: DatabaseManager, version: str) -> list[dict]:
    try:
        rows = db.execute("SELECT * FROM nfl_first_td_market_grades WHERE version = %s ORDER BY kickoff", (version,))
    except Exception:  # noqa: BLE001 -- table not created yet
        return []
    return [dict(r) for r in rows]


def main(argv: list[str] | None = None) -> int:
    from model import nfl_first_td as n
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("grade")
    rep = sub.add_parser("report")
    rep.add_argument("--version", default=n.VERSION_LINES)
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.cmd == "grade":
        print(f"{grade(db)} forecasts market-graded")
        return 0
    print(json.dumps(summarize(stored_rows(db, args.version)), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
