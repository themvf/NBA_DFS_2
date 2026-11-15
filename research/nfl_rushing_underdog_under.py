"""Forward test: rushing-yards unders on underdogs of 3+ points (`nfl-rushing-underdog-under-v1`).

docs/nfl-rushing-underdog-under-study.md. Every player with a rushing-yards line whose team
is an underdog of 3 or more points, bet under at the listed US book with the highest line
(ties to the best price), 2026 games that kick off after the registration. Measurement
only: nothing is bet on it and no result changes a model or a display by itself.

    python -m research.nfl_rushing_underdog_under grade [--write-db] [--out path]
"""
from __future__ import annotations

import argparse
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from research import nfl_prop_under_study as under
from research import nfl_rushing_multibook as mb
from research import nfl_rushing_under as ru

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-rushing-underdog-under-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())


def underdog_by(game_id: str, team: str, home_spread: float | None) -> float | None:
    """Points the team is an underdog by (negative when favoured); `home_spread` positive = home favoured."""
    if home_spread is None:
        return None
    away, home = game_id.split("_")[2:4]
    if team == home:
        return -float(home_spread)
    return float(home_spread) if team == away else None


def pick(row: Mapping[str, Any], books: list[str]) -> tuple[str, float, float] | None:
    """(book, line, under price): the highest line among the listed books, ties to the best price."""
    quotes = [(q["line"], mb.decimal(q["under"]), q["under"], b) for b, q in row["books"].items() if b in books]
    if len(quotes) < 2:
        return None
    line, _, price, book = max(quotes)
    return book, line, price


def cohort_bets(rows: list[dict], teams: Mapping[tuple[str, str], str], spreads: Mapping[str, float | None],
                registration: Mapping[str, Any] = REGISTRATION) -> list[dict]:
    start = ru._ts(registration["registered_at"])
    first, last = registration["weeks"]
    bets = []
    for row in rows:
        if not first <= row["week"] <= last or ru._ts(row["kickoff"]) <= start:
            continue
        chosen = pick(row, registration["books"])
        team = teams.get((row["game_id"], row["key"]))
        # No stat row means no team to class the player by, so he is not bet (the parent rule voids him).
        if chosen is None or team is None or row["actual"] is None:
            continue
        dog = underdog_by(row["game_id"], team, spreads.get(row["game_id"]))
        if dog is None or dog < registration["underdog_points"]:
            continue
        book, line, price = chosen
        result, pnl = under.settle(line, row["actual"], price)
        bets.append({"game_id": row["game_id"], "week": row["week"], "player": row["player"], "team": team,
                     "underdog_by": dog, "book": book, "line": line, "under_price": price,
                     "actual": row["actual"], "result": result, "pnl": pnl, "void_reason": None})
    return bets


def summarize(bets: list[dict], final: bool, registration: Mapping[str, Any] = REGISTRATION) -> dict:
    primary = under.summarize(bets, registration=registration)
    floors = registration["floors"]
    met = primary["bets"] >= floors["graded_bets"] and primary["game_weeks"] >= floors["game_weeks"]
    books: dict[str, int] = {}
    for b in bets:
        books[b["book"]] = books.get(b["book"], 0) + 1
    return {"study": registration["study"], "primary": primary, "floors": floors, "floors_met": met,
            "verdict_if_first": ru.verdict(primary, met) if final else None,
            "status": "final" if final else "descriptive until the final look", "books": books,
            "by_week": {w: under.summarize([b for b in bets if b["week"] == w], registration, with_interval=False)
                        for w in sorted({b["week"] for b in bets})}}


def load() -> tuple[list[dict], dict, dict, bool]:
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    season = REGISTRATION["season"]
    rows = mb.load_season(season)
    conn = under._connect()
    cur = conn.cursor()
    cur.execute("""SELECT nflverse_game_id, week, kickoff, completed, market_spread_line, market_captured_at
                   FROM nfl_season_games WHERE season = %s AND game_type = 'REG' AND nflverse_game_id IS NOT NULL""",
                (season,))
    games = cur.fetchall()
    conn.rollback()
    conn.close()
    spreads = {g[0]: (float(g[4]) if g[4] is not None and g[5] is not None and g[5] < g[2] else None) for g in games}
    response = requests.get(NFLVERSE_WEEKLY_STATS_URL.format(season=season), timeout=120)
    response.raise_for_status()
    frame = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    teams = {(g, ru.normalize_name(n)): t
             for g, n, t in zip(frame["game_id"], frame["player_display_name"], frame["team"]) if isinstance(g, str)}
    start = ru._ts(REGISTRATION["registered_at"])
    first, last = REGISTRATION["weeks"]
    cohort = [g for g in games if first <= int(g[1]) <= last and ru._ts(g[2]) > start]
    graded = {r["game_id"] for r in rows}
    final = bool(cohort) and all(g[3] and g[0] in graded for g in cohort)
    return rows, teams, spreads, final


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("grade")
    g.add_argument("--out", type=Path)
    g.add_argument("--write-db", action="store_true")
    a = p.parse_args()
    if REGISTRATION.get("status") != "registered":
        raise SystemExit("DRAFT: the study is not registered")
    rows, teams, spreads, final = load()
    bets = cohort_bets(rows, teams, spreads)
    out = summarize(bets, final)
    out.update(evaluated_at=datetime.now(timezone.utc).isoformat(),
               registration_sha256=under.file_digest(REGISTRATION_PATH))
    full = {**out, "bets": bets}
    if a.write_db:
        out["frozen_verdict"] = ru.write_run(out, full)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(full, indent=1, default=str))
    print(json.dumps({k: v for k, v in out.items() if k != "by_week"}, indent=1, default=str))


if __name__ == "__main__":
    main()
