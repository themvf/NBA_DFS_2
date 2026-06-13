"""Settle soccer bets — the accountability half of the framework.

Fills final scores from The Odds API `/scores` endpoint, then settles bets:

  * **Group winner** — fully automated once all 6 group games are scored: compute
    standings (3/1/0, tiebreak on goal difference then goals for) and mark the
    winner 'won', the rest 'lost'.
  * **Outright winner** — settled manually when the champion is known
    (``--champion "Spain"``): that team 'won', everyone else 'lost'.
  * **First goal scorer** — settled manually per game
    (``--first-scorer GAME_ID "Player Name"``): that player 'won', all other
    first-scorer selections for the game 'lost'.  (No free goal-events feed; a
    manual entry keeps settlement fully traceable.)

Every settlement stamps status + settled_at + result_detail on the locked ledger
row, so the backtest is reproducible.

Usage:
    python -m ingest.soccer_results                              # scores + auto group settle
    python -m ingest.soccer_results --champion "Spain"
    python -m ingest.soccer_results --first-scorer <game_id> "Lionel Messi"
"""

from __future__ import annotations

import argparse
import logging
import unicodedata

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

SPORT_KEY = "soccer_fifa_world_cup"
ODDS_BASE = "https://api.the-odds-api.com/v4"


def _norm(s: str) -> str:
    text = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return " ".join(text.lower().split())


def fetch_scores(db: DatabaseManager, api_key: str, days_from: int = 3) -> int:
    """Pull completed-match scores from the Odds API into soccer_matchups."""
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch scores")
        return 0
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/scores",
            params={"apiKey": api_key, "daysFrom": days_from, "dateFormat": "iso"},
            timeout=25,
        )
        r.raise_for_status()
        events = r.json()
    except requests.RequestException as e:
        logger.warning("Scores fetch failed: %s", e)
        return 0

    updated = 0
    for ev in events:
        if not ev.get("completed"):
            continue
        scores = ev.get("scores") or []
        if not scores:
            continue
        by_name = {_norm(s.get("name", "")): s.get("score") for s in scores}
        home = _norm(ev.get("home_team", ""))
        away = _norm(ev.get("away_team", ""))
        if home not in by_name or away not in by_name:
            continue
        try:
            hs, as_ = int(by_name[home]), int(by_name[away])
        except (TypeError, ValueError):
            continue
        res = db.execute_one(
            """
            UPDATE soccer_matchups SET home_score = %s, away_score = %s
            WHERE game_id = %s AND (home_score IS NULL OR away_score IS NULL)
            RETURNING id
            """,
            (hs, as_, ev.get("id")),
        )
        if res:
            updated += 1
    print(f"Scores: {updated} matches updated")
    return updated


def settle_group_winners(db: DatabaseManager) -> int:
    """Settle group-winner bets for groups whose 6 games are all scored."""
    groups = db.execute("SELECT team_id, group_label FROM soccer_groups")
    if not groups:
        return 0
    by_label: dict[str, list[int]] = {}
    for g in groups:
        by_label.setdefault(g["group_label"], []).append(g["team_id"])

    settled = 0
    for label, members in by_label.items():
        # All intra-group matches with scores.
        rows = db.execute(
            """
            SELECT home_team_id, away_team_id, home_score, away_score
            FROM soccer_matchups
            WHERE home_team_id = ANY(%s) AND away_team_id = ANY(%s)
              AND home_score IS NOT NULL AND away_score IS NOT NULL
            """,
            (members, members),
        )
        # A 4-team round-robin is 6 games; require them all before settling.
        if len(rows) < 6:
            continue
        pts = {t: 0 for t in members}
        gd = {t: 0 for t in members}
        gf = {t: 0 for t in members}
        for m in rows:
            h, a, hs, as_ = m["home_team_id"], m["away_team_id"], m["home_score"], m["away_score"]
            gd[h] += hs - as_; gd[a] += as_ - hs
            gf[h] += hs; gf[a] += as_
            if hs > as_:
                pts[h] += 3
            elif hs < as_:
                pts[a] += 3
            else:
                pts[h] += 1; pts[a] += 1
        winner = max(members, key=lambda t: (pts[t], gd[t], gf[t]))

        # Settle every group_winner bet for this group (all model versions).
        bets = db.execute(
            "SELECT id, subject_team_id FROM soccer_bets "
            "WHERE bet_type = 'group_winner' AND scope = %s AND status = 'pending'",
            (f"Group {label}",),
        )
        for b in bets:
            status = "won" if b["subject_team_id"] == winner else "lost"
            db.execute(
                "UPDATE soccer_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
                (status, f"Group {label} winner: team_id {winner}", b["id"]),
            )
            settled += 1
    if settled:
        print(f"Group winners: {settled} bets settled")
    return settled


def settle_outright(db: DatabaseManager, champion_name: str) -> int:
    """Settle all outright-winner bets given the champion's name."""
    team = db.execute_one(
        "SELECT team_id, name FROM soccer_teams WHERE lower(name) = lower(%s)", (champion_name,)
    )
    if not team:
        print(f"No soccer_teams match for champion '{champion_name}'")
        return 0
    bets = db.execute(
        "SELECT id, subject_team_id FROM soccer_bets "
        "WHERE bet_type = 'outright_winner' AND status = 'pending'"
    )
    settled = 0
    for b in bets:
        status = "won" if b["subject_team_id"] == team["team_id"] else "lost"
        db.execute(
            "UPDATE soccer_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
            (status, f"Champion: {team['name']}", b["id"]),
        )
        settled += 1
    print(f"Outright: {settled} bets settled (champion {team['name']})")
    return settled


def settle_first_scorer(db: DatabaseManager, game_id: str, scorer_name: str) -> int:
    """Settle first-scorer bets for one game given the actual first scorer."""
    target = _norm(scorer_name)
    bets = db.execute(
        "SELECT id, selection_label FROM soccer_bets "
        "WHERE bet_type = 'first_scorer' AND scope = %s AND status = 'pending'",
        (str(game_id),),
    )
    if not bets:
        print(f"No pending first-scorer bets for game {game_id}")
        return 0
    settled = 0
    matched = False
    for b in bets:
        won = _norm(b["selection_label"]) == target
        matched = matched or won
        db.execute(
            "UPDATE soccer_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
            ("won" if won else "lost", f"First scorer: {scorer_name}", b["id"]),
        )
        settled += 1
    if not matched:
        # "No goalscorer" or a player not in our pool → all listed selections lost (correct).
        logger.info("Scorer '%s' not among listed selections for %s — all marked lost", scorer_name, game_id)
    print(f"First scorer ({game_id}): {settled} bets settled, winner found={matched}")
    return settled


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Settle soccer bets")
    parser.add_argument("--days-from", type=int, default=3, help="Scores look-back window")
    parser.add_argument("--champion", help="Settle outright winner with this champion name")
    parser.add_argument("--first-scorer", nargs=2, metavar=("GAME_ID", "PLAYER"),
                        help="Settle first-scorer for a game")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    fetch_scores(db, config.odds_api.api_key, args.days_from)
    settle_group_winners(db)
    if args.champion:
        settle_outright(db, args.champion)
    if args.first_scorer:
        settle_first_scorer(db, args.first_scorer[0], args.first_scorer[1])
