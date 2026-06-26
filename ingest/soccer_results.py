"""Settle soccer bets — the accountability half of the framework.

Fills final scores from The Odds API `/scores` endpoint, then settles bets:

  * **Group winner** — fully automated once all 6 group games are scored: compute
    standings (3/1/0, tiebreak on goal difference then goals for) and mark the
    winner 'won', the rest 'lost'.
  * **Outright winner** — settled manually when the champion is known
    (``--champion "Spain"``): that team 'won', everyone else 'lost'.
  * **First goal scorer** — auto-settled via TheSportsDB timeline API (free tier,
    API key "123").  Falls back to manual ``--first-scorer GAME_ID "Player Name"``
    if the auto-lookup fails for a specific game.

Every settlement stamps status + settled_at + result_detail on the locked ledger
row, so the backtest is reproducible.

Usage:
    python -m ingest.soccer_results                              # scores + auto group/firstscorer settle
    python -m ingest.soccer_results --champion "Spain"
    python -m ingest.soccer_results --first-scorer <game_id> "Lionel Messi"
    python -m ingest.soccer_results --no-auto-first-scorer      # skip TheSportsDB lookup
"""

from __future__ import annotations

import argparse
import logging
import os
import unicodedata
from datetime import datetime, timedelta, timezone

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

SPORT_KEY = "soccer_fifa_world_cup"
ODDS_BASE = "https://api.the-odds-api.com/v4"
TSDB_BASE = "https://www.thesportsdb.com/api/v1/json"


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

    # Minimum time after kickoff before we trust a "completed" score.
    # Covers full 90 min + stoppage + 30 min ET + 15 min penalties + publish lag.
    _MIN_ELAPSED = timedelta(hours=3)
    now = datetime.now(timezone.utc)

    updated = 0
    corrected = 0
    skipped_live = 0
    for ev in events:
        if not ev.get("completed"):
            continue
        # Guard: skip if the game hasn't had enough time to actually finish.
        commence_str = ev.get("commence_time", "")
        if commence_str:
            try:
                commence_dt = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
                if now - commence_dt < _MIN_ELAPSED:
                    skipped_live += 1
                    continue
            except ValueError:
                pass
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
        ev_id = ev.get("id")
        prev = db.execute_one(
            "SELECT home_score, away_score FROM soccer_matchups WHERE game_id = %s", (ev_id,)
        )
        if prev is None:
            continue
        if (prev["home_score"], prev["away_score"]) == (hs, as_):
            continue  # already correct
        # Write the completed-game score, correcting any stale/in-progress value
        # that got frozen earlier (the old NULL-only guard could never fix a wrong
        # score — e.g. a game polled at 0-0 while live).
        db.execute(
            "UPDATE soccer_matchups SET home_score = %s, away_score = %s WHERE game_id = %s",
            (hs, as_, ev_id),
        )
        updated += 1
        # If we corrected a previously-recorded (non-NULL) score, reopen this
        # game's settled bets so the next settle pass re-grades against the truth.
        if prev["home_score"] is not None or prev["away_score"] is not None:
            db.execute(
                "UPDATE soccer_bets SET status = 'pending', settled_at = NULL, result_detail = NULL "
                "WHERE scope = %s AND status IN ('won', 'lost', 'void')",
                (ev_id,),
            )
            corrected += 1
    parts = [f"{updated} matches updated"]
    if corrected:
        parts.append(f"{corrected} score corrections -> bets reopened")
    if skipped_live:
        parts.append(f"{skipped_live} skipped (kickoff < {_MIN_ELAPSED} ago)")
    print(f"Scores: {', '.join(parts)}")
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
        if len(members) != 4:
            continue  # only settle complete 4-team groups
        # Intra-group matches with scores, earliest first.  We take only the first
        # 6 (the round-robin) so a later knockout rematch between two group-mates
        # can't corrupt the group standings.
        rows = db.execute(
            """
            SELECT home_team_id, away_team_id, home_score, away_score
            FROM soccer_matchups
            WHERE home_team_id = ANY(%s) AND away_team_id = ANY(%s)
              AND home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY commence_time ASC NULLS LAST
            """,
            (members, members),
        )
        # A 4-team round-robin is 6 games; require them all before settling.
        if len(rows) < 6:
            continue
        rows = rows[:6]
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


def settle_game_bets(db: DatabaseManager) -> int:
    """Settle moneyline + totals bets for matches that now have a final score."""
    games = db.execute(
        """
        SELECT sm.game_id, sm.home_team_id, sm.away_team_id, sm.home_score, sm.away_score
        FROM soccer_matchups sm
        WHERE sm.game_id IS NOT NULL
          AND sm.home_score IS NOT NULL AND sm.away_score IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM soccer_bets b
            WHERE b.scope = sm.game_id AND b.status = 'pending'
              AND b.bet_type IN ('moneyline', 'total')
          )
        """,
    )
    settled = 0
    for g in games:
        gid = g["game_id"]
        hs, as_ = int(g["home_score"]), int(g["away_score"])
        total = hs + as_
        # Winning moneyline side.
        if hs > as_:
            ml_winner = "home"
        elif hs < as_:
            ml_winner = "away"
        else:
            ml_winner = "draw"

        bets = db.execute(
            "SELECT id, bet_type, selection_label, subject_team_id, inputs_json "
            "FROM soccer_bets WHERE scope = %s AND status = 'pending' "
            "AND bet_type IN ('moneyline', 'total')",
            (gid,),
        )
        for b in bets:
            status = None
            detail = f"Final {hs}-{as_}"
            if b["bet_type"] == "moneyline":
                side = (b["inputs_json"] or {}).get("side")
                status = "won" if side == ml_winner else "lost"
            else:  # total
                line = (b["inputs_json"] or {}).get("line")
                if line is None:
                    continue
                is_over = b["selection_label"].lower().startswith("over")
                if total == line:
                    status = "void"
                elif (total > line) == is_over:
                    status = "won"
                else:
                    status = "lost"
            db.execute(
                "UPDATE soccer_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
                (status, detail, b["id"]),
            )
            settled += 1
    if settled:
        print(f"Game bets: {settled} moneyline/total bets settled")
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


def _tsdb_find_event(game_date: str, home_name: str, away_name: str, api_key: str) -> str | None:
    """Return TheSportsDB idEvent for a match, matched by date + normalized team names."""
    try:
        r = requests.get(
            f"{TSDB_BASE}/{api_key}/eventsday.php",
            params={"d": game_date, "s": "Soccer"},
            timeout=15,
        )
        r.raise_for_status()
        events = r.json().get("events") or []
    except requests.RequestException as e:
        logger.warning("TheSportsDB eventsday failed for %s: %s", game_date, e)
        return None

    hn, an = _norm(home_name), _norm(away_name)
    for ev in events:
        if _norm(ev.get("strHomeTeam", "")) == hn and _norm(ev.get("strAwayTeam", "")) == an:
            return ev.get("idEvent")
    # Substring fallback for minor name divergence (e.g. "Côte d'Ivoire" vs "Ivory Coast")
    for ev in events:
        eh = _norm(ev.get("strHomeTeam", ""))
        ea = _norm(ev.get("strAwayTeam", ""))
        if (hn in eh or eh in hn) and (an in ea or ea in an):
            return ev.get("idEvent")
    return None


def _tsdb_first_scorer(tsdb_event_id: str, api_key: str) -> str | None:
    """Return the first goal scorer's name from a TheSportsDB timeline, or None if no goals."""
    try:
        r = requests.get(
            f"{TSDB_BASE}/{api_key}/lookuptimeline.php",
            params={"id": tsdb_event_id},
            timeout=15,
        )
        r.raise_for_status()
        timeline = r.json().get("timeline") or []
    except requests.RequestException as e:
        logger.warning("TheSportsDB lookuptimeline failed for event %s: %s", tsdb_event_id, e)
        return None

    goals = [e for e in timeline if e.get("strTimeline") == "Goal"]
    if not goals:
        return None
    goals.sort(key=lambda x: int(x.get("intTime") or 999))
    return goals[0].get("strPlayer")


def settle_first_scorer_auto(db: DatabaseManager, tsdb_api_key: str = "123") -> int:
    """Auto-settle first-scorer bets for completed games via TheSportsDB timeline API.

    Looks up each completed game that still has pending first-scorer bets, fetches
    the goal timeline, and calls ``settle_first_scorer`` with the actual first scorer.
    Games with no goals settle all selections as 'void'.
    """
    games = db.execute(
        """
        SELECT sm.game_id, sm.game_date, sm.home_score, sm.away_score,
               ht.name AS home_name, at.name AS away_name
        FROM soccer_matchups sm
        JOIN soccer_teams ht ON ht.team_id = sm.home_team_id
        JOIN soccer_teams at ON at.team_id = sm.away_team_id
        WHERE sm.home_score IS NOT NULL AND sm.away_score IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM soccer_bets b
            WHERE b.scope = sm.game_id AND b.status = 'pending'
              AND b.bet_type = 'first_scorer'
          )
        ORDER BY sm.game_date
        """
    )
    if not games:
        return 0

    total_settled = 0
    for g in games:
        game_date = str(g["game_date"])[:10]
        game_id = g["game_id"]
        home_name = g["home_name"]
        away_name = g["away_name"]

        tsdb_id = _tsdb_find_event(game_date, home_name, away_name, tsdb_api_key)
        if not tsdb_id:
            logger.warning(
                "TheSportsDB: no event found for %s vs %s on %s — skipping auto-settle",
                home_name, away_name, game_date,
            )
            continue

        scorer = _tsdb_first_scorer(tsdb_id, tsdb_api_key)
        total_goals = (g["home_score"] or 0) + (g["away_score"] or 0)
        if scorer is None and total_goals > 0:
            # TheSportsDB had no timeline for a game that clearly had goals — this
            # is a DATA GAP, not a 0-0.  Voiding here would wrongly wipe real
            # win/loss outcomes (and corrupt the backtest), so leave the bets
            # pending for manual `--first-scorer` settlement.
            logger.warning(
                "TheSportsDB: no goal timeline for %s vs %s (final %d-%d) — "
                "leaving first-scorer bets pending for manual settle",
                home_name, away_name, g["home_score"], g["away_score"],
            )
            continue
        if scorer is None:
            # Genuine 0-0 → all first-scorer selections are void.
            # Defensive: reaching here with goals on the board means the data-gap
            # guard above was bypassed (e.g. a future refactor). Fail SAFE — log
            # loudly and skip rather than crash the batch or resurrect the
            # wrong-void bug that silently wiped real results.
            if total_goals != 0:
                logger.error(
                    "BUG: void path reached for %s vs %s with %d goals — skipping "
                    "(would have wrongly voided real outcomes)",
                    home_name, away_name, total_goals,
                )
                continue
            bets = db.execute(
                "SELECT id FROM soccer_bets "
                "WHERE bet_type = 'first_scorer' AND scope = %s AND status = 'pending'",
                (game_id,),
            )
            for b in bets:
                db.execute(
                    "UPDATE soccer_bets SET status = 'void', settled_at = NOW(), "
                    "result_detail = 'No goals scored' WHERE id = %s",
                    (b["id"],),
                )
            n = len(bets)
            if n:
                print(f"First scorer ({game_id} {home_name} vs {away_name}): no goals, {n} bets voided")
            total_settled += n
        else:
            logger.info("TheSportsDB: first scorer for %s = %s", game_id, scorer)
            n = settle_first_scorer(db, game_id, scorer)
            total_settled += n

    return total_settled


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

    # Identify the winning bet.  Try exact normalized match first; if none, fall
    # back to a last-name/substring match so "Mbappe" settles "Kylian Mbappe".
    # Only accept the fallback when it is UNAMBIGUOUS (exactly one candidate).
    def _matches(label: str) -> bool:
        return _norm(label) == target

    def _loose(label: str) -> bool:
        nl = _norm(label)
        tt = set(target.split())
        lt = set(nl.split())
        if not tt:
            return False
        # Token-subset either way handles a short feed name vs a long market name:
        # "luis romo" ⊆ "luis francisco romo barron".  Plus surname fallback.
        return (tt <= lt or lt <= tt
                or target in nl.split() or nl.endswith(" " + target)
                or target.split()[-1] in lt)

    winner_id = None
    exact = [b for b in bets if _matches(b["selection_label"])]
    if len(exact) == 1:
        winner_id = exact[0]["id"]
    elif not exact:
        loose = [b for b in bets if _loose(b["selection_label"])]
        if len(loose) == 1:
            winner_id = loose[0]["id"]
        elif len(loose) > 1:
            logger.warning("Ambiguous scorer '%s' for %s (%d candidates) — enter the full name",
                           scorer_name, game_id, len(loose))
            print(f"First scorer ({game_id}): ambiguous '{scorer_name}', nothing settled")
            return 0

    settled = 0
    for b in bets:
        won = b["id"] == winner_id
        db.execute(
            "UPDATE soccer_bets SET status = %s, settled_at = NOW(), result_detail = %s WHERE id = %s",
            ("won" if won else "lost", f"First scorer: {scorer_name}", b["id"]),
        )
        settled += 1
    if winner_id is None:
        # "No goalscorer" or a scorer not in our pool → all listed selections lost (correct).
        logger.info("Scorer '%s' not among listed selections for %s — all marked lost", scorer_name, game_id)
    print(f"First scorer ({game_id}): {settled} bets settled, winner found={winner_id is not None}")
    return settled


def check_settlement_health(db: DatabaseManager, stale_days: int = 2, annotate: bool = False) -> int:
    """Surface settlement problems loudly so silent mis-settlement can't rot.

    A wrong settlement (or a game stuck unsettled) is invisible until someone
    eyeballs a dashboard — exactly how the wrong-void bug went unnoticed. This
    check runs every settlement pass and reports two failure classes:

      1. IMPOSSIBLE STATE — first-scorer bets voided 'No goals scored' on a game
         that actually had goals. The void-guard makes this unreachable now; if a
         row ever shows up here, a settlement path regressed — reopen & re-settle.
      2. STALE PENDING — first-scorer bets still pending more than `stale_days`
         after a completed game that had goals. TheSportsDB timelines are
         eventually-consistent, so a short lag is normal and self-heals on the next
         cron run; anything past the window is a genuine data hole that needs a
         manual `--first-scorer`. This is the manual-settle queue.

    When `annotate` is set and we're running under GitHub Actions, each issue is
    also emitted as a `::warning::` workflow annotation so it surfaces in the
    Actions run summary instead of being buried in step logs.

    Returns the number of distinct problems found (0 = healthy).
    """
    problems = 0
    gh_annotate = annotate and os.environ.get("GITHUB_ACTIONS") == "true"

    def _warn(msg: str) -> None:
        if gh_annotate:
            # GitHub parses ::warning:: lines from stdout into run annotations.
            print(f"::warning::{msg}")

    bad_voids = db.execute(
        """
        SELECT b.scope, ht.name AS h, at.name AS a,
               sm.home_score AS hs, sm.away_score AS as_, COUNT(*) AS n
        FROM soccer_bets b
        JOIN soccer_matchups sm ON sm.game_id = b.scope
        JOIN soccer_teams ht ON ht.team_id = sm.home_team_id
        JOIN soccer_teams at ON at.team_id = sm.away_team_id
        WHERE b.bet_type = 'first_scorer' AND b.status = 'void'
          AND b.result_detail = 'No goals scored'
          AND (sm.home_score + sm.away_score) > 0
        GROUP BY b.scope, ht.name, at.name, sm.home_score, sm.away_score
        ORDER BY ht.name
        """
    )
    if bad_voids:
        problems += len(bad_voids)
        print("\n  [!] IMPOSSIBLE STATE — first-scorer bets voided 'No goals' on games WITH goals:")
        for r in bad_voids:
            print(f"      {r['h']} {r['hs']}-{r['as_']} {r['a']}  ({r['n']} bets) — reopen & re-settle")
            _warn(f"Settlement: {r['h']} {r['hs']}-{r['as_']} {r['a']} voided 'No goals' but had goals "
                  f"({r['n']} first-scorer bets) — reopen & re-settle")

    stale = db.execute(
        """
        SELECT b.scope, ht.name AS h, at.name AS a,
               sm.home_score AS hs, sm.away_score AS as_,
               COALESCE(sm.commence_time, sm.game_date::timestamptz) AS kickoff,
               COUNT(*) AS n
        FROM soccer_bets b
        JOIN soccer_matchups sm ON sm.game_id = b.scope
        JOIN soccer_teams ht ON ht.team_id = sm.home_team_id
        JOIN soccer_teams at ON at.team_id = sm.away_team_id
        WHERE b.bet_type = 'first_scorer' AND b.status = 'pending'
          AND sm.home_score IS NOT NULL AND (sm.home_score + sm.away_score) > 0
          AND COALESCE(sm.commence_time, sm.game_date::timestamptz)
              < NOW() - (%s || ' days')::interval
        GROUP BY b.scope, ht.name, at.name, sm.home_score, sm.away_score, kickoff
        ORDER BY kickoff
        """,
        (str(stale_days),),
    )
    if stale:
        problems += len(stale)
        print(f"\n  [!] STALE — first-scorer bets pending > {stale_days}d after a completed game with goals")
        print("      (TheSportsDB never produced a usable timeline — settle manually):")
        for r in stale:
            print(f"      python -m ingest.soccer_results --first-scorer {r['scope']} \"<player>\""
                  f"   # {r['h']} {r['hs']}-{r['as_']} {r['a']}")
            _warn(f"Settlement: {r['h']} {r['hs']}-{r['as_']} {r['a']} has {r['n']} first-scorer bets "
                  f"stuck pending >{stale_days}d — settle manually "
                  f"(python -m ingest.soccer_results --first-scorer {r['scope']} \"<player>\")")

    if problems == 0:
        print("Settlement health: OK (no impossible-state voids, no stale pending first-scorer bets)")
    return problems


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Settle soccer bets")
    parser.add_argument("--days-from", type=int, default=3, help="Scores look-back window")
    parser.add_argument("--champion", help="Settle outright winner with this champion name")
    parser.add_argument("--first-scorer", nargs=2, metavar=("GAME_ID", "PLAYER"),
                        help="Manually settle first-scorer for a game")
    parser.add_argument("--no-auto-first-scorer", action="store_true",
                        help="Skip automatic TheSportsDB first-scorer settlement")
    parser.add_argument("--health-check", action="store_true",
                        help="Only run the settlement health check (no fetch/settle); "
                             "emits GitHub annotations under Actions")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    tsdb_key = os.getenv("THESPORTSDB_API_KEY", "123")

    if args.health_check:
        # Dedicated, standalone pass — used by the workflow's own step so issues
        # surface as run annotations even if an earlier settle step failed.
        check_settlement_health(db, annotate=True)
        raise SystemExit(0)

    fetch_scores(db, config.odds_api.api_key, args.days_from)
    settle_game_bets(db)
    settle_group_winners(db)
    if not args.no_auto_first_scorer:
        settle_first_scorer_auto(db, tsdb_key)
    if args.champion:
        settle_outright(db, args.champion)
    if args.first_scorer:
        settle_first_scorer(db, args.first_scorer[0], args.first_scorer[1])

    # Loud, last so it isn't buried — settlement health is the headline of every run.
    check_settlement_health(db)
