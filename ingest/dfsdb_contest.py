"""Import ONE dfsdb.com contest from a pasted link: ownership, payouts, who won.

dfsdb ("The DFS Record Book") archives public DraftKings results. Paste a
contest link and this pulls the contest card, the athlete table (ownership,
salary, actual points), the per-user standings, and, on request, the dfsdb
profiles of the users who finished on top and any of the contest's lineups in
dfsdb's top-lineups feed. Everything lands in the `dfsdb_*` tables; an NFL
showdown can additionally be mirrored into `nfl_dfs_field_*` so the ownership
calibration can use it.

Read `model/dfsdb_contest.py` first for what each part of the payload does and
does not represent (50-athlete cap, one standings row per USER, winnings pooled
across a user's entries, FLEX-only showdown ownership).

Terms of use, stated so nobody has to rediscover them: dfsdb's terms forbid
automated tools that "systematically access or download data from the site",
and its robots.txt disallows `/api/`. This tool is therefore ONE contest per
run, driven by a link a person pasted, a second between requests, identified
in its User-Agent, retried gently and never scheduled. The lineups feed is
global (not per contest), so `--lineups-pages` is capped; do not turn this
into a crawler. The built-in browser is served "Access denied" by dfsdb; plain
requests are not, as of 2026-10-10.

Usage:
    python -m ingest.dfsdb_contest https://www.dfsdb.com/contest/<uuid>
    python -m ingest.dfsdb_contest <link> --standings-pages 20      # top 2,000 users
    python -m ingest.dfsdb_contest <link> --all-standings            # every user (88k entries ~ 270 calls)
    python -m ingest.dfsdb_contest <link> --top-users 10             # profiles + history of the top 10
    python -m ingest.dfsdb_contest <link> --lineups-pages 10         # scan the top-lineups feed
    python -m ingest.dfsdb_contest <link> --mirror-field [--upload <slate-upload-id>]
    python -m ingest.dfsdb_contest <link> --dry-run                  # fetch + report, write nothing
"""

from __future__ import annotations

import argparse
import sys
import time

import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from model import dfsdb_contest as m

BASE = "https://www.dfsdb.com"
USER_AGENT = "Mozilla/5.0 NBADFS-v2 dfsdb-contest-import/1.0 (one contest per run, on request)"
MAX_LINEUP_PAGES = 50
LINEUP_PAGE_SIZE = 50
HISTORY_PAGE_SIZE = 50


class DfsdbError(RuntimeError):
    pass


class Client:
    """Polite JSON client: one request at a time, a pause between them, gentle retries."""

    def __init__(self, delay: float = 1.0, retries: int = 3, timeout: int = 60, session=None):
        self.delay, self.retries, self.timeout = delay, retries, timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        self.calls = 0
        self._last = 0.0

    def get(self, path: str, params: dict | None = None) -> dict:
        url = BASE + path
        for attempt in range(self.retries + 1):
            wait = self.delay - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            self.calls += 1
            response = self.session.get(url, params=params, timeout=self.timeout,
                                        headers={"Referer": BASE + "/contests"})
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    raise DfsdbError(f"{url}: not JSON ({exc})") from exc
            if response.status_code == 404:
                raise DfsdbError(f"{url}: not found on dfsdb")
            if response.status_code == 403:
                raise DfsdbError(f"{url}: dfsdb refused the request (403); it blocks some clients outright")
            if response.status_code == 400:
                raise DfsdbError(f"{url}: rejected ({response.text[:120]})")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.retries:
                time.sleep(self.delay * 2 ** (attempt + 1))
                continue
            raise DfsdbError(f"{url}: HTTP {response.status_code} after {attempt + 1} attempts")
        raise DfsdbError(f"{url}: gave up")  # pragma: no cover

    # -- endpoints --------------------------------------------------------

    def contest_page(self, contest_id: str, page: int, limit: int = m.STANDINGS_PAGE_LIMIT) -> dict:
        return self.get(f"/api/contest/{contest_id}",
                        {"page": page, "limit": limit, "sortBy": "rank", "sortOrder": "asc"})

    def user(self, user_id: str) -> dict:
        return self.get(f"/api/player/{user_id}", {"summary": "1"})

    def user_history(self, user_id: str, sport: str, page: int) -> dict:
        return self.get(f"/api/player/{user_id}",
                        {"history": "1", "sport": sport, "page": page, "limit": HISTORY_PAGE_SIZE,
                         "sort": "date", "dir": "desc"})

    def lineups(self, sport: str, year: int, page: int) -> dict:
        return self.get("/api/lineups", {"sport": sport, "year": year, "page": page, "limit": LINEUP_PAGE_SIZE})


# --------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------

def fetch_contest(client: Client, contest_id: str, *, pages: int, all_pages: bool) -> dict:
    """The contest payload plus as many standings pages as asked for."""
    first = client.contest_page(contest_id, 1)
    if not first.get("contest"):
        raise DfsdbError(f"contest {contest_id}: payload has no contest card")
    pagination = first.get("pagination") or {}
    total_pages = int(pagination.get("totalPages") or 0)
    total_users = pagination.get("totalCount")
    results = list(first.get("results") or [])
    want = total_pages if all_pages else min(pages, total_pages)
    for page in range(2, want + 1):
        chunk = client.contest_page(contest_id, page)
        rows = chunk.get("results") or []
        if not rows:
            break
        results.extend(rows)
    return {
        "payload": first,
        "results": results,
        "standings_total": total_users,
        "complete": want >= total_pages,
    }


def fetch_users(client: Client, user_ids: list[str], sport: str, history_pages: int) -> list[dict]:
    out = []
    for user_id in user_ids:
        payload = client.user(user_id)
        if not (payload.get("player") or {}).get("id"):
            continue
        history: list[dict] = []
        for page in range(1, history_pages + 1):
            chunk = client.user_history(user_id, sport, page)
            rows = chunk.get("history") or []
            history.extend(rows)
            pagination = chunk.get("pagination") or {}
            if chunk.get("restricted") or page >= int(pagination.get("totalPages") or 0):
                break
            if chunk.get("anonMaxPage") and page >= int(chunk["anonMaxPage"]):
                break
        out.append({"payload": payload, "history": history})
    return out


def fetch_lineups(client: Client, sport: str, year: int, pages: int) -> tuple[list[dict], str | None]:
    """Up to `pages` pages of the global top-lineups feed; stops on the first server error.

    dfsdb's NFL 2026 slice answered 503 on every try on 2026-10-10 while 2025
    and NBA answered, so a failure here is reported, not raised.
    """
    rows: list[dict] = []
    for page in range(1, min(pages, MAX_LINEUP_PAGES) + 1):
        try:
            chunk = client.lineups(sport, year, page)
        except DfsdbError as exc:
            return rows, str(exc)
        data = chunk.get("data") or []
        rows.extend(data)
        pagination = chunk.get("pagination") or {}
        if not data or page >= int(pagination.get("totalPages") or 0):
            break
    return rows, None


# --------------------------------------------------------------------------
# Persist
# --------------------------------------------------------------------------

def persist_contest(db: DatabaseManager, contest: dict, athletes: list[dict], standings: list[dict]) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO dfsdb_contests
                     (dfsdb_id, platform, sport, contest_name, contest_date, buy_in, prize_pool,
                      total_entries, contest_series, contest_type, contest_category, validation_status,
                      format, stats, payout_curve, standings_users, standings_fetched, standings_complete,
                      source_url, payload_digest, import_version)
                   VALUES (%(dfsdb_id)s, %(platform)s, %(sport)s, %(contest_name)s, %(contest_date)s,
                           %(buy_in)s, %(prize_pool)s, %(total_entries)s, %(contest_series)s,
                           %(contest_type)s, %(contest_category)s, %(validation_status)s, %(format)s,
                           %(stats)s, %(payout_curve)s, %(standings_users)s, %(standings_fetched)s,
                           %(standings_complete)s, %(source_url)s, %(payload_digest)s, %(import_version)s)
                   ON CONFLICT (dfsdb_id) DO UPDATE SET
                     platform = EXCLUDED.platform, sport = EXCLUDED.sport,
                     contest_name = EXCLUDED.contest_name, contest_date = EXCLUDED.contest_date,
                     buy_in = EXCLUDED.buy_in, prize_pool = EXCLUDED.prize_pool,
                     total_entries = EXCLUDED.total_entries, contest_series = EXCLUDED.contest_series,
                     contest_type = EXCLUDED.contest_type, contest_category = EXCLUDED.contest_category,
                     validation_status = EXCLUDED.validation_status, format = EXCLUDED.format,
                     stats = EXCLUDED.stats, payout_curve = EXCLUDED.payout_curve,
                     standings_users = EXCLUDED.standings_users,
                     standings_fetched = GREATEST(dfsdb_contests.standings_fetched, EXCLUDED.standings_fetched),
                     standings_complete = dfsdb_contests.standings_complete OR EXCLUDED.standings_complete,
                     source_url = EXCLUDED.source_url, payload_digest = EXCLUDED.payload_digest,
                     import_version = EXCLUDED.import_version, captured_at = NOW()""",
                {**contest, "stats": Json(contest["stats"]), "payout_curve": Json(contest["payout_curve"])})
            if athletes:
                execute_values(
                    cursor,
                    """INSERT INTO dfsdb_contest_athletes
                         (dfsdb_id, athlete_name, normalized_name, position, team, salary,
                          fantasy_points, ownership_pct)
                       VALUES %s
                       ON CONFLICT (dfsdb_id, normalized_name, team) DO UPDATE SET
                         athlete_name = EXCLUDED.athlete_name, position = EXCLUDED.position,
                         salary = EXCLUDED.salary, fantasy_points = EXCLUDED.fantasy_points,
                         ownership_pct = EXCLUDED.ownership_pct""",
                    [(a["dfsdb_id"], a["athlete_name"], a["normalized_name"], a["position"], a["team"],
                      a["salary"], a["fantasy_points"], a["ownership_pct"]) for a in athletes],
                    page_size=500)
            if standings:
                execute_values(
                    cursor,
                    """INSERT INTO dfsdb_contest_standings
                         (entry_id, dfsdb_id, rank, points, winnings, cash_winnings, entry_cost,
                          entry_count, user_id, username)
                       VALUES %s
                       ON CONFLICT (entry_id) DO UPDATE SET
                         rank = EXCLUDED.rank, points = EXCLUDED.points, winnings = EXCLUDED.winnings,
                         cash_winnings = EXCLUDED.cash_winnings, entry_cost = EXCLUDED.entry_cost,
                         entry_count = EXCLUDED.entry_count, user_id = EXCLUDED.user_id,
                         username = EXCLUDED.username, captured_at = NOW()""",
                    [(r["entry_id"], r["dfsdb_id"], r["rank"], r["points"], r["winnings"],
                      r["cash_winnings"], r["entry_cost"], r["entry_count"], r["user_id"], r["username"])
                     for r in standings],
                    page_size=500)


def persist_users(db: DatabaseManager, users: list[dict]) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            for entry in users:
                row = m.user_row(entry["payload"], m.payload_digest(entry["payload"]))
                if not row["user_id"]:
                    continue
                cursor.execute(
                    """INSERT INTO dfsdb_users
                         (user_id, display_name, dfsdb_created_at, summary, stats, splits, payload_digest)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (user_id) DO UPDATE SET
                         display_name = EXCLUDED.display_name, dfsdb_created_at = EXCLUDED.dfsdb_created_at,
                         summary = EXCLUDED.summary, stats = EXCLUDED.stats, splits = EXCLUDED.splits,
                         payload_digest = EXCLUDED.payload_digest, captured_at = NOW()""",
                    (row["user_id"], row["display_name"], row["dfsdb_created_at"], Json(row["summary"]),
                     Json(row["stats"]), Json(row["splits"]), row["payload_digest"]))
                history = m.history_rows(row["user_id"], entry["history"])
                if history:
                    execute_values(
                        cursor,
                        """INSERT INTO dfsdb_user_contest_history
                             (entry_id, user_id, dfsdb_contest_id, contest_name, contest_date, sport,
                              buy_in, prize_pool, total_entries, rank, points, winnings, cash_winnings,
                              entry_cost, entry_count)
                           VALUES %s
                           ON CONFLICT (entry_id) DO UPDATE SET
                             rank = EXCLUDED.rank, points = EXCLUDED.points, winnings = EXCLUDED.winnings,
                             cash_winnings = EXCLUDED.cash_winnings, entry_cost = EXCLUDED.entry_cost,
                             entry_count = EXCLUDED.entry_count, captured_at = NOW()""",
                        [(h["entry_id"], h["user_id"], h["dfsdb_contest_id"], h["contest_name"],
                          h["contest_date"], h["sport"], h["buy_in"], h["prize_pool"], h["total_entries"],
                          h["rank"], h["points"], h["winnings"], h["cash_winnings"], h["entry_cost"],
                          h["entry_count"]) for h in history],
                        page_size=500)


def persist_lineups(db: DatabaseManager, lineups: list[dict]) -> None:
    if not lineups:
        return
    with db.connect() as connection:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """INSERT INTO dfsdb_lineups
                     (lineup_id, dfsdb_contest_id, sport, contest_name, contest_date, user_id, username,
                      rank, points, winnings, lineup_hash, lineup_players)
                   VALUES %s
                   ON CONFLICT (lineup_id) DO UPDATE SET
                     rank = EXCLUDED.rank, points = EXCLUDED.points, winnings = EXCLUDED.winnings,
                     lineup_hash = EXCLUDED.lineup_hash, lineup_players = EXCLUDED.lineup_players,
                     captured_at = NOW()""",
                [(l["lineup_id"], l["dfsdb_contest_id"], l["sport"], l["contest_name"], l["contest_date"],
                  l["user_id"], l["username"], l["rank"], l["points"], l["winnings"], l["lineup_hash"],
                  Json(l["lineup_players"])) for l in lineups],
                page_size=500)


def mirror_field(db: DatabaseManager, contest: dict, athletes: list[dict], upload_id: str | None) -> dict | None:
    """Write the showdown into nfl_dfs_field_contests / nfl_dfs_field_ownership; returns the matched slate."""
    from ingest.nfl_dfs_field_audit import resolve_upload

    payload = m.mirror_payload(contest, athletes, contest["payout_curve"])
    upload = resolve_upload(db, payload, "showdown", upload_id)
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO nfl_dfs_field_contests
                     (contest_id, contest_name, format, season, week, slate_upload_id,
                      entry_count, winning_score, file_name, file_digest)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (contest_id) DO UPDATE SET
                     contest_name = EXCLUDED.contest_name, slate_upload_id = EXCLUDED.slate_upload_id,
                     season = EXCLUDED.season, week = EXCLUDED.week,
                     entry_count = EXCLUDED.entry_count, winning_score = EXCLUDED.winning_score,
                     file_name = EXCLUDED.file_name, file_digest = EXCLUDED.file_digest""",
                (payload["contest_id"], payload["contest_name"], payload["format"],
                 (upload or {}).get("season"), (upload or {}).get("week"), (upload or {}).get("upload_id"),
                 payload["entry_count"], payload["winning_score"], payload["file_name"], payload["file_digest"]))
            execute_values(
                cursor,
                """INSERT INTO nfl_dfs_field_ownership
                     (contest_id, player_name, normalized_name, drafted_pct, drafted_by_slot, fpts)
                   VALUES %s
                   ON CONFLICT (contest_id, normalized_name) DO UPDATE SET
                     drafted_pct = EXCLUDED.drafted_pct, drafted_by_slot = EXCLUDED.drafted_by_slot,
                     fpts = EXCLUDED.fpts""",
                [(payload["contest_id"], p["name"], p["normalized_name"], p["drafted_pct"],
                  Json(p["drafted_by_slot"]), p["fpts"]) for p in payload["players"].values()],
                page_size=500)
    return upload


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run(args, client: Client, db: DatabaseManager | None) -> str:
    contest_id = m.contest_id_from_link(args.link)
    fetched = fetch_contest(client, contest_id, pages=args.standings_pages, all_pages=args.all_standings)
    payload = fetched["payload"]
    standings = m.standings_rows(contest_id, fetched["results"])
    athletes = m.athlete_rows(contest_id, payload.get("athletes") or [])
    contest = m.contest_row(contest_id, payload, source_url=m.contest_url(contest_id),
                            digest=m.payload_digest(payload), standings=standings,
                            standings_total=fetched["standings_total"], complete=fetched["complete"])
    sport = contest["sport"]

    users: list[dict] = []
    if args.top_users:
        seen, ids = set(), []
        for r in standings:
            if r["user_id"] and r["user_id"] not in seen:
                seen.add(r["user_id"])
                ids.append(r["user_id"])
            if len(ids) >= args.top_users:
                break
        users = fetch_users(client, ids, sport, args.history_pages)
    profiles = [m.user_profile(u["payload"], sport) for u in users]

    lineups, lineup_error, matched = [], None, []
    if args.lineups_pages:
        year = int(str(contest["contest_date"] or "")[:4] or 0) or None
        if year:
            raw, lineup_error = fetch_lineups(client, sport, year, args.lineups_pages)
            lineups = m.lineup_rows(sport, raw)
            matched = [m.annotate_lineup(l, athletes) for l in lineups if l["dfsdb_contest_id"] == contest_id]

    notes = []
    if db is not None:
        persist_contest(db, contest, athletes, standings)
        persist_users(db, users)
        persist_lineups(db, lineups)
        notes.append(f"stored: contest, {len(athletes)} athletes, {len(standings):,} standings rows, "
                     f"{len(users)} user profiles, {len(lineups)} lineups ({len(matched)} from this contest)")
        if args.mirror_field:
            try:
                upload = mirror_field(db, contest, athletes, args.upload)
            except ValueError as exc:
                notes.append(f"mirror refused: {exc}")
            else:
                notes.append("mirrored into nfl_dfs_field_* as contest dfsdb-%s%s" % (
                    contest_id, f", slate {upload['upload_id']} ({upload['season']} week {upload['week']})"
                    if upload else "; NO slate matched, re-run with --upload <id> to attach"))
    else:
        notes.append("dry run: nothing written")
    if lineup_error:
        notes.append(f"lineups feed stopped: {lineup_error}")
    elif args.lineups_pages and not matched:
        notes.append("lineups feed: none of the scanned top lineups belong to this contest "
                     "(the feed is global by points, so a showdown score rarely ranks)")
    notes.append(f"{client.calls} requests to dfsdb")
    return m.format_report(contest, athletes, standings, profiles, matched) + "\n  " + "\n  ".join(notes)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("link", help="a dfsdb contest link (or its uuid)")
    parser.add_argument("--standings-pages", type=int, default=10,
                        help="standings pages of 100 users to fetch, from the top (default 10)")
    parser.add_argument("--all-standings", action="store_true", help="fetch every standings page")
    parser.add_argument("--top-users", type=int, default=0,
                        help="also fetch dfsdb profiles and history for the top N finishers")
    parser.add_argument("--history-pages", type=int, default=1,
                        help="pages of 50 history rows per user, in this contest's sport (default 1)")
    parser.add_argument("--lineups-pages", type=int, default=0,
                        help=f"scan up to N pages (max {MAX_LINEUP_PAGES}) of dfsdb's global top-lineups feed "
                             "for this sport/year and keep lineups from this contest")
    parser.add_argument("--mirror-field", action="store_true",
                        help="NFL showdown only: also write nfl_dfs_field_contests / nfl_dfs_field_ownership")
    parser.add_argument("--upload", help="slate upload id for --mirror-field when auto-matching cannot resolve it")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests (default 1)")
    parser.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    args = parser.parse_args(argv)
    if args.standings_pages < 1:
        parser.error("--standings-pages must be at least 1")

    db = None if args.dry_run else DatabaseManager(load_config().database_url)
    try:
        print(run(args, Client(delay=max(args.delay, 0.5)), db))
    except (DfsdbError, ValueError) as exc:
        sys.exit(f"dfsdb import failed: {exc}")


if __name__ == "__main__":
    main()
