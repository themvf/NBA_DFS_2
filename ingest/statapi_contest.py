"""Import a DraftKings contest's standings and its top finishers' lineups from stat-api.com.

The DraftKings export only exists for contests you entered. stat-api keeps
every lineup of every DraftKings contest, serves each week's flagship contests
(the Millionaire, the biggest Thursday and Monday Showdown) to anyone with no
key, and opens every other contest to a free account's key (`STAT_API_KEY`).
This pulls what the export path would have given, into the same tables, keyed
by DraftKings' own contest id:

    nfl_dfs_field_contests      the contest, slate link, score curve (when every entry was fetched)
    nfl_dfs_field_ownership     per-slot field ownership of every player the fetched users rostered
    nfl_dfs_field_top_entries   every fetched lineup ranked within --keep-top, roster and all
    nfl_dfs_field_user_builds   each fetched user's whole portfolio: record, stacks, dispersion, exposure

Read `model/statapi_contest.py` first for what each endpoint returns and the
ownership-per-slot convention. Ownership comes from the fetched users' seats,
so it covers the players THEY used; the contest is linked to a slate (which is
what `calibrate:nfl-ownership` grades) only when that accounts for at least
--ownership-min-coverage of the field's ownership mass (roster slots x 100%). Below that the
contest, builds and lineups are still stored, with no slate link, so the
calibration never reads an unseen player as 0% owned.

Usage:
    python -m ingest.statapi_contest --contest 20754235                     # stat-api contest id
    python -m ingest.statapi_contest --contest 20754235 --top-users 50 --all-standings
    python -m ingest.statapi_contest --date 2026-10-04 --sport nfl           # the day's biggest contests
    python -m ingest.statapi_contest --date 2026-10-08 --search "TB @ DAL" --format showdown
    python -m ingest.statapi_contest --contest 20754235 --dry-run
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time

import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from db.database import DatabaseManager
from model import statapi_contest as m

BASE = "https://api.stat-api.com/api/v1/dfs"
USER_AGENT = "Mozilla/5.0 NBADFS-v2 statapi-contest-import/1.0"
STANDINGS_PAGE = 1000
MAX_LIST = 100          # contests per slate without a key; a key lifts it


class StatApiError(RuntimeError):
    pass


class Client:
    """JSON client: Bearer key when configured, a short pause, Retry-After honoured."""

    def __init__(self, api_key: str | None = None, delay: float = 0.25, retries: int = 3,
                 timeout: int = 120, session=None):
        self.delay, self.retries, self.timeout = delay, retries, timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"
        self.keyed = bool(api_key)
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
            response = self.session.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    raise StatApiError(f"{url}: not JSON ({exc})") from exc
            body = ""
            try:
                body = (response.json().get("message") or response.json().get("error") or "")[:200]
            except ValueError:
                body = response.text[:200]
            if response.status_code in (401, 402, 403):
                hint = "" if self.keyed else " (no STAT_API_KEY set; a free account's key opens every contest)"
                raise StatApiError(f"{url}: {response.status_code} {body}{hint}")
            if response.status_code == 404:
                raise StatApiError(f"{url}: not found")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.retries:
                retry_after = response.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else self.delay * 2 ** (attempt + 1))
                continue
            raise StatApiError(f"{url}: HTTP {response.status_code} {body}")
        raise StatApiError(f"{url}: gave up")  # pragma: no cover

    # -- endpoints --------------------------------------------------------

    def standings(self, contest_id: int, from_row: int = 1, limit: int = STANDINGS_PAGE) -> dict:
        return self.get(f"/contests/{contest_id}/standings", {"limit": limit, "from_row": from_row})

    def user_lineups(self, contest_id: int, username: str) -> dict:
        return self.get(f"/contests/{contest_id}/users/{requests.utils.quote(username, safe='')}/lineups")

    def slates(self, date: str, sport: str, operator_id: int = 1) -> dict:
        return self.get("/slates", {"date": date, "operator_id": operator_id, "sport": sport})

    def contests(self, slate_id: int) -> dict:
        return self.get("/contests", {"slate_id": slate_id, "limit": MAX_LIST})


# --------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------

def fetch_standings(client: Client, contest_id: int, *, rows: int, all_rows: bool) -> tuple[dict, list[dict]]:
    """The contest card and standings rows: the first page, then more while asked for and available."""
    first = client.standings(contest_id, 1, min(STANDINGS_PAGE, rows))
    contest = first.get("contest") or {}
    if not contest:
        raise StatApiError(f"contest {contest_id}: no contest card in the standings payload")
    total = int(contest.get("total_entries") or 0)
    out = list(first.get("standings") or [])
    want = total if all_rows else min(rows, total or rows)
    while len(out) < want:
        page = client.standings(contest_id, len(out) + 1, min(STANDINGS_PAGE, want - len(out)))
        chunk = page.get("standings") or []
        if not chunk:
            break
        out.extend(chunk)
    return contest, out


def fetch_user_payloads(client: Client, contest_id: int, usernames: list[str]) -> list[dict]:
    out = []
    for name in usernames:
        try:
            out.append(client.user_lineups(contest_id, name))
        except StatApiError as exc:
            print(f"  user {name}: {exc}")
    return out


def discover(client: Client, date: str, sport: str, *, formats: tuple[str, ...]) -> list[dict]:
    """Every contest stat-api lists for the operator's slates that day (100 per slate without a key)."""
    slates = [s for s in (client.slates(date, sport).get("slates") or [])
              if str(s.get("game_type") or "").lower() in formats and "In-Game" not in str(s.get("name") or "")]
    rows: list[dict] = []
    for slate in slates:
        listing = client.contests(int(slate["id"]))
        rows.extend(m.listing_contests(listing.get("contests") or [], slate))
    return rows


# --------------------------------------------------------------------------
# Persist
# --------------------------------------------------------------------------

def persist(db: DatabaseManager, contest: dict, summary: dict, ownership: dict, builds: list[dict],
            entries: list[dict], upload: dict | None, digest: str, *, write_ownership: bool) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO nfl_dfs_field_contests
                     (contest_id, contest_name, format, season, week, slate_upload_id, entry_count,
                      winning_score, median_score, min_score, file_name, file_digest, score_curve)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (contest_id) DO UPDATE SET
                     contest_name = EXCLUDED.contest_name,
                     slate_upload_id = COALESCE(EXCLUDED.slate_upload_id, nfl_dfs_field_contests.slate_upload_id),
                     season = COALESCE(EXCLUDED.season, nfl_dfs_field_contests.season),
                     week = COALESCE(EXCLUDED.week, nfl_dfs_field_contests.week),
                     entry_count = EXCLUDED.entry_count,
                     winning_score = COALESCE(EXCLUDED.winning_score, nfl_dfs_field_contests.winning_score),
                     median_score = COALESCE(EXCLUDED.median_score, nfl_dfs_field_contests.median_score),
                     min_score = COALESCE(EXCLUDED.min_score, nfl_dfs_field_contests.min_score),
                     file_name = EXCLUDED.file_name, file_digest = EXCLUDED.file_digest,
                     score_curve = COALESCE(EXCLUDED.score_curve, nfl_dfs_field_contests.score_curve)""",
                (contest["contest_id"], contest["contest_name"], contest["format"],
                 (upload or {}).get("season"), (upload or {}).get("week"), (upload or {}).get("upload_id"),
                 contest["entry_count"], summary["winning_score"], summary["median_score"], summary["min_score"],
                 f"{m.SOURCE}:{contest['statapi_id']}", digest,
                 Json(summary["score_curve"]) if summary["score_curve"] else None))
            if write_ownership and ownership["players"]:
                execute_values(
                    cursor,
                    """INSERT INTO nfl_dfs_field_ownership
                         (contest_id, player_name, normalized_name, drafted_pct, drafted_by_slot, fpts)
                       VALUES %s
                       ON CONFLICT (contest_id, normalized_name) DO UPDATE SET
                         drafted_pct = EXCLUDED.drafted_pct, drafted_by_slot = EXCLUDED.drafted_by_slot,
                         fpts = EXCLUDED.fpts""",
                    [(contest["contest_id"], p["name"], p["normalized_name"], p["drafted_pct"],
                      Json(p["drafted_by_slot"]), p["fpts"]) for p in ownership["players"].values()],
                    page_size=500)
            if builds:
                execute_values(
                    cursor,
                    """INSERT INTO nfl_dfs_field_user_builds
                         (contest_id, username, entries, best_rank, cashed, total_payout, avg_points,
                          players_used, analysis, exposure, lineups, source)
                       VALUES %s
                       ON CONFLICT (contest_id, username) DO UPDATE SET
                         entries = EXCLUDED.entries, best_rank = EXCLUDED.best_rank, cashed = EXCLUDED.cashed,
                         total_payout = EXCLUDED.total_payout, avg_points = EXCLUDED.avg_points,
                         players_used = EXCLUDED.players_used, analysis = EXCLUDED.analysis,
                         exposure = EXCLUDED.exposure, lineups = EXCLUDED.lineups, source = EXCLUDED.source,
                         captured_at = NOW()""",
                    [(b["contest_id"], b["username"], b["entries"], b["best_rank"], b["cashed"], b["total_payout"],
                      b["avg_points"], b["players_used"], Json(b["analysis"]), Json(b["exposure"]),
                      Json(b["lineups"]), b["source"]) for b in builds],
                    page_size=100)
            if entries:
                execute_values(
                    cursor,
                    """INSERT INTO nfl_dfs_field_top_entries
                         (contest_id, entry_id, rank, entry_name, username, user_entries, points,
                          lineup_text, players, ownership_sum)
                       VALUES %s
                       ON CONFLICT (contest_id, entry_id) DO UPDATE SET
                         rank = EXCLUDED.rank, entry_name = EXCLUDED.entry_name, username = EXCLUDED.username,
                         user_entries = EXCLUDED.user_entries, points = EXCLUDED.points,
                         lineup_text = EXCLUDED.lineup_text, players = EXCLUDED.players,
                         ownership_sum = EXCLUDED.ownership_sum, captured_at = NOW()""",
                    [(contest["contest_id"], e["entry_id"], e["rank"], e["entry_name"], e["username"], e["user_entries"],
                      e["points"], e["lineup_text"],
                      Json([{"slot": p["slot"], "name": p["name"], "normalized_name": p["normalized_name"],
                             "drafted_pct": p["drafted_pct"]} for p in e["players"]]),
                      e["ownership_sum"]) for e in entries],
                    page_size=500)


# --------------------------------------------------------------------------
# One contest
# --------------------------------------------------------------------------

def import_contest(args, client: Client, db: DatabaseManager | None, statapi_id: int) -> str:
    raw_contest, raw_rows = fetch_standings(client, statapi_id, rows=args.standings_rows, all_rows=args.all_standings)
    contest = m.contest_row(raw_contest)
    standings = m.standings_rows(raw_rows)
    summary = m.standings_summary(standings, contest["entry_count"])

    # Top finishers: the first N distinct usernames in rank order.
    usernames: list[str] = []
    for r in standings:
        if r["username"] not in usernames:
            usernames.append(r["username"])
        if len(usernames) >= args.top_users:
            break
    payloads = fetch_user_payloads(client, statapi_id, usernames) if args.top_users else []
    builds = [m.user_build(p, contest["contest_id"]) for p in payloads]
    lineups_per_user = [m.user_lineups(p) for p in payloads]
    entries = m.top_entries(lineups_per_user, args.keep_top)
    ownership = m.field_ownership(payloads, contest["contest_id"])
    covered = ownership["mass"] is not None and ownership["mass"] >= args.ownership_min_coverage

    notes = []
    upload = None
    if db is not None:
        if contest["sport"] != "nfl":
            notes.append("not an NFL contest: nothing written (the field tables are NFL)")
        else:
            if covered:
                from ingest.nfl_dfs_field_audit import resolve_upload
                upload = resolve_upload(db, {"players": ownership["players"]}, contest["format"], args.upload)
            persist(db, contest, summary, ownership, builds, entries, upload,
                    m.payload_digest(raw_rows), write_ownership=covered)
            notes.append(f"stored: contest {contest['contest_id']}, {len(standings):,} standings rows"
                         + (f" (complete: median/min/score curve written)" if summary["complete"] else "")
                         + f", {len(builds)} user builds, {len(entries)} lineups within the top {args.keep_top}, "
                         + (f"{len(ownership['players'])} ownership rows" if covered else "ownership NOT written"))
            if covered:
                notes.append(f"  slate: " + (f"{upload['upload_id']} ({upload['season']} week {upload['week']})" if upload
                                              else "NO slate matched; re-run with --upload <id> to attach"))
            else:
                notes.append(f"  ownership in hand is {ownership['mass'] if ownership['mass'] is not None else '?'} of the "
                             f"field's ownership mass, under --ownership-min-coverage {args.ownership_min_coverage}: "
                             f"no slate link, so the calibration does not read unseen players as 0%. "
                             f"Raise --top-users to cover more of the pool.")
    else:
        notes.append("dry run: nothing written")
    return m.format_report(contest, standings, summary, ownership, builds, entries) + "\n  " + "\n  ".join(notes)


def run(args, client: Client, db: DatabaseManager | None) -> str:
    if args.contest:
        texts = [import_contest(args, client, db, cid) for cid in args.contest]
        return "\n\n".join(texts) + f"\n\n{client.calls} requests to stat-api"

    formats = {"showdown": ("showdown",), "classic": ("classic",)}.get(args.format, ("classic", "showdown"))
    listed = discover(client, args.date, args.sport, formats=formats)
    picked = m.select_contests(listed, min_entries=args.min_entries, max_contests=args.max_contests,
                               search=args.search, formats=formats)
    lines = [f"{args.sport} DraftKings contests on {args.date}" + (f' matching "{args.search}"' if args.search else "")
             + f": {len(listed)} listed, {len(picked)} selected (>= {args.min_entries:,} entries, max {args.max_contests})"]
    for r in picked:
        lines.append(f"  {r['entry_count']:>8,}  ${r['entry_fee'] or 0:>7g}  {r['format']:<8} {r['name'][:70]}")
    if not client.keyed:
        lines.append("  (no STAT_API_KEY: only each week's flagship contests answer in full; "
                     "others need a free account's key)")
    for r in picked:
        lines.append("")
        try:
            lines.append(import_contest(args, client, db, r["statapi_id"]))
        except StatApiError as exc:
            lines.append(f"{r['name']}\n  FAILED: {exc}")
    lines.append(f"\n{client.calls} requests to stat-api")
    return "\n".join(lines)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contest", type=int, nargs="+", help="stat-api contest id(s)")
    parser.add_argument("--date", help="instead of --contest: import a day's DraftKings contests (YYYY-MM-DD)")
    parser.add_argument("--sport", default="nfl", help="with --date: sport (default nfl)")
    parser.add_argument("--search", help="with --date: keep contests whose name contains this, e.g. \"TB @ DAL\"")
    parser.add_argument("--format", choices=("classic", "showdown", "any"), default="any")
    parser.add_argument("--min-entries", type=int, default=1000)
    parser.add_argument("--max-contests", type=int, default=5)
    parser.add_argument("--standings-rows", type=int, default=STANDINGS_PAGE,
                        help=f"standings rows to fetch from the top (default {STANDINGS_PAGE})")
    parser.add_argument("--all-standings", action="store_true",
                        help="fetch every entry (writes median, min and the score curve the web ranks lineups with)")
    parser.add_argument("--top-users", type=int, default=25,
                        help="fetch every lineup of the top N distinct finishers (default 25)")
    parser.add_argument("--keep-top", type=int, default=100,
                        help="store fetched lineups ranked within the top N (default 100)")
    parser.add_argument("--ownership-min-coverage", type=float, default=0.95,
                        help="write ownership and link the slate only when the fetched users' seats account for this "
                             "share of the field's ownership mass (roster slots x 100%%; default 0.95)")
    parser.add_argument("--upload", help="slate upload id when auto-matching cannot resolve it")
    parser.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    args = parser.parse_args(argv)
    if bool(args.contest) == bool(args.date):
        parser.error("pass --contest ID [ID ...] or --date YYYY-MM-DD (not both)")
    if args.date and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
        parser.error("--date must be YYYY-MM-DD")
    if args.top_users < 0 or args.keep_top < 1 or args.standings_rows < 1:
        parser.error("--top-users >= 0, --keep-top >= 1, --standings-rows >= 1")

    db = None if args.dry_run else DatabaseManager(load_config().database_url)
    try:
        print(run(args, Client(api_key=os.environ.get("STAT_API_KEY") or None), db))
    except (StatApiError, ValueError) as exc:
        sys.exit(f"stat-api import failed: {exc}")


if __name__ == "__main__":
    main()
