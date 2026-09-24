"""Capture DraftKings' own live NFL player pool.

## Why

The salary CSV is a photograph. It records what DraftKings listed at the moment
it was downloaded, and a slate uploaded on Tuesday and drafted on Sunday morning
is five days stale -- which is precisely the window in which a player is ruled
out. The week-2 field audit made the cost concrete: Brock Bowers was tagged
Doubtful early in the week and Puka Nacua was a late scratch, and our workspace
could not tell those two situations apart, because it only ever saw one
snapshot.

Two DraftKings endpoints answer this live and need no authentication (verified
2026-09-23, not assumed):

    www.draftkings.com/lobby/getcontests?sport=NFL          -> draft groups
    www.draftkings.com/lineup/getavailableplayers?draftGroupId=N -> the pool

`api.draftkings.com/draftgroups/v1/draftgroups/{id}/draftables` returns **403**.
That is the endpoint CLAUDE.md's NBA-derived "listing endpoints are auth-gated"
note refers to, and it is the one that would bridge DraftKings' player id to the
draftable id our salary CSV stores -- so the join to a slate is by name and team
and must fail closed. See `model/nfl_dfs_dk_pool_match.py`.

## What it does not do

It never writes `nfl_dfs_slate_players`. That row is the record of what the
workspace showed when a lineup was built; rewriting it would silently restate
the input to lineups already exported. The read layer joins the two and prefers
whichever observation is newer.

Usage:
    python -m ingest.nfl_dfs_dk_pool                 # poll everything in window
    python -m ingest.nfl_dfs_dk_pool --hours 96      # widen the window
    python -m ingest.nfl_dfs_dk_pool --draft-group 153775
    python -m ingest.nfl_dfs_dk_pool --report
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

import requests
from psycopg2.extras import Json, execute_values

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_dk_pool_match import normalize_name

LOBBY_URL = "https://www.draftkings.com/lobby/getcontests?sport=NFL"
POOL_URL = "https://www.draftkings.com/lineup/getavailableplayers?draftGroupId={id}"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# DraftKings' contest-type ids for the two salary-cap formats we play. Every
# other id in the lobby is a different game -- Snake, Best Ball, Tiers, Single
# Stat, in-game halves, and simulated "Madden Stream" matchups that carry real
# team abbreviations and would otherwise be mistaken for a live NFL slate.
CONTEST_TYPE_FORMAT = {21: "classic", 96: "showdown"}

# ...and the id is not trusted on its own. A salary-cap pool prices players on
# DraftKings' $100 grid with many players sharing a price; the draft formats
# above put a RANK in the salary field (1..N, every value distinct), which is
# why this is checked against the payload rather than assumed from the id.
MIN_REAL_SALARY = 200


def fetch_draft_groups(session: requests.Session | None = None) -> list[dict]:
    """Salary-cap NFL draft groups from the public lobby, newest start first."""
    get = (session or requests).get
    payload = get(LOBBY_URL, headers=HEADERS, timeout=45).json()
    groups = []
    for group in payload.get("DraftGroups", []):
        fmt = CONTEST_TYPE_FORMAT.get(group.get("ContestTypeId"))
        if not fmt:
            continue
        groups.append({
            "draft_group_id": int(group["DraftGroupId"]),
            "contest_type_id": int(group["ContestTypeId"]),
            "format": fmt,
            "start_date": _parse_start(group.get("StartDate")),
            "game_count": group.get("GameCount"),
            "suffix": (group.get("ContestStartTimeSuffix") or "").strip(),
        })
    groups.sort(key=lambda g: (g["start_date"] or datetime.max.replace(tzinfo=timezone.utc)))
    return groups


def _parse_start(value: str | None) -> datetime | None:
    if not value:
        return None
    # DraftKings emits 7 fractional digits, which `fromisoformat` rejects.
    text = value.replace("Z", "+00:00")
    if "." in text:
        head, _, tail = text.partition(".")
        digits = "".join(ch for ch in tail if ch.isdigit())[:6]
        offset = tail[len(tail) - 6:] if tail[-6] in "+-" else "+00:00"
        text = f"{head}.{digits:<06}{offset}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def fetch_pool(draft_group_id: int, session: requests.Session | None = None) -> dict:
    get = (session or requests).get
    url = POOL_URL.format(id=draft_group_id)
    response = get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return {"url": url, "payload": response.json()}


def is_salary_cap_pool(players: list[dict]) -> bool:
    """Does this pool price players, or does it rank them?

    A rank field masquerading as a salary would produce a pool that matches a
    real slate on team set and format and disagrees with it on every price.
    Cheap to check, and it is the only structural difference between the two.
    """
    salaries = [p.get("s") for p in players if isinstance(p.get("s"), int)]
    if len(salaries) < 2 or min(salaries) < MIN_REAL_SALARY:
        return False
    if any(salary % 100 for salary in salaries):
        return False
    return len(set(salaries)) < len(salaries)


def normalize_players(players: list[dict]) -> list[dict]:
    rows = []
    for player in players:
        team = (player.get("htabbr") if player.get("tid") == player.get("htid")
                else player.get("atabbr"))
        opponent = (player.get("atabbr") if player.get("tid") == player.get("htid")
                    else player.get("htabbr"))
        name = " ".join(part for part in (player.get("fn"), player.get("ln")) if part).strip()
        rows.append({
            "dk_pid": int(player["pid"]),
            "pdkid": player.get("pdkid") or None,
            "tsid": player.get("tsid") or None,
            "name": name,
            "normalized_name": normalize_name(name),
            "position": player.get("pn"),
            "team": team,
            "opponent": opponent,
            "salary": player.get("s"),
            # DraftKings writes "" for a player carrying no tag. Empty string
            # and "no tag" are the same fact; NULL says so without inviting a
            # caller to compare against "".
            "status": (player.get("i") or "").strip().upper() or None,
            "is_disabled": bool(player.get("IsDisabledFromDrafting")),
            "swappable": player.get("swp"),
            "news": player.get("news"),
        })
    rows.sort(key=lambda row: row["dk_pid"])
    return rows


def payload_digest(rows: list[dict]) -> str:
    """Hash only what a decision depends on.

    Deliberately excludes `news` and `swappable`: a news counter ticking over
    is not a change of availability, and `swappable` flips for every player at
    kickoff, which would otherwise manufacture a snapshot for the whole slate at
    the moment its content stops mattering.
    """
    material = [
        [row["dk_pid"], row["status"], row["is_disabled"], row["salary"], row["team"]]
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(material, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def capture(db: PipelineDatabase, group: dict, *, session=None) -> dict:
    """Poll one draft group. Always records the look; writes a snapshot only on change."""
    gid = group["draft_group_id"]
    try:
        fetched = fetch_pool(gid, session)
        players = fetched["payload"].get("playerList") or []
        if not players:
            raise ValueError("empty player list")
        if not is_salary_cap_pool(players):
            raise ValueError("pool prices players by rank, not salary -- not a salary-cap slate")
    except Exception as exc:  # noqa: BLE001 - the failure is the record
        _record_poll(db, gid, ok=False, changed=False, snapshot_id=None, detail=str(exc)[:500])
        return {"draft_group_id": gid, "ok": False, "changed": False, "detail": str(exc)[:200]}

    rows = normalize_players(players)
    digest = payload_digest(rows)
    existing = db.execute_one(
        "SELECT snapshot_id FROM nfl_dfs_dk_pool_snapshots WHERE draft_group_id=%s AND payload_digest=%s",
        (gid, digest),
    )
    if existing:
        _record_poll(db, gid, ok=True, changed=False, snapshot_id=existing["snapshot_id"], detail=None)
        return {"draft_group_id": gid, "ok": True, "changed": False, "players": len(rows)}

    snapshot_id = str(uuid.uuid4())
    teams = sorted({row["team"] for row in rows if row["team"]})
    db.execute(
        """INSERT INTO nfl_dfs_dk_pool_snapshots
           (snapshot_id, draft_group_id, contest_type_id, format, start_date, game_count,
            teams, player_count, payload_digest, source_url)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (snapshot_id, gid, group.get("contest_type_id"), group["format"], group.get("start_date"),
         group.get("game_count"), Json(teams), len(rows), digest, fetched["url"]),
    )
    with db.connect() as connection:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """INSERT INTO nfl_dfs_dk_pool_player_status
                   (snapshot_id, dk_pid, pdkid, tsid, name, normalized_name, position, team,
                    opponent, salary, status, is_disabled, swappable, news)
                   VALUES %s""",
                [(snapshot_id, r["dk_pid"], r["pdkid"], r["tsid"], r["name"], r["normalized_name"],
                  r["position"], r["team"], r["opponent"], r["salary"], r["status"],
                  r["is_disabled"], r["swappable"], r["news"]) for r in rows],
            )
    _record_poll(db, gid, ok=True, changed=True, snapshot_id=snapshot_id, detail=None)
    return {"draft_group_id": gid, "ok": True, "changed": True, "players": len(rows),
            "flagged": sum(1 for r in rows if r["status"]), "snapshot_id": snapshot_id}


def _record_poll(db, gid, *, ok, changed, snapshot_id, detail):
    db.execute(
        """INSERT INTO nfl_dfs_dk_pool_polls (draft_group_id, ok, changed, snapshot_id, detail)
           VALUES (%s,%s,%s,%s,%s)""",
        (gid, ok, changed, snapshot_id, detail),
    )


def in_window(group: dict, *, hours: int, now: datetime) -> bool:
    """Games not yet started, starting inside the window.

    A pool whose games have all kicked off has nothing left to tell us, and a
    pool three weeks out changes for reasons that are not availability.
    """
    start = group.get("start_date")
    if start is None:
        return False
    return now <= start <= now + timedelta(hours=hours)


REQUIRED_TRIGGERS = ("nfl_dfs_dk_pool_snapshots_immutable", "nfl_dfs_dk_pool_player_status_immutable")


def require_append_only(db: PipelineDatabase) -> None:
    """Refuse to write observations a later process could quietly edit.

    `PipelineDatabase` deliberately applies only NFL DFS table DDL, not the
    migrations that carry these triggers, so a fresh database gets the tables
    and not the guarantee. Checking is cheap; discovering afterwards that the
    record was editable all along is not.
    """
    present = {
        row["tgname"] for row in db.execute(
            "SELECT tgname FROM pg_trigger WHERE tgname = ANY(%s)", (list(REQUIRED_TRIGGERS),)
        )
    }
    missing = [name for name in REQUIRED_TRIGGERS if name not in present]
    if missing:
        raise RuntimeError(
            "DraftKings pool tables are missing their append-only triggers "
            f"({', '.join(missing)}). Apply db/schema.py MIGRATIONS before capturing."
        )


def run(db: PipelineDatabase, *, hours: int = 120, only: int | None = None) -> list[dict]:
    require_append_only(db)
    session = requests.Session()
    now = datetime.now(timezone.utc)
    groups = fetch_draft_groups(session)
    if only is not None:
        groups = [g for g in groups if g["draft_group_id"] == only]
        if not groups:
            groups = [{"draft_group_id": only, "contest_type_id": None, "format": "classic",
                       "start_date": None, "game_count": None, "suffix": "(explicit)"}]
    else:
        groups = [g for g in groups if in_window(g, hours=hours, now=now)]
    return [capture(db, group, session=session) for group in groups]


def report(db: PipelineDatabase) -> None:
    rows = db.execute(
        """SELECT s.draft_group_id, s.format, s.start_date, s.game_count, s.player_count,
                  s.captured_at, s.teams,
                  (SELECT COUNT(*) FROM nfl_dfs_dk_pool_player_status p
                    WHERE p.snapshot_id = s.snapshot_id AND p.status IS NOT NULL) AS flagged
             FROM nfl_dfs_dk_pool_snapshots s
            ORDER BY s.captured_at DESC LIMIT 25"""
    )
    if not rows:
        print("No DraftKings pool snapshots captured yet.")
        return
    print("Latest DraftKings pool snapshots (content changes only):")
    for row in rows:
        teams = row["teams"] if isinstance(row["teams"], list) else json.loads(row["teams"])
        label = f"{len(teams)} teams" if len(teams) > 2 else "@".join(teams)
        print(f"  {row['captured_at']:%Y-%m-%d %H:%M} UTC  dg={row['draft_group_id']:<7}"
              f" {row['format']:<9} {label:<12} {row['player_count']:>4} players"
              f"  {row['flagged']:>3} tagged")
    polls = db.execute(
        """SELECT draft_group_id, MAX(polled_at) AS last_poll,
                  COUNT(*) FILTER (WHERE NOT ok) AS failures, COUNT(*) AS polls
             FROM nfl_dfs_dk_pool_polls GROUP BY draft_group_id ORDER BY last_poll DESC LIMIT 25"""
    )
    print("\nPolls (a look that found nothing new is still a look):")
    for row in polls:
        print(f"  dg={row['draft_group_id']:<7} last {row['last_poll']:%Y-%m-%d %H:%M} UTC"
              f"  {row['polls']:>4} polls  {row['failures']:>3} failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=120,
                        help="Poll draft groups starting within this many hours (default 120).")
    parser.add_argument("--draft-group", type=int, default=None)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    db = PipelineDatabase(load_config().database_url)
    if args.report:
        report(db)
        return
    results = run(db, hours=args.hours, only=args.draft_group)
    if not results:
        print(f"No salary-cap NFL draft groups start within {args.hours}h.")
        return
    for result in results:
        if not result["ok"]:
            print(f"  dg={result['draft_group_id']:<7} FAILED  {result['detail']}")
        elif result["changed"]:
            print(f"  dg={result['draft_group_id']:<7} changed  {result['players']} players,"
                  f" {result['flagged']} tagged")
        else:
            print(f"  dg={result['draft_group_id']:<7} unchanged")
    print(f"{sum(r['changed'] for r in results)} of {len(results)} draft groups changed.")


if __name__ == "__main__":
    main()
