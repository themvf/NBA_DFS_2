"""Inventory of frozen NFL DFS forecasts and pool captures (ownership handoff, task 1).

Read-only. One row per (slate, source, capture): which fields the capture holds
(projections, availability, DK status, ownership + version, per-player market
anchor), its time relative to the slate's first kickoff, and whether it is the
canonical-horizon capture (the LAST one at or before first kickoff minus 20
minutes) or the early one (the FIRST one inside 24 hours of first kickoff).
Distinct slates are counted separately from captures: more captures of one
slate are timing evidence, not more validation slates.

Sources (docs/nfl-ownership-handoff-2026-10-19.md, task 1):
- `nfl_dfs_pool_captures`: one row per (upload, game, minute); rows sharing a
  `capture_key` are one capture instant and are grouped here. Each capture
  covers only the games that were due, so the slate-level coverage (games in
  the capture / games on the slate) is reported beside it.
- `nfl_dfs_optimizer_runs.input_snapshot`: OWNER builds only. Research runs
  (`settings ? 'researchStudy'`, status `research_*`) and builds on system
  slates (`origin = 'system'`) are excluded.
- `nfl_dfs_ghost_freezes`: one frozen input per ghost slate (system uploads).

A slate is keyed by (format, sorted DraftKings game keys, first kickoff), so
several uploads of one salary file, and the ghost's system upload of the same
slate, collapse to one slate.

    python -m research.nfl_ownership_inventory [--season 2026] [--out PATH]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

INVENTORY_VERSION = "nfl-ownership-inventory-v1"
CANONICAL_LEAD = timedelta(minutes=20)
EARLY_WINDOW = timedelta(hours=24)
ET = ZoneInfo("America/New_York")
_GAME_INFO = re.compile(r"^\s*(\S+@\S+)\s+(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2}[AP]M)\s+ET", re.I)
_MATCHUP = re.compile(r"\(([A-Z]{2,3})\s*(?:@|vs\.?)\s*([A-Z]{2,3})\)")


def connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from dotenv import load_dotenv

    root = Path(__file__).resolve().parents[1]
    for env in (root / "web" / ".env.local", root / ".env"):
        if env.exists():
            load_dotenv(env, override=False)
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not set (web/.env.local or .env)")
    conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '90s'")
    return conn


def rows(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def parse_kickoff(game_info: str | None) -> datetime | None:
    m = _GAME_INFO.match(game_info or "")
    if not m:
        return None
    local = datetime.strptime(f"{m.group(2)} {m.group(3).upper()}", "%m/%d/%Y %I:%M%p")
    return local.replace(tzinfo=ET).astimezone(timezone.utc)


# --------------------------------------------------------------------------
# Field presence, computed in SQL so the 1+ GB of payloads never leaves the DB
# --------------------------------------------------------------------------

# For a jsonb array of players `p` (pool capture: p->'evidence'; snapshots: p).
def _presence_sql(player: str, own_total: str, own_source: str, status_key: str) -> str:
    return f"""
      count(*) AS players,
      count(*) FILTER (WHERE jsonb_typeof({player}->'ourProj') = 'number') AS projection,
      count(*) FILTER (WHERE jsonb_typeof({player}->'availability') = 'object') AS availability,
      count(*) FILTER (WHERE {status_key}) AS dk_status_key,
      count(*) FILTER (WHERE jsonb_typeof({own_total}) = 'number') AS own_pct,
      count(*) FILTER (WHERE jsonb_typeof({player}->'marketAnchor') = 'object') AS market_anchor,
      count(*) FILTER (WHERE jsonb_typeof({player}->'absenceCommit') = 'object') AS absence_commit,
      array_remove(array_agg(DISTINCT {own_source}), NULL) AS own_versions"""


POOL_SQL = f"""
SELECT c.upload_id::text, c.capture_key, c.game_id, c.kickoff, c.observed_at, c.captured_at,
       c.payload->>'origin' AS origin, x.*
FROM nfl_dfs_pool_captures c
CROSS JOIN LATERAL (
  SELECT {_presence_sql("e.p->'evidence'", "e.p->'evidence'->'ownPct'", "e.p->'evidence'->>'ownSource'",
                        "e.p->'evidence' ? 'dkStatus'")}
  FROM jsonb_array_elements(c.payload->'players') AS e(p)) x
WHERE c.upload_id = %s::uuid
"""

SNAPSHOT_PRESENCE = _presence_sql(
    "e.p",
    "CASE WHEN jsonb_typeof(e.p->'ownership') = 'object' THEN e.p->'ownership'->'total' "
    "ELSE COALESCE(e.p->'ownership', e.p->'ownPct') END",
    "CASE WHEN jsonb_typeof(e.p->'ownership') = 'object' THEN e.p->'ownership'->>'source' "
    "ELSE e.p->>'ownSource' END",
    "(e.p ? 'status' OR e.p ? 'dkStatus')")

RUN_SQL = f"""
SELECT r.run_id::text, r.upload_id::text, r.created_at, r.status, r.mode, r.optimizer_version, x.*
FROM nfl_dfs_optimizer_runs r
JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id
CROSS JOIN LATERAL (
  SELECT {SNAPSHOT_PRESENCE}
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(r.input_snapshot) = 'array' THEN r.input_snapshot
                                 ELSE '[]'::jsonb END) AS e(p)) x
WHERE r.run_id = %s::uuid
"""

GHOST_SQL = f"""
SELECT g.freeze_id::text, g.upload_id::text, g.format, g.season, g.week, g.first_kickoff, g.frozen_at, x.*
FROM nfl_dfs_ghost_freezes g
CROSS JOIN LATERAL (
  SELECT {SNAPSHOT_PRESENCE}
  FROM jsonb_array_elements(g.input_snapshot) AS e(p)) x
WHERE g.freeze_id = %s::uuid
"""


# --------------------------------------------------------------------------
# Slates
# --------------------------------------------------------------------------

@dataclass
class Slate:
    key: str
    fmt: str
    games: tuple[str, ...]
    first_kickoff: datetime | None
    season: int | None = None
    week: int | None = None
    uploads: set[str] = field(default_factory=set)
    origins: set[str] = field(default_factory=set)
    contests: list[dict] = field(default_factory=list)
    captures: list[dict] = field(default_factory=list)

    @property
    def cutoff(self) -> datetime | None:
        return self.first_kickoff - CANONICAL_LEAD if self.first_kickoff else None

    @property
    def label(self) -> str:
        games = ", ".join(self.games) if len(self.games) <= 2 else f"{len(self.games)} games"
        return f"{self.fmt} {games}"


def load_slates(conn, season: int) -> tuple[dict[str, Slate], dict[str, str]]:
    uploads = rows(conn, """
        SELECT u.upload_id::text, u.format, u.origin, r.season, r.week,
               (SELECT array_agg(DISTINCT p.game_info) FROM nfl_dfs_slate_players p
                 WHERE p.upload_id = u.upload_id) AS game_infos
        FROM nfl_dfs_slate_uploads u
        LEFT JOIN nfl_dfs_projection_runs r ON r.run_id = u.projection_run_id""")
    slates: dict[str, Slate] = {}
    upload_slate: dict[str, str] = {}
    for u in uploads:
        infos = [i for i in (u["game_infos"] or []) if i]
        kicks = [k for k in (parse_kickoff(i) for i in infos) if k]
        games = tuple(sorted({(_GAME_INFO.match(i).group(1) if _GAME_INFO.match(i) else i) for i in infos}))
        first = min(kicks) if kicks else None
        if first is not None and first.astimezone(ET).year != season:
            continue
        key = f"{u['format']}|{'+'.join(games)}|{first.isoformat() if first else 'unknown'}"
        s = slates.setdefault(key, Slate(key, u["format"], games, first))
        s.uploads.add(u["upload_id"])
        s.origins.add(u["origin"])
        if u["season"] is not None:
            s.season, s.week = u["season"], u["week"]
        upload_slate[u["upload_id"]] = key
    return slates, upload_slate


def season_games(conn, season: int) -> list[dict]:
    return rows(conn, """
        SELECT g.season, g.week, g.kickoff, a.abbreviation AS away, h.abbreviation AS home
        FROM nfl_season_games g
        JOIN nfl_teams h ON h.team_id = g.home_team_id
        JOIN nfl_teams a ON a.team_id = g.away_team_id
        WHERE g.season = %s""", (season,))


def attach_contests(conn, slates: dict[str, Slate], upload_slate: dict[str, str], season: int) -> list[dict]:
    """Every imported contest of the season, with the slate it belongs to (or the reason it has none)."""
    games = season_games(conn, season)
    contests = rows(conn, """
        SELECT contest_id, contest_name, format, season, week, slate_upload_id::text, entry_count, file_name,
               score_curve IS NOT NULL AS full_standings, imported_at
        FROM nfl_dfs_field_contests ORDER BY contest_id""")
    out = []
    for c in contests:
        c = dict(c)
        c["link"] = None
        if c["slate_upload_id"] and c["slate_upload_id"] in upload_slate:
            c["slate_key"] = upload_slate[c["slate_upload_id"]]
            c["link"] = "slate_upload_id"
        else:
            c["slate_key"] = None
            m = _MATCHUP.search(c["contest_name"] or "")
            if m:
                pair = {m.group(1), m.group(2)}
                # Divisional pairs meet twice: keep games already played when the contest was imported,
                # and of those the latest (a contest is imported after its game, never before).
                hits = [g for g in games if {g["away"], g["home"]} == pair
                        and (c["week"] is None or g["week"] == c["week"]) and g["kickoff"] <= c["imported_at"]]
                hits = sorted(hits, key=lambda g: g["kickoff"])[-1:] if c["week"] is not None else hits
                if len(hits) == 1:
                    g = hits[0]
                    c["season"], c["week"] = c["season"] or g["season"], c["week"] or g["week"]
                    cands = [s for s in slates.values() if s.fmt == c["format"] and s.first_kickoff
                             and s.first_kickoff == g["kickoff"] and len(s.games) == 1]
                    if len(cands) == 1:
                        c["slate_key"] = cands[0].key
                        c["link"] = "inferred_from_contest_name"
                    else:
                        c["link"] = f"no_salary_upload ({g['away']}@{g['home']} week {g['week']})"
        if c["season"] not in (None, season):
            continue
        if c["slate_key"]:
            slates[c["slate_key"]].contests.append(c)
            s = slates[c["slate_key"]]
            if s.week is None and c["week"] is not None:
                s.season, s.week = c["season"], c["week"]
        out.append(c)
    return out


# --------------------------------------------------------------------------
# Captures
# --------------------------------------------------------------------------

def _fields(r: dict) -> dict:
    return {k: r[k] for k in ("players", "projection", "availability", "dk_status_key", "own_pct",
                              "market_anchor", "absence_commit")} | {"own_versions": sorted(r["own_versions"] or [])}


def pool_captures(conn, slates: dict[str, Slate], upload_slate: dict[str, str]) -> None:
    uploads = [u["upload_id"] for u in rows(conn, "SELECT DISTINCT upload_id::text FROM nfl_dfs_pool_captures")]
    for upload in uploads:
        key = upload_slate.get(upload)
        if key is None:
            continue
        grouped: dict[str, list[dict]] = defaultdict(list)
        for r in rows(conn, POOL_SQL, (upload,)):
            grouped[r["capture_key"]].append(r)
        for capture_key, parts in grouped.items():
            agg = {k: sum(p[k] for p in parts) for k in ("players", "projection", "availability", "dk_status_key",
                                                          "own_pct", "market_anchor", "absence_commit")}
            agg["own_versions"] = sorted({v for p in parts for v in (p["own_versions"] or [])})
            slates[key].captures.append({
                "source": "pool_capture", "origin": parts[0]["origin"], "id": f"{upload[:8]}:{capture_key}",
                "upload_id": upload, "at": min(p["observed_at"] for p in parts),
                "games_covered": len({p["game_id"] for p in parts}), **agg})


def owner_runs(conn, slates: dict[str, Slate], upload_slate: dict[str, str]) -> None:
    ids = rows(conn, """
        SELECT r.run_id::text FROM nfl_dfs_optimizer_runs r
        JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id
        WHERE NOT (r.settings ? 'researchStudy') AND r.status NOT LIKE 'research%%' AND u.origin = 'upload'""")
    for i in ids:
        r = rows(conn, RUN_SQL, (i["run_id"],))[0]
        key = upload_slate.get(r["upload_id"])
        if key is None:
            continue
        slates[key].captures.append({
            "source": "optimizer_run", "origin": f"{r['status']}/{r['mode']}", "id": r["run_id"][:8],
            "upload_id": r["upload_id"], "at": r["created_at"], "games_covered": None, **_fields(r)})


def ghost_freezes(conn, slates: dict[str, Slate], upload_slate: dict[str, str]) -> None:
    for g in rows(conn, "SELECT freeze_id::text FROM nfl_dfs_ghost_freezes"):
        r = rows(conn, GHOST_SQL, (g["freeze_id"],))[0]
        key = upload_slate.get(r["upload_id"])
        if key is None:
            continue
        s = slates[key]
        if s.week is None:
            s.season, s.week = r["season"], r["week"]
        s.captures.append({
            "source": "ghost_freeze", "origin": "system", "id": r["freeze_id"][:8], "upload_id": r["upload_id"],
            "at": r["frozen_at"], "games_covered": None, **_fields(r)})


def mark_horizons(s: Slate) -> None:
    """Canonical = last capture at/before kickoff - 20 min; early = first capture inside 24 h (pregame).

    Marked per SOURCE and also across all sources ("any"), because the scorecard
    prefers pool captures, then the saved run, then the ghost freeze.
    """
    for c in s.captures:
        c["canonical"] = c["early"] = False
        c["canonical_any"] = c["early_any"] = False
        c["minutes_to_kickoff"] = ((s.first_kickoff - c["at"]).total_seconds() / 60) if s.first_kickoff else None
    if not s.first_kickoff:
        return
    def pick(caps, tag):
        before = [c for c in caps if c["at"] <= s.cutoff]
        if before:
            max(before, key=lambda c: c["at"])[f"canonical{tag}"] = True
        early = [c for c in caps if s.first_kickoff - EARLY_WINDOW <= c["at"] < s.first_kickoff]
        if early:
            min(early, key=lambda c: c["at"])[f"early{tag}"] = True
    for source in {c["source"] for c in s.captures}:
        pick([c for c in s.captures if c["source"] == source], "")
    pick(s.captures, "_any")


def holds_forecast(c: dict) -> bool:
    return c["own_pct"] > 0 and c["projection"] > 0


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def _pct(n: int, d: int) -> str:
    return f"{n}/{d}" if d else "0/0"


def slate_status(s: Slate) -> tuple[str, dict | None]:
    canon = [c for c in s.captures if c.get("canonical") and holds_forecast(c)]
    pref = {"pool_capture": 0, "optimizer_run": 1, "ghost_freeze": 2}
    if canon:
        best = sorted(canon, key=lambda c: pref[c["source"]])[0]
        full = best["source"] != "pool_capture" or best["games_covered"] == len(s.games)
        return ("canonical" + ("" if full else "_partial_games"), best)
    if not s.first_kickoff:
        return "no_kickoff_parsed", None
    if not s.captures:
        return "no_frozen_forecast: no capture of any source", None
    before = [c for c in s.captures if c["at"] <= s.cutoff]
    if not before:
        return "no_frozen_forecast: every capture is after kickoff minus 20 min", None
    earlier = [c for c in before if holds_forecast(c)]
    if earlier:
        last = max(earlier, key=lambda c: c["at"])
        return (f"no_frozen_forecast: the canonical-horizon capture of every source holds no ownPct "
                f"(an earlier {last['source']} {last['id']} at {last['at']:%m-%d %H:%M} does)"), None
    return "no_frozen_forecast: no capture before kickoff minus 20 min holds ownPct", None


def render(slates: dict[str, Slate], contests: list[dict], season: int) -> str:
    lines = [f"# NFL DFS ownership — frozen forecast inventory ({INVENTORY_VERSION})", "",
             f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `python -m research.nfl_ownership_inventory "
             f"--season {season}`. Read-only.", "",
             "Definitions: canonical horizon = the LAST capture at or before first kickoff minus 20 minutes; early = the "
             "FIRST pregame capture inside 24 h of first kickoff; both are marked per source. A capture holds a forecast "
             "when at least one player has a numeric `ownPct` and a projection. Pool captures are grouped by "
             "`capture_key` (one minute) and cover only the games due, so `games` = games in the capture / games on the "
             "slate. `dkStatus` counts players whose record carries the DK status KEY (a null value is still a capture of "
             "\"no status\"). Optimizer runs are OWNER builds only (no `researchStudy`, no `research_*` status, no "
             "system slates).", ""]
    in_scope = [s for s in slates.values() if s.captures or s.contests]
    in_scope.sort(key=lambda s: (s.first_kickoff or datetime.max.replace(tzinfo=timezone.utc), s.fmt))

    # Summary
    lines += ["## 1. Distinct slates", "",
              "| week | format | slates | with canonical forecast (any source) | of which pool capture covers every game | with an imported contest | contest slates without a canonical forecast |",
              "|---:|---|---:|---:|---:|---:|---:|"]
    summ = defaultdict(lambda: [0, 0, 0, 0, 0])
    for s in in_scope:
        st, best = slate_status(s)
        row = summ[(s.week, s.fmt)]
        row[0] += 1
        row[1] += st.startswith("canonical")
        row[2] += any(c.get("canonical") and c["source"] == "pool_capture" and holds_forecast(c)
                      and c["games_covered"] == len(s.games) for c in s.captures)
        row[3] += bool(s.contests)
        row[4] += bool(s.contests) and not st.startswith("canonical")
    for (week, fmt), r in sorted(summ.items(), key=lambda kv: (kv[0][0] or 0, kv[0][1])):
        lines.append(f"| {week if week is not None else '?'} | {fmt} | {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |")
    tot = [sum(r[i] for r in summ.values()) for i in range(5)]
    lines += [f"| all | all | {tot[0]} | {tot[1]} | {tot[2]} | {tot[3]} | {tot[4]} |", "",
              f"Captures: {sum(len(s.captures) for s in in_scope)} rows over {len(in_scope)} distinct slates "
              f"(pool {sum(c['source']=='pool_capture' for s in in_scope for c in s.captures)}, owner runs "
              f"{sum(c['source']=='optimizer_run' for s in in_scope for c in s.captures)}, ghost freezes "
              f"{sum(c['source']=='ghost_freeze' for s in in_scope for c in s.captures)}).", ""]
    pool = [(s, c) for s in in_scope for c in s.captures if c["source"] == "pool_capture" and s.first_kickoff]
    late = sum(1 for s, c in pool if s.cutoff < c["at"] < s.first_kickoff)
    pre = sum(1 for s, c in pool if c["at"] <= s.cutoff)
    lines += [f"Pool captures relative to the slate's FIRST kickoff: {pre} at or before kickoff minus 20 min, {late} inside "
              f"the final 20 minutes, {len(pool) - pre - late} at or after first kickoff (later games of a multi-game "
              "slate). The scheduler's final-window ticks (`poolCaptureDue`: remaining <= 20 min) are by construction "
              "after the canonical cutoff (kickoff minus 20 min), so a pool capture can be canonical only when it is the "
              "24-hour capture or an on-upload capture.", ""]

    # Contests
    lines += ["## 2. Every imported contest", "",
              "| contest | name | format | wk | entries | import | slate link | slate first kickoff (UTC) | canonical forecast | source / capture | min before kickoff | ownership version | anchored / players |",
              "|---|---|---|---:|---:|---|---|---|---|---|---:|---|---|"]
    for c in sorted(contests, key=lambda c: (c["week"] or 0, c["contest_id"])):
        s = slates.get(c["slate_key"]) if c["slate_key"] else None
        imp = "stat-api" if (c["file_name"] or "").startswith("stat-api:") else (
            "dfsdb" if c["contest_id"].startswith("dfsdb-") else "DK export")
        if s is None:
            status, best = f"no slate: {c['link'] or 'contest name has no matchup and no slate_upload_id'}", None
        else:
            status, best = slate_status(s)
        lines.append("| {} | {} | {} | {} | {:,} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            c["contest_id"], (c["contest_name"] or "")[:60].replace("|", "/"), c["format"], c["week"] or "?",
            c["entry_count"], imp, c["link"] or "-",
            f"{s.first_kickoff:%Y-%m-%d %H:%M}" if s and s.first_kickoff else "-", status,
            f"{best['source']} {best['id']}" if best else "-",
            f"{best['minutes_to_kickoff']:.0f}" if best else "-",
            (", ".join(best["own_versions"]) or "unversioned") if best else "-",
            _pct(best["market_anchor"], best["players"]) if best else "-"))
    lines.append("")

    # Captures
    lines += ["## 3. Captures, one row per (slate, source, capture)", "",
              "Columns `proj`, `avail`, `dkStatus`, `ownPct`, `anchor`, `commit` are player counts out of `players`. "
              "`C` = canonical horizon for that source, `E` = early horizon for that source, `*` = canonical/early "
              "across all sources.", ""]
    for s in in_scope:
        st, _ = slate_status(s)
        lines += [f"### {s.label} — week {s.week if s.week is not None else '?'}, first kickoff "
                  f"{s.first_kickoff:%Y-%m-%d %H:%M} UTC" if s.first_kickoff else f"### {s.label}", "",
                  f"Uploads {len(s.uploads)} ({', '.join(sorted(s.origins))}); contests "
                  f"{', '.join(c['contest_id'] for c in s.contests) or 'none'}; status **{st}**.", ""]
        if not s.captures:
            lines += ["No capture of any source.", ""]
            continue
        lines += ["| source | id | origin | at (UTC) | min before kickoff | horizon | games | players | proj | avail | dkStatus | ownPct | ownership version | anchor | commit |",
                  "|---|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---|---:|---:|"]
        for c in sorted(s.captures, key=lambda c: c["at"]):
            hz = "".join(t for t, k in (("C", "canonical"), ("E", "early"), ("C*", "canonical_any"), ("E*", "early_any")) if c.get(k))
            lines.append("| {} | {} | {} | {:%Y-%m-%d %H:%M} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                c["source"], c["id"], c["origin"], c["at"],
                f"{c['minutes_to_kickoff']:.0f}" if c["minutes_to_kickoff"] is not None else "-", hz or "-",
                f"{c['games_covered']}/{len(s.games)}" if c["games_covered"] is not None else "all",
                c["players"], c["projection"], c["availability"], c["dk_status_key"], c["own_pct"],
                ", ".join(c["own_versions"]) or ("unversioned" if c["own_pct"] else "-"), c["market_anchor"], c["absence_commit"]))
        lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out", default=None, help="markdown path (default: print)")
    args = ap.parse_args(argv)
    conn = connect()
    try:
        slates, upload_slate = load_slates(conn, args.season)
        contests = attach_contests(conn, slates, upload_slate, args.season)
        pool_captures(conn, slates, upload_slate)
        owner_runs(conn, slates, upload_slate)
        ghost_freezes(conn, slates, upload_slate)
        for s in slates.values():
            mark_horizons(s)
        text = render(slates, contests, args.season)
    finally:
        conn.close()
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
