"""Pure helpers for one dfsdb.com contest import (no I/O).

dfsdb ("The DFS Record Book") archives public DraftKings contest results. Its
contest page is a client-rendered shell; the data behind it comes from
`/api/contest/{id}` and that is what `ingest/dfsdb_contest.py` reads. What the
payload actually is, measured 2026-10-10, and what each part can and cannot
support:

* `contest`   -- the card: platform, sport, name, date, buy-in, prize pool,
                 total entries, series/type.
* `stats`     -- cashing entries, min cash, first-place prize, prize totals.
* `athletes`  -- CAPPED AT 50 rows by dfsdb. Position, team, salary, actual
                 points and ownership. A showdown pool is ~50-60 players so
                 this is near-complete; a classic pool is ~400, so it is a
                 fraction (Saquon Barkley was missing from a Milly Maker's
                 50). Showdown ownership is FLEX-slot only (sums to ~500%);
                 captain ownership is not broken out.
* `results`   -- ONE ROW PER USER, not per entry: the user's best rank, best
                 points, and `winnings` summed across EVERY entry they had in
                 the contest. So "rank -> winnings" is only the payout for
                 that rank when the user had exactly one entry. The payout
                 curve below is built from single-entry users only and says
                 how many ranks that covered.

Everything here is deterministic over the JSON; the module is the unit under
test and the ingest module is a thin client around it.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from typing import Any

from model.nfl_dfs_field_audit import normalize_name

VERSION = "dfsdb-contest-import-v1"

CONTEST_LINK = re.compile(r"(?:dfsdb\.com/contest/)?([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
                          r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")

#: dfsdb's hard page-size ceiling on the standings endpoint (`limit=200` is a 400).
STANDINGS_PAGE_LIMIT = 100
#: dfsdb's athlete table cap; a payload with fewer rows is the whole pool.
ATHLETE_CAP = 50


def contest_id_from_link(text: str) -> str:
    """The contest uuid from a pasted dfsdb link (or a bare uuid)."""
    match = CONTEST_LINK.search(str(text or ""))
    if not match:
        raise ValueError(f"not a dfsdb contest link: {text!r}")
    return match.group(1).lower()


def contest_url(contest_id: str) -> str:
    return f"https://www.dfsdb.com/contest/{contest_id}"


def payload_digest(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def contest_format(contest: dict) -> str:
    series = str(contest.get("contest_series") or "")
    name = str(contest.get("contest_name") or "")
    return "showdown" if "showdown" in (series + " " + name).lower() else "classic"


def athlete_rows(contest_id: str, athletes: list[dict]) -> list[dict]:
    """Normalized athlete rows, one per (name, team). A duplicate keeps the higher ownership."""
    out: dict[tuple[str, str], dict] = {}
    for a in athletes or []:
        name = str(a.get("athlete_name") or "").strip()
        if not name:
            continue
        key = (normalize_name(name), str(a.get("team") or ""))
        row = {
            "dfsdb_id": contest_id,
            "athlete_name": name,
            "normalized_name": key[0],
            "position": a.get("position"),
            "team": a.get("team"),
            "salary": _int(a.get("salary")),
            "fantasy_points": _float(a.get("fantasy_points")),
            "ownership_pct": _float(a.get("ownership_pct")),
        }
        prior = out.get(key)
        if prior is None or (row["ownership_pct"] or 0) > (prior["ownership_pct"] or 0):
            out[key] = row
    return sorted(out.values(), key=lambda r: (-(r["ownership_pct"] or 0), r["athlete_name"]))


def standings_rows(contest_id: str, results: list[dict]) -> list[dict]:
    out = []
    for r in results or []:
        if not r.get("id") or r.get("rank") is None:
            continue
        user = r.get("players") or {}
        out.append({
            "entry_id": r["id"],
            "dfsdb_id": contest_id,
            "rank": int(r["rank"]),
            "points": _float(r.get("points")),
            "winnings": _float(r.get("winnings")),
            "cash_winnings": _float(r.get("cash_winnings")),
            "entry_cost": _float(r.get("entry_cost")),
            "entry_count": _int(r.get("entry_count")),
            "user_id": r.get("player_id") or user.get("id"),
            "username": user.get("display_name"),
        })
    return out


def payout_curve(rows: list[dict], stats: dict | None) -> dict:
    """Rank -> payout, from users who had exactly ONE entry (their winnings ARE the rank's payout).

    A multi-entry user's `winnings` pools every entry they had, so it cannot be
    read as the payout of their best rank. The curve therefore has holes where
    every user at a rank was multi-entry; `coverage` states how many of the
    fetched ranks were resolved, and `cash_line_points` is filled only when the
    last cashing rank (from dfsdb's stats) is inside the fetched standings.
    """
    stats = stats or {}
    by_rank: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        by_rank[r["rank"]].append(r)
    points, payouts = [], []
    for rank in sorted(by_rank):
        at = by_rank[rank]
        points.append([rank, max((r["points"] or 0) for r in at), len(at)])
        singles = [r["winnings"] for r in at if r["entry_count"] == 1 and r["winnings"] is not None]
        if singles:
            payouts.append([rank, round(max(singles), 2)])
    cashing = _int(stats.get("cashingEntries"))
    cash_line = None
    if cashing and by_rank:
        # The last cashing rank is covered when some fetched rank is at or past it.
        at_or_past = [p for p in points if p[0] >= cashing]
        if at_or_past:
            before = [p for p in points if p[0] <= cashing]
            cash_line = before[-1][1] if before else None
    return {
        "version": VERSION,
        "ranks_fetched": len(by_rank),
        "max_rank_fetched": max(by_rank) if by_rank else None,
        "score_by_rank": points,            # [rank, best points at rank, users at rank]
        "payout_by_rank": payouts,          # [rank, payout] from single-entry users only
        "coverage": round(len(payouts) / len(by_rank), 3) if by_rank else 0.0,
        "first_place_prize": _float(stats.get("firstPlacePrize")),
        "min_cash": _float(stats.get("minCash")),
        "cashing_entries": cashing,
        "cash_line_points": cash_line,
    }


def contest_row(contest_id: str, payload: dict, *, source_url: str, digest: str,
                standings: list[dict], standings_total: int | None, complete: bool) -> dict:
    c = payload.get("contest") or {}
    return {
        "dfsdb_id": contest_id,
        "platform": c.get("platform"),
        "sport": str(c.get("sport") or "").lower() or "unknown",
        "contest_name": c.get("contest_name") or "(unnamed)",
        "contest_date": c.get("contest_date"),
        "buy_in": _float(c.get("buy_in")),
        "prize_pool": _float(c.get("prize_pool")),
        "total_entries": _int(c.get("total_entries")),
        "contest_series": c.get("contest_series"),
        "contest_type": c.get("contest_type"),
        "contest_category": c.get("contest_category"),
        "validation_status": c.get("validation_status"),
        "format": contest_format(c),
        "stats": payload.get("stats") or {},
        "payout_curve": payout_curve(standings, payload.get("stats")),
        "standings_users": standings_total,
        "standings_fetched": len(standings),
        "standings_complete": bool(complete),
        "source_url": source_url,
        "payload_digest": digest,
        "import_version": VERSION,
    }


# --------------------------------------------------------------------------
# A slate's contests (dfsdb's /api/contests listing, date-descending)
# --------------------------------------------------------------------------

#: Hard ceiling on contests imported in one date run, whatever --max-contests says.
MAX_DAY_CONTESTS = 50


def contest_listing_rows(data: list[dict]) -> list[dict]:
    out = []
    for c in data or []:
        if not c.get("id") or not c.get("contest_date"):
            continue
        out.append({"id": c["id"], "contest_date": str(c["contest_date"]), "sport": str(c.get("sport") or "").lower(),
                    "contest_name": c.get("contest_name") or "(unnamed)", "buy_in": _float(c.get("buy_in")),
                    "prize_pool": _float(c.get("prize_pool")), "total_entries": _int(c.get("total_entries")) or 0,
                    "contest_type": c.get("contest_type"), "contest_series": c.get("contest_series"),
                    "format": contest_format(c)})
    return out


def listing_is_past(data: list[dict], date: str) -> bool:
    """True once a date-descending page has run past the wanted date (no more pages needed)."""
    dates = [str(c.get("contest_date") or "") for c in data or [] if c.get("contest_date")]
    return bool(dates) and min(dates) < date


def select_day_contests(rows: list[dict], date: str, *, min_entries: int, max_contests: int,
                        formats: tuple[str, ...] = ("classic", "showdown")) -> list[dict]:
    """The day's contests worth importing: largest fields first, one row per id, capped.

    Entries decide, not prize pool: ownership and the payout curve are only as
    informative as the field is big, and a $0.10 contest with 47k entries says
    more about what the public drafted than a $333 contest with 2k.
    """
    seen, picked = set(), []
    for r in sorted(rows, key=lambda r: (-r["total_entries"], r["contest_name"])):
        if r["contest_date"] != date or r["id"] in seen or r["format"] not in formats:
            continue
        if r["total_entries"] < min_entries:
            continue
        seen.add(r["id"])
        picked.append(r)
        if len(picked) >= min(max_contests, MAX_DAY_CONTESTS):
            break
    return picked


# --------------------------------------------------------------------------
# Users ("who beat this contest")
# --------------------------------------------------------------------------

def user_row(payload: dict, digest: str) -> dict:
    p = payload.get("player") or {}
    return {
        "user_id": p.get("id"),
        "display_name": p.get("display_name") or "?",
        "dfsdb_created_at": p.get("created_at"),
        "summary": payload.get("summary") or {},
        "stats": payload.get("stats") or [],
        "splits": payload.get("splits") or [],
        "payload_digest": digest,
    }


def history_rows(user_id: str, history: list[dict]) -> list[dict]:
    out = []
    for h in history or []:
        c = h.get("contests") or {}
        if not h.get("id") or not c.get("id"):
            continue
        out.append({
            "entry_id": h["id"], "user_id": user_id, "dfsdb_contest_id": c["id"],
            "contest_name": c.get("contest_name"), "contest_date": c.get("contest_date"),
            "sport": c.get("sport"), "buy_in": _float(c.get("buy_in")),
            "prize_pool": _float(c.get("prize_pool")), "total_entries": _int(c.get("total_entries")),
            "rank": _int(h.get("rank")), "points": _float(h.get("points")),
            "winnings": _float(h.get("winnings")), "cash_winnings": _float(h.get("cash_winnings")),
            "entry_cost": _float(h.get("entry_cost")), "entry_count": _int(h.get("entry_count")),
        })
    return out


def user_profile(payload: dict, sport: str) -> dict:
    """The flat 'how does this user play' view the report prints.

    Volume (entries per contest), selectivity (contests per sport), ROI and
    cash rate on dfsdb's own numbers, plus the buy-in and contest-type splits
    where they make or lose their money. Portfolio-level thinking only: dfsdb
    exposes no per-entry lineups here.
    """
    p = payload.get("player") or {}
    stats = {str(s.get("sport") or "").lower(): s for s in payload.get("stats") or []}
    sport_stats = stats.get(sport.lower(), {})
    splits = payload.get("splits") or []
    by_type = {}
    for s in splits:
        by_type.setdefault(s.get("split_type"), []).append(s)

    def best(kind):
        rows = [s for s in by_type.get(kind, []) if (s.get("entries") or 0) >= 20]
        if not rows:
            return None
        s = max(rows, key=lambda s: (s.get("roi") or 0))
        return {"key": s.get("split_key"), "roi": s.get("roi"), "entries": s.get("entries"),
                "cash_rate": s.get("cash_rate")}

    contests = sport_stats.get("contests_played") or 0
    entries = sport_stats.get("total_entries") or 0
    return {
        "user_id": p.get("id"),
        "display_name": p.get("display_name"),
        "sport": sport,
        "contests_played": contests,
        "total_entries": entries,
        "entries_per_contest": round(entries / contests, 1) if contests else None,
        "roi_pct": sport_stats.get("roi_pct"),
        "cash_rate": sport_stats.get("cash_rate"),
        "net_profit": sport_stats.get("net_profit"),
        "first_contest": sport_stats.get("first_contest"),
        "last_contest": sport_stats.get("last_contest"),
        "sports_played": sorted(stats),
        "win_count": (payload.get("summary") or {}).get("win_count"),
        "best_buy_in": best("buy_in"),
        "best_contest_type": best("contest_type"),
        "best_slate_size": best("slate_size"),
    }


# --------------------------------------------------------------------------
# Lineups (dfsdb's global "top lineups" feed)
# --------------------------------------------------------------------------

def lineup_rows(sport: str, data: list[dict]) -> list[dict]:
    out = []
    for row in data or []:
        if not row.get("id") or not row.get("contest_id") or not row.get("lineup_players"):
            continue
        out.append({
            "lineup_id": row["id"], "dfsdb_contest_id": row["contest_id"],
            "sport": str(row.get("sport") or sport).lower(),
            "contest_name": row.get("contest_name"), "contest_date": row.get("contest_date"),
            "user_id": row.get("player_id"), "username": row.get("username"),
            "rank": _int(row.get("rank")), "points": _float(row.get("points")),
            "winnings": _float(row.get("winnings")), "lineup_hash": row.get("lineup_hash"),
            "lineup_players": row["lineup_players"],
        })
    return out


def annotate_lineup(lineup: dict, athletes: list[dict]) -> dict:
    """Read a lineup against the contest's ownership: what the builder paid, who they faded."""
    own = {(a["normalized_name"], a.get("team") or ""): a for a in athletes}
    by_name = {a["normalized_name"]: a for a in athletes}
    players, missing, teams = [], 0, defaultdict(int)
    total_own, salary = 0.0, 0
    for p in lineup.get("lineup_players") or []:
        key = normalize_name(p.get("name"))
        a = own.get((key, p.get("team") or "")) or by_name.get(key)
        pct = a["ownership_pct"] if a else None
        if pct is None:
            missing += 1
        else:
            total_own += pct
        salary += _int(p.get("salary")) or 0
        teams[p.get("team") or "?"] += 1
        players.append({"name": p.get("name"), "position": p.get("position"), "team": p.get("team"),
                        "salary": p.get("salary"), "points": p.get("points"), "ownership_pct": pct})
    return {
        "username": lineup.get("username"), "rank": lineup.get("rank"), "points": lineup.get("points"),
        "salary_used": salary, "ownership_sum": round(total_own, 1),
        "ownership_unknown": missing, "max_same_team": max(teams.values()) if teams else 0,
        "players": players,
    }


# --------------------------------------------------------------------------
# Mirror into the DraftKings-export tables (NFL showdown only)
# --------------------------------------------------------------------------

def mirror_payload(contest: dict, athletes: list[dict], curve: dict) -> dict:
    """What `nfl_dfs_field_contests` / `nfl_dfs_field_ownership` would store for this contest.

    Refused for anything but an NFL showdown: the athlete table is capped at 50,
    and `calibrate:nfl-ownership` counts a slate player with no field row as 0%
    owned, so mirroring a classic contest would grade the model against a field
    that mostly reads 0%. Showdown ownership is FLEX-only and recorded as such.
    """
    if contest["sport"] != "nfl":
        raise ValueError("mirror is for NFL contests only (the field tables are NFL)")
    if contest["format"] != "showdown":
        raise ValueError("mirror refused for a classic contest: dfsdb returns at most 50 athletes "
                         "and the calibration reads a missing player as 0% owned")
    if not athletes:
        raise ValueError("no athletes to mirror")
    top = curve["score_by_rank"][0][1] if curve.get("score_by_rank") else None
    return {
        "contest_id": f"dfsdb-{contest['dfsdb_id']}",
        "contest_name": contest["contest_name"],
        "format": "showdown",
        "entry_count": contest["total_entries"] or 0,
        "winning_score": top,
        "file_name": contest["source_url"],
        "file_digest": contest["payload_digest"],
        "players": {a["normalized_name"]: {
            "name": a["athlete_name"], "normalized_name": a["normalized_name"],
            "drafted_pct": a["ownership_pct"] or 0.0,
            "drafted_by_slot": {"FLEX": a["ownership_pct"] or 0.0},
            "fpts": a["fantasy_points"],
        } for a in athletes},
    }


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def format_report(contest: dict, athletes: list[dict], standings: list[dict],
                  profiles: list[dict], lineups: list[dict]) -> str:
    curve = contest["payout_curve"]
    lines = [
        f"{contest['contest_name']}",
        f"  {contest['platform'] or '?'} {contest['sport']} {contest['format']}  {contest['contest_date']}  "
        f"${_fmt(contest['buy_in'])} entry  ${_fmt(contest['prize_pool'])} pool  "
        f"{contest['total_entries'] or 0:,} entries",
        f"  payouts: 1st ${_fmt(curve['first_place_prize'])}, min cash ${_fmt(curve['min_cash'])}, "
        f"{curve['cashing_entries'] or 0:,} cashing"
        + (f", cash line {curve['cash_line_points']:.2f} pts" if curve["cash_line_points"] is not None
           else ", cash line not inside fetched standings"),
        f"  standings: {contest['standings_fetched']:,} of {contest['standings_users'] or 0:,} users fetched; "
        f"payout curve resolved at {curve['coverage']:.0%} of fetched ranks (single-entry users only)",
    ]
    if standings:
        lines.append("  top finishers (one row per user; winnings pool all their entries):")
        for r in standings[:10]:
            lines.append(f"    #{r['rank']:<5} {str(r['username'] or '?')[:22]:<22} {r['points'] or 0:8.2f}  "
                         f"${_fmt(r['winnings']):>12}  {r['entry_count'] or 0:>4} entries")
    if athletes:
        cap = " (dfsdb caps this at 50; classic pools are larger)" if len(athletes) >= ATHLETE_CAP else ""
        lines.append(f"  ownership, {len(athletes)} athletes{cap}:")
        for a in athletes[:10]:
            lines.append(f"    {a['athlete_name'][:22]:<22} {a['position'] or '':<4} {a['team'] or '':<4} "
                         f"${a['salary'] or 0:>6,}  {a['fantasy_points'] if a['fantasy_points'] is not None else 0:6.2f} pts  "
                         f"{a['ownership_pct'] or 0:5.1f}%")
    if profiles:
        lines.append(f"  who beat it ({contest['sport']} record on dfsdb):")
        lines.append("    %-20s %6s %7s %7s %7s %6s  %s" % ("USER", "CONT", "ENTRIES", "ENT/CON", "ROI%", "CASH%", "BEST BUY-IN / TYPE"))
        for p in profiles:
            bb, bt = p.get("best_buy_in") or {}, p.get("best_contest_type") or {}
            lines.append("    %-20s %6s %7s %7s %7s %6s  %s / %s" % (
                str(p["display_name"] or "?")[:20], p["contests_played"], p["total_entries"],
                p["entries_per_contest"] if p["entries_per_contest"] is not None else "-",
                p["roi_pct"] if p["roi_pct"] is not None else "-",
                p["cash_rate"] if p["cash_rate"] is not None else "-",
                f"{bb.get('key')} ({bb.get('roi')}%)" if bb else "-",
                f"{bt.get('key')} ({bt.get('roi')}%)" if bt else "-"))
    if lineups:
        lines.append(f"  lineups from dfsdb's top-lineups feed for this contest: {len(lineups)}")
        for l in lineups[:5]:
            lines.append(f"    #{l['rank']} {l['username']} {l['points']} pts, ${l['salary_used']:,} salary, "
                         f"ownership sum {l['ownership_sum']}% ({l['ownership_unknown']} unknown), "
                         f"max same team {l['max_same_team']}")
            lines.append("      " + ", ".join(
                f"{p['position']} {p['name']} ({p['ownership_pct'] if p['ownership_pct'] is not None else '?'}%)"
                for p in l["players"]))
    return "\n".join(lines)


def _fmt(value) -> str:
    return "?" if value is None else f"{value:,.2f}".rstrip("0").rstrip(".")


def _int(value) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _float(value) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
