"""Pure helpers for importing a DraftKings contest from stat-api.com (no I/O).

stat-api (`api.stat-api.com/api/v1/dfs`) stores every lineup of every
DraftKings and FanDuel contest since 2021. What the free surface gives,
measured 2026-10-10:

* `/contests/{id}/standings` -- every entry's row, rank, username, points,
  payout, the user's entry count, and the lineup's stack shape. 1,000 rows a
  call, `from_row` cursor. Each week's flagship contests (the DraftKings
  Millionaire, the biggest Thursday and Monday Showdown) answer in full with
  NO key (`x-preview-exempt: flagship`); a free account's key opens every
  other contest.
* `/contests/{id}/users/{username}/lineups` -- every lineup that user
  entered, seat by seat: slot, player, salary, points and the FIELD's
  ownership of that player IN THAT SLOT (`field_pct`), plus a portfolio
  analysis (stacks, dispersion, chalk exposure, leverage).
* `/slates?date&operator_id=1&sport=nfl` and `/contests?slate_id` -- open
  discovery of a day's slates and their contests (100 per slate without a key).
* `/contests/{id}/download` -- the full lineup file; Pro only (5-row preview).

The contest's `external_id` IS DraftKings' contest id, so everything here is
keyed exactly as the DraftKings export path (`ingest/nfl_dfs_field_audit.py`)
keys it: `nfl_dfs_field_contests.contest_id = "196151357"`. Ownership is
per SLOT (a showdown captain's share and flex share are separate numbers, as
in DraftKings' own export), summed into `drafted_pct` the same way.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any

from model.nfl_dfs_field_audit import normalize_name

VERSION = "statapi-contest-import-v1"
SOURCE = "stat-api"

#: The web's score curve (web/src/lib/nfl-dfs/slate-results.ts): exact ranks
#: 1..100, then one point every 0.5% of the field, then the last rank.
EXACT_TOP_RANKS = 100
CURVE_STEP_SHARE = 0.005


def payload_digest(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def contest_format(contest: dict) -> str:
    game_type = str(contest.get("game_type") or "").lower()
    if game_type in ("showdown", "classic"):
        return game_type
    slots = [str(s).upper() for s in contest.get("slots") or []]
    if "CPT" in slots:
        return "showdown"
    return "showdown" if "showdown" in str(contest.get("name") or "").lower() else "classic"


def contest_row(contest: dict) -> dict:
    """The contest card as the NFL field tables key it (DraftKings' id as `contest_id`)."""
    external = contest.get("external_id")
    if external is None:
        raise ValueError(f"contest {contest.get('contest_id')} has no operator id (external_id)")
    return {
        "statapi_id": int(contest["contest_id"]),
        "contest_id": str(external),
        "contest_name": contest.get("name") or "(unnamed)",
        "format": contest_format(contest),
        "operator": contest.get("operator"),
        "sport": str(contest.get("sport") or "").lower(),
        "date": contest.get("date"),
        "slate_id": contest.get("slate_id"),
        "slate_name": contest.get("slate_name"),
        "entry_fee": (contest.get("entry_fee_cents") or 0) / 100,
        "prize_pool": (contest.get("prize_pool_cents") or 0) / 100,
        "entry_count": int(contest.get("total_entries") or 0),
        "stored_lineups": int(contest.get("stored_lineups") or 0),
        "paid_places": contest.get("paid_places"),
        "slots": list(contest.get("slots") or []),
    }


# --------------------------------------------------------------------------
# Standings
# --------------------------------------------------------------------------

def standings_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows or []:
        if r.get("rank") is None:
            continue
        out.append({
            "row": int(r.get("row") or 0), "rank": int(r["rank"]), "username": r.get("username") or "?",
            "points": _float(r.get("points")), "payout": (r.get("payout_cents") or 0) / 100,
            "user_entries": _int(r.get("user_entries")), "stack": r.get("stack"),
            "stack_team": r.get("stack_team"), "bring_back_team": r.get("bring_back_team"),
        })
    return out


def build_score_curve(scores: list[float]) -> list[list[float]]:
    """Port of the web's buildScoreCurve: [rank, score] for ranks 1..100, every 0.5%, and the last."""
    sorted_scores = sorted((s for s in scores if s is not None), reverse=True)
    n = len(sorted_scores)
    if not n:
        return []
    ranks = set(range(1, min(EXACT_TOP_RANKS, n) + 1))
    step = max(1, round(n * CURVE_STEP_SHARE))
    ranks.update(range(EXACT_TOP_RANKS, n + 1, step))
    ranks.add(n)
    return [[r, sorted_scores[r - 1]] for r in sorted(ranks)]


def standings_summary(rows: list[dict], total_entries: int) -> dict:
    """Winning / median / min score and the curve, filled only when every entry was fetched."""
    scores = sorted((r["points"] for r in rows if r["points"] is not None), reverse=True)
    complete = bool(scores) and len(rows) >= total_entries > 0
    return {
        "fetched": len(rows), "complete": complete,
        "winning_score": scores[0] if scores else None,
        "median_score": scores[len(scores) // 2] if complete else None,
        "min_score": scores[-1] if complete else None,
        "score_curve": build_score_curve(scores) if complete else None,
    }


# --------------------------------------------------------------------------
# Users: lineups, seats, ownership
# --------------------------------------------------------------------------

def user_lineups(payload: dict) -> list[dict]:
    """Each lineup of one user with its roster in seat order."""
    seats_by_row: dict[int, list[dict]] = defaultdict(list)
    for s in payload.get("seats") or []:
        seats_by_row[int(s.get("lineup_row") or 0)].append(s)
    captain = captain_shares(payload)
    user = payload.get("user") or {}
    username = user.get("username") or "?"
    entries = int(user.get("entries") or 0)
    out = []
    for i, lu in enumerate(sorted(payload.get("lineups") or [], key=lambda l: (l.get("rank") or 10**9, l.get("row") or 0)), 1):
        row = int(lu.get("row") or 0)
        seats = sorted(seats_by_row.get(row, []), key=lambda s: s.get("slot_index") or 0)
        roster = [{"slot": s.get("slot"), "name": s.get("name"), "normalized_name": normalize_name(s.get("name")),
                   "position": s.get("position"), "team": s.get("team"), "salary": _int(s.get("salary")),
                   "fantasy_points": _float(s.get("fantasy_points")),
                   "drafted_pct": seat_share(s, captain), "stack_role": s.get("stack_role")} for s in seats]
        out.append({
            "entry_id": f"statapi-{row}", "row": row, "rank": _int(lu.get("rank")),
            "username": username, "user_entries": entries or None,
            "entry_name": f"{username} ({i}/{entries})" if entries > 1 else username,
            "points": _float(lu.get("points")), "payout": (lu.get("payout_cents") or 0) / 100,
            "salary_used": _int(lu.get("salary_used")), "ownership_sum": _float(lu.get("total_ownership")),
            "stack": lu.get("stack"), "stack_team": lu.get("stack_team"), "bring_back_team": lu.get("bring_back_team"),
            "lineup_text": " ".join(f"{r['slot']} {r['name']}" for r in roster),
            "players": roster,
        })
    return out


def user_build(payload: dict, contest_id: str) -> dict:
    """The user's whole portfolio in this contest: the record, stat-api's analysis, exposure, every lineup."""
    user = payload.get("user") or {}
    lineups = user_lineups(payload)
    return {
        "contest_id": contest_id, "username": user.get("username") or "?",
        "entries": _int(user.get("entries")), "best_rank": _int(user.get("best_rank")),
        "cashed": _int(user.get("cashed")), "total_payout": (user.get("total_payout_cents") or 0) / 100,
        "avg_points": _float(user.get("avg_points")), "players_used": _int(user.get("players_used")),
        "analysis": {**(payload.get("analysis") or {}),
                     "field": {k: (payload.get("field") or {}).get(k) for k in
                               ("roster_slots", "pool_size", "players_owned", "avg_points_per_slot", "field_avg_lineup")}},
        "exposure": [{k: e.get(k) for k in ("name", "position", "team", "salary", "fantasy_points", "lineups",
                                             "captain_lineups", "exposure_pct", "field_pct", "leverage", "edge")}
                     for e in payload.get("exposure") or []],
        "lineups": [{"row": l["row"], "rank": l["rank"], "points": l["points"], "payout": l["payout"],
                     "salary_used": l["salary_used"], "ownership_sum": l["ownership_sum"], "stack": l["stack"],
                     "stack_team": l["stack_team"], "bring_back_team": l["bring_back_team"],
                     "roster": [[r["slot"], r["name"], r["team"], r["position"], r["salary"], r["fantasy_points"],
                                 r["drafted_pct"]] for r in l["players"]]} for l in lineups],
        "source": SOURCE,
    }


def captain_shares(payload: dict) -> dict[str, float]:
    """Captain share per player, from CPT seats (a showdown's only slot-specific number)."""
    out: dict[str, float] = {}
    for seat in payload.get("seats") or []:
        if str(seat.get("slot")).upper() == "CPT" and seat.get("field_pct") is not None:
            out.setdefault(normalize_name(seat.get("name")), float(seat["field_pct"]))
    return out


def seat_share(seat: dict, captain: dict[str, float]) -> float | None:
    """The field's share for this seat, in DraftKings' %Drafted convention.

    A seat's `field_pct` is the player's OVERALL ownership (any slot), except a
    CPT seat, which carries the captain share. DraftKings' export lists a
    showdown player twice, CPT and FLEX, and the FLEX row excludes captains, so
    a flex seat here is overall minus the captain share.
    """
    pct = _float(seat.get("field_pct"))
    if pct is None:
        return None
    slot = str(seat.get("slot")).upper()
    key = normalize_name(seat.get("name"))
    if slot == "CPT":
        return pct
    if slot == "FLEX" and key in captain:
        return round(pct - captain[key], 4)
    return pct


def field_ownership(user_payloads: list[dict], contest_id: str) -> dict:
    """Field ownership of every player any fetched user rostered, as the export path stores it.

    `drafted_pct` is the player's overall share (any slot). `drafted_by_slot`
    splits a showdown player into CPT and FLEX exactly as DraftKings' export
    does (FLEX = overall - CPT); a classic player is keyed by his position,
    because stat-api reports one overall number, not a per-slot split. The
    mass (sum of overall shares over roster slots x 100) says how much of the
    field is in hand; players no fetched user used are absent, never 0.
    """
    overall: dict[str, float] = {}
    captain: dict[str, float] = {}
    names: dict[str, str] = {}
    positions: dict[str, str] = {}
    fpts: dict[str, float | None] = {}
    players_owned = None
    slots = 0
    showdown = False
    for payload in user_payloads:
        field = payload.get("field") or {}
        players_owned = players_owned or field.get("players_owned")
        slots = slots or int(field.get("roster_slots") or 0)
        for key, value in captain_shares(payload).items():
            captain.setdefault(key, value)
        for s in payload.get("seats") or []:
            key = normalize_name(s.get("name"))
            if not key or s.get("field_pct") is None:
                continue
            slot = str(s.get("slot")).upper()
            if slot == "CPT":
                showdown = True
                continue
            overall.setdefault(key, float(s["field_pct"]))
            names.setdefault(key, s.get("name"))
            positions.setdefault(key, str(s.get("position") or slot))
            fpts.setdefault(key, _float(s.get("fantasy_points")))
    for key, cpt in captain.items():          # a player seen only as captain
        showdown = True
        overall.setdefault(key, cpt)
        names.setdefault(key, key)
    players = {}
    for key, pct in overall.items():
        if showdown:
            cpt = captain.get(key, 0.0)
            by_slot = {"CPT": round(cpt, 4), "FLEX": round(pct - cpt, 4)}
        else:
            by_slot = {positions.get(key, "FLEX"): pct}
        players[key] = {"name": names.get(key) or key, "normalized_name": key, "drafted_pct": round(pct, 4),
                        "drafted_by_slot": by_slot, "fpts": fpts.get(key)}
    coverage = (len(players) / players_owned) if players_owned else None
    mass = (sum(p["drafted_pct"] for p in players.values()) / (slots * 100)) if slots else None
    return {"contest_id": contest_id, "players": players, "players_owned": players_owned,
            "coverage": round(coverage, 3) if coverage is not None else None,
            "mass": round(mass, 4) if mass is not None else None}


def top_entries(builds_lineups: list[list[dict]], keep_top: int) -> list[dict]:
    """Every fetched lineup ranked within the top N, de-duplicated by row, best rank first."""
    seen, out = set(), []
    for lineups in builds_lineups:
        for l in lineups:
            if l["rank"] is None or l["rank"] > keep_top or l["row"] in seen:
                continue
            seen.add(l["row"])
            out.append(l)
    return sorted(out, key=lambda l: (l["rank"], l["row"]))


# --------------------------------------------------------------------------
# Discovery (slates -> contests)
# --------------------------------------------------------------------------

def listing_contests(contests: list[dict], slate: dict) -> list[dict]:
    rows = []
    for c in contests or []:
        if c.get("id") is None:
            continue
        rows.append({
            "statapi_id": int(c["id"]), "external_id": c.get("external_id"), "name": c.get("name") or "(unnamed)",
            "entry_count": int(c.get("entry_count") or 0), "entry_fee": _float(c.get("entry_fee")),
            "prize_pool": _float(c.get("prize_pool")), "lineups_status": c.get("lineups_status"),
            "slate_id": slate.get("id"), "slate_name": slate.get("name"),
            "format": str(slate.get("game_type") or "").lower() or contest_format({"name": c.get("name")}),
        })
    return rows


def select_contests(rows: list[dict], *, min_entries: int, max_contests: int, search: str | None,
                    formats: tuple[str, ...] = ("classic", "showdown")) -> list[dict]:
    """Biggest fields first, lineups available, one per id."""
    needle = (search or "").lower()
    seen, picked = set(), []
    for r in sorted(rows, key=lambda r: (-r["entry_count"], r["name"])):
        if r["statapi_id"] in seen or r["format"] not in formats or r["entry_count"] < min_entries:
            continue
        if needle and needle not in r["name"].lower():
            continue
        if r.get("lineups_status") not in (None, "available"):
            continue
        seen.add(r["statapi_id"])
        picked.append(r)
        if len(picked) >= max_contests:
            break
    return picked


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def format_report(contest: dict, standings: list[dict], summary: dict, ownership: dict,
                  builds: list[dict], entries: list[dict]) -> str:
    lines = [
        f"{contest['contest_name']}",
        f"  {contest['operator']} {contest['sport']} {contest['format']}  {contest['date']}  ${contest['entry_fee']:g} entry  "
        f"${contest['prize_pool']:,.0f} pool  {contest['entry_count']:,} entries  (DK contest {contest['contest_id']}, "
        f"stat-api {contest['statapi_id']})",
        f"  standings: {summary['fetched']:,} of {contest['entry_count']:,} entries fetched"
        + (f"; win {summary['winning_score']}, median {summary['median_score']}, min {summary['min_score']}"
           if summary["complete"] else f"; top score {summary['winning_score']}"),
    ]
    if ownership["players"]:
        lines.append(f"  ownership covered: {len(ownership['players'])} of {ownership['players_owned'] or '?'} "
                     f"players the field used ({(ownership['coverage'] or 0):.0%} of players, "
                     f"{(ownership['mass'] or 0):.1%} of ownership mass), from {len(builds)} users' seats")
    if standings:
        lines.append("  top of the standings (one row per entry):")
        for r in standings[:10]:
            lines.append(f"    #{r['rank']:<5} {str(r['username'])[:22]:<22} {r['points'] or 0:8.2f}  ${r['payout']:>12,.2f}  "
                         f"{r['user_entries'] or 0:>4} entries  {r['stack'] or '':<14} {r['stack_team'] or ''}")
    if builds:
        lines.append("  how the top finishers built (every lineup they entered):")
        lines.append("    %-20s %5s %5s %6s %12s %7s %8s  %s" % ("USER", "ENT", "CASH", "BEST", "WON", "AVGPTS", "OWN/LU", "SHAPE"))
        for b in builds:
            a = b["analysis"] or {}
            st = a.get("stacks") or {}
            disp = a.get("dispersion") or {}
            own = a.get("ownership") or {}
            shape = []
            if st:
                shape.append(f"QB+2: {st.get('qb_plus_2', 0)}/{a.get('lineups')}  bring-back {st.get('with_bring_back', 0)}  "
                             f"QBs {st.get('distinct_qbs')}")
            if disp:
                shape.append(f"most-used player in {disp.get('most_used_pct')}%")
            lines.append("    %-20s %5s %5s %6s %12s %7s %8s  %s" % (
                str(b["username"])[:20], b["entries"], b["cashed"], b["best_rank"], f"${b['total_payout']:,.0f}",
                b["avg_points"], f"{own.get('avg')}%" if own.get("avg") is not None else "-", "; ".join(shape)))
    if entries:
        lines.append(f"  lineups kept (rank within the top cut): {len(entries)}")
        for l in entries[:5]:
            lines.append(f"    #{l['rank']:<4} {l['username']:<20} {l['points'] or 0:7.2f}  ${l['salary_used'] or 0:,}  "
                         f"own sum {l['ownership_sum'] or 0:5.1f}%  {l['stack'] or ''} {l['stack_team'] or ''}")
            lines.append("      " + ", ".join(f"{p['slot']} {p['name']} ({p['drafted_pct']:.1f}%)" if p["drafted_pct"] is not None
                                              else f"{p['slot']} {p['name']}" for p in l["players"]))
    return "\n".join(lines)


def _int(value):
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _float(value):
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
