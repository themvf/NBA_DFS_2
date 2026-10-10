"""How the top finishers of a DraftKings contest built, and the same breakdown for our build.

Reads what `ingest/statapi_contest.py` stored (`nfl_dfs_field_user_builds`,
`nfl_dfs_field_ownership`, the contest's score curve) and prints, for each of
the top N finishers, the three views stat-api shows: the player table
(exposure vs the field, leverage, edge), every lineup seat by seat, and the
portfolio analysis (stacks, dispersion, ownership, salary). Then it computes
the IDENTICAL analysis for our optimizer run on the slate the contest was
played on, scoring our lineups with the week's actual DK points and placing
each one in the contest's real score curve. Same vocabulary both sides, so the
difference is a comparison, not an impression.

Definitions (matching stat-api's, so the two halves line up):
  exposure   share of the user's lineups with the player
  field      share of the contest's lineups with the player (ownership)
  leverage   exposure - field
  edge       leverage / 100 x (player's points - the field's average points per seat)
  stack      the QB plus his teammates in the lineup (any position but DST);
             bring-back = a player from the QB's opponent
  dispersion most-used player's share; players two lineups share on average;
             how many players a lineup differs from its closest twin by

Usage:
    python -m research.nfl_top_builds --contest 196151357 --top 5
    python -m research.nfl_top_builds --contest 196151357 --top 5 --ours af17aac3-...   # a specific run
    python -m research.nfl_top_builds --contest 196151357 --ours none
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import Counter
from itertools import combinations

from config import load_config
from db.database import DatabaseManager
from model.nfl_dfs_field_audit import normalize_name


# --------------------------------------------------------------------------
# Pure: portfolio analysis over lineups of {"roster": [seat...], ...}
#   seat = {"slot","name","team","position","salary","fpts","field_pct","opponent"}
# --------------------------------------------------------------------------

def stack_shape(roster: list[dict]) -> dict:
    """QB + teammates, and whether the opponent is brought back. Showdown (no QB slot) is 'no QB stack'."""
    qb = next((s for s in roster if (s.get("position") or s.get("slot")) == "QB"), None)
    if not qb or not qb.get("team"):
        return {"qb": None, "teammates": 0, "bring_back": False, "label": "no QB"}
    mates = [s for s in roster if s is not qb and s.get("team") == qb["team"] and (s.get("position") or "") != "DST"]
    opp = qb.get("opponent")
    bring = [s for s in roster if opp and s.get("team") == opp and (s.get("position") or "") != "DST"]
    label = "QB" + "".join("/" + (s.get("position") or "?") for s in mates) + ("+BB" if bring else "")
    return {"qb": qb.get("name"), "qb_team": qb["team"], "teammates": len(mates), "bring_back": bool(bring),
            "bring_back_team": opp if bring else None, "label": label}


def analyze_portfolio(lineups: list[dict], field_avg_per_slot: float | None) -> dict:
    n = len(lineups)
    if not n:
        return {"lineups": 0}
    shapes = [stack_shape(l["roster"]) for l in lineups]
    counts = Counter()
    for sh in shapes:
        if sh["qb"] is None:
            counts["no_qb"] += 1
        elif sh["teammates"] == 0:
            counts["without_stack"] += 1
        elif sh["teammates"] == 1:
            counts["qb_plus_1"] += 1
        elif sh["teammates"] == 2:
            counts["qb_plus_2"] += 1
        else:
            counts["qb_plus_3_or_more"] += 1
        if sh["bring_back"]:
            counts["with_bring_back"] += 1
    usage = Counter()
    for l in lineups:
        for s in l["roster"]:
            usage[normalize_name(s["name"])] += 1
    sets = [frozenset(normalize_name(s["name"]) for s in l["roster"]) for l in lineups]
    shared = [len(a & b) for a, b in combinations(sets, 2)] if n > 1 else []
    twins = [max((len(a & b) for b in sets if b is not a), default=0) for a in sets] if n > 1 else []
    roster_size = max(len(l["roster"]) for l in lineups)
    own_sums = [sum((s.get("field_pct") or 0) for s in l["roster"]) for l in lineups]
    salaries = [l.get("salary") or sum((s.get("salary") or 0) for s in l["roster"]) for l in lineups]
    # Exposure vs field per player.
    first = {}
    for l in lineups:
        for s in l["roster"]:
            first.setdefault(normalize_name(s["name"]), s)
    players = []
    for key, c in usage.items():
        s = first[key]
        exp = 100 * c / n
        field = s.get("field_pct")
        lev = (exp - field) if field is not None else None
        edge = (lev / 100 * (s["fpts"] - field_avg_per_slot)) if (lev is not None and s.get("fpts") is not None
                                                                   and field_avg_per_slot is not None) else None
        players.append({"name": s["name"], "position": s.get("position"), "team": s.get("team"), "salary": s.get("salary"),
                        "fpts": s.get("fpts"), "lineups": c, "exposure_pct": round(exp, 1), "field_pct": field,
                        "leverage": round(lev, 1) if lev is not None else None,
                        "edge": round(edge, 2) if edge is not None else None})
    players.sort(key=lambda p: (-p["lineups"], -(p["fpts"] or 0)))
    return {
        "lineups": n,
        "stacks": {**{k: counts.get(k, 0) for k in ("qb_plus_1", "qb_plus_2", "qb_plus_3_or_more", "without_stack",
                                                    "with_bring_back", "no_qb")},
                   "distinct_qbs": len({sh["qb"] for sh in shapes if sh["qb"]}),
                   "stack_teams": Counter(sh["qb_team"] for sh in shapes if sh.get("qb_team") and sh["teammates"]),
                   "bring_back_teams": Counter(sh["bring_back_team"] for sh in shapes if sh["bring_back_team"])},
        "dispersion": {"players_used": len(usage), "most_used_lineups": usage.most_common(1)[0][1],
                       "most_used_pct": round(100 * usage.most_common(1)[0][1] / n, 1),
                       "shared_avg": round(statistics.mean(shared), 2) if shared else None,
                       "shared_max": max(shared) if shared else None, "shared_min": min(shared) if shared else None,
                       "closest_twin_avg": round(statistics.mean(roster_size - t for t in twins), 2) if twins else None,
                       "closest_twin_min": (roster_size - max(twins)) if twins else None},
        "ownership": {"min": round(min(own_sums), 1), "avg": round(statistics.mean(own_sums), 1), "max": round(max(own_sums), 1)},
        "salary": {"min": min(salaries), "avg": round(statistics.mean(salaries)), "max": max(salaries)},
        "players": players,
        "edge_total": round(sum(p["edge"] for p in players if p["edge"] is not None), 2),
    }


def estimate_rank(score: float, curve: list[list[float]]) -> int | None:
    """1 + entries scoring strictly more, read off the contest's rank->score curve (interpolated)."""
    if not curve:
        return None
    above = -1
    for i, (r, s) in enumerate(curve):
        if s > score:
            above = i
        else:
            break
    if above == -1:
        return 1
    r0, s0 = curve[above]
    if above == len(curve) - 1:
        return int(r0) + 1
    r1, s1 = curve[above + 1]
    if r1 - r0 == 1 or s0 == s1:
        return int(r0) + 1
    frac = (s0 - score) / (s0 - s1)
    return int(round(r0 + frac * (r1 - 1 - r0))) + 1


# --------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------

def fmt_analysis(a: dict, indent: str = "    ") -> list[str]:
    st, d, o, sal = a["stacks"], a["dispersion"], a["ownership"], a["salary"]
    teams = ", ".join(f"{t} {c}" for t, c in sorted(st["stack_teams"].items(), key=lambda x: -x[1])) or "-"
    bb = ", ".join(f"{t} {c}" for t, c in sorted(st["bring_back_teams"].items(), key=lambda x: -x[1])) or "-"
    return [
        f"{indent}STACKS ({a['lineups']} lineups): QB+1 {st['qb_plus_1']}  QB+2 {st['qb_plus_2']}  QB+3+ {st['qb_plus_3_or_more']}  "
        f"no stack {st['without_stack']}  with bring-back {st['with_bring_back']}  different QBs {st['distinct_qbs']}",
        f"{indent}  stack teams: {teams}   bring-back teams: {bb}",
        f"{indent}DISPERSION: players used {d['players_used']}; most-used player in {d['most_used_pct']}% "
        f"({d['most_used_lineups']} lineups); two lineups share {d['shared_avg']} players on average "
        f"(most {d['shared_max']}, least {d['shared_min']}); a lineup differs from its closest twin by "
        f"{d['closest_twin_avg']} (least {d['closest_twin_min']})",
        f"{indent}OWNERSHIP sum of seats per lineup: lowest {o['min']}%  average {o['avg']}%  highest {o['max']}%"
        f"      SALARY: lowest ${sal['min']:,}  average ${sal['avg']:,}  highest ${sal['max']:,}",
    ]


def fmt_players(players: list[dict], limit: int, indent: str = "    ") -> list[str]:
    lines = [f"{indent}{'PLAYER':<24}{'POS':<4}{'TEAM':<5}{'SAL':>6}{'FPTS':>7}{'LU':>4}{'EXP':>7}{'FIELD':>7}{'LEV':>7}{'EDGE':>7}"]
    for p in players[:limit]:
        lines.append(f"{indent}{p['name'][:23]:<24}{(p['position'] or ''):<4}{(p['team'] or ''):<5}{p['salary'] or 0:>6,}"
                     f"{(p['fpts'] if p['fpts'] is not None else 0):>7.1f}{p['lineups']:>4}{p['exposure_pct']:>6.1f}%"
                     f"{(p['field_pct'] if p['field_pct'] is not None else 0):>6.1f}%"
                     f"{(p['leverage'] if p['leverage'] is not None else 0):>+7.1f}{(p['edge'] if p['edge'] is not None else 0):>+7.2f}")
    return lines


def fmt_lineup(l: dict, indent: str = "    ") -> list[str]:
    head = (f"{indent}#{l.get('rank') or '?'}  {l.get('points') or 0:.2f} pts  "
            + (f"${l['payout']:,.0f}  " if l.get("payout") is not None else "")
            + f"{stack_shape(l['roster'])['label']}  salary ${l.get('salary') or 0:,}  own sum "
            f"{sum((s.get('field_pct') or 0) for s in l['roster']):.1f}%"
            + (f"  {l['note']}" if l.get("note") else ""))
    seats = ", ".join(f"{s['slot']} {s['name']} {s['fpts'] if s.get('fpts') is not None else '?'}/{(s.get('field_pct') or 0):.1f}%"
                      for s in l["roster"])
    return [head, f"{indent}  {seats}"]


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------

def load_builds(db, contest_id: str, top: int) -> list[dict]:
    rows = db.execute(
        """SELECT username, entries, best_rank, cashed, total_payout, avg_points, players_used, analysis, exposure, lineups
           FROM nfl_dfs_field_user_builds WHERE contest_id = %s ORDER BY best_rank, total_payout DESC LIMIT %s""",
        (contest_id, top))
    out = []
    for r in rows:
        r = dict(r)
        for k in ("analysis", "exposure", "lineups"):
            if isinstance(r[k], str):
                r[k] = json.loads(r[k])
        out.append(r)
    return out


def build_lineups_from_store(build: dict, opponents: dict[str, str]) -> list[dict]:
    """A stored build's lineups in the analysis shape (roster seats with team/position/salary/fpts/field)."""
    out = []
    for l in build["lineups"]:
        roster = []
        for seat in l["roster"]:
            slot, name = seat[0], seat[1]
            team = seat[2] if len(seat) > 2 else None
            roster.append({"slot": slot, "name": name, "team": team, "position": seat[3] if len(seat) > 3 else slot,
                           "salary": seat[4] if len(seat) > 4 else None, "fpts": seat[5] if len(seat) > 5 else None,
                           "field_pct": seat[6] if len(seat) > 6 else None, "opponent": opponents.get(team)})
        out.append({"rank": l.get("rank"), "points": l.get("points"), "payout": l.get("payout"),
                    "salary": l.get("salary_used"), "roster": roster})
    return out


def load_ours(db, contest: dict, run_id: str | None) -> tuple[dict | None, list[dict]]:
    """Our optimizer run on the contest's slate, scored with the week's actual DK points and field ownership."""
    if not contest["slate_upload_id"]:
        return None, []
    if run_id:
        runs = db.execute("SELECT run_id, created_at, generated_lineups, mode FROM nfl_dfs_optimizer_runs WHERE run_id = %s", (run_id,))
    else:
        runs = db.execute(
            """SELECT run_id, created_at, generated_lineups, mode FROM nfl_dfs_optimizer_runs
               WHERE upload_id = %s AND generated_lineups > 0 ORDER BY created_at DESC LIMIT 1""", (contest["slate_upload_id"],))
    if not runs:
        return None, []
    run = dict(runs[0])
    players = {str(r["dk_player_id"]): dict(r) for r in db.execute(
        "SELECT dk_player_id, name, position, team, opponent, salary FROM nfl_dfs_slate_players WHERE upload_id = %s",
        (contest["slate_upload_id"],))}
    actual = {}
    for r in db.execute(
        """SELECT f.canonical_name AS name, r.position, r.team, r.actual_dk_fpts AS pts FROM nfl_dfs_player_week_results r
           JOIN ff_players f ON f.id = r.player_id WHERE r.season = %s AND r.week = %s""", (contest["season"], contest["week"])):
        actual.setdefault(normalize_name(r["name"]), r["pts"])
        if r["position"] == "DST":
            actual.setdefault(normalize_name(r["team"]), r["pts"])
    field = {r["normalized_name"]: dict(r) for r in db.execute(
        "SELECT normalized_name, drafted_pct, drafted_by_slot FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest["contest_id"],))}
    lineups = []
    for l in db.execute("SELECT lineup_number, slots, total_salary FROM nfl_dfs_lineups WHERE run_id = %s ORDER BY lineup_number", (run["run_id"],)):
        slots = l["slots"] if isinstance(l["slots"], list) else json.loads(l["slots"])
        roster = []
        for s in slots:
            p = players.get(str(s.get("dkPlayerId"))) or {}
            name = s.get("name") or p.get("name")
            key = normalize_name(name)
            pts = actual.get(key)
            if pts is None and p.get("position") == "DST":
                pts = actual.get(normalize_name(p.get("team")))
            fo = field.get(key)
            slot = re.sub(r"\d+$", "", str(s.get("slot")))
            pct = None
            if fo:
                by = fo["drafted_by_slot"] if isinstance(fo["drafted_by_slot"], dict) else json.loads(fo["drafted_by_slot"] or "{}")
                pct = by.get("CPT") if slot == "CPT" and "CPT" in by else fo["drafted_pct"]
            roster.append({"slot": slot, "name": name, "team": p.get("team") or s.get("team"), "position": p.get("position"),
                           "salary": p.get("salary") or s.get("salary"), "fpts": pts, "field_pct": pct, "opponent": p.get("opponent")})
        points = sum((r["fpts"] or 0) for r in roster)
        lineups.append({"rank": None, "points": round(points, 2), "salary": l["total_salary"], "roster": roster,
                        "lineup_number": l["lineup_number"]})
    return run, lineups


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contest", help="DraftKings contest id (default: the newest stat-api import)")
    parser.add_argument("--top", type=int, default=5, help="top finishers to show (default 5)")
    parser.add_argument("--ours", default="latest", help="our optimizer run id, 'latest' on the slate (default), or 'none'")
    parser.add_argument("--players", type=int, default=12, help="players per table (default 12)")
    parser.add_argument("--lineups", type=int, default=5, help="lineups shown per user (default 5; all of them in the analysis)")
    args = parser.parse_args(argv)

    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    where = "contest_id = %s" if args.contest else "file_name LIKE 'stat-api:%%'"
    rows = db.execute(f"""SELECT contest_id, contest_name, format, season, week, slate_upload_id, entry_count,
                                 winning_score, median_score, score_curve FROM nfl_dfs_field_contests
                          WHERE {where} ORDER BY imported_at DESC LIMIT 1""", (args.contest,) if args.contest else None)
    if not rows:
        raise SystemExit("no such contest imported; run python -m ingest.statapi_contest first")
    contest = dict(rows[0])
    curve = contest["score_curve"] if isinstance(contest["score_curve"], list) else json.loads(contest["score_curve"] or "[]")
    builds = load_builds(db, contest["contest_id"], args.top)
    if not builds:
        raise SystemExit(f"contest {contest['contest_id']} has no stored user builds")
    field_block = (builds[0]["analysis"] or {}).get("field") or {}
    avg_slot = field_block.get("avg_points_per_slot")
    opponents = {r["team"]: r["opponent"] for r in db.execute(
        "SELECT DISTINCT team, opponent FROM nfl_dfs_slate_players WHERE upload_id = %s", (contest["slate_upload_id"],))} \
        if contest["slate_upload_id"] else {}

    out = [f"{contest['contest_name']}  (DK {contest['contest_id']}, {contest['format']}, {contest['season']} week {contest['week']})",
           f"  {contest['entry_count']:,} entries; winning {contest['winning_score']}, median {contest['median_score']}; "
           f"field average {field_block.get('field_avg_lineup')} per lineup, {avg_slot} per seat"]
    for b in builds:
        lineups = build_lineups_from_store(b, opponents)
        a = analyze_portfolio(lineups, avg_slot)
        out += ["", f"== #{b['best_rank']}  {b['username']}: {b['entries']} entries, cashed {b['cashed']}, won ${b['total_payout']:,.0f}, "
                    f"avg lineup {b['avg_points']} (field {field_block.get('field_avg_lineup')}), players used {b['players_used']}, "
                    f"edge total {a['edge_total']:+.2f}"]
        out += fmt_analysis(a)
        out.append("    PLAYERS (by exposure):")
        out += fmt_players(a["players"], args.players, indent="      ")
        shown = sorted(lineups, key=lambda l: (l["rank"] or 10**9))[:args.lineups]
        out.append(f"    LINEUPS (best {len(shown)} of {len(lineups)}):")
        for l in shown:
            out += fmt_lineup(l, indent="      ")

    if args.ours != "none":
        run, ours = load_ours(db, contest, None if args.ours == "latest" else args.ours)
        if not ours:
            out += ["", "== OURS: no optimizer run with lineups on this contest's slate"
                    + ("" if contest["slate_upload_id"] else " (contest not linked to a slate)")]
        else:
            for l in ours:
                l["rank"] = estimate_rank(l["points"], curve)
            a = analyze_portfolio(ours, avg_slot)
            scores = sorted((l["points"] for l in ours), reverse=True)
            paid = next((s for r, s in curve if r >= (contest["entry_count"] or 0) * 0.23), None)
            out += ["", f"== OURS: run {run['run_id'][:8]} ({run['mode']}, {run['created_at']:%Y-%m-%d %H:%M} UTC), {len(ours)} lineups, "
                        f"scored with actual points: best {scores[0]:.2f} (~#{estimate_rank(scores[0], curve):,} of {contest['entry_count']:,}), "
                        f"median {statistics.median(scores):.2f}, field median {contest['median_score']}; edge total {a['edge_total']:+.2f}"]
            out += fmt_analysis(a)
            out.append("    PLAYERS (by exposure):")
            out += fmt_players(a["players"], args.players, indent="      ")
            out.append(f"    LINEUPS (best {min(args.lineups, len(ours))} of {len(ours)}, rank estimated from the contest's score curve):")
            for l in sorted(ours, key=lambda l: -l["points"])[:args.lineups]:
                out += fmt_lineup({**l, "note": f"(our #{l['lineup_number']})"}, indent="      ")
    print("\n".join(out))


if __name__ == "__main__":
    main()
