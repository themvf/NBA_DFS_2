"""Role-change tag on the flagged unders of nfl-prop-under-v2 (DRAFT registration).

Reads the same priced rows and bets as research/nfl_prop_under_study.py (imported, not
changed), then tags each flagged under whose player lost a same-position teammate to a
fresh absence: a teammate the forecast's own frozen availability request ruled out, who
played his team's previous game with material usage. Reports tagged against untagged
flagged unders. Read-only: it writes nothing to the database.

    python -m research.nfl_prop_under_role_change sizes --season 2026 --week 6
    python -m research.nfl_prop_under_role_change grade [--out path]
"""
from __future__ import annotations

import argparse
import io
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

from research import nfl_prop_under_study as under

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-prop-under-role-change-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())
REQUESTS_ROOT = ROOT / "artifacts" / "nfl-game-leaders"

POSITION_GROUP = {"RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE"}
# Request (Sleeper-style) team spellings that differ from nflverse's game-id codes.
TEAM_ALIASES = {"WSH": "WAS", "LAR": "LA", "JAC": "JAX"}


def position_group(position: str | None) -> str | None:
    return POSITION_GROUP.get((position or "").upper())


def _ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def load_request_index(root: Path = REQUESTS_ROOT) -> dict[tuple[str, datetime], dict]:
    """Every frozen game-leaders availability request, keyed by (game_id, decision_at)."""
    index: dict[tuple[str, datetime], dict] = {}
    for path in sorted(set(root.glob("**/requests*.json")) | set(root.glob("*requests*.json"))):
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        for game_id, request in payload.items():
            if isinstance(request, dict) and "players" in request and request.get("decision_at"):
                index[(game_id, _ts(request["decision_at"]))] = request
    return index


def team_codes(game_id: str, request: Mapping[str, Any]) -> dict[str, str]:
    """Request team abbreviation -> nflverse team code. The game id carries the nflverse
    codes; a request may spell one differently (WSH vs WAS), so match the leftover."""
    away, home = game_id.split("_")[2:4]
    request_teams = sorted({p["team"] for p in request["players"]})
    codes = {away, home}
    mapping = {t: TEAM_ALIASES.get(t, t) for t in request_teams if TEAM_ALIASES.get(t, t) in codes}
    left_req = [t for t in request_teams if t not in mapping]
    left_codes = sorted(codes - set(mapping.values()))
    if len(left_req) == 1 and len(left_codes) == 1:
        mapping[left_req[0]] = left_codes[0]
    return mapping


def usage_window(usage: Mapping[tuple[str, int], Mapping[str, Mapping[str, float]]],
                 team: str, week: int, games: int) -> list[int]:
    """The team's last `games` weeks with stat rows, before `week`."""
    weeks = sorted({w for (t, w) in usage if t == team and w < week})
    return weeks[-games:]


def tag_bet(bet: Mapping[str, Any], request: Mapping[str, Any] | None,
            usage: Mapping[tuple[str, int], Mapping[str, Mapping[str, float]]],
            registration: Mapping[str, Any] = REGISTRATION) -> dict:
    """Tag one flagged under. `usage[(team_code, week)][player_id] = {targets, carries}`."""
    rule = registration["tag"]
    if request is None:
        return {"tag": "unknown", "reason": "no frozen availability request for this forecast"}
    players = {p["identity"]: p for p in request["players"]}
    me = players.get(bet["identity"])
    group = position_group(me["position"]) if me else None
    if me is None or group is None:
        return {"tag": "not_taggable", "reason": "player is not an RB/WR/TE in the request"}
    mapping = team_codes(bet["game_id"], request)
    code = mapping.get(me["team"])
    if code is None:
        return {"tag": "unknown", "reason": f"cannot map team {me['team']} to the game id"}
    window = usage_window(usage, code, bet["week"], rule["window_games"])
    if not window:
        return {"tag": "unknown", "reason": "team has no completed games this season"}
    last = window[-1]
    fresh, continuing, cross = [], [], []
    for p in request["players"]:
        if p["identity"] == bet["identity"] or p["team"] != me["team"] or p.get("status") != "out":
            continue
        rows = [usage[(code, w)][p["identity"]] for w in window if p["identity"] in usage.get((code, w), {})]
        if not rows:
            continue
        carries = float(np.mean([r["carries"] for r in rows]))
        targets = float(np.mean([r["targets"] for r in rows]))
        material = carries >= rule["material_carries"] or targets >= rule["material_targets"]
        if not material:
            continue
        entry = {"identity": p["identity"], "name": p["name"], "position": p["position"],
                 "carries_per_game": round(carries, 2), "targets_per_game": round(targets, 2),
                 "games": len(rows)}
        played_last = p["identity"] in usage.get((code, last), {})
        if position_group(p["position"]) != group:
            if played_last:
                cross.append(entry)
            continue
        (fresh if played_last else continuing).append(entry)
    tag = "fresh_absence" if fresh else ("continuing_absence" if continuing else "none")
    return {"tag": tag, "fresh": fresh, "continuing": continuing, "cross_position_fresh": cross,
            "window_weeks": window}


def load_usage(season: int) -> tuple[dict, dict]:
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    url = NFLVERSE_WEEKLY_STATS_URL.format(season=season)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    frame = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    usage: dict[tuple[str, int], dict[str, dict[str, float]]] = defaultdict(dict)
    for r in frame.itertuples(index=False):
        if isinstance(r.player_id, str):
            usage[(str(r.team), int(r.week))][r.player_id] = {
                "targets": float(r.targets or 0) if r.targets == r.targets else 0.0,
                "carries": float(r.carries or 0) if r.carries == r.carries else 0.0}
    import hashlib
    source = {"url": url, "sha256": hashlib.sha256(response.content).hexdigest(), "rows": int(len(frame))}
    return dict(usage), source


def paired_week_interval(tagged: list[dict], untagged: list[dict], resamples: int, seed: int,
                         level: float = 0.95) -> tuple[float, float] | None:
    """ROI(tagged) - ROI(untagged), resampling game weeks with both arms drawn together."""
    weeks = sorted({b["week"] for b in tagged} | {b["week"] for b in untagged})
    if len(weeks) < 2:
        return None
    def sums(bets):
        s, n = defaultdict(float), defaultdict(int)
        for b in bets:
            s[b["week"]] += b["pnl"]
            n[b["week"]] += 1
        return np.array([s[w] for w in weeks]), np.array([n[w] for w in weeks])
    ts, tn = sums(tagged)
    us, un = sums(untagged)
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(weeks), size=(resamples, len(weeks)))
    tn_d, un_d = tn[draws].sum(axis=1), un[draws].sum(axis=1)
    ok = (tn_d > 0) & (un_d > 0)
    diff = ts[draws].sum(axis=1)[ok] / tn_d[ok] - us[draws].sum(axis=1)[ok] / un_d[ok]
    if len(diff) < resamples * 0.9:
        return None
    tail = (1 - level) / 2
    return float(np.quantile(diff, tail)), float(np.quantile(diff, 1 - tail))


def summarize(bets: list[dict], registration: Mapping[str, Any] = REGISTRATION, final: bool = False) -> dict:
    """`final` is True only once every regular-season game is graded: the single look."""
    graded = [b for b in bets if b["result"] != "void"]
    tagged = [b for b in graded if b["role_change"]["tag"] == "fresh_absence"]
    untagged = [b for b in graded if b["role_change"]["tag"] in ("none", "continuing_absence")]
    boot = registration["bootstrap"]
    def arm(rows):
        out = under.summarize(rows, registration=under_registration(), with_interval=False)
        return out
    floors = registration["floors"]
    floors_met = (len(tagged) >= floors["tagged_bets"] and len({b["week"] for b in tagged}) >= floors["tagged_game_weeks"])
    interval = paired_week_interval(tagged, untagged, boot["resamples"], boot["seed"], boot["interval"])
    verdict = None
    if final:
        if not floors_met or not interval:
            verdict = "INCONCLUSIVE"
        else:
            verdict = ("TAGGED_WORSE" if interval[1] < 0 else "TAGGED_BETTER" if interval[0] > 0 else "NO_DIFFERENCE")
    roi_t = arm(tagged)["roi"]
    roi_u = arm(untagged)["roi"]
    return {
        "study": registration["study"],
        "tagged_fresh_absence": arm(tagged),
        "untagged": arm(untagged),
        "difference_roi": round(roi_t - roi_u, 4) if roi_t is not None and roi_u is not None else None,
        "difference_interval": [round(x, 4) for x in interval] if interval else None,
        "floors": floors, "floors_met": floors_met, "verdict_if_first": verdict,
        "status": "final" if final else "descriptive until the final look",
        "descriptive": {
            "continuing_absence": arm([b for b in graded if b["role_change"]["tag"] == "continuing_absence"]),
            "cross_position_fresh_only": arm([b for b in graded if b["role_change"]["tag"] == "none"
                                              and b["role_change"].get("cross_position_fresh")]),
            "unknown_or_not_taggable": sum(b["role_change"]["tag"] in ("unknown", "not_taggable") for b in graded),
        },
    }


def under_registration() -> dict:
    return json.loads((ROOT / REGISTRATION["parent_registration"]).read_text())


def attach_decisions(cur, rows: list[dict], parent: Mapping[str, Any]) -> None:
    """Give each priced row the decision time of the forecast it was priced from."""
    cur.execute("""SELECT game_id, metric, identity, computed_at, forecast_decision_at FROM nfl_game_leader_market
                   WHERE game_id LIKE %s AND version = %s AND book = %s""",
                (f"{parent['season']}_%", parent["market_version"], parent["book"]))
    decision = {(g, m, i, c): f for g, m, i, c, f in cur.fetchall()}
    for r in rows:
        r["forecast_decision_at"] = decision.get((r["game_id"], r["metric"], r["identity"], r["computed_at"]))


def tag_rows(rows: Iterable[dict], index: Mapping, usage: Mapping, week_of: Mapping[str, int]) -> dict:
    """(game_id, metric, identity) -> tag, for chosen pregame rows."""
    tags = {}
    for r in rows:
        fda = r.get("forecast_decision_at")
        request = index.get((r["game_id"], _ts(fda))) if fda else None
        tags[(r["game_id"], r["metric"], r["identity"])] = tag_bet(
            {**r, "week": week_of[r["game_id"]]}, request, usage)
    return tags


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sizes", help="count tags on one week's flagged rows; no results are read")
    s.add_argument("--week", type=int, required=True)
    g = sub.add_parser("grade")
    g.add_argument("--out", type=Path)
    a = p.parse_args()
    parent = under_registration()
    conn = under._connect()
    cur = conn.cursor()
    rows, kickoffs, weeks, completed = under.load_inputs(cur, parent["season"])
    attach_decisions(cur, rows, parent)
    conn.rollback()
    conn.close()
    index = load_request_index()
    usage, source = load_usage(parent["season"])
    if a.command == "sizes":
        chosen = under.select_rows(rows, kickoffs, None, None, registration=parent)
        flagged = [r for r in chosen if under.flagged(r, parent) and weeks.get(r["game_id"]) == a.week]
        tags = tag_rows(flagged, index, usage, weeks)
        counts = defaultdict(int)
        for t in tags.values():
            counts[t["tag"]] += 1
        examples = sorted((k[0], k[2], k[1], [f["name"] for f in t["fresh"]])
                          for k, t in tags.items() if t["tag"] == "fresh_absence")
        names = {(r["game_id"], r["metric"], r["identity"]): r["player"] for r in flagged}
        print(json.dumps({"week": a.week, "flagged_rows": len(flagged), "tags": counts,
                          "fresh_examples": [(g, names[(g, m, i)], m, f) for g, i, m, f in examples]},
                         indent=1, default=str))
        return
    if not REGISTRATION.get("registered_at"):
        raise SystemExit("DRAFT: registered_at is not set; the study has no cohort yet")
    start = datetime.fromisoformat(REGISTRATION["registered_at"])
    stats, covered, nfl_source = under.load_stats(parent["season"], weeks, completed)
    report = under.grade(rows, kickoffs, weeks, stats, covered, registration=parent)
    c_start, c_end = under.cohort_window(parent)
    chosen = under.select_rows(rows, kickoffs, c_start, c_end, registration=parent)
    tags = tag_rows(chosen, index, usage, weeks)
    bets = [{**b, "role_change": tags[(b["game_id"], b["metric"], b["identity"])]}
            for b in report["bets"] if kickoffs[b["game_id"]] > start]
    last = REGISTRATION["verdict"]["last_week"]
    final = all(g in covered for g, w in weeks.items() if w <= last)
    out = summarize(bets, final=final)
    out.update(evaluated_at=datetime.now(timezone.utc).isoformat(), nflverse_usage=source,
               nflverse_results=nfl_source, parent_study=parent["study"])
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps({**out, "bets": bets}, indent=1, default=str))
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
