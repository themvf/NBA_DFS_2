"""College football DraftKings Classic: baseline projections and a lineup set.

UNVALIDATED BASELINE. Built on 2026-09-25 for the same-night slate; nothing
here has been graded against results yet. Every constant below is a stated
prior, not a fitted value.

Projection, per DK player:
  1. Match to CFBD player-game rows (cfb_player_game_stats) by normalized name,
     preferring the CFBD id that played for this DK team in 2026. The CFBD id
     follows a transfer, so his old school's games come with him.
  2. Rate = (2026 points + W_PRIOR * 2025 points) /
            (2026 appearances + MISSED_GAME_WEIGHT * 2026 missed team games
             + W_PRIOR * 2025 appearances).
     2026 dominates: a full 2025 season counts about as much as one 2026 game,
     because transfers and new starters make last year's role a poor guide.
     A missed 2026 game counts half: a starter who sat one game hurt is dented,
     not cut by a third, and a backup with one appearance still reads as one.
  3. Environment: rate * (team implied total / team 2026 points per game)^ENV_POWER,
     the ratio clamped to [0.6, 1.4]. A 20-point favourite that scored 45 a
     game against weak teams is pulled down; a team facing a soft defence up.
  4. OUT, O and D players are excluded; Q is kept and flagged.

Lineups (DraftKings CFB Classic): QB, RB, RB, WR, WR, WR, FLEX (RB/WR),
SUPER FLEX (QB/RB/WR); $50,000 cap; players from at least 2 games. Exactly
equivalent to: 8 players, 1-2 QBs, at least 2 RBs, at least 3 WRs, no other
positions. GPP lineups perturb projections per lineup (seeded), cap exposure,
and require MIN_UNIQUE different players from every earlier lineup.

    python -m model.cfb_dfs_baseline "C:/path/DKSalaries.csv" --lineups 20
"""
from __future__ import annotations

import argparse
import csv
import math
import random
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pulp

from config import load_config
from db.database import DatabaseManager

VERSION = "cfb-dfs-baseline-v1"
W_PRIOR = 0.1
MISSED_GAME_WEIGHT = 0.5
SURNAME_MATCH_TOLERANCE = 1.0
ENV_POWER = 0.5
SALARY_CAP = 50_000
MIN_UNIQUE = 2
MAX_EXPOSURE = 0.7
NOISE_SD = 0.18
SEED = 20260925
EXCLUDED_STATUS = {"OUT", "O", "D", "DOUBTFUL", "IR"}
DK_TEAM_NAMES = {"NAVY": "Navy", "UAB": "UAB", "NW": "Northwestern", "IU": "Indiana", "CLEM": "Clemson",
                 "CAL": "California"}
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    parts = [p for p in re.split(r"[^a-z0-9]+", text) if p and p not in SUFFIXES]
    return "".join(parts)


def surname(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    parts = [p for p in re.split(r"[^a-z0-9]+", text) if p and p not in SUFFIXES]
    return parts[-1] if parts else ""


def read_dk(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    out = []
    for r in rows:
        away, home = r["Game Info"].split(" ")[0].split("@")
        out.append({"dk_id": r["ID"], "name": r["Name"], "position": r["Position"], "salary": int(r["Salary"]),
                    "team": r["TeamAbbrev"], "opponent": home if r["TeamAbbrev"] == away else away,
                    "game": f"{away}@{home}", "dk_avg": float(r["AvgPointsPerGame"] or 0),
                    "status": (r.get("Status") or "").strip().upper()})
    return out


def load_history(db: DatabaseManager, season: int):
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT cfbd_player_id, player_name, team, season, week, season_type, cfbd_game_id, dk_points
                       FROM cfb_player_game_stats WHERE season IN (%s, %s)""", (season, season - 1))
        stats = cur.fetchall()
        cur.execute("""SELECT team, season, count(DISTINCT cfbd_game_id) AS games FROM cfb_player_game_stats
                       WHERE season IN (%s, %s) GROUP BY team, season""", (season, season - 1))
        team_games = {(r["team"], r["season"]): r["games"] for r in cur.fetchall()}
        cur.execute("""SELECT t.name AS team, avg(CASE WHEN m.home_team_id = t.team_id THEN m.home_score ELSE m.away_score END) AS ppg
                       FROM cfb_matchups m JOIN cfb_teams t ON t.team_id IN (m.home_team_id, m.away_team_id)
                       WHERE m.season = %s AND m.completed GROUP BY t.name""", (season,))
        team_ppg = {r["team"]: float(r["ppg"]) for r in cur.fetchall() if r["ppg"] is not None}
    return stats, team_games, team_ppg


def load_implied(db: DatabaseManager, games: set[str]) -> dict[str, float]:
    """Implied team totals for the slate's games, by DK abbreviation."""
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT a.name AS away, h.name AS home, m.away_implied, m.home_implied
                       FROM cfb_matchups m JOIN cfb_teams h ON h.team_id = m.home_team_id
                       JOIN cfb_teams a ON a.team_id = m.away_team_id
                       WHERE m.commence_time BETWEEN NOW() - INTERVAL '12 hours' AND NOW() + INTERVAL '36 hours'""")
        by_name = {}
        for r in cur.fetchall():
            by_name[r["away"]] = r["away_implied"]
            by_name[r["home"]] = r["home_implied"]
    name_of = DK_TEAM_NAMES
    return {abbr: float(by_name[name_of[abbr]]) for game in games for abbr in game.split("@")
            if name_of.get(abbr) in by_name and by_name[name_of[abbr]] is not None}


def project(players: list[dict], stats, team_games, team_ppg, implied, season: int) -> list[dict]:
    by_name = defaultdict(list)
    by_team_surname = defaultdict(set)
    for row in stats:
        by_name[normalize(row["player_name"])].append(row)
        if row["season"] == season:
            by_team_surname[(row["team"], surname(row["player_name"]))].add(row["cfbd_player_id"])
    by_id = defaultdict(list)
    for row in stats:
        by_id[row["cfbd_player_id"]].append(row)
    for p in players:
        team = DK_TEAM_NAMES.get(p["team"], p["team"])
        rows = by_name.get(normalize(p["name"]), [])
        ids = {r["cfbd_player_id"] for r in rows if r["season"] == season and r["team"] == team}
        if len(ids) != 1:
            ids = {r["cfbd_player_id"] for r in rows} if len({r["cfbd_player_id"] for r in rows}) == 1 else set()
        method = "name" if ids else None
        if not ids:
            # DK uses nicknames CFBD does not (Rod/Roderick, Bam/Braylon, Ty/Tiaquelin):
            # accept the ONE 2026 player on this team with the same surname.
            # DK's average comes from the same 2026 games, so a true match agrees
            # with CFBD's per-appearance mean; two UAB Robinsons proved it is needed.
            same = by_team_surname.get((team, surname(p["name"])), set())
            if len(same) == 1:
                games26 = [r["dk_points"] for r in by_id[next(iter(same))] if r["season"] == season]
                if games26 and abs(sum(games26) / len(games26) - p["dk_avg"]) <= SURNAME_MATCH_TOLERANCE:
                    ids, method = same, "team+surname"
        mine = [r for rid in ids for r in by_id[rid]]
        cur = [r for r in mine if r["season"] == season]
        prior = [r for r in mine if r["season"] == season - 1]
        missed = max(0, team_games.get((team, season), 0) - len(cur))
        denom = len(cur) + MISSED_GAME_WEIGHT * missed + W_PRIOR * len(prior)
        rate = (sum(r["dk_points"] for r in cur) + W_PRIOR * sum(r["dk_points"] for r in prior)) / denom if denom else 0.0
        env = 1.0
        if p["team"] in implied and team_ppg.get(team):
            env = max(0.6, min(1.4, implied[p["team"]] / team_ppg[team])) ** ENV_POWER
        excluded = p["status"] in EXCLUDED_STATUS
        p.update({"proj": 0.0 if excluded else round(rate * env, 2), "rate": round(rate, 2), "env": round(env, 3),
                  "games_2026": len(cur), "games_2025": len(prior), "matched": bool(mine), "match": method,
                  "excluded": excluded, "flag": "Q" if p["status"] == "Q" else ""})
    return players


def build_lineups(players: list[dict], n: int, min_proj: float = 1.0) -> list[list[dict]]:
    pool = [p for p in players if not p["excluded"] and p["proj"] >= min_proj]
    rng = random.Random(SEED)
    lineups: list[list[dict]] = []
    counts: dict[str, int] = defaultdict(int)
    cap = max(1, math.floor(MAX_EXPOSURE * n))
    for i in range(n):
        noisy = {p["dk_id"]: p["proj"] * (math.exp(rng.gauss(0, NOISE_SD)) if i else 1.0) for p in pool}
        prob = pulp.LpProblem(f"cfb_{i}", pulp.LpMaximize)
        x = {p["dk_id"]: pulp.LpVariable(f"x_{p['dk_id']}", cat="Binary") for p in pool}
        prob += pulp.lpSum(noisy[k] * v for k, v in x.items())
        by_pos = lambda pos: [x[p["dk_id"]] for p in pool if p["position"] == pos]
        prob += pulp.lpSum(x.values()) == 8
        prob += pulp.lpSum(p["salary"] * x[p["dk_id"]] for p in pool) <= SALARY_CAP
        prob += pulp.lpSum(by_pos("QB")) >= 1
        prob += pulp.lpSum(by_pos("QB")) <= 2
        prob += pulp.lpSum(by_pos("RB")) >= 2
        prob += pulp.lpSum(by_pos("WR")) >= 3
        games = sorted({p["game"] for p in pool})
        use = {g: pulp.LpVariable(f"g_{j}", cat="Binary") for j, g in enumerate(games)}
        for g in games:
            prob += use[g] <= pulp.lpSum(x[p["dk_id"]] for p in pool if p["game"] == g)
        prob += pulp.lpSum(use.values()) >= 2
        for k in x:
            if counts[k] >= cap:
                prob += x[k] == 0
        for prev in lineups:
            prob += pulp.lpSum(x[p["dk_id"]] for p in prev) <= 8 - MIN_UNIQUE
        prob.solve(pulp.PULP_CBC_CMD(msg=False))
        if pulp.LpStatus[prob.status] != "Optimal":
            break
        chosen = [p for p in pool if x[p["dk_id"]].value() > 0.5]
        for p in chosen:
            counts[p["dk_id"]] += 1
        lineups.append(chosen)
    return lineups


def assign_slots(lineup: list[dict]) -> list[dict]:
    qbs = sorted([p for p in lineup if p["position"] == "QB"], key=lambda p: -p["proj"])
    rbs = sorted([p for p in lineup if p["position"] == "RB"], key=lambda p: -p["proj"])
    wrs = sorted([p for p in lineup if p["position"] == "WR"], key=lambda p: -p["proj"])
    slots = [qbs[0], rbs[0], rbs[1], wrs[0], wrs[1], wrs[2]]
    rest = rbs[2:] + wrs[3:]
    flex = rest.pop(0)
    super_flex = qbs[1] if len(qbs) > 1 else rest.pop(0)
    return slots + [flex, super_flex]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("salaries")
    parser.add_argument("--lineups", type=int, default=20)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    players = read_dk(Path(args.salaries))
    stats, team_games, team_ppg = load_history(db, args.season)
    implied = load_implied(db, {p["game"] for p in players})
    players = project(players, stats, team_games, team_ppg, implied, args.season)
    out = Path(args.out or Path(args.salaries).with_name("cfb_baseline"))
    out.mkdir(parents=True, exist_ok=True)

    with (out / "projections.csv").open("w", newline="", encoding="utf-8") as handle:
        w = csv.writer(handle)
        w.writerow(["Name", "ID", "Pos", "Team", "Salary", "Proj", "Rate", "Env", "Games2026", "Games2025", "DK avg", "Status"])
        for p in sorted(players, key=lambda p: -p["proj"]):
            w.writerow([p["name"], p["dk_id"], p["position"], p["team"], p["salary"], p["proj"], p["rate"], p["env"],
                        p["games_2026"], p["games_2025"], p["dk_avg"], p["status"]])
    lineups = build_lineups(players, args.lineups)
    with (out / "dk_upload.csv").open("w", newline="", encoding="utf-8") as handle:
        w = csv.writer(handle)
        w.writerow(["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"])
        for lineup in lineups:
            w.writerow([p["dk_id"] for p in assign_slots(lineup)])
    with (out / "lineups_readable.csv").open("w", newline="", encoding="utf-8") as handle:
        w = csv.writer(handle)
        w.writerow(["#", "QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX", "Salary", "Proj"])
        for i, lineup in enumerate(lineups, 1):
            w.writerow([i, *[f"{p['name']} ({p['team']})" for p in assign_slots(lineup)],
                        sum(p["salary"] for p in lineup), round(sum(p["proj"] for p in lineup), 1)])

    print(f"{VERSION}: implied totals {implied}")
    matched = [p for p in players if p["matched"]]
    print(f"Matched {len(matched)}/{len(players)} DK players to CFBD history")
    print("\nTop projections:")
    for p in sorted(players, key=lambda p: -p["proj"])[:25]:
        print(f"  {p['name']:<26} {p['position']:>2} {p['team']:<4} ${p['salary']:>5}  proj {p['proj']:>5}  "
              f"rate {p['rate']:>5} env {p['env']:>5}  g26 {p['games_2026']} g25 {p['games_2025']}  dk {p['dk_avg']:>5} {p['status']}")
    notable = [p for p in players if not p["matched"] and p["dk_avg"] >= 3]
    if notable:
        print("\nNo CFBD history but DK average >= 3:", ", ".join(f"{p['name']} ({p['team']} {p['dk_avg']})" for p in notable))
    print(f"\n{len(lineups)} lineups -> {out}")
    exposure = defaultdict(int)
    for lineup in lineups:
        for p in lineup:
            exposure[p["name"]] += 1
    print("Exposure:", ", ".join(f"{k} {v}" for k, v in sorted(exposure.items(), key=lambda kv: -kv[1])[:16]))


if __name__ == "__main__":
    main()
