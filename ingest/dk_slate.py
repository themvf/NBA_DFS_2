"""Ingest DraftKings NBA salary CSV + LineStar projections into dk_players.

Usage:
    python -m ingest.dk_slate --dk DKSalaries.csv --linestar LineStar.csv
    python -m ingest.dk_slate --draft-group-id 144324 --linestar-api
    python -m ingest.dk_slate --contest-id 189058648 --linestar LineStar.csv

Pipeline for each player:
  1. Merge DK + LineStar by name + salary
  2. Match team_abbrev → team_id via abbreviation cache (30 NBA teams, trivial)
  3. Find today's nba_matchup for that team → matchup_id + vegas_total
  4. Load nba_team_stats for team + opponent → pace + def_rtg
  5. Fuzzy-match player name → nba_player_stats within same team
  6. Compute our independent projection (pace / defense adjusted, no blowout curve)
  7. Compute GPP leverage (low ownership × ceiling bonus)
  8. Save to dk_slates + dk_players
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import sys
from datetime import datetime

from rapidfuzz import fuzz, process

from config import load_config
from db.database import DatabaseManager
from db.queries import build_team_abbrev_cache, upsert_dk_player, upsert_dk_slate
from model.dfs_projections import compute_baseline_ownership, compute_leverage, compute_monte_carlo, compute_our_projection

logger = logging.getLogger(__name__)

# DK NBA abbreviation overrides — much simpler than NCAA (30 teams, standard codes).
DK_ABBREV_OVERRIDES: dict[str, str] = {
    "GS":  "GSW",
    "SA":  "SAS",
    "NO":  "NOP",
    "NY":  "NYK",
    "PHO": "PHX",
    "OKL": "OKC",
    "UTH": "UTA",
}


# ── CSV Parsers ──────────────────────────────────────────────


def parse_dk_csv(content: str) -> list[dict]:
    """Parse DraftKings salary CSV.

    Columns: Position, Name+ID, Name, ID, Roster Position, Salary,
             Game Info, TeamAbbrev, AvgPointsPerGame
    """
    players = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        name = (row.get("Name") or "").strip()
        if not name:
            continue
        dk_id_str = (row.get("ID") or "").strip()
        if not dk_id_str:
            continue
        salary_str = (row.get("Salary") or "0").replace("$", "").replace(",", "").strip()
        players.append({
            "name":               name,
            "dk_id":              int(dk_id_str),
            "team_abbrev":        (row.get("TeamAbbrev") or "").strip().upper(),
            "eligible_positions": (row.get("Roster Position") or "UTIL").strip(),
            "salary":             int(salary_str) if salary_str.isdigit() else 0,
            "game_info":          (row.get("Game Info") or "").strip(),
            "avg_fpts_dk":        _safe_float(row.get("AvgPointsPerGame")),
        })
    return players


def parse_linestar_csv(content: str) -> dict[tuple, dict]:
    """Parse LineStar CSV into lookup map keyed by (name_lower, salary_int).

    Columns (positional): Pos, Team(blank/logo), Player, Salary($X),
                          projOwn%, actualOwn%, Diff, Proj
    """
    lookup: dict[tuple, dict] = {}
    reader = csv.reader(io.StringIO(content))
    for i, row in enumerate(reader):
        if i == 0 or len(row) < 8:
            continue
        player_name = row[2].strip()
        salary_str  = row[3].strip().replace("$", "").replace(",", "")
        proj_own    = _safe_float(row[4].strip().replace("%", "")) or 0.0
        proj        = _safe_float(row[7].strip()) or 0.0
        if not player_name or (proj == 0.0 and proj_own == 0.0):
            continue
        salary = int(salary_str) if salary_str.isdigit() else 0
        is_out = proj == 0.0
        lookup[(player_name.lower(), salary)] = {
            "linestar_proj": proj,
            "linestar_own_pct": proj_own,
            "proj_own_pct":  proj_own,
            "is_out":        is_out,
        }
    return lookup


# ── Team + Player Matching ───────────────────────────────────


def match_team_id(abbrev: str, cache: dict[str, int]) -> int | None:
    """Resolve DK team abbreviation to team_id.

    NBA abbreviations are standardized — direct lookup after override check.
    """
    a = abbrev.strip().upper()
    canonical = DK_ABBREV_OVERRIDES.get(a, a)
    return cache.get(canonical)


def match_player_stats(dk_name: str, candidates: list[dict]) -> dict | None:
    """Fuzzy-match DK player name to nba_player_stats within the same team."""
    if not candidates:
        return None
    names  = [p["name"] for p in candidates]
    result = process.extractOne(
        dk_name, names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=75,
    )
    if result:
        return candidates[names.index(result[0])]
    return None


# ── Projection Pipeline ──────────────────────────────────────


def build_player_pool(
    db: DatabaseManager,
    dk_players: list[dict],
    linestar_map: dict[tuple, dict],
    slate_date: str,
    season: str = "2025-26",
) -> list[dict]:
    """Merge DK, LineStar, and DB data into an enriched player pool."""
    from datetime import date as _date

    # Single-query caches
    abbrev_cache = build_team_abbrev_cache(db)

    # Today's matchups (home + away team → matchup row)
    matchups = db.execute(
        """
        SELECT id, home_team_id, away_team_id, vegas_total, home_ml, away_ml
        FROM nba_matchups
        WHERE game_date = %s
        """,
        (slate_date,),
    )
    matchup_by_team: dict[int, dict] = {}
    for m in matchups:
        matchup_by_team[m["home_team_id"]] = m
        matchup_by_team[m["away_team_id"]] = m

    # Team pace + ratings for all teams with matchups today
    active_ids = list(
        {m["home_team_id"] for m in matchups} | {m["away_team_id"] for m in matchups}
    )
    if active_ids:
        placeholders = ",".join(["%s"] * len(active_ids))
        ratings = db.execute(
            f"""
            SELECT team_id, pace, off_rtg, def_rtg
            FROM nba_team_stats
            WHERE season = %s AND team_id IN ({placeholders})
            """,
            [season] + active_ids,
        )
    else:
        ratings = []
    ratings_by_team: dict[int, dict] = {r["team_id"]: r for r in ratings}

    # Player stats for active teams only (includes fpts_std for Monte Carlo)
    if active_ids:
        player_stats = db.execute(
            f"""
            SELECT name, team_id, avg_minutes, ppg, rpg, apg,
                   spg, bpg, tovpg, threefgm_pg, usage_rate, dd_rate, fpts_std
            FROM nba_player_stats
            WHERE season = %s AND team_id IN ({placeholders})
            """,
            [season] + active_ids,
        )
    else:
        player_stats = []
    players_by_team: dict[int, list[dict]] = {}
    for ps in player_stats:
        players_by_team.setdefault(ps["team_id"], []).append(ps)

    # Days of rest per team: query last game before today to compute rest days.
    # B2B detection relies on nba_matchups history being populated.
    today = _date.fromisoformat(slate_date)
    team_days_rest: dict[int, int | None] = {}
    for tid in active_ids:
        rows = db.execute(
            """
            SELECT MAX(game_date) AS last_game
            FROM nba_matchups
            WHERE game_date < %s AND (home_team_id = %s OR away_team_id = %s)
            """,
            (slate_date, tid, tid),
        )
        last_game = rows[0]["last_game"] if rows and rows[0]["last_game"] else None
        if last_game:
            if isinstance(last_game, str):
                last_game = _date.fromisoformat(last_game)
            team_days_rest[tid] = (today - last_game).days
        else:
            team_days_rest[tid] = None

    # Process each player
    enriched = []
    matched_linestar = matched_team = matched_stats = 0

    for p in dk_players:
        result = dict(p)

        # DK injury status — authoritative; set before LineStar merge
        dk_is_out = p.get("is_disabled", False) or p.get("dk_status", "None").upper() in ("O", "OUT")

        # LineStar merge
        ls_key  = (p["name"].lower(), p["salary"])
        ls_data = linestar_map.get(ls_key)
        if ls_data is None:
            for (ls_name, ls_sal), ls_info in linestar_map.items():
                if ls_sal == p["salary"] and fuzz.token_sort_ratio(p["name"].lower(), ls_name) >= 85:
                    ls_data = ls_info
                    break
        if ls_data:
            result.update(ls_data)
            matched_linestar += 1
        else:
            result["linestar_proj"] = None
            result["linestar_own_pct"] = None
            result["proj_own_pct"]  = None
            result["is_out"]        = False

        # Combine DK status + LineStar signal; DK is authoritative
        result["is_out"] = dk_is_out or result.get("is_out", False)

        # Team + matchup lookup
        team_id = match_team_id(p["team_abbrev"], abbrev_cache)
        result["team_id"] = team_id
        if team_id:
            matched_team += 1

        matchup = matchup_by_team.get(team_id) if team_id else None
        result["matchup_id"] = matchup["id"] if matchup else None

        # Player stats + our projection
        stats    = match_player_stats(p["name"], players_by_team.get(team_id or -1, []))
        our_proj = None
        proj_floor = proj_ceiling = boom_rate = None
        if stats and matchup and team_id:
            is_home   = (matchup["home_team_id"] == team_id)
            opp_id    = matchup["away_team_id"] if is_home else matchup["home_team_id"]
            team_r    = ratings_by_team.get(team_id, {})
            opp_r     = ratings_by_team.get(opp_id, {})
            days_rest = team_days_rest.get(team_id)
            our_proj  = compute_our_projection(
                stats, team_r, opp_r,
                vegas_total=matchup.get("vegas_total"),
                home_ml=matchup.get("home_ml"),
                away_ml=matchup.get("away_ml"),
                is_home=is_home,
                days_rest=days_rest,
            )
            if our_proj:
                matched_stats += 1
                # Monte Carlo: simulate FPTS distribution using historical variance
                fpts_std = stats.get("fpts_std")
                if fpts_std and fpts_std > 0:
                    proj_floor, proj_ceiling, boom_rate = compute_monte_carlo(our_proj, fpts_std)
        result["our_proj"]    = our_proj
        result["proj_floor"]  = proj_floor
        result["proj_ceiling"] = proj_ceiling
        result["boom_rate"]   = boom_rate

        # Leverage
        is_out = result.get("is_out", False)
        proj_for_leverage = 0 if is_out else (our_proj or result.get("linestar_proj"))
        our_leverage = None
        if proj_for_leverage and result.get("proj_own_pct") is not None:
            # field_proj = what other contestants see; DK's projection is the
            # primary ownership driver; LineStar is a reasonable fallback.
            field_proj = p.get("avg_fpts_dk") or result.get("linestar_proj")
            our_leverage = compute_leverage(
                proj_for_leverage,
                result["proj_own_pct"],
                field_proj=field_proj,
                spg=stats.get("spg", 0.0) if stats else 0.0,
                bpg=stats.get("bpg", 0.0) if stats else 0.0,
            )
        result["our_leverage"] = our_leverage
        result["_spg"] = stats.get("spg", 0.0) if stats else 0.0
        result["_bpg"] = stats.get("bpg", 0.0) if stats else 0.0

        enriched.append(result)

    # ── Baseline ownership when LineStar is unavailable ──────────────────────
    # Compute pool average of the reference projection (avg_fpts_dk preferred).
    # Players that already have LineStar proj_own_pct keep it unchanged.
    ref_projs = [
        v for p in enriched
        if p.get("proj_own_pct") is None   # only for players without real ownership
        for v in [(p.get("avg_fpts_dk") or p.get("our_proj") or 0)]
        if v > 0   # exclude zero-projection players from diluting the average
    ]
    pool_avg = sum(ref_projs) / len(ref_projs) if ref_projs else 0.0

    baseline_applied = 0
    for p in enriched:
        if p.get("proj_own_pct") is not None:
            continue   # real LineStar data — keep it
        ref = p.get("avg_fpts_dk") or p.get("our_proj") or 0
        if not ref or pool_avg <= 0:
            continue
        p["proj_own_pct"] = compute_baseline_ownership(ref, pool_avg)
        # Re-compute leverage with the now-available ownership estimate
        proj_for_lev = 0 if p.get("is_out") else (p.get("our_proj") or p.get("linestar_proj"))
        if proj_for_lev:
            field_proj = p.get("avg_fpts_dk") or p.get("linestar_proj")
            p["our_leverage"] = compute_leverage(
                proj_for_lev,
                p["proj_own_pct"],
                field_proj=field_proj,
                spg=p.get("_spg", 0.0),
                bpg=p.get("_bpg", 0.0),
            )
        baseline_applied += 1

    n = len(dk_players)
    print(f"  {n} DK players processed")
    print(f"  LineStar match: {matched_linestar}/{n} ({100*matched_linestar//n if n else 0}%)")
    print(f"  Team resolved:  {matched_team}/{n}")
    print(f"  Stats matched:  {matched_stats}/{n}")
    if baseline_applied:
        print(f"  Baseline own%:  {baseline_applied} players (no LineStar data)")
    return enriched


# ── Entry point ──────────────────────────────────────────────


def _parse_slate_date(game_info: str) -> str | None:
    """Extract YYYY-MM-DD from 'LAL@BOS 03/24/2026 07:30PM ET'."""
    m = re.search(r"(\d{2}/\d{2}/\d{4})", game_info)
    if m:
        return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
    return None


def _safe_float(val) -> float | None:
    if not val:
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def run(
    dk_path: str | None = None,
    linestar_path: str | None = None,
    draft_group_id: int | None = None,
    contest_id: int | None = None,
    linestar_api: bool = False,
    dnn_cookie: str | None = None,
    date_override: str | None = None,
    season: str = "2025-26",
) -> None:
    config = load_config()
    db     = DatabaseManager(config.database_url)

    # DK player source
    dgid: int | None = None
    if draft_group_id or contest_id:
        from ingest.dk_api import fetch_dk_players, fetch_draft_group_id as _resolve_dgid
        dgid       = draft_group_id or _resolve_dgid(contest_id)
        dk_players = fetch_dk_players(dgid)
        print(f"API: {len(dk_players)} players from draftGroupId {dgid}")
    elif dk_path:
        with open(dk_path, encoding="utf-8-sig") as f:
            dk_players = parse_dk_csv(f.read())
        print(f"CSV: {len(dk_players)} DK players parsed")
    else:
        raise ValueError("Provide --dk, --draft-group-id, or --contest-id")

    # LineStar source
    if linestar_path:
        with open(linestar_path, encoding="utf-8-sig") as f:
            linestar_map = parse_linestar_csv(f.read())
        print(f"LineStar CSV: {len(linestar_map)} entries")
    elif linestar_api:
        if not dgid:
            raise ValueError("--linestar-api requires --draft-group-id or --contest-id")
        from ingest.linestar_fetch import fetch_linestar_for_draft_group
        cookie       = dnn_cookie or os.environ.get("DNN_COOKIE", "")
        linestar_map = fetch_linestar_for_draft_group(dgid, dnn_cookie=cookie)
        print(f"LineStar API: {len(linestar_map)} entries")
    else:
        linestar_map = {}
        print("LineStar: not provided — linestar_proj and proj_own_pct will be NULL")

    # Determine slate date
    slate_date = date_override
    if not slate_date:
        for p in dk_players:
            d = _parse_slate_date(p.get("game_info", ""))
            if d:
                slate_date = d
                break
    if not slate_date:
        slate_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Slate date: {slate_date}")

    # Game count
    game_count = len({
        p["game_info"].split()[0]
        for p in dk_players
        if p.get("game_info")
    })

    slate_id = upsert_dk_slate(db, slate_date, game_count, dk_draft_group_id=dgid)
    print(f"Slate ID: {slate_id}")

    pool = build_player_pool(db, dk_players, linestar_map, slate_date, season)

    saved = 0
    for p in pool:
        upsert_dk_player(db, slate_id, {
            "dk_player_id":       p["dk_id"],
            "name":               p["name"],
            "team_abbrev":        p["team_abbrev"],
            "eligible_positions": p["eligible_positions"],
            "salary":             p["salary"],
            "team_id":            p.get("team_id"),
            "matchup_id":         p.get("matchup_id"),
            "game_info":          p.get("game_info"),
            "avg_fpts_dk":        p.get("avg_fpts_dk"),
            "linestar_proj":      p.get("linestar_proj"),
            "linestar_own_pct":   p.get("linestar_own_pct"),
            "proj_own_pct":       p.get("proj_own_pct"),
            "our_proj":           p.get("our_proj"),
            "our_leverage":       p.get("our_leverage"),
            "proj_floor":         p.get("proj_floor"),
            "proj_ceiling":       p.get("proj_ceiling"),
            "boom_rate":          p.get("boom_rate"),
            "dk_in_starting_lineup":   None,
            "dk_starting_lineup_order": None,
            "dk_team_lineup_confirmed": None,
            "dk_status":          p.get("dk_status"),
            "is_out":             p.get("is_out", False),
        })
        saved += 1

    print(f"Saved {saved} players to slate {slate_id} ({slate_date})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest DK + LineStar NBA DFS data")
    dk_src = parser.add_mutually_exclusive_group(required=True)
    dk_src.add_argument("--dk",             help="Path to DK salary CSV")
    dk_src.add_argument("--draft-group-id", type=int, metavar="ID")
    dk_src.add_argument("--contest-id",     type=int, metavar="ID")
    parser.add_argument("--linestar",        help="Path to LineStar CSV")
    parser.add_argument("--linestar-api",    action="store_true")
    parser.add_argument("--dnn-cookie",      default=None)
    parser.add_argument("--date",            help="Slate date YYYY-MM-DD")
    parser.add_argument("--season",          default="2025-26")
    args = parser.parse_args()

    run(
        dk_path=args.dk,
        linestar_path=args.linestar,
        draft_group_id=args.draft_group_id,
        contest_id=args.contest_id,
        linestar_api=args.linestar_api,
        dnn_cookie=args.dnn_cookie,
        date_override=args.date,
        season=args.season,
    )
