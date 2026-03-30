"""Ingest DraftKings MLB salary CSV + LineStar projections into dk_players.

Mirrors dk_slate.py but adapted for MLB:
  - sport="mlb" in dk_slates
  - mlb_team_id (not team_id) in dk_players
  - Matchup → mlb_matchups (not nba_matchups)
  - Separate batter / pitcher stat lookup
  - Full projection model: env factor, park factors, xFIP quality,
    batting-order PA weight, L/R splits, opposing K%, win probability.

DK MLB DraftGroup API note:
  Same endpoint as NBA: /draftgroups/v1/draftgroups/{id}/draftables
  The _MLB_PROJ_STAT_ID = 279 is a best guess — verify on first real MLB slate.
  If no projection attribute is found, avg_fpts_dk will be NULL and we fall
  back entirely to our own avg_fpts_pg projection.

LineStar MLB:
  CSV input is supported (--linestar path).  LineStar API for MLB uses
  sport=2 (NBA=5) but requires a separate period discovery — deferred to Phase 6.

Usage:
    python -m ingest.mlb_slate --dk DKSalaries_MLB.csv
    python -m ingest.mlb_slate --dk DKSalaries_MLB.csv --linestar LineStar_MLB.csv
    python -m ingest.mlb_slate --draft-group-id 155000
    python -m ingest.mlb_slate --contest-id 200000 --linestar LineStar_MLB.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import datetime

from rapidfuzz import fuzz, process

from config import load_config
from db.database import DatabaseManager
from db.queries import build_mlb_team_abbrev_cache, upsert_dk_player, upsert_dk_slate
from ingest.dk_slate import (
    _parse_slate_date,
    _safe_float,
    parse_dk_csv,
    parse_linestar_csv,
)
from model.dfs_projections import (
    compute_baseline_ownership,
    compute_leverage,
    compute_monte_carlo,
)
from model.mlb_projections import compute_batter_projection, compute_pitcher_projection

logger = logging.getLogger(__name__)

# DK MLB abbreviation overrides.  Most MLB teams match our mlb_teams.abbreviation
# directly.  Only deviations are listed — verify against real DK MLB slates.
MLB_DK_ABBREV_OVERRIDES: dict[str, str] = {
    "CHW": "CWS",   # DK sometimes uses CHW for Chicago White Sox
    "ATH": "OAK",   # Athletics (Sacramento 2025) may appear as ATH
    "KCR": "KC",    # Baseball Reference form; DK uses KC
    "SFG": "SF",    # Baseball Reference form; DK uses SF
    "SDP": "SD",    # Baseball Reference form; DK uses SD
    "TBR": "TB",    # Baseball Reference form; DK uses TB
    "WAS": "WSH",   # FanGraphs uses WAS; DK uses WSH
}

# DK stat attribute ID for projected FPTS.  NBA uses 279 — MLB may share it
# or use a different ID.  Verify on first real MLB draft group.
_MLB_PROJ_STAT_ID = 279

# DK MLB boom thresholds (FPTS ≥ N defines a "boom" game for GPP targeting)
_SP_BOOM_THRESHOLD  = 40.0   # starter:  40+ FPTS is tournament-winning
_RP_BOOM_THRESHOLD  = 15.0   # reliever: rarely exceeds this range
_BAT_BOOM_THRESHOLD = 25.0   # batter:   25+ FPTS in one game


# ── Team + player resolution ─────────────────────────────────────────────────

def match_mlb_team_id(abbrev: str, cache: dict[str, int]) -> int | None:
    """Resolve DK MLB team abbreviation to mlb_teams.team_id."""
    a = abbrev.strip().upper()
    canonical = MLB_DK_ABBREV_OVERRIDES.get(a, a)
    tid = cache.get(canonical)
    if not tid:
        logger.debug("No mlb_team_id for DK abbrev '%s' (mapped '%s')", abbrev, canonical)
    return tid


def _is_pitcher(eligible_positions: str) -> bool:
    """True if any slot in eligible_positions is SP, RP, or P."""
    return any(p in ("SP", "RP", "P") for p in eligible_positions.split("/"))


def _is_sp(eligible_positions: str) -> bool:
    return "SP" in eligible_positions.split("/")


def match_player_stats(dk_name: str, candidates: list[dict]) -> dict | None:
    """Fuzzy-match DK player name to a stats row within the same team."""
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


# ── Projection pipeline ───────────────────────────────────────────────────────

def build_player_pool_mlb(
    db: DatabaseManager,
    dk_players: list[dict],
    linestar_map: dict[tuple, dict],
    slate_date: str,
    season: str = "2025",
) -> list[dict]:
    """Merge DK, LineStar, MLB stats, and park factors into an enriched pool.

    Phase 5 projection (full model):
        Batters: env_factor (implied total), park runs/HR factors, xFIP quality,
                 batting-order PA weight, L/R split wRC+ ratio.
        Pitchers: opposing lineup wRC+ / K%, park runs factor, win probability
                  blend (historical win_pct + team moneyline).
    """
    abbrev_cache = build_mlb_team_abbrev_cache(db)

    # Today's MLB matchups
    matchups = db.execute(
        """
        SELECT id, home_team_id, away_team_id,
               vegas_total, home_ml, away_ml,
               home_implied, away_implied, ballpark
        FROM mlb_matchups
        WHERE game_date = %s
        """,
        (slate_date,),
    )
    matchup_by_team: dict[int, dict] = {}
    for m in matchups:
        matchup_by_team[m["home_team_id"]] = {**m, "_is_home": True}
        matchup_by_team[m["away_team_id"]] = {**m, "_is_home": False}

    if not matchups:
        logger.warning("No mlb_matchups found for %s — matchup_id will be NULL for all players", slate_date)

    # Park factors indexed by home team_id
    home_team_ids = [m["home_team_id"] for m in matchups if m.get("home_team_id")]
    park_factors: dict[int, dict] = {}   # home_team_id → {runs_factor, hr_factor}
    if home_team_ids:
        placeholders = ",".join(["%s"] * len(home_team_ids))
        pf_rows = db.execute(
            f"""
            SELECT team_id, runs_factor, hr_factor
            FROM mlb_park_factors
            WHERE season = %s AND team_id IN ({placeholders})
            """,
            [season] + home_team_ids,
        )
        park_factors = {r["team_id"]: r for r in pf_rows}

    # Active team IDs from today's slate
    active_team_ids = list(matchup_by_team.keys())

    # Batter and pitcher stats for active teams
    batters_by_team:   dict[int, list[dict]] = {}
    pitchers_by_team:  dict[int, list[dict]] = {}
    team_stats_by_team: dict[int, dict]     = {}

    if active_team_ids:
        ph = ",".join(["%s"] * len(active_team_ids))

        batter_rows = db.execute(
            f"""
            SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                   pa_pg, bb_pct, batting_order,
                   singles_pg, doubles_pg, triples_pg, hr_pg,
                   rbi_pg, runs_pg, hbp_pg, sb_pg,
                   wrc_plus, k_pct, wrc_plus_vs_l, wrc_plus_vs_r
            FROM mlb_batter_stats
            WHERE season = %s AND team_id IN ({ph})
            """,
            [season] + active_team_ids,
        )
        for row in batter_rows:
            batters_by_team.setdefault(row["team_id"], []).append(row)

        pitcher_rows = db.execute(
            f"""
            SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                   ip_pg, k_per_9, bb_per_9, era, xfip, whip,
                   hand, win_pct, qs_pct
            FROM mlb_pitcher_stats
            WHERE season = %s AND team_id IN ({ph})
            """,
            [season] + active_team_ids,
        )
        for row in pitcher_rows:
            pitchers_by_team.setdefault(row["team_id"], []).append(row)

        team_stat_rows = db.execute(
            f"""
            SELECT team_id, team_wrc_plus, team_k_pct
            FROM mlb_team_stats
            WHERE season = %s AND team_id IN ({ph})
            """,
            [season] + active_team_ids,
        )
        for row in team_stat_rows:
            team_stats_by_team[row["team_id"]] = row

    # SP pre-pass: identify today's starting pitchers from DK eligible_positions.
    # Stored by the SP's own team_id so batters can look up their opp_sp via
    # the opposing team_id at projection time.
    sp_by_team: dict[int, dict] = {}
    for _p in dk_players:
        if not _is_sp(_p.get("eligible_positions", "")):
            continue
        _tid = match_mlb_team_id(_p["team_abbrev"], abbrev_cache)
        if not _tid or _tid in sp_by_team:
            continue
        _sp_stats = match_player_stats(_p["name"], pitchers_by_team.get(_tid, []))
        if _sp_stats:
            sp_by_team[_tid] = _sp_stats

    enriched = []
    matched_linestar = matched_team = matched_stats = 0

    for p in dk_players:
        result = dict(p)

        # DK injury status
        dk_is_out = p.get("is_disabled", False) or p.get("dk_status", "None").upper() in ("O", "OUT")

        # LineStar merge — same key format (name_lower, salary)
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
            result["proj_own_pct"]  = None
            result["is_out"]        = False

        result["is_out"] = dk_is_out or result.get("is_out", False)

        # Team resolution — mlb_team_id, not team_id
        mlb_team_id = match_mlb_team_id(p["team_abbrev"], abbrev_cache)
        result["team_id"]     = None          # not an NBA player
        result["mlb_team_id"] = mlb_team_id
        if mlb_team_id:
            matched_team += 1

        matchup = matchup_by_team.get(mlb_team_id) if mlb_team_id else None
        result["matchup_id"] = matchup["id"] if matchup else None

        # Game context: home/away status, park dict, opposing team stats
        is_home = matchup.get("_is_home", False) if matchup else False
        park = park_factors.get(matchup["home_team_id"]) if matchup else None
        opp_team_id = None
        if matchup:
            opp_team_id = matchup["away_team_id"] if is_home else matchup["home_team_id"]
        opp_team = team_stats_by_team.get(opp_team_id) if opp_team_id else None

        # Player type + stats match
        positions    = p.get("eligible_positions", "")
        pitcher_flag = _is_pitcher(positions)
        sp_flag      = _is_sp(positions)

        stats = None
        if mlb_team_id:
            if pitcher_flag:
                stats = match_player_stats(p["name"], pitchers_by_team.get(mlb_team_id, []))
            else:
                stats = match_player_stats(p["name"], batters_by_team.get(mlb_team_id, []))

        # Phase 5 projection: full MLB model
        our_proj = None
        proj_floor = proj_ceiling = boom_rate = None
        if stats and not result.get("is_out"):
            if pitcher_flag:
                our_proj = compute_pitcher_projection(
                    pitcher=stats,
                    matchup=matchup or {},
                    opp_team=opp_team,
                    park=park,
                    is_home=is_home,
                )
                boom_threshold = _SP_BOOM_THRESHOLD if sp_flag else _RP_BOOM_THRESHOLD
            else:
                opp_sp = sp_by_team.get(opp_team_id) if opp_team_id else None
                our_proj = compute_batter_projection(
                    batter=stats,
                    matchup=matchup or {},
                    opp_sp=opp_sp,
                    park=park,
                    is_home=is_home,
                )
                boom_threshold = _BAT_BOOM_THRESHOLD

            fpts_std = stats.get("fpts_std")
            if our_proj and fpts_std and fpts_std > 0:
                proj_floor, proj_ceiling, boom_rate = compute_monte_carlo(
                    our_proj, float(fpts_std), boom_threshold=boom_threshold,
                )
            if our_proj:
                matched_stats += 1

        result["our_proj"]     = our_proj
        result["proj_floor"]   = proj_floor
        result["proj_ceiling"] = proj_ceiling
        result["boom_rate"]    = boom_rate

        # Leverage
        is_out = result.get("is_out", False)
        proj_for_leverage = 0 if is_out else (our_proj or result.get("linestar_proj"))
        our_leverage = None
        if proj_for_leverage and result.get("proj_own_pct") is not None:
            field_proj = p.get("avg_fpts_dk") or result.get("linestar_proj")
            our_leverage = compute_leverage(
                proj_for_leverage,
                result["proj_own_pct"],
                field_proj=field_proj,
            )
        result["our_leverage"] = our_leverage

        enriched.append(result)

    # Baseline ownership for players missing LineStar data
    ref_projs = [
        v
        for p in enriched
        if p.get("proj_own_pct") is None
        for v in [(p.get("avg_fpts_dk") or p.get("our_proj") or 0)]
        if v > 0
    ]
    pool_avg = sum(ref_projs) / len(ref_projs) if ref_projs else 0.0

    baseline_applied = 0
    for p in enriched:
        if p.get("proj_own_pct") is not None:
            continue
        ref = p.get("avg_fpts_dk") or p.get("our_proj") or 0
        if not ref or pool_avg <= 0:
            continue
        p["proj_own_pct"] = compute_baseline_ownership(ref, pool_avg)
        proj_for_lev = 0 if p.get("is_out") else (p.get("our_proj") or p.get("linestar_proj"))
        if proj_for_lev:
            field_proj = p.get("avg_fpts_dk") or p.get("linestar_proj")
            p["our_leverage"] = compute_leverage(
                proj_for_lev,
                p["proj_own_pct"],
                field_proj=field_proj,
            )
        baseline_applied += 1

    n = len(dk_players)
    print(f"  {n} DK MLB players processed")
    print(f"  LineStar match: {matched_linestar}/{n} ({100*matched_linestar//n if n else 0}%)")
    print(f"  Team resolved:  {matched_team}/{n}")
    print(f"  Stats matched:  {matched_stats}/{n}")
    if baseline_applied:
        print(f"  Baseline own%:  {baseline_applied} players (no LineStar data)")
    return enriched


# ── Entry point ───────────────────────────────────────────────────────────────

def run(
    dk_path: str | None = None,
    linestar_path: str | None = None,
    draft_group_id: int | None = None,
    contest_id: int | None = None,
    date_override: str | None = None,
    season: str = "2025",
    contest_type: str = "main",
    contest_format: str = "gpp",
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
        print(f"CSV: {len(dk_players)} DK MLB players parsed")
    else:
        raise ValueError("Provide --dk, --draft-group-id, or --contest-id")

    # LineStar CSV (API for MLB is Phase 6)
    if linestar_path:
        with open(linestar_path, encoding="utf-8-sig") as f:
            linestar_map = parse_linestar_csv(f.read())
        print(f"LineStar CSV: {len(linestar_map)} entries")
    else:
        linestar_map = {}
        print("LineStar: not provided — linestar_proj and proj_own_pct will be NULL")

    # Determine slate date from game_info
    slate_date = date_override
    if not slate_date:
        for p in dk_players:
            d = _parse_slate_date(p.get("game_info", ""))
            if d:
                slate_date = d
                break
    if not slate_date:
        slate_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Slate date: {slate_date}  Sport: MLB")

    game_count = len({
        p["game_info"].split()[0]
        for p in dk_players
        if p.get("game_info")
    })

    slate_id = upsert_dk_slate(
        db, slate_date, game_count,
        dk_draft_group_id=dgid,
        contest_type=contest_type,
        contest_format=contest_format,
        sport="mlb",
    )
    print(f"Slate ID: {slate_id}")

    pool = build_player_pool_mlb(db, dk_players, linestar_map, slate_date, season)

    saved = 0
    for p in pool:
        upsert_dk_player(db, slate_id, {
            "dk_player_id":       p["dk_id"],
            "name":               p["name"],
            "team_abbrev":        p["team_abbrev"],
            "eligible_positions": p["eligible_positions"],
            "salary":             p["salary"],
            "team_id":            None,                  # not an NBA player
            "mlb_team_id":        p.get("mlb_team_id"),
            "matchup_id":         p.get("matchup_id"),
            "game_info":          p.get("game_info"),
            "avg_fpts_dk":        p.get("avg_fpts_dk"),
            "linestar_proj":      p.get("linestar_proj"),
            "proj_own_pct":       p.get("proj_own_pct"),
            "our_proj":           p.get("our_proj"),
            "our_leverage":       p.get("our_leverage"),
            "proj_floor":         p.get("proj_floor"),
            "proj_ceiling":       p.get("proj_ceiling"),
            "boom_rate":          p.get("boom_rate"),
            "is_out":             p.get("is_out", False),
        })
        saved += 1

    print(f"Saved {saved} players to MLB slate {slate_id} ({slate_date})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest DK + LineStar MLB DFS data")
    dk_src = parser.add_mutually_exclusive_group(required=True)
    dk_src.add_argument("--dk",             help="Path to DK MLB salary CSV")
    dk_src.add_argument("--draft-group-id", type=int, metavar="ID")
    dk_src.add_argument("--contest-id",     type=int, metavar="ID")
    parser.add_argument("--linestar",        help="Path to LineStar CSV")
    parser.add_argument("--date",            help="Slate date YYYY-MM-DD (overrides CSV)")
    parser.add_argument("--season",          default="2025", help="Season year (default 2025)")
    parser.add_argument("--contest-type",    default="main", choices=["main", "showdown"])
    parser.add_argument("--contest-format",  default="gpp",  choices=["gpp", "cash"])
    args = parser.parse_args()

    run(
        dk_path=args.dk,
        linestar_path=args.linestar,
        draft_group_id=args.draft_group_id,
        contest_id=args.contest_id,
        date_override=args.date,
        season=args.season,
        contest_type=args.contest_type,
        contest_format=args.contest_format,
    )
