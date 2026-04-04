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
  fetch_dk_players() resolves the FPPG stat attribute dynamically from the
  response metadata, because MLB does not use the NBA stat id.

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
import math
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

# DK MLB boom thresholds (FPTS ≥ N defines a "boom" game for GPP targeting)
_SP_BOOM_THRESHOLD  = 40.0   # starter:  40+ FPTS is tournament-winning
_RP_BOOM_THRESHOLD  = 15.0   # reliever: rarely exceeds this range
_BAT_BOOM_THRESHOLD = 25.0   # batter:   25+ FPTS in one game
_MLB_PITCHER_OWNERSHIP_BUDGET = 200.0
_MLB_HITTER_OWNERSHIP_BUDGET = 800.0
_MLB_PITCHER_SOFTMAX_K = 2.0


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


def _is_confirmed_lineup_order(order: int | None) -> bool:
    return isinstance(order, int) and 1 <= order <= 9


def _dk_pitcher_is_probable(player: dict) -> bool:
    """Return True unless DK explicitly marks an MLB pitcher as non-probable."""
    signals = [
        player.get("starting_pitcher"),
        player.get("likely_pitcher"),
        player.get("probable_starter"),
    ]
    present = [bool(v) for v in signals if v is not None]
    if not present:
        return True
    return any(present)


def _infer_team_lineup_confirmed(dk_players: list[dict]) -> dict[str, bool]:
    """Infer whether DK has posted a confirmed batting order for each MLB team."""
    grouped: dict[str, list[dict]] = {}
    for player in dk_players:
        if _is_pitcher(player.get("eligible_positions", "")):
            continue
        team_abbrev = (player.get("team_abbrev") or "").upper()
        if not team_abbrev:
            continue
        grouped.setdefault(team_abbrev, []).append(player)

    confirmed_by_team: dict[str, bool] = {}
    for team_abbrev, hitters in grouped.items():
        ordered_hitters = sum(
            1 for hitter in hitters
            if _is_confirmed_lineup_order(hitter.get("starting_lineup_order"))
        )
        explicit_flags = sum(
            1 for hitter in hitters
            if hitter.get("in_starting_lineup") is not None
        )
        confirmed_by_team[team_abbrev] = ordered_hitters > 0 or explicit_flags >= 5
    return confirmed_by_team


def _sanitize_projection(value) -> float | None:
    finite = _safe_float(value)
    if finite is None:
        return None
    return max(0.0, finite)


def _sanitize_ownership_pct(value) -> float | None:
    finite = _safe_float(value)
    if finite is None:
        return None
    return max(0.0, min(100.0, finite))


def _sanitize_leverage(value) -> float | None:
    return _safe_float(value)


def _normalize_ownership_scores(scores: list[tuple[int, float]], budget: float) -> dict[int, float]:
    valid = [(idx, score) for idx, score in scores if math.isfinite(score) and score > 0]
    total = sum(score for _, score in valid)
    if not math.isfinite(total) or total <= 0:
        return {}
    result: dict[int, float] = {}
    for idx, score in valid:
        own_pct = round((score / total) * budget, 1)
        sanitized = _sanitize_ownership_pct(own_pct)
        if sanitized is not None:
            result[idx] = sanitized
    return result


def _get_mlb_proxy_projection(player: dict) -> float | None:
    our_proj = player.get("our_proj")
    linestar_proj = player.get("linestar_proj")
    return _sanitize_projection(our_proj if our_proj is not None else linestar_proj)


def _get_mlb_reference_projection(player: dict) -> float | None:
    avg_fpts = player.get("avg_fpts_dk")
    if avg_fpts is not None:
        return _sanitize_projection(avg_fpts)
    our_proj = player.get("our_proj")
    if our_proj is not None:
        return _sanitize_projection(our_proj)
    return _sanitize_projection(player.get("linestar_proj"))


def _compute_mlb_baseline_ownership_score(ref_proj: float, pool_avg: float) -> float:
    return max(1.0, min(50.0, (ref_proj / pool_avg) * 15.0))


def _apply_mlb_ownership_models(players: list[dict]) -> int:
    indexed = list(enumerate(players))
    active_pitchers = [
        (idx, player)
        for idx, player in indexed
        if not player.get("is_out") and _is_pitcher(player.get("eligible_positions", "")) and (player.get("salary") or 0) > 0
    ]
    active_hitters = [
        (idx, player)
        for idx, player in indexed
        if not player.get("is_out") and not _is_pitcher(player.get("eligible_positions", "")) and (player.get("salary") or 0) > 0
    ]

    hitter_fallback_refs = [
        ref
        for _, player in active_hitters
        if _sanitize_ownership_pct(player.get("proj_own_pct")) is None
        for ref in [_get_mlb_reference_projection(player)]
        if ref is not None and ref > 0
    ]
    hitter_pool_avg = (
        sum(hitter_fallback_refs) / len(hitter_fallback_refs)
        if hitter_fallback_refs else 0.0
    )

    pitcher_field_scores: list[tuple[int, float]] = []
    for idx, player in active_pitchers:
        proxy_proj = _get_mlb_proxy_projection(player)
        ls_own = _sanitize_ownership_pct(player.get("proj_own_pct")) or 0.0
        if proxy_proj is not None and proxy_proj > 0:
            value_score = proxy_proj / (player["salary"] / 1000.0)
            score = math.exp(value_score * _MLB_PITCHER_SOFTMAX_K) * (1.0 + ls_own / 100.0)
            if math.isfinite(score) and score > 0:
                pitcher_field_scores.append((idx, score))
        elif ls_own > 0:
            pitcher_field_scores.append((idx, ls_own))

    hitter_field_scores: list[tuple[int, float]] = []
    for idx, player in active_hitters:
        ls_own = _sanitize_ownership_pct(player.get("proj_own_pct"))
        if ls_own is not None and ls_own > 0:
            hitter_field_scores.append((idx, ls_own))
            continue
        ref_proj = _get_mlb_reference_projection(player)
        if ref_proj is None or ref_proj <= 0 or hitter_pool_avg <= 0:
            continue
        hitter_field_scores.append((idx, _compute_mlb_baseline_ownership_score(ref_proj, hitter_pool_avg)))

    our_pitcher_scores: list[tuple[int, float]] = []
    for idx, player in active_pitchers:
        proxy_proj = _get_mlb_proxy_projection(player)
        if proxy_proj is None or proxy_proj <= 0:
            continue
        score = proxy_proj / math.sqrt(player["salary"] / 1000.0)
        if math.isfinite(score) and score > 0:
            our_pitcher_scores.append((idx, score))

    our_hitter_scores: list[tuple[int, float]] = []
    for idx, player in active_hitters:
        proxy_proj = _get_mlb_proxy_projection(player)
        if proxy_proj is None or proxy_proj <= 0:
            continue
        score = proxy_proj / math.sqrt(player["salary"] / 1000.0)
        if math.isfinite(score) and score > 0:
            our_hitter_scores.append((idx, score))

    field_pitcher_map = _normalize_ownership_scores(pitcher_field_scores, _MLB_PITCHER_OWNERSHIP_BUDGET)
    field_hitter_map = _normalize_ownership_scores(hitter_field_scores, _MLB_HITTER_OWNERSHIP_BUDGET)
    our_pitcher_map = _normalize_ownership_scores(our_pitcher_scores, _MLB_PITCHER_OWNERSHIP_BUDGET)
    our_hitter_map = _normalize_ownership_scores(our_hitter_scores, _MLB_HITTER_OWNERSHIP_BUDGET)

    baseline_applied = 0
    for idx, player in indexed:
        if player.get("is_out"):
            player["proj_own_pct"] = 0.0
            player["our_own_pct"] = 0.0
            player["our_leverage"] = None
            continue

        pitcher_flag = _is_pitcher(player.get("eligible_positions", ""))
        proj_own_pct = field_pitcher_map.get(idx, 0.0) if pitcher_flag else field_hitter_map.get(idx, 0.0)
        our_own_pct = our_pitcher_map.get(idx, 0.0) if pitcher_flag else our_hitter_map.get(idx, 0.0)
        if not pitcher_flag and _sanitize_ownership_pct(player.get("proj_own_pct")) is None and proj_own_pct > 0:
            baseline_applied += 1

        player["proj_own_pct"] = _sanitize_ownership_pct(proj_own_pct)
        player["our_own_pct"] = _sanitize_ownership_pct(our_own_pct)

        proj_for_leverage = _get_mlb_proxy_projection(player)
        if proj_for_leverage is not None and proj_for_leverage > 0 and player.get("proj_own_pct") is not None:
            field_proj = _sanitize_projection(player.get("avg_fpts_dk") or player.get("linestar_proj"))
            player["our_leverage"] = _sanitize_leverage(
                compute_leverage(proj_for_leverage, player["proj_own_pct"], field_proj=field_proj)
            )
        else:
            player["our_leverage"] = None

    return baseline_applied


_MLB_MAX_CURRENT_SEASON_WEIGHT = 0.90
_BATTER_PRIOR_SEASON_PIVOT = 80.0
_BATTER_TEAM_CHANGE_PIVOT = 40.0
_PITCHER_PRIOR_SEASON_PIVOT = 15.0
_PITCHER_TEAM_CHANGE_PIVOT = 8.0
_TEAM_PRIOR_SEASON_PIVOT = 20.0
_BATTER_TEAM_CHANGE_MIN_WEIGHT = 0.35
_PITCHER_TEAM_CHANGE_MIN_WEIGHT = 0.40
_CONTEXT_WEIGHT_BONUS = 0.05
_TEAM_CHANGE_CONTEXT_WEIGHT_BONUS = 0.15


def _infer_prior_season(season: str) -> str | None:
    try:
        year = int(season)
    except (TypeError, ValueError):
        return None
    if year <= 2000:
        return None
    return str(year - 1)


def _canonicalize_name(name: str) -> str:
    return (
        (name or "")
        .lower()
        .replace(".", "")
        .replace(",", "")
        .replace("'", "")
        .replace("-", " ")
    ).strip()


def _normalize_name(name: str) -> str:
    cleaned = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", _canonicalize_name(name))
    return " ".join(sorted(token for token in cleaned.split() if token))


def _build_match_meta(rows: list[dict], sample_fn) -> list[dict]:
    meta_rows: list[dict] = []
    for row in rows:
        canonical = _canonicalize_name(row.get("name", ""))
        tokens = canonical.split()
        meta_rows.append({
            "row": row,
            "canonical_name": canonical,
            "normalized_name": _normalize_name(row.get("name", "")),
            "first_initial": tokens[0][0] if tokens else "",
            "last_token": tokens[-1] if tokens else "",
            "sample_score": sample_fn(row),
        })
    return meta_rows


def _prefer_meta(best: dict | None, candidate: dict) -> dict:
    if best is None:
        return candidate
    if candidate["sample_score"] != best["sample_score"]:
        return candidate if candidate["sample_score"] > best["sample_score"] else best
    return candidate if len(candidate["canonical_name"]) < len(best["canonical_name"]) else best


def _find_best_player_stats(dk_name: str, dk_team_id: int | None, meta_rows: list[dict]) -> dict | None:
    if not meta_rows:
        return None

    canonical = _canonicalize_name(dk_name)
    normalized = _normalize_name(dk_name)

    best_team_canonical = None
    best_team_normalized = None
    best_canonical = None
    best_normalized = None

    for meta in meta_rows:
        row_team_id = meta["row"].get("team_id")
        if dk_team_id is not None and row_team_id == dk_team_id and meta["canonical_name"] == canonical:
            best_team_canonical = _prefer_meta(best_team_canonical, meta)
        if dk_team_id is not None and row_team_id == dk_team_id and meta["normalized_name"] == normalized:
            best_team_normalized = _prefer_meta(best_team_normalized, meta)
        if meta["canonical_name"] == canonical:
            best_canonical = _prefer_meta(best_canonical, meta)
        if meta["normalized_name"] == normalized:
            best_normalized = _prefer_meta(best_normalized, meta)

    if best_team_canonical:
        return best_team_canonical["row"]
    if best_team_normalized:
        return best_team_normalized["row"]
    if best_canonical:
        return best_canonical["row"]
    if best_normalized:
        return best_normalized["row"]

    tokens = canonical.split()
    first_initial = tokens[0][0] if tokens else ""
    last_token = tokens[-1] if tokens else ""
    if not first_initial or not last_token:
        return None

    best = None
    best_score = 0.0
    for meta in meta_rows:
        if meta["first_initial"] != first_initial or meta["last_token"] != last_token:
            continue
        score = fuzz.ratio(canonical, meta["canonical_name"])
        if score < 80:
            continue
        if score > best_score:
            best_score = score
            best = meta
            continue
        if score == best_score:
            if dk_team_id is not None:
                best_team_match = best and best["row"].get("team_id") == dk_team_id
                candidate_team_match = meta["row"].get("team_id") == dk_team_id
                if candidate_team_match != best_team_match:
                    if candidate_team_match:
                        best = meta
                    continue
            best = _prefer_meta(best, meta)
    return best["row"] if best else None


def _batter_sample(row: dict | None) -> float:
    if not row:
        return 0.0
    return max(0.0, float(row.get("pa_pg") or 0.0) * float(row.get("games") or 0.0))


def _pitcher_sample(row: dict | None) -> float:
    if not row:
        return 0.0
    return max(0.0, float(row.get("ip_pg") or 0.0) * float(row.get("games") or 0.0))


def _current_season_weight(
    current_sample: float,
    prior_pivot: float,
    team_change_pivot: float,
    team_changed: bool,
    team_change_min_weight: float,
) -> float:
    if current_sample <= 0:
        return 0.0
    pivot = team_change_pivot if team_changed else prior_pivot
    weight = current_sample / (current_sample + pivot)
    if team_changed:
        weight = max(weight, team_change_min_weight)
    return max(0.0, min(_MLB_MAX_CURRENT_SEASON_WEIGHT, weight))


def _context_weight(current_weight: float, team_changed: bool) -> float:
    if current_weight <= 0:
        return 0.0
    bonus = _TEAM_CHANGE_CONTEXT_WEIGHT_BONUS if team_changed else _CONTEXT_WEIGHT_BONUS
    return max(0.0, min(0.95, current_weight + bonus))


def _match_player_stats_across_seasons(
    dk_name: str,
    dk_team_id: int | None,
    current_meta: list[dict],
    prior_meta: list[dict],
    sample_fn,
    prior_pivot: float,
    team_change_pivot: float,
    team_change_min_weight: float,
) -> dict:
    current = _find_best_player_stats(dk_name, dk_team_id, current_meta)
    prior = _find_best_player_stats(dk_name, dk_team_id, prior_meta)
    team_changed = bool(
        current
        and prior
        and current.get("team_id") is not None
        and prior.get("team_id") is not None
        and current.get("team_id") != prior.get("team_id")
    )
    current_weight = _current_season_weight(
        sample_fn(current),
        prior_pivot,
        team_change_pivot,
        team_changed,
        team_change_min_weight,
    )
    return {
        "current": current,
        "prior": prior,
        "match_type": (
            "blended" if current and prior else
            "current_only" if current else
            "prior_only" if prior else
            "unmatched"
        ),
        "team_changed": team_changed,
        "current_weight": current_weight,
        "context_weight": _context_weight(current_weight, team_changed),
    }


def _track_coverage(counter: dict[str, int], match: dict) -> None:
    counter[match["match_type"]] += 1
    if match["team_changed"] and match["current_weight"] > 0:
        counter["team_change_accelerated"] += 1


def _blend_num(current_value, prior_value, current_weight: float):
    current = _safe_float(current_value)
    prior = _safe_float(prior_value)
    if current is None and prior is None:
        return None
    if current is None:
        return prior
    if prior is None:
        return current
    return current * current_weight + prior * (1.0 - current_weight)


def _blend_int(current_value, prior_value, current_weight: float):
    blended = _blend_num(current_value, prior_value, current_weight)
    return int(round(blended)) if blended is not None else None


def _prefer_scalar(current_value, prior_value):
    return current_value if current_value is not None else prior_value


def _blend_batter_stats(match: dict, dk_team_id: int | None) -> dict | None:
    current = match["current"]
    prior = match["prior"]
    if not current and not prior:
        return None
    current_weight = match["current_weight"]
    context_weight = match["context_weight"]
    base = dict(current or prior)
    games = None
    if current and prior and current.get("games") is not None and prior.get("games") is not None:
        games = int(current["games"]) + int(prior["games"])
    else:
        games = _prefer_scalar(current.get("games") if current else None, prior.get("games") if prior else None)

    base.update({
        "team_id": dk_team_id if dk_team_id is not None else _prefer_scalar(current.get("team_id") if current else None, prior.get("team_id") if prior else None),
        "name": _prefer_scalar(current.get("name") if current else None, prior.get("name") if prior else None),
        "games": games,
        "batting_order": _blend_int(current.get("batting_order") if current else None, prior.get("batting_order") if prior else None, context_weight),
        "pa_pg": _blend_num(current.get("pa_pg") if current else None, prior.get("pa_pg") if prior else None, context_weight),
        "avg": _blend_num(current.get("avg") if current else None, prior.get("avg") if prior else None, current_weight),
        "obp": _blend_num(current.get("obp") if current else None, prior.get("obp") if prior else None, current_weight),
        "slg": _blend_num(current.get("slg") if current else None, prior.get("slg") if prior else None, current_weight),
        "iso": _blend_num(current.get("iso") if current else None, prior.get("iso") if prior else None, current_weight),
        "babip": _blend_num(current.get("babip") if current else None, prior.get("babip") if prior else None, current_weight),
        "wrc_plus": _blend_num(current.get("wrc_plus") if current else None, prior.get("wrc_plus") if prior else None, current_weight),
        "k_pct": _blend_num(current.get("k_pct") if current else None, prior.get("k_pct") if prior else None, current_weight),
        "bb_pct": _blend_num(current.get("bb_pct") if current else None, prior.get("bb_pct") if prior else None, current_weight),
        "hr_pg": _blend_num(current.get("hr_pg") if current else None, prior.get("hr_pg") if prior else None, current_weight),
        "singles_pg": _blend_num(current.get("singles_pg") if current else None, prior.get("singles_pg") if prior else None, current_weight),
        "doubles_pg": _blend_num(current.get("doubles_pg") if current else None, prior.get("doubles_pg") if prior else None, current_weight),
        "triples_pg": _blend_num(current.get("triples_pg") if current else None, prior.get("triples_pg") if prior else None, current_weight),
        "rbi_pg": _blend_num(current.get("rbi_pg") if current else None, prior.get("rbi_pg") if prior else None, context_weight),
        "runs_pg": _blend_num(current.get("runs_pg") if current else None, prior.get("runs_pg") if prior else None, context_weight),
        "sb_pg": _blend_num(current.get("sb_pg") if current else None, prior.get("sb_pg") if prior else None, current_weight),
        "hbp_pg": _blend_num(current.get("hbp_pg") if current else None, prior.get("hbp_pg") if prior else None, current_weight),
        "wrc_plus_vs_l": _blend_num(current.get("wrc_plus_vs_l") if current else None, prior.get("wrc_plus_vs_l") if prior else None, current_weight),
        "wrc_plus_vs_r": _blend_num(current.get("wrc_plus_vs_r") if current else None, prior.get("wrc_plus_vs_r") if prior else None, current_weight),
        "avg_fpts_pg": _blend_num(current.get("avg_fpts_pg") if current else None, prior.get("avg_fpts_pg") if prior else None, context_weight),
        "fpts_std": _blend_num(current.get("fpts_std") if current else None, prior.get("fpts_std") if prior else None, context_weight),
    })
    return base


def _blend_pitcher_stats(match: dict, dk_team_id: int | None) -> dict | None:
    current = match["current"]
    prior = match["prior"]
    if not current and not prior:
        return None
    current_weight = match["current_weight"]
    context_weight = match["context_weight"]
    base = dict(current or prior)
    games = None
    if current and prior and current.get("games") is not None and prior.get("games") is not None:
        games = int(current["games"]) + int(prior["games"])
    else:
        games = _prefer_scalar(current.get("games") if current else None, prior.get("games") if prior else None)

    base.update({
        "team_id": dk_team_id if dk_team_id is not None else _prefer_scalar(current.get("team_id") if current else None, prior.get("team_id") if prior else None),
        "name": _prefer_scalar(current.get("name") if current else None, prior.get("name") if prior else None),
        "hand": _prefer_scalar(current.get("hand") if current else None, prior.get("hand") if prior else None),
        "games": games,
        "ip_pg": _blend_num(current.get("ip_pg") if current else None, prior.get("ip_pg") if prior else None, context_weight),
        "era": _blend_num(current.get("era") if current else None, prior.get("era") if prior else None, current_weight),
        "fip": _blend_num(current.get("fip") if current else None, prior.get("fip") if prior else None, current_weight),
        "xfip": _blend_num(current.get("xfip") if current else None, prior.get("xfip") if prior else None, current_weight),
        "k_per_9": _blend_num(current.get("k_per_9") if current else None, prior.get("k_per_9") if prior else None, current_weight),
        "bb_per_9": _blend_num(current.get("bb_per_9") if current else None, prior.get("bb_per_9") if prior else None, current_weight),
        "hr_per_9": _blend_num(current.get("hr_per_9") if current else None, prior.get("hr_per_9") if prior else None, current_weight),
        "k_pct": _blend_num(current.get("k_pct") if current else None, prior.get("k_pct") if prior else None, current_weight),
        "bb_pct": _blend_num(current.get("bb_pct") if current else None, prior.get("bb_pct") if prior else None, current_weight),
        "hr_fb_pct": _blend_num(current.get("hr_fb_pct") if current else None, prior.get("hr_fb_pct") if prior else None, current_weight),
        "whip": _blend_num(current.get("whip") if current else None, prior.get("whip") if prior else None, current_weight),
        "avg_fpts_pg": _blend_num(current.get("avg_fpts_pg") if current else None, prior.get("avg_fpts_pg") if prior else None, context_weight),
        "fpts_std": _blend_num(current.get("fpts_std") if current else None, prior.get("fpts_std") if prior else None, context_weight),
        "win_pct": _blend_num(current.get("win_pct") if current else None, prior.get("win_pct") if prior else None, context_weight),
        "qs_pct": _blend_num(current.get("qs_pct") if current else None, prior.get("qs_pct") if prior else None, context_weight),
    })
    return base


def _build_team_sample_map(batter_rows: list[dict], pitcher_rows: list[dict]) -> dict[int, int]:
    samples: dict[int, int] = {}
    for row in batter_rows + pitcher_rows:
        team_id = row.get("team_id")
        if not team_id:
            continue
        samples[team_id] = max(samples.get(team_id, 0), int(row.get("games") or 0))
    return samples


def _blend_team_stats(current: dict | None, prior: dict | None, current_games: int) -> dict | None:
    if not current and not prior:
        return None
    base = dict(current or prior)
    current_weight = 0.0
    if current_games > 0:
        current_weight = max(
            0.0,
            min(_MLB_MAX_CURRENT_SEASON_WEIGHT, current_games / (current_games + _TEAM_PRIOR_SEASON_PIVOT))
        )
    base.update({
        "team_wrc_plus": _blend_num(current.get("team_wrc_plus") if current else None, prior.get("team_wrc_plus") if prior else None, current_weight),
        "team_k_pct": _blend_num(current.get("team_k_pct") if current else None, prior.get("team_k_pct") if prior else None, current_weight),
        "team_bb_pct": _blend_num(current.get("team_bb_pct") if current else None, prior.get("team_bb_pct") if prior else None, current_weight),
        "team_iso": _blend_num(current.get("team_iso") if current else None, prior.get("team_iso") if prior else None, current_weight),
        "team_ops": _blend_num(current.get("team_ops") if current else None, prior.get("team_ops") if prior else None, current_weight),
        "bullpen_era": _blend_num(current.get("bullpen_era") if current else None, prior.get("bullpen_era") if prior else None, current_weight),
        "bullpen_fip": _blend_num(current.get("bullpen_fip") if current else None, prior.get("bullpen_fip") if prior else None, current_weight),
        "staff_k_pct": _blend_num(current.get("staff_k_pct") if current else None, prior.get("staff_k_pct") if prior else None, current_weight),
        "staff_bb_pct": _blend_num(current.get("staff_bb_pct") if current else None, prior.get("staff_bb_pct") if prior else None, current_weight),
    })
    return base


# ── Projection pipeline ───────────────────────────────────────────────────────

def build_player_pool_mlb(
    db: DatabaseManager,
    dk_players: list[dict],
    linestar_map: dict[tuple, dict],
    slate_date: str,
    season: str,
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

    prior_season = _infer_prior_season(season)

    home_team_ids = [m["home_team_id"] for m in matchups if m.get("home_team_id")]
    park_factors: dict[int, dict] = {}
    active_team_ids = list(matchup_by_team.keys())

    batter_rows: list[dict] = []
    prior_batter_rows: list[dict] = []
    pitcher_rows: list[dict] = []
    prior_pitcher_rows: list[dict] = []
    team_stat_rows: list[dict] = []
    prior_team_stat_rows: list[dict] = []
    park_rows: list[dict] = []
    prior_park_rows: list[dict] = []

    if active_team_ids:
        ph = ",".join(["%s"] * len(active_team_ids))

        batter_rows = db.execute(
            """
            SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                   pa_pg, bb_pct, batting_order,
                   singles_pg, doubles_pg, triples_pg, hr_pg,
                   rbi_pg, runs_pg, hbp_pg, sb_pg,
                   wrc_plus, k_pct, wrc_plus_vs_l, wrc_plus_vs_r
            FROM mlb_batter_stats
            WHERE season = %s
            """,
            [season],
        )
        pitcher_rows = db.execute(
            """
            SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                   ip_pg, k_per_9, bb_per_9, era, xfip, whip,
                   hand, win_pct, qs_pct
            FROM mlb_pitcher_stats
            WHERE season = %s
            """,
            [season],
        )
        team_stat_rows = db.execute(
            f"""
            SELECT team_id, team_wrc_plus, team_k_pct, team_bb_pct, team_iso, team_ops,
                   bullpen_era, bullpen_fip, staff_k_pct, staff_bb_pct
            FROM mlb_team_stats
            WHERE season = %s AND team_id IN ({ph})
            """,
            [season] + active_team_ids,
        )

        if prior_season:
            prior_batter_rows = db.execute(
                """
                SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                       pa_pg, bb_pct, batting_order,
                       singles_pg, doubles_pg, triples_pg, hr_pg,
                       rbi_pg, runs_pg, hbp_pg, sb_pg,
                       wrc_plus, k_pct, wrc_plus_vs_l, wrc_plus_vs_r
                FROM mlb_batter_stats
                WHERE season = %s
                """,
                [prior_season],
            )
            prior_pitcher_rows = db.execute(
                """
                SELECT name, team_id, games, avg_fpts_pg, fpts_std,
                       ip_pg, k_per_9, bb_per_9, era, xfip, whip,
                       hand, win_pct, qs_pct
                FROM mlb_pitcher_stats
                WHERE season = %s
                """,
                [prior_season],
            )
            prior_team_stat_rows = db.execute(
                f"""
                SELECT team_id, team_wrc_plus, team_k_pct, team_bb_pct, team_iso, team_ops,
                       bullpen_era, bullpen_fip, staff_k_pct, staff_bb_pct
                FROM mlb_team_stats
                WHERE season = %s AND team_id IN ({ph})
                """,
                [prior_season] + active_team_ids,
            )

    if home_team_ids:
        placeholders = ",".join(["%s"] * len(home_team_ids))
        park_rows = db.execute(
            f"""
            SELECT team_id, runs_factor, hr_factor
            FROM mlb_park_factors
            WHERE season = %s AND team_id IN ({placeholders})
            """,
            [season] + home_team_ids,
        )
        if prior_season:
            prior_park_rows = db.execute(
                f"""
                SELECT team_id, runs_factor, hr_factor
                FROM mlb_park_factors
                WHERE season = %s AND team_id IN ({placeholders})
                """,
                [prior_season] + home_team_ids,
            )

    current_batter_meta = _build_match_meta(batter_rows, _batter_sample)
    prior_batter_meta = _build_match_meta(prior_batter_rows, _batter_sample)
    current_pitcher_meta = _build_match_meta(pitcher_rows, _pitcher_sample)
    prior_pitcher_meta = _build_match_meta(prior_pitcher_rows, _pitcher_sample)

    current_team_stats = {row["team_id"]: row for row in team_stat_rows}
    prior_team_stats = {row["team_id"]: row for row in prior_team_stat_rows}
    current_team_samples = _build_team_sample_map(batter_rows, pitcher_rows)
    team_stats_by_team: dict[int, dict] = {}
    for team_id in set(current_team_stats) | set(prior_team_stats):
        blended_team = _blend_team_stats(
            current_team_stats.get(team_id),
            prior_team_stats.get(team_id),
            current_team_samples.get(team_id, 0),
        )
        if blended_team:
            team_stats_by_team[team_id] = blended_team

    current_parks = {row["team_id"]: row for row in park_rows}
    prior_parks = {row["team_id"]: row for row in prior_park_rows}
    for team_id in set(current_parks) | set(prior_parks):
        park_factors[team_id] = current_parks.get(team_id) or prior_parks.get(team_id)

    # SP pre-pass: identify today's starting pitchers from DK eligible_positions.
    # Stored by the SP's own team_id so batters can look up their opp_sp via
    # the opposing team_id at projection time.
    sp_by_team: dict[int, dict] = {}
    for _p in dk_players:
        if not _is_sp(_p.get("eligible_positions", "")) or not _dk_pitcher_is_probable(_p):
            continue
        _tid = match_mlb_team_id(_p["team_abbrev"], abbrev_cache)
        if not _tid or _tid in sp_by_team:
            continue
        _sp_match = _match_player_stats_across_seasons(
            _p["name"],
            _tid,
            current_pitcher_meta,
            prior_pitcher_meta,
            _pitcher_sample,
            _PITCHER_PRIOR_SEASON_PIVOT,
            _PITCHER_TEAM_CHANGE_PIVOT,
            _PITCHER_TEAM_CHANGE_MIN_WEIGHT,
        )
        _sp_stats = _blend_pitcher_stats(_sp_match, _tid)
        if _sp_stats:
            sp_by_team[_tid] = _sp_stats

    lineup_confirmed_by_team = _infer_team_lineup_confirmed(dk_players)

    enriched = []
    matched_linestar = matched_team = matched_stats = 0
    batter_coverage = {
        "current_only": 0,
        "blended": 0,
        "prior_only": 0,
        "unmatched": 0,
        "team_change_accelerated": 0,
    }
    pitcher_coverage = {
        "current_only": 0,
        "blended": 0,
        "prior_only": 0,
        "unmatched": 0,
        "team_change_accelerated": 0,
    }

    for p in dk_players:
        result = dict(p)

        team_abbrev = (p.get("team_abbrev") or "").upper()
        positions    = p.get("eligible_positions", "")
        pitcher_flag = _is_pitcher(positions)
        sp_flag      = _is_sp(positions)
        dk_order     = p.get("starting_lineup_order")
        lineup_confirmed = lineup_confirmed_by_team.get(team_abbrev, False)
        confirmed_batter_out = (
            not pitcher_flag
            and lineup_confirmed
            and not _is_confirmed_lineup_order(dk_order)
            and p.get("in_starting_lineup") is not True
        )

        # DK injury status
        dk_is_out = (
            p.get("is_disabled", False)
            or p.get("dk_status", "None").upper() in ("O", "OUT")
            or (pitcher_flag and not _dk_pitcher_is_probable(p))
        )

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

        result["is_out"] = dk_is_out or confirmed_batter_out or result.get("is_out", False)
        result["dk_in_starting_lineup"] = p.get("in_starting_lineup")
        result["dk_starting_lineup_order"] = dk_order if _is_confirmed_lineup_order(dk_order) else None
        result["dk_team_lineup_confirmed"] = lineup_confirmed

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

        stats = None
        if pitcher_flag:
            pitcher_match = _match_player_stats_across_seasons(
                p["name"],
                mlb_team_id,
                current_pitcher_meta,
                prior_pitcher_meta,
                _pitcher_sample,
                _PITCHER_PRIOR_SEASON_PIVOT,
                _PITCHER_TEAM_CHANGE_PIVOT,
                _PITCHER_TEAM_CHANGE_MIN_WEIGHT,
            )
            _track_coverage(pitcher_coverage, pitcher_match)
            stats = _blend_pitcher_stats(pitcher_match, mlb_team_id)
        else:
            batter_match = _match_player_stats_across_seasons(
                p["name"],
                mlb_team_id,
                current_batter_meta,
                prior_batter_meta,
                _batter_sample,
                _BATTER_PRIOR_SEASON_PIVOT,
                _BATTER_TEAM_CHANGE_PIVOT,
                _BATTER_TEAM_CHANGE_MIN_WEIGHT,
            )
            _track_coverage(batter_coverage, batter_match)
            stats = _blend_batter_stats(batter_match, mlb_team_id)

        # Phase 5 projection: full MLB model
        our_proj = None
        proj_floor = proj_ceiling = boom_rate = None
        if stats and not result.get("is_out") and not confirmed_batter_out:
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
                    confirmed_order=result.get("dk_starting_lineup_order"),
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
        result["our_own_pct"] = None
        result["our_leverage"] = None

        enriched.append(result)

    baseline_applied = _apply_mlb_ownership_models(enriched)

    n = len(dk_players)
    print(f"  {n} DK MLB players processed")
    print(f"  LineStar match: {matched_linestar}/{n} ({100*matched_linestar//n if n else 0}%)")
    print(f"  Team resolved:  {matched_team}/{n}")
    print(f"  Stats matched:  {matched_stats}/{n}")
    print(
        "  Batter mix:    "
        f"{batter_coverage['current_only']} current | "
        f"{batter_coverage['blended']} blended | "
        f"{batter_coverage['prior_only']} prior"
    )
    print(
        "  Pitcher mix:   "
        f"{pitcher_coverage['current_only']} current | "
        f"{pitcher_coverage['blended']} blended | "
        f"{pitcher_coverage['prior_only']} prior"
    )
    print(
        "  Team changers: "
        f"{batter_coverage['team_change_accelerated'] + pitcher_coverage['team_change_accelerated']}"
    )
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
    season: str | None = None,
    contest_type: str = "main",
    contest_format: str = "gpp",
) -> None:
    config = load_config()
    db     = DatabaseManager(config.database_url)
    season = season or config.mlb_api.season

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
            "our_own_pct":        p.get("our_own_pct"),
            "our_leverage":       p.get("our_leverage"),
            "proj_floor":         p.get("proj_floor"),
            "proj_ceiling":       p.get("proj_ceiling"),
            "boom_rate":          p.get("boom_rate"),
            "dk_in_starting_lineup":   p.get("dk_in_starting_lineup"),
            "dk_starting_lineup_order": p.get("dk_starting_lineup_order"),
            "dk_team_lineup_confirmed": p.get("dk_team_lineup_confirmed"),
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
    parser.add_argument("--season",          default=None, help="Season year (defaults to current year)")
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
