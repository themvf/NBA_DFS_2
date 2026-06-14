"""Download and aggregate player-level stats from StatsBomb open data.

Fetches FIFA World Cup 2018 + 2022 and continental tournament event data from
the free StatsBomb GitHub repository.  Aggregates per-player across all matches:
goals, shots, shots on target, xG, npxG, minutes played.  Writes the combined
results to soccer_player_stats for use by the firstscorer-v3 model.

Raw JSON files are cached to data/statsbomb/ so subsequent runs are fast and
fully offline.  Re-download with --force.

Competitions included (StatsBomb competition_id / season_id):
  43/106  FIFA World Cup 2022 (Qatar)
  43/3    FIFA World Cup 2018 (Russia)
  55/43   UEFA Euro 2020 (played 2021)
  16/44   Copa América 2021
  6/37    Africa Cup of Nations 2021
  6/281   Africa Cup of Nations 2023

Usage:
    python -m ingest.soccer_player_history
    python -m ingest.soccer_player_history --force   # force re-download
    python -m ingest.soccer_player_history --dry-run # print totals, don't write DB
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import requests

from config import DATA_DIR, load_config
from db.database import DatabaseManager
from db.queries import build_soccer_team_name_cache, upsert_soccer_player_stat

logger = logging.getLogger(__name__)

STATSBOMB_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
CACHE_DIR = DATA_DIR / "statsbomb"
CACHE_TTL_DAYS = 30   # re-download match files after 30 days (data rarely changes)

# Competitions to include.  Tuple: (competition_id, season_id, label, weight)
# weight: tournament importance multiplier (WC data weighted higher — strongest
# opposition, most comparable to WC 2026).
# Source: StatsBomb open data (github.com/statsbomb/open-data).
# Note: AFCON is not in the free dataset; Copa America 2021 has 1 match only.
COMPETITIONS = [
    (43, 106, "WC2022", 2.0),
    (43,   3, "WC2018", 1.8),
    (55,  43, "EURO2020", 1.0),
    (16,  44, "COPA2021", 1.0),
]


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum() or ch == " ").strip()


def _cached_get(url: str, cache_path: Path, force: bool = False) -> dict | list | None:
    """Fetch JSON from url, caching to cache_path.  Returns None on error."""
    if cache_path.exists() and not force:
        age = (time.time() - cache_path.stat().st_mtime) / 86400
        if age < CACHE_TTL_DAYS:
            try:
                return json.loads(cache_path.read_bytes())
            except Exception:
                pass

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(resp.content)
        return resp.json()
    except requests.RequestException as e:
        logger.warning("Fetch failed %s: %s", url, e)
        return None


def _get_matches(comp_id: int, season_id: int, force: bool) -> list[dict]:
    url = f"{STATSBOMB_BASE}/matches/{comp_id}/{season_id}.json"
    cache = CACHE_DIR / "matches" / f"{comp_id}_{season_id}.json"
    data = _cached_get(url, cache, force)
    return data if isinstance(data, list) else []


def _get_events(match_id: int, force: bool) -> list[dict]:
    url = f"{STATSBOMB_BASE}/events/{match_id}.json"
    cache = CACHE_DIR / "events" / f"{match_id}.json"
    data = _cached_get(url, cache, force)
    return data if isinstance(data, list) else []


def _get_lineups(match_id: int, force: bool) -> list[dict]:
    url = f"{STATSBOMB_BASE}/lineups/{match_id}.json"
    cache = CACHE_DIR / "lineups" / f"{match_id}.json"
    data = _cached_get(url, cache, force)
    return data if isinstance(data, list) else []


def _classify_position(positions: list[str]) -> str:
    """Map a list of StatsBomb position names to FW/MF/DF/GK."""
    joined = " ".join(positions).lower()
    if "goalkeeper" in joined:
        return "GK"
    if any(x in joined for x in ("forward", "striker", "winger", "center forward")):
        return "FW"
    if any(x in joined for x in ("midfield", "attacking mid", "defensive mid")):
        return "MF"
    if any(x in joined for x in ("back", "defender", "wing back", "centre back")):
        return "DF"
    return "MF"  # default unknown → midfielder


def _parse_player_minutes(events: list[dict], lineups: list[dict]) -> dict[str, float]:
    """Estimate minutes played per player from lineup + substitution events.

    Returns {player_name: minutes}.  Extra time is capped at 120 min.
    """
    # Determine match length in minutes (90 or 120 if went to ET).
    max_minute = max((e.get("minute", 0) for e in events), default=90)
    match_length = 120.0 if max_minute > 90 else 90.0

    # Build starters set per team from lineup data.
    starters: set[str] = set()
    for team_lineup in lineups:
        for player in team_lineup.get("lineup", []):
            pname = player.get("player_name") or player.get("player", {}).get("name", "")
            if pname:
                starters.add(pname)

    # Default: starters play the full match; subs play 0 initially.
    minutes: dict[str, float] = {p: match_length for p in starters}

    # Process substitution events to correct start/end times.
    for ev in events:
        if ev.get("type", {}).get("name") != "Substitution":
            continue
        minute = float(ev.get("minute", 90))
        # Player subbed out.
        player_off = ev.get("player", {}).get("name", "")
        if player_off and player_off in minutes:
            minutes[player_off] = minute
        # Player subbed in.
        sub_name = (ev.get("substitution", {}) or {}).get("replacement", {}).get("name", "")
        if sub_name:
            minutes[sub_name] = match_length - minute

    return minutes


def aggregate_tournament(
    comp_id: int,
    season_id: int,
    weight: float,
    force: bool,
) -> dict[str, dict]:
    """Download and aggregate one competition.

    Returns {player_name: {team, position, goals, shots, sot, xg, npxg, minutes,
    weight_sum}} where weight is the tournament importance weight.
    """
    matches = _get_matches(comp_id, season_id, force)
    if not matches:
        logger.warning("No matches for comp %d season %d", comp_id, season_id)
        return {}

    acc: dict[str, dict] = {}  # player_name → accumulators

    for i, match in enumerate(matches):
        mid = match.get("match_id")
        if mid is None:
            continue

        if i % 10 == 0:
            logger.info("  Comp %d/%d — match %d/%d (id=%d)...",
                        comp_id, season_id, i + 1, len(matches), mid)

        events = _get_events(mid, force)
        lineups = _get_lineups(mid, force)
        player_minutes = _parse_player_minutes(events, lineups)

        # Register all players who played (even those with 0 shots).
        # Collect per-player positions from lineup data.
        position_map: dict[str, str] = {}
        team_map: dict[str, str] = {}
        for team_lineup in lineups:
            team_name = team_lineup.get("team_name", "")
            for player in team_lineup.get("lineup", []):
                pname = player.get("player_name") or player.get("player", {}).get("name", "")
                if not pname:
                    continue
                team_map[pname] = team_name
                pos_list = []
                for p in player.get("positions", []):
                    # StatsBomb: position is a plain string ("Right Center Back")
                    pos_val = p.get("position", "")
                    if isinstance(pos_val, dict):
                        pos_val = pos_val.get("name", "")
                    if pos_val:
                        pos_list.append(pos_val)
                if pos_list:
                    position_map[pname] = _classify_position(pos_list)

        # Accumulate per-player for this match.
        for pname, mins in player_minutes.items():
            if mins <= 0:
                continue
            if pname not in acc:
                acc[pname] = {
                    "team": team_map.get(pname, ""),
                    "position": position_map.get(pname, "MF"),
                    "matches": 0, "minutes": 0.0,
                    "goals": 0, "shots": 0, "sot": 0,
                    "xg": 0.0, "npxg": 0.0,
                    "weight_sum": 0.0,
                }
            row = acc[pname]
            row["matches"] += 1
            row["minutes"] += mins
            row["weight_sum"] += weight * mins   # weighted contribution

        # Process shot events.
        for ev in events:
            if ev.get("type", {}).get("name") != "Shot":
                continue
            pname = ev.get("player", {}).get("name", "")
            if not pname:
                continue
            shot = ev.get("shot", {}) or {}
            xg_val = float(shot.get("statsbomb_xg") or 0.0)
            outcome = (shot.get("outcome", {}) or {}).get("name", "")
            is_goal = outcome == "Goal"
            is_sot = outcome in ("Goal", "Saved", "Saved to Post")
            is_penalty = (shot.get("type", {}) or {}).get("name", "") == "Penalty"
            npxg_val = 0.0 if is_penalty else xg_val

            if pname not in acc:
                acc[pname] = {
                    "team": team_map.get(pname, ""),
                    "position": position_map.get(pname, "MF"),
                    "matches": 0, "minutes": 0.0,
                    "goals": 0, "shots": 0, "sot": 0,
                    "xg": 0.0, "npxg": 0.0,
                    "weight_sum": 0.0,
                }
            row = acc[pname]
            row["goals"] += int(is_goal)
            row["shots"] += 1
            row["sot"] += int(is_sot)
            row["xg"] += xg_val
            row["npxg"] += npxg_val

        time.sleep(0.05)  # gentle rate limit

    return acc


def combine_tournaments(
    tournament_data: list[tuple[dict, float]],
) -> dict[str, dict]:
    """Merge per-tournament accumulators into a single weighted combined row per player.

    For xg_per_90 we use a weighted average across tournaments: each tournament
    contributes (weight × minutes) worth of evidence.
    """
    combined: dict[str, dict] = {}

    for acc, weight in tournament_data:
        for pname, row in acc.items():
            if pname not in combined:
                combined[pname] = {
                    "team": row["team"],
                    "position": row["position"],
                    "matches": 0, "minutes": 0.0,
                    "goals": 0, "shots": 0, "sot": 0,
                    "xg_raw": 0.0, "npxg_raw": 0.0,
                    "xg_weighted": 0.0, "npxg_weighted": 0.0,
                    "weight_sum": 0.0,
                }
            c = combined[pname]
            mins = row["minutes"]
            if mins <= 0:
                continue
            c["matches"] += row["matches"]
            c["minutes"] += mins
            c["goals"] += row["goals"]
            c["shots"] += row["shots"]
            c["sot"] += row["sot"]
            c["xg_raw"] += row["xg"]
            c["npxg_raw"] += row["npxg"]
            # For the weighted-average xG/90:
            #   accumulator = Σ(xg_per_90_t × weight × mins_t)
            #   weight_sum  = Σ(weight × mins_t)
            #   xg_per_90   = accumulator / weight_sum
            # which simplifies the accumulator to Σ(xg_t × weight × 90 / mins_t × mins_t)
            # = Σ(xg_t × weight × 90).  We store without the 90 factor and divide later.
            w = weight * mins
            xg_rate = row["xg"] / (mins / 90.0)   # xG/90 for this tournament
            c["xg_weighted"] += xg_rate * w        # Σ(xg/90 × w)
            c["npxg_weighted"] += (row["npxg"] / (mins / 90.0)) * w
            c["weight_sum"] += w

    # Derive per-90 rates from weighted totals.
    result: dict[str, dict] = {}
    for pname, c in combined.items():
        if c["minutes"] < 45:  # skip players with < 45 min (noise)
            continue
        mins = c["minutes"]
        w_sum = c["weight_sum"]
        # Weighted average xG/90 = Σ(xg/90 × w) / Σ(w)  (no extra ×90 needed)
        xg_per_90 = (c["xg_weighted"] / w_sum) if w_sum > 0 else 0.0
        npxg_per_90 = (c["npxg_weighted"] / w_sum) if w_sum > 0 else 0.0
        result[pname] = {
            "team": c["team"],
            "position": c["position"],
            "matches": c["matches"],
            "minutes": mins,
            "goals": c["goals"],
            "shots": c["shots"],
            "sot": c["sot"],
            "xg": round(c["xg_raw"], 4),
            "npxg": round(c["npxg_raw"], 4),
            "goals_per_90": round(c["goals"] / mins * 90.0, 4),
            "shots_per_90": round(c["shots"] / mins * 90.0, 4),
            "xg_per_90": round(xg_per_90, 4),
            "npxg_per_90": round(npxg_per_90, 4),
        }
    return result


def ingest(
    db: DatabaseManager,
    force: bool = False,
    dry_run: bool = False,
) -> int:
    """Download all competitions, aggregate, and write to soccer_player_stats."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Build team-name → team_id lookup for FK assignment.
    team_cache = build_soccer_team_name_cache(db)
    norm_to_id = {_norm(name): tid for name, tid in team_cache.items()}

    tournament_data: list[tuple[dict, float]] = []
    for comp_id, season_id, label, weight in COMPETITIONS:
        logger.info("Fetching %s (comp=%d season=%d, weight=%.1f)…",
                    label, comp_id, season_id, weight)
        acc = aggregate_tournament(comp_id, season_id, weight, force)
        logger.info("  %s: %d players aggregated", label, len(acc))
        tournament_data.append((acc, weight))

    combined = combine_tournaments(tournament_data)
    logger.info("Combined: %d unique players with ≥45 min", len(combined))

    if dry_run:
        top = sorted(combined.items(), key=lambda kv: kv[1].get("xg_per_90", 0), reverse=True)[:20]
        print(f"\n{'Player':<30} {'Team':<20} {'Pos':<4} {'xG/90':>7} {'Mins':>6}")
        for name, row in top:
            # Strip non-ASCII for safe terminal output on any platform.
            safe_name = name.encode("ascii", "replace").decode("ascii")
            safe_team = (row["team"] or "").encode("ascii", "replace").decode("ascii")
            print(f"{safe_name:<30} {safe_team:<20} {row.get('position', '?'):<4} "
                  f"{row['xg_per_90']:>7.3f} {row['minutes']:>6.0f}")
        print(f"\n{len(combined)} players would be written (dry run — nothing saved)")
        return len(combined)

    written = 0
    for pname, row in combined.items():
        norm = _norm(pname)
        team_id = norm_to_id.get(_norm(row["team"]))
        upsert_soccer_player_stat(
            db,
            player_name=pname,
            normalized_name=norm,
            team_id=team_id,
            team_name=row["team"] or None,
            season="combined",
            position=row["position"],
            matches=row["matches"],
            minutes_played=round(row["minutes"], 1),
            goals=row["goals"],
            shots=row["shots"],
            shots_on_target=row["sot"],
            xg=round(row["xg"], 4),
            npxg=round(row["npxg"], 4),
            goals_per_90=row["goals_per_90"],
            shots_per_90=row["shots_per_90"],
            xg_per_90=row["xg_per_90"],
            npxg_per_90=row["npxg_per_90"],
        )
        written += 1

    print(f"Soccer player history: {written} players upserted (season=combined)")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest StatsBomb player history")
    parser.add_argument("--force", action="store_true", help="Force re-download all match files")
    parser.add_argument("--dry-run", action="store_true", help="Print top players, don't write DB")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    ingest(db, force=args.force, dry_run=args.dry_run)
