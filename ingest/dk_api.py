"""DraftKings API client — fetch NBA slate/player data without manual CSV download.

Public endpoints (no auth required):
  GET /contests/v1/contests/{contestId}
      → returns draftGroupId, contest name, start time

  GET /draftgroups/v1/draftgroups/{draftGroupId}/draftables
      → full player pool with salaries, positions, projections

NBA eligible_positions are derived from all roster slot entries per player:
  PG-only       → "PG/UTIL"
  PG/SG flex    → "PG/SG/G/UTIL"
  C-only        → "C/UTIL"
  etc.

Returns dicts compatible with parse_dk_csv() so dk_slate.py works unchanged.

Usage:
    from ingest.dk_api import fetch_dk_players, fetch_draft_group_id

    players = fetch_dk_players(144324)
    dgid    = fetch_draft_group_id(189058648)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import requests

logger = logging.getLogger(__name__)

_DEFAULT_PROJ_STAT_ID = 279
_ET_ZONE      = ZoneInfo("America/New_York")  # handles EDT/EST automatically
_TIMEOUT      = 15
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Canonical ordering for position string construction
_POS_ORDER = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"]

_PGA_STARTING_LINEUP_ORDER = 99
_PGA_IN_STARTING_LINEUP   = 100
_PGA_PROBABLE_STARTER     = 130
_PGA_LIKELY_PITCHER       = 137
_PGA_STARTING_PITCHER     = 110


def _resolve_proj_stat_id(payload: dict[str, Any]) -> int:
    """Resolve the DraftKings FPPG stat attribute ID from response metadata."""
    for stat in payload.get("draftStats", []):
        stat_id = stat.get("id")
        if not isinstance(stat_id, int):
            continue
        if stat.get("abbr") == "FPPG" or stat.get("name") == "Fantasy Points Per Game":
            return stat_id
    return _DEFAULT_PROJ_STAT_ID


def _player_game_attr_map(entry: dict[str, Any]) -> dict[int, Any]:
    attrs: dict[int, Any] = {}
    for attr in entry.get("playerGameAttributes", []):
        attr_id = attr.get("id")
        if isinstance(attr_id, int):
            attrs[attr_id] = attr.get("value")
    return attrs


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (ValueError, TypeError):
        return None
    return parsed if parsed > 0 else None


def fetch_draft_group_id(contest_id: int) -> int:
    """Resolve a DK contestId → draftGroupId."""
    url  = f"https://api.draftkings.com/contests/v1/contests/{contest_id}"
    resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    dgid: int = resp.json()["contestDetail"]["draftGroupId"]
    logger.info("Contest %d → draftGroupId %d", contest_id, dgid)
    return dgid


def fetch_dk_players(draft_group_id: int) -> list[dict]:
    """Fetch the full NBA player pool from DK API for a given draftGroupId.

    Returns player dicts with:
        name, dk_id, team_abbrev, eligible_positions, salary, game_info, avg_fpts_dk

    eligible_positions is built from all slot entries per player (e.g. "PG/G/UTIL"),
    matching what the DK salary CSV exports in the "Roster Position" column.
    """
    url  = f"https://api.draftkings.com/draftgroups/v1/draftgroups/{draft_group_id}/draftables"
    resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()

    payload = resp.json()
    raw: list[dict] = payload.get("draftables", [])
    proj_stat_id = _resolve_proj_stat_id(payload)

    # Group by playerId — each player has one entry per eligible roster slot
    by_player: dict[int, list[dict]] = defaultdict(list)
    for entry in raw:
        by_player[entry["playerId"]].append(entry)

    players = []
    for player_id, entries in by_player.items():
        entries_sorted = sorted(entries, key=lambda e: e["rosterSlotId"])
        canonical = entries_sorted[0]  # lowest rosterSlotId = primary slot

        # Collect all unique positions across slot entries
        all_positions: set[str] = set()
        for entry in entries_sorted:
            pos = entry.get("position", "")
            if pos:
                all_positions.add(pos)
        # Always include UTIL (every NBA player is UTIL-eligible)
        all_positions.add("UTIL")
        sorted_pos = sorted(all_positions, key=lambda p: _POS_ORDER.index(p) if p in _POS_ORDER else 99)
        eligible_positions = "/".join(sorted_pos)

        # DK's own FPTS projection (FPPG stat id varies by sport)
        avg_fpts_dk: float | None = None
        for attr in canonical.get("draftStatAttributes", []):
            if attr.get("id") == proj_stat_id:
                try:
                    avg_fpts_dk = float(attr["value"])
                except (ValueError, TypeError):
                    pass
                break

        player_game_attrs = _player_game_attr_map(canonical)

        # Injury / availability status from DK
        dk_status   = canonical.get("status", "None") or "None"  # "None","O","Q","GTD","D"
        is_disabled = bool(canonical.get("isDisabled", False))    # True = DK locked player out

        players.append({
            "name":               canonical.get("displayName", ""),
            "dk_id":              canonical["draftableId"],
            "team_abbrev":        (canonical.get("teamAbbreviation") or "").upper(),
            "eligible_positions": eligible_positions,
            "salary":             canonical.get("salary", 0),
            "game_info":          _format_game_info(canonical.get("competition", {})),
            "avg_fpts_dk":        avg_fpts_dk,
            "dk_status":          dk_status,
            "is_disabled":        is_disabled,
            "starting_lineup_order": _positive_int(player_game_attrs.get(_PGA_STARTING_LINEUP_ORDER)),
            "in_starting_lineup":    _optional_bool(player_game_attrs.get(_PGA_IN_STARTING_LINEUP)),
            "probable_starter":      _optional_bool(player_game_attrs.get(_PGA_PROBABLE_STARTER)),
            "likely_pitcher":        _optional_bool(player_game_attrs.get(_PGA_LIKELY_PITCHER)),
            "starting_pitcher":      _optional_bool(player_game_attrs.get(_PGA_STARTING_PITCHER)),
        })

    logger.info("Fetched %d players from draftGroupId %d", len(players), draft_group_id)
    return players


def _format_game_info(competition: dict) -> str:
    """Format competition dict → DK CSV game_info string.

    e.g. {"name": "LAL @ BOS", "startTime": "2026-03-24T00:00:00Z"}
      → "LAL@BOS 03/23/2026 08:00PM ET"
    """
    name         = competition.get("name", "")
    name_compact = name.replace(" @ ", "@").replace(" ", "")
    start_time   = competition.get("startTime", "")
    if not start_time:
        return name_compact
    try:
        dt_utc = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        dt_et  = dt_utc.astimezone(_ET_ZONE)   # correct for both EST and EDT
        return f"{name_compact} {dt_et.strftime('%m/%d/%Y')} {dt_et.strftime('%I:%M%p')} ET"
    except (ValueError, AttributeError):
        return name_compact
