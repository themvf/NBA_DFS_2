"""Capture CFBD roster, returning-production, transfer, talent, and coach context.

Annual CFBD rosters are useful descriptive references.  They become eligible
for prospective features only when this job captures them before kickoff with
``--prospective``; fetching an old season today never reconstructs what was
known on an old game date.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import (
    build_cfb_team_name_cache,
    upsert_cfb_roster_snapshot,
    upsert_cfb_staff_regime,
)
from ingest.cfb_history import _TransactionDb

logger = logging.getLogger(__name__)
CFBD_BASE = "https://api.collegefootballdata.com"


def _normal(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def position_group(position: str | None) -> str | None:
    value = str(position or "").upper()
    if value in {"QB"}:
        return "QB"
    if value in {"RB", "FB"}:
        return "RB"
    if value in {"WR", "TE"}:
        return "PASS_CATCHER"
    if value in {"OT", "OG", "OL", "C", "G", "T"}:
        return "OL"
    if value in {"DE", "DT", "DL", "EDGE", "NT"}:
        return "DL"
    if value in {"LB", "ILB", "OLB"}:
        return "LB"
    if value in {"CB", "S", "DB", "SAF"}:
        return "SECONDARY"
    if value in {"K", "P", "LS"}:
        return "SPECIAL_TEAMS"
    return value or None


def request_cfbd(endpoint: str, api_key: str, **params) -> list[dict]:
    if not api_key:
        raise ValueError("CFBD_API_KEY is required")
    for attempt in range(4):
        try:
            response = requests.get(
                f"{CFBD_BASE}/{endpoint}", params=params,
                headers={"Authorization": f"Bearer {api_key}"}, timeout=60,
            )
            response.raise_for_status()
            payload = response.json() or []
            if not isinstance(payload, list):
                raise ValueError(f"CFBD /{endpoint} returned a non-list payload")
            logger.info(
                "CFBD %s: %s rows; quota remaining=%s last=%s", endpoint, len(payload),
                response.headers.get("x-requests-remaining", "?"),
                response.headers.get("x-requests-last", "?"),
            )
            return payload
        except (requests.RequestException, ValueError) as exc:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
            logger.warning("CFBD /%s retry after %s", endpoint, exc)
    return []


def summarize_roster(
    players: list[dict], previous_ids: set[str], returning: dict | None,
    talent: float | None, transfers_in: list[dict], transfers_out: list[dict],
) -> dict:
    ids = {str(player.get("id")) for player in players if player.get("id") is not None}
    groups = Counter(position_group(player.get("position")) or "UNKNOWN" for player in players)
    returning_ids = ids & previous_ids
    quarterbacks = [player for player in players if position_group(player.get("position")) == "QB"]
    offensive_line = [player for player in players if position_group(player.get("position")) == "OL"]
    return {
        "players": len(players),
        "position_groups": dict(groups),
        "returning_roster_count": len(returning_ids),
        "roster_continuity_pct": len(returning_ids) / len(ids) if ids else None,
        "quarterbacks": len(quarterbacks),
        "returning_quarterbacks": sum(str(player.get("id")) in previous_ids for player in quarterbacks),
        "offensive_line": len(offensive_line),
        "returning_offensive_line": sum(str(player.get("id")) in previous_ids for player in offensive_line),
        "returning_production": returning or {},
        "talent_composite": talent,
        "transfers_in": len(transfers_in),
        "transfers_out": len(transfers_out),
        "transfer_rating_in": sum(float(item.get("rating") or 0) for item in transfers_in),
        "transfer_rating_out": sum(float(item.get("rating") or 0) for item in transfers_out),
        "availability_source": "not_provided_by_cfbd_roster",
    }


def _previous_player_ids(db: DatabaseManager, team_id: int, season: int) -> set[str]:
    rows = db.execute(
        """
        SELECT rp.source_player_id
        FROM cfb_roster_players rp
        JOIN cfb_roster_snapshots rs ON rs.id=rp.snapshot_id
        WHERE rs.team_id=%s AND rs.season=%s
        ORDER BY rs.captured_at DESC
        """,
        (team_id, season - 1),
    )
    return {str(row["source_player_id"]) for row in rows}


def ingest_roster_context(
    db: DatabaseManager, *, season: int, api_key: str, prospective: bool,
) -> dict:
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    roster = request_cfbd("roster", api_key, year=season, classification="fbs")
    returning = request_cfbd("player/returning", api_key, year=season)
    portal = request_cfbd("player/portal", api_key, year=season)
    talent = request_cfbd("talent", api_key, year=season)
    coaches = request_cfbd("coaches", api_key, year=season)
    team_cache = {_normal(name): team_id for name, team_id in build_cfb_team_name_cache(db, "cfbd").items()}
    roster_by_team: dict[str, list[dict]] = defaultdict(list)
    for player in roster:
        roster_by_team[_normal(player.get("team"))].append(player)
    returning_by_team = {_normal(item.get("team")): item for item in returning}
    talent_by_team = {_normal(item.get("team")): float(item["talent"]) for item in talent if item.get("talent") is not None}
    transfers_in: dict[str, list[dict]] = defaultdict(list)
    transfers_out: dict[str, list[dict]] = defaultdict(list)
    for item in portal:
        if item.get("destination"):
            transfers_in[_normal(item["destination"])].append(item)
        if item.get("origin"):
            transfers_out[_normal(item["origin"])].append(item)
    from psycopg2.extras import Json, execute_values

    snapshots = unmapped = staff_written = 0
    player_values = []
    with db.connect() as connection:
        tx = _TransactionDb(connection)
        for team_key, players in roster_by_team.items():
            team_id = team_cache.get(team_key)
            if not team_id:
                unmapped += 1
                continue
            previous_ids = _previous_player_ids(tx, team_id, season)
            summary = summarize_roster(
                players, previous_ids, returning_by_team.get(team_key), talent_by_team.get(team_key),
                transfers_in.get(team_key, []), transfers_out.get(team_key, []),
            )
            digest = _hash({"season": season, "team": team_key, "players": players, "summary": summary})
            snapshot_id = upsert_cfb_roster_snapshot(tx, {
                "team_id": team_id, "season": season, "source": "cfbd_annual_roster",
                "available_at": captured_at, "captured_at": captured_at, "payload_hash": digest,
                "confidence": 0.8 if prospective else 0.5, "is_complete": bool(players),
                "point_in_time_eligible": prospective, "summary_json": summary,
            })
            snapshots += int(bool(snapshot_id))
            for player in players:
                display_name = " ".join(filter(None, [player.get("firstName"), player.get("lastName")])).strip()
                player_values.append((
                    snapshot_id, str(player.get("id")), _normal(display_name), display_name,
                    player.get("position"), position_group(player.get("position")),
                    player.get("year"), None, None, None, None, Json(player),
                ))
        if player_values:
            execute_values(connection.cursor(), """
                INSERT INTO cfb_roster_players (
                    snapshot_id, source_player_id, normalized_name, display_name,
                    position, position_group, class_year, previous_team_id, depth_role,
                    availability_status, availability_confidence, attributes_json
                ) VALUES %s
                ON CONFLICT (snapshot_id, source_player_id) DO UPDATE SET
                    normalized_name=EXCLUDED.normalized_name,
                    display_name=EXCLUDED.display_name,
                    position=EXCLUDED.position,
                    position_group=EXCLUDED.position_group,
                    class_year=EXCLUDED.class_year,
                    attributes_json=EXCLUDED.attributes_json
            """, player_values, page_size=1000)
        for coach in coaches:
            person_name = " ".join(filter(None, [coach.get("firstName"), coach.get("lastName")])).strip()
            seasons_by_team: dict[int, list[int]] = defaultdict(list)
            for item in coach.get("seasons") or []:
                team_id = team_cache.get(_normal(item.get("school")))
                if team_id and item.get("year") is not None:
                    seasons_by_team[team_id].append(int(item["year"]))
            for team_id, years in seasons_by_team.items():
                staff_written += int(bool(upsert_cfb_staff_regime(tx, {
                    "team_id": team_id, "role": "HEAD_COACH",
                    "source_person_id": str(coach.get("id")), "person_name": person_name,
                    "start_season": min(years), "start_week": 0, "end_season": max(years),
                    "source": "cfbd_coaches", "available_at": captured_at,
                    "captured_at": captured_at, "source_json": coach,
                })))
    players_written = len(player_values)
    return {
        "season": season, "prospective": prospective, "snapshots": snapshots,
        "players": players_written, "staff_regimes": staff_written,
        "unmapped_roster_teams": unmapped,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--prospective", action="store_true", help="Mark this capture eligible for games after capture time")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = load_config()
    db = DatabaseManager(config.database_url or "")
    result = ingest_roster_context(
        db, season=args.season, api_key=os.getenv("CFBD_API_KEY", ""),
        prospective=args.prospective,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
