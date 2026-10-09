"""Frozen CFBD-backed context definitions for the first vertical slice."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Iterable, Mapping


DRIVE_VOLUME_DEFINITION = ("cfb_offensive_drive_volume", 1)


def offensive_drive_volume(
    rows: Iterable[Mapping[str, object]], *, team_id: int, target_event_id: int,
    as_of_at: datetime, history_window_games: int = 4,
) -> dict:
    """Mean distinct offensive drives in latest eligible completed FBS games.

    ``rows`` are drive/game joins. A game's observations are eligible only if
    every selected drive was ingested by the as-of boundary. Existing late
    backfill therefore cannot masquerade as historically available evidence.
    """
    games: dict[int, dict] = {}
    exclusions: dict[str, int] = {}
    for row in rows:
        reason = None
        if int(row["game_id"]) == target_event_id:
            reason = "target_event"
        elif not row.get("completed"):
            reason = "not_completed"
        elif str(row.get("home_classification") or "").casefold() != "fbs" or str(row.get("away_classification") or "").casefold() != "fbs":
            reason = "not_fbs_vs_fbs"
        elif row.get("commence_time") is None or row["commence_time"] >= as_of_at:
            reason = "not_before_as_of"
        elif row.get("ingested_at") is None or row["ingested_at"] > as_of_at:
            reason = "unavailable_at_as_of"
        elif row.get("offense_team_id") != team_id or row.get("cfbd_drive_id") is None:
            reason = "not_subject_offense_or_missing_identity"
        if reason:
            exclusions[reason] = exclusions.get(reason, 0) + 1
            continue
        game = games.setdefault(int(row["game_id"]), {"commence_time": row["commence_time"], "drives": set()})
        game["drives"].add(int(row["cfbd_drive_id"]))
    selected = sorted(games.items(), key=lambda item: (item[1]["commence_time"], item[0]), reverse=True)[:history_window_games]
    counts = [len(game["drives"]) for _, game in selected]
    coverage = "missing" if not counts else "complete" if len(counts) == history_window_games else "partial"
    return {
        "definition_id": DRIVE_VOLUME_DEFINITION[0], "definition_version": DRIVE_VOLUME_DEFINITION[1],
        "subject_key": f"cfb:team:{team_id}", "target_event_key": f"cfb:event:{target_event_id}",
        "as_of_at": as_of_at.isoformat(), "scalar_value": mean(counts) if counts else None,
        "unit": "offensive_drives_per_game", "coverage_state": coverage,
        "included_game_ids": [game_id for game_id, _ in selected], "included_game_count": len(selected),
        "drive_counts": counts, "excluded_row_counts": exclusions, "measurement_kind": "observed",
        "availability_basis": "drive.ingested_at<=as_of_at", "origin": "prospective",
    }
