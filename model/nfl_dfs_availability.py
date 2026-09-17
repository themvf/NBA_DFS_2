"""Pre-kickoff availability: zero the absent, hand their work to the backup.

Pure. Takes projection dicts and an injury status map, returns adjusted
projection dicts. No database, no clock, no provider knowledge.

The rule, in order:
  1. A player carrying an OUT-class designation projects zero. He is not
     playing; nothing about his history is relevant any more.
  2. His OPPORTUNITY transfers to the replacement. His production does not.
     The replacement is scaled to the absent player's volume and keeps his own
     efficiency, because a backup taking 35 attempts is not the starter taking
     35 attempts.

Everything here is driven by data supplied at call time. There is no list of
players, no hand-maintained override, and no season-specific constant.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from model.nfl_dfs_historical import draftkings_points

VERSION = "nfl-dfs-availability-v1"

# A designation in this set means "not playing". QUESTIONABLE is deliberately
# absent: most Questionable players play, so zeroing them would fire on far
# more players and be a larger error than the one being fixed.
OUT_CLASS = frozenset({"OUT", "IR", "PUP", "NFI", "SUSPENDED"})

# The stat that stands for "how much work this position gets". Opportunity, not
# production — these are carried in stat_means but never scored.
OPPORTUNITY_KEY = {"QB": "attempts", "RB": "carries", "WR": "receptions", "TE": "receptions"}

# Everything that should grow with volume. Interceptions and fumbles are in
# here on purpose: more attempts means more chances to turn it over.
VOLUME_SCALED = (
    "passing_yards", "passing_tds", "passing_interceptions",
    "rushing_yards", "rushing_tds", "receiving_yards", "receiving_tds",
    "receptions", "fumbles_lost_total",
)

# A backup with a handful of mop-up snaps has a noisy efficiency estimate.
# Scaling it 17x turns that noise into a projection, so the multiplier is
# capped and the cap is reported rather than hidden.
MAX_TRANSFER_MULTIPLIER = 4.0


def is_out(status: str | None) -> bool:
    return bool(status) and str(status).strip().upper() in OUT_CLASS


def _num(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if result == result and abs(result) != float("inf") else 0.0


def zero_out(projection: Mapping[str, Any], status: str) -> dict[str, Any]:
    """A player who is not playing scores zero. Not shrunk — zero."""
    zeroed = dict(projection)
    for key in ("model_proj_fpts", "baseline_fpts", "floor_fpts", "median_fpts",
                "ceiling_fpts", "boom_rate"):
        zeroed[key] = 0.0
    zeroed["stat_means"] = {key: 0.0 for key in projection.get("stat_means") or {}}
    zeroed["projection_status"] = "out"
    zeroed["availability"] = {"version": VERSION, "rule": "zeroed", "status": status}
    return zeroed


def replacement_for(
    absent: Mapping[str, Any],
    teammates: Iterable[Mapping[str, Any]],
    statuses: Mapping[int, str],
) -> dict[str, Any] | None:
    """The next man up: same team and position, available, shallowest depth.

    Returns None rather than guessing when no candidate has a depth order —
    a silent wrong handoff is worse than no handoff.
    """
    candidates = [
        player for player in teammates
        if player.get("player_id") != absent.get("player_id")
        and player.get("team") == absent.get("team")
        and player.get("position") == absent.get("position")
        and not is_out(statuses.get(player.get("player_id")))
        and player.get("depth_order") is not None
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda player: (int(player["depth_order"]), str(player.get("player_name") or "")))


def transfer_opportunity(
    absent: Mapping[str, Any],
    replacement: Mapping[str, Any],
    *,
    cap: float = MAX_TRANSFER_MULTIPLIER,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Scale the replacement to the absent player's volume, keeping his own rates.

    Returns (projection, note). The note always explains what happened,
    including when nothing did.
    """
    position = str(replacement.get("position") or "")
    key = OPPORTUNITY_KEY.get(position)
    note: dict[str, Any] = {"version": VERSION, "rule": "inherits",
                            "from_player_id": absent.get("player_id"),
                            "from_player": absent.get("player_name"),
                            "opportunity_key": key}
    if key is None:
        note.update(applied=False, reason="no opportunity stat defined for this position")
        return dict(replacement), note

    target = _num((absent.get("stat_means") or {}).get(key))
    current = _num((replacement.get("stat_means") or {}).get(key))
    note.update(target_opportunity=round(target, 3), current_opportunity=round(current, 3))
    if target <= 0 or current <= 0:
        # No volume history to scale, or nothing to scale it to. Say so.
        note.update(applied=False, reason="no usable opportunity history for the transfer")
        return dict(replacement), note

    raw = target / current
    factor = min(raw, cap)
    note.update(raw_multiplier=round(raw, 3), multiplier=round(factor, 3), capped=raw > cap)

    stats = {k: _num(v) for k, v in (replacement.get("stat_means") or {}).items()}
    for stat in VOLUME_SCALED:
        if stat in stats:
            stats[stat] *= factor
    if key in stats:
        stats[key] = current * factor

    updated = dict(replacement)
    before = _num(replacement.get("model_proj_fpts"))
    after = draftkings_points(position, stats)
    updated["stat_means"] = {k: round(v, 4) for k, v in stats.items()}
    updated["model_proj_fpts"] = round(after, 4)
    # The interval is scaled proportionally rather than re-simulated. Crude but
    # honest, and flagged as such so nobody reads it as a fresh distribution.
    ratio = (after / before) if before > 0 else factor
    for bound in ("floor_fpts", "median_fpts", "ceiling_fpts"):
        if updated.get(bound) is not None:
            updated[bound] = round(_num(updated[bound]) * ratio, 4)
    note.update(applied=True, points_before=round(before, 3), points_after=round(after, 3),
                interval_scaling="proportional, not re-simulated")
    updated["availability"] = note
    return updated, note


def apply(
    projections: list[dict[str, Any]],
    statuses: Mapping[int, str],
    *,
    positions: Iterable[str] = ("QB",),
    cap: float = MAX_TRANSFER_MULTIPLIER,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Zero every OUT-class player; hand their opportunity to the replacement.

    `positions` limits which positions receive a transfer. Zeroing always
    applies to everyone — a player who is out scores zero regardless of
    whether we know who replaces him.
    """
    by_id = {player.get("player_id"): player for player in projections}
    result = {pid: dict(player) for pid, player in by_id.items()}
    report = {"version": VERSION, "zeroed": [], "transfers": [], "unresolved": []}

    absent = [p for p in projections if is_out(statuses.get(p.get("player_id")))]
    for player in absent:
        status = statuses.get(player.get("player_id"))
        result[player["player_id"]] = zero_out(player, str(status))
        report["zeroed"].append({"player_id": player.get("player_id"),
                                 "player": player.get("player_name"),
                                 "position": player.get("position"),
                                 "team": player.get("team"), "status": status})
        if player.get("position") not in set(positions):
            continue
        backup = replacement_for(player, projections, statuses)
        if backup is None:
            report["unresolved"].append({"player": player.get("player_name"),
                                         "team": player.get("team"),
                                         "position": player.get("position"),
                                         "reason": "no available teammate with a depth order"})
            continue
        updated, note = transfer_opportunity(player, result[backup["player_id"]], cap=cap)
        result[backup["player_id"]] = updated
        report["transfers"].append({"to": backup.get("player_name"), **note})

    return [result[player.get("player_id")] for player in projections], report
