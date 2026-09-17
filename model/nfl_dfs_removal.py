"""Did a player leave the game, or did he just not produce?

This module answers one narrow question from play-by-play participation:
between "he stopped appearing" and "he appeared and nothing came of it".
It PROPOSES; a human confirms. Nothing here writes a tag, and nothing here
changes a projection or a score.

Why it can be trusted more at QB than at WR/TE
----------------------------------------------
`nfl_pbp_play_participants` records EVENT participation, not snaps: a row
exists because a player was the passer, rusher or receiver on that play, not
because he was on the field. A quarterback touches nearly every offensive
snap, so his last appearance is a good proxy for his last snap. A receiver can
run twenty routes without a row. So the same silence means "he left" at QB and
means very little at WR. Every verdict carries that distinction explicitly
rather than flattening it into one confidence number.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

VERSION = "nfl-dfs-removal-v1"

# Roles that mean "the ball came to this player". Blocking, coverage and
# special-teams roles are deliberately excluded: they say the player was on
# the field, which is a different (and for this purpose better) question that
# this source cannot answer.
BALL_ROLES = frozenset({"passer", "rusher", "receiver"})

# Fraction of the team's offensive plays after which a player's silence stops
# looking like a gap between touches and starts looking like an absence.
SILENCE_THRESHOLD = 0.25

# A quarterback who takes this share of his team's offensive snaps is the
# starter for the purpose of "did the starter leave".
QB_WORKLOAD_FLOOR = 0.5

VERDICTS = ("LIKELY_REMOVED", "OPPORTUNITY_NO_CONVERSION", "NO_OPPORTUNITY", "NORMAL", "UNKNOWN")


@dataclass(frozen=True)
class Appearance:
    """One player's footprint in one game, counted from participation rows."""
    player_id: str | None
    player_name: str
    team: str | None
    first_play: int
    last_play: int
    plays: int
    by_role: Mapping[str, int]

    @property
    def targets(self) -> int:
        return self.by_role.get("receiver", 0)

    @property
    def carries(self) -> int:
        return self.by_role.get("rusher", 0)

    @property
    def dropbacks(self) -> int:
        return self.by_role.get("passer", 0)


def team_offensive_plays(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[int]]:
    """Sorted, de-duplicated offensive play ids per team.

    De-duplication matters: the table is one row per player per role, so a
    single snap contributes a passer row and a receiver row and would
    otherwise be counted twice, deflating every share computed from it.

    Counted from BALL_ROLES only, so the denominator is "snaps where somebody
    threw, carried or was thrown to" rather than every credited offensive
    player. That is a slight undercount of true snaps, and it is the
    definition the TypeScript port uses too — the two must agree exactly, or
    a tag recorded from one becomes unexplainable by the other.
    """
    seen: dict[str, set[int]] = defaultdict(set)
    for row in rows:
        if row.get("side") != "offense" or row.get("role") not in BALL_ROLES:
            continue
        team, play = row.get("team"), row.get("play_id")
        if team is None or play is None:
            continue
        seen[team].add(int(play))
    return {team: sorted(plays) for team, plays in seen.items()}


def appearances(rows: Iterable[Mapping[str, Any]]) -> dict[str, Appearance]:
    """Collapse participation rows into one footprint per player.

    Keyed on player_id when present and on name otherwise — the source leaves
    player_id null often enough that dropping those rows would silently lose
    players, and a name collision is the lesser error here because the result
    is only ever a proposal a human reads.
    """
    acc: dict[str, dict[str, Any]] = {}
    for row in rows:
        role = row.get("role")
        if role not in BALL_ROLES:
            continue
        play = row.get("play_id")
        name = row.get("player_name")
        if play is None or not name:
            continue
        play = int(play)
        key = str(row.get("player_id") or f"name:{name}")
        entry = acc.get(key)
        if entry is None:
            entry = acc[key] = {
                "player_id": row.get("player_id"), "player_name": name,
                "team": row.get("team"), "first": play, "last": play,
                "plays": set(), "roles": Counter(),
            }
        entry["first"] = min(entry["first"], play)
        entry["last"] = max(entry["last"], play)
        entry["plays"].add(play)
        entry["roles"][role] += 1
    return {
        key: Appearance(
            player_id=e["player_id"], player_name=e["player_name"], team=e["team"],
            first_play=e["first"], last_play=e["last"], plays=len(e["plays"]),
            by_role=dict(e["roles"]),
        )
        for key, e in acc.items()
    }



def silence_share(appearance: Appearance, team_plays: Sequence[int]) -> float | None:
    """Share of the team's offensive plays that happened after this player's
    last appearance. None when the team's plays are unknown."""
    if not team_plays:
        return None
    after = sum(1 for play in team_plays if play > appearance.last_play)
    return after / len(team_plays)


def classify(
    *,
    position: str,
    appearance: Appearance | None,
    team_plays: Sequence[int],
    receptions: float | None = None,
) -> dict[str, Any]:
    """Propose one verdict, with the evidence that produced it.

    Never returns a probability. The caller shows this to a person, who
    decides; a number here would invite treating a proposal as a finding.
    """
    position = (position or "").upper()
    if appearance is None:
        return {
            "verdict": "NO_OPPORTUNITY", "version": VERSION, "confidence": "low",
            "reason": "No passer, rusher or receiver row in this game.",
            "evidence": {"plays": 0, "targets": 0, "carries": 0, "dropbacks": 0},
        }

    share = silence_share(appearance, team_plays)
    touches = appearance.targets + appearance.carries + appearance.dropbacks
    evidence = {
        "plays": appearance.plays, "targets": appearance.targets,
        "carries": appearance.carries, "dropbacks": appearance.dropbacks,
        "last_play": appearance.last_play, "team_plays": len(team_plays),
        "silence_share": share, "receptions": receptions,
    }

    # A quarterback's silence is meaningful; a receiver's mostly is not.
    if position == "QB":
        workload = appearance.dropbacks / len(team_plays) if team_plays else None
        started = workload is not None and workload >= QB_WORKLOAD_FLOOR
        if share is not None and share >= SILENCE_THRESHOLD and started:
            return {
                "verdict": "LIKELY_REMOVED", "version": VERSION, "confidence": "high",
                "reason": (f"Took {workload:.0%} of offensive snaps, then none of the "
                           f"final {share:.0%}."),
                "evidence": evidence,
            }
        if share is not None and share >= SILENCE_THRESHOLD:
            return {
                "verdict": "UNKNOWN", "version": VERSION, "confidence": "low",
                "reason": "Absent late, but never carried a starter's snap share — "
                          "more likely a backup than a removal.",
                "evidence": evidence,
            }
    elif share is not None and share >= SILENCE_THRESHOLD and touches:
        return {
            "verdict": "UNKNOWN", "version": VERSION, "confidence": "low",
            "reason": (f"No touch in the final {share:.0%} of snaps. At this position "
                       "that is common without an injury — participation rows record "
                       "touches, not snaps."),
            "evidence": evidence,
        }

    if touches and (receptions is not None and receptions <= 0) and appearance.targets > 0:
        return {
            "verdict": "OPPORTUNITY_NO_CONVERSION", "version": VERSION, "confidence": "high",
            "reason": f"Targeted {appearance.targets}x with no catch — the ball came, "
                      "it did not stick. Not an availability problem.",
            "evidence": evidence,
        }
    if not touches:
        return {
            "verdict": "NO_OPPORTUNITY", "version": VERSION, "confidence": "medium",
            "reason": "On the roster, never targeted or handed the ball.",
            "evidence": evidence,
        }
    return {
        "verdict": "NORMAL", "version": VERSION, "confidence": "medium",
        "reason": f"{touches} touches spread across the game.",
        "evidence": evidence,
    }
