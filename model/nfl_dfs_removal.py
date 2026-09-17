"""Was this player injured out of the game, or did he simply not produce?

The question is deliberately coarse. We are not trying to detect a hamstring
that cost somebody two series; we are trying to remove from the accuracy
population the players who were not available to earn their projection. A man
who tweaked something and came back is, for this purpose, a man who played.

Evidence, strongest first
------------------------
1. `nfl_pbp_archetypes.description` NAMES the injured player -- nflverse
   writes "M.Evans was injured during the play." into the play text. That is
   an observation with a timestamp, not an inference.
2. PRESENCE after that point. A target, carry or dropback in the fourth
   quarter is near-proof a player finished the game, and it works at every
   position.
3. ABSENCE. Weak, and weak asymmetrically: a quarterback touches nearly every
   snap so his silence means something, while a receiver can run a half of
   routes without producing a single row. Absence never produces a confident
   verdict here.

What the source cannot tell us
------------------------------
Whether a player was on the field without being thrown to. That is nflverse's
`participation` dataset (`offense_players` per play), which is published after
the postseason -- `pbp_participation_2026.parquet` is a 404 today. So in-season
there is no snap rate, and this module must not pretend otherwise.

The injury note also gives the team's real-time guess at return status
("His return is Questionable"), never the outcome. Return is inferred from a
later appearance, which is the strong direction.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

VERSION = "nfl-dfs-removal-v2"

# Roles meaning "the ball came to this player". Blocking and coverage roles
# would answer the better question -- was he on the field -- which this source
# cannot answer, so they are excluded rather than half-used.
BALL_ROLES = frozenset({"passer", "rusher", "receiver"})

# nflverse's fixed phrasing. Anchored on the phrase rather than on a name
# pattern: the phrase is stable, names are not (suffixes, apostrophes,
# hyphens, and the abbreviated "M.Evans" form).
_INJURY_RE = re.compile(
    r"((?:[A-Z][A-Za-z'\-]*[. ])?[A-Z][A-Za-z'\-]+(?: (?:Jr|Sr|II|III|IV)\.?)?)"
    r"\s+was injured during the play",
)

# A player last seen before this quarter, whose team kept playing, is a
# candidate worth a human glance. Quarter 4 (and overtime, 5) clears him.
LATE_QUARTER = 4

# Below this many team plays after a player's last appearance, silence is just
# the end of the game arriving.
MIN_PLAYS_AFTER = 10

VERDICTS = (
    "INJURED_OUT",        # named injured, never appeared again -- observed
    "INJURED_RETURNED",   # named injured, appeared later -- he played
    "PLAYED_LATE",        # present in Q4/OT -- cleared, whatever the box score says
    "LAST_SEEN_EARLY",    # the only ambiguous bucket; needs a human
    "NO_OPPORTUNITY",     # never threw, carried or was thrown to
)


def normalise_name(name: str) -> str:
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def name_keys(name: str) -> list[str]:
    """Every spelling one player might be written under.

    The report card says "Colston Loveland"; play-by-play says "C.Loveland".
    A missed join is indistinguishable from a player who never touched the
    ball -- which is the one distinction this module exists to draw -- so both
    forms are indexed.
    """
    keys = [normalise_name(name)]
    parts = [p for p in (name or "").replace(".", ". ").split() if p]
    if len(parts) >= 2:
        keys.append(normalise_name(parts[0][:1] + parts[-1]))
    return [k for k in keys if k]


@dataclass
class Appearance:
    """One player's footprint in one game, counted from participation rows."""
    player_name: str
    team: str | None = None
    first_play: int = 0
    last_play: int = 0
    last_quarter: int | None = None
    plays: set[int] = field(default_factory=set)
    roles: Counter = field(default_factory=Counter)

    @property
    def targets(self) -> int:
        return self.roles.get("receiver", 0)

    @property
    def carries(self) -> int:
        return self.roles.get("rusher", 0)

    @property
    def dropbacks(self) -> int:
        return self.roles.get("passer", 0)

    @property
    def touches(self) -> int:
        return self.targets + self.carries + self.dropbacks


@dataclass(frozen=True)
class InjuryEvent:
    game_id: str
    play_id: int
    quarter: int | None
    clock: str | None
    player_name: str
    description: str


def injury_events(plays: Iterable[Mapping[str, Any]]) -> list[InjuryEvent]:
    """Named injuries, read from the play text rather than inferred.

    `injury_on_play` is already stored as a boolean derived from this same
    phrase, but the boolean drops the name, which is the only part that lets
    an injury be attached to a projection.
    """
    out: list[InjuryEvent] = []
    for play in plays:
        text = play.get("description") or ""
        for match in _INJURY_RE.finditer(text):
            out.append(InjuryEvent(
                game_id=str(play.get("game_id") or ""),
                play_id=int(play.get("play_id") or 0),
                quarter=play.get("quarter"),
                clock=play.get("clock"),
                player_name=match.group(1).strip(),
                description=text,
            ))
    return out


def team_offensive_plays(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], list[int]]:
    """Sorted, de-duplicated offensive play ids per (game, team).

    De-duplication is load-bearing: one snap yields a passer row AND a
    receiver row, so counting rows would double every denominator.

    Counted from BALL_ROLES only, matching the TypeScript port exactly -- the
    two must agree, or a tag recorded from one becomes unexplainable by the
    other.
    """
    seen: dict[tuple[str, str], set[int]] = defaultdict(set)
    for row in rows:
        if row.get("side") != "offense" or row.get("role") not in BALL_ROLES:
            continue
        team, play, game = row.get("team"), row.get("play_id"), row.get("game_id")
        if team is None or play is None:
            continue
        seen[(str(game or ""), str(team))].add(int(play))
    return {key: sorted(plays) for key, plays in seen.items()}


def appearances(
    participants: Iterable[Mapping[str, Any]],
    quarters: Mapping[tuple[str, int], int] | None = None,
) -> dict[tuple[str, str], Appearance]:
    """One footprint per (game, player-name-key), indexed under every spelling.

    Keyed by game as well as name: two games in a week can carry the same
    surname, and a cross-game merge would invent a late appearance that never
    happened -- the exact error that would clear an injured player.
    """
    acc: dict[tuple[str, str], Appearance] = {}
    canonical: dict[tuple[str, str], Appearance] = {}
    for row in participants:
        role, name, play = row.get("role"), row.get("player_name"), row.get("play_id")
        if role not in BALL_ROLES or not name or play is None:
            continue
        play, game = int(play), str(row.get("game_id") or "")
        primary = (game, normalise_name(name))
        entry = canonical.get(primary)
        if entry is None:
            entry = canonical[primary] = Appearance(
                player_name=name, team=row.get("team"), first_play=play, last_play=play)
        entry.first_play = min(entry.first_play, play)
        entry.plays.add(play)
        entry.roles[role] += 1
        quarter = (quarters or {}).get((game, play))
        if play >= entry.last_play:
            entry.last_play = play
            # Only overwrite with a known quarter; an unmapped play must not
            # erase a quarter we already established.
            if quarter is not None:
                entry.last_quarter = quarter
        for key in name_keys(name):
            acc.setdefault((game, key), entry)
    return acc


def find(index: Mapping[tuple[str, str], Appearance], game_id: str, name: str) -> Appearance | None:
    for key in name_keys(name):
        found = index.get((game_id, key))
        if found is not None:
            return found
    return None


def injuries_for(events: Sequence[InjuryEvent], game_id: str, name: str) -> list[InjuryEvent]:
    keys = set(name_keys(name))
    return sorted(
        (e for e in events
         if e.game_id == game_id and keys & set(name_keys(e.player_name))),
        key=lambda e: e.play_id,
    )


def classify(
    *,
    position: str,
    appearance: Appearance | None,
    team_plays: Sequence[int] = (),
    injuries: Sequence[InjuryEvent] = (),
) -> dict[str, Any]:
    """Propose one availability verdict, with the evidence that produced it.

    Availability only. Whether a player converted the chances he got is a
    different axis and is reported as evidence (targets, carries), never
    folded into this label -- one verdict per question, or neither can be
    acted on.

    No probability is returned. A number here would invite reading a proposal
    as a finding.
    """
    position = (position or "").upper()
    evidence: dict[str, Any] = {
        "targets": 0, "carries": 0, "dropbacks": 0, "plays": 0,
        "last_play": None, "last_quarter": None, "team_plays": len(team_plays),
        "plays_after": None,
        "injuries": [{"quarter": e.quarter, "clock": e.clock, "name": e.player_name,
                      "description": e.description} for e in injuries],
    }

    if appearance is None:
        evidence["injured_without_appearing"] = bool(injuries)
        return {
            "verdict": "NO_OPPORTUNITY", "version": VERSION,
            "confidence": "high" if injuries else "low",
            "reason": ("Named as injured and never threw, carried or was thrown to."
                       if injuries else
                       "No passer, rusher or receiver row in this game."),
            "evidence": evidence,
        }

    plays_after = sum(1 for p in team_plays if p > appearance.last_play)
    evidence.update({
        "targets": appearance.targets, "carries": appearance.carries,
        "dropbacks": appearance.dropbacks, "plays": len(appearance.plays),
        "last_play": appearance.last_play, "last_quarter": appearance.last_quarter,
        "plays_after": plays_after if team_plays else None,
    })

    if injuries:
        first = injuries[0]
        after = [p for p in appearance.plays if p > first.play_id]
        if after:
            return {
                "verdict": "INJURED_RETURNED", "version": VERSION, "confidence": "high",
                "reason": (f"Injury noted in Q{first.quarter or '?'}, then {len(after)} "
                           "further touches. He came back and played."),
                "evidence": evidence,
            }
        return {
            "verdict": "INJURED_OUT", "version": VERSION, "confidence": "high",
            "reason": (f"Named as injured in Q{first.quarter or '?'}"
                       f"{f' ({first.clock})' if first.clock else ''} and never "
                       "touched the ball again."),
            "evidence": evidence,
        }

    quarter = appearance.last_quarter
    if quarter is not None and quarter >= LATE_QUARTER:
        return {
            "verdict": "PLAYED_LATE", "version": VERSION, "confidence": "high",
            "reason": (f"Still being given the ball in Q{quarter}. Whatever the box "
                       "score says, availability was not the problem."),
            "evidence": evidence,
        }
    if not appearance.touches:
        return {
            "verdict": "NO_OPPORTUNITY", "version": VERSION, "confidence": "medium",
            "reason": "On the roster, never targeted or handed the ball.",
            "evidence": evidence,
        }
    if team_plays and plays_after < MIN_PLAYS_AFTER:
        return {
            "verdict": "PLAYED_LATE", "version": VERSION, "confidence": "medium",
            "reason": (f"Last touch came with only {plays_after} offensive plays left. "
                       "That is the game ending, not an exit."),
            "evidence": evidence,
        }
    detail = (f"Last touch in Q{quarter}, with {plays_after} team plays after it."
              if quarter is not None else
              f"{plays_after} team plays came after his last touch.")
    note = ("A quarterback touches nearly every snap, so this absence is meaningful."
            if position == "QB" else
            "At this position, participation rows record touches rather than snaps, "
            "so absence is weak evidence on its own.")
    return {
        "verdict": "LAST_SEEN_EARLY", "version": VERSION,
        "confidence": "medium" if position == "QB" else "low",
        "reason": f"{detail} No injury was recorded against his name. {note}",
        "evidence": evidence,
    }
