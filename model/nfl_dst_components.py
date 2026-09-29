"""DST scoring components derived from play-by-play rather than team aggregates.

## Why this exists

DST points were built from nflverse's team-week aggregate release
(`raw_team_stats`). That release drops events. Measured against DraftKings'
own published contest scoring for the 2026 week-2 main slate (contest
195648006, 26 team-defenses with a known DK score):

| component source | agrees with DraftKings |
|---|---|
| team-week aggregate            | 24 / 26 |
| play-by-play, v1 of this module | 26 / 26 |

The two failures were not rounding. New England was credited 15.0 against
DK's 21.0 because a strip-sack fumble returned for a touchdown reached the
aggregate as `def_tds: 0`; Carolina 25.0 against 26.0 because a third sack
was recorded as two. Both events are present and unambiguous in the
play-by-play, which is the record the aggregate is built from.

## v2 (2026-09-28): special-teams fumble recoveries, and only the ruling that stands

Week 3 broke v1. Against the four imported 2026 contests (weeks 2 and 3,
classic and showdown, 56 team-defenses with a DK score):

| DST points from | week 2 | week 3 |
|---|---|---|
| team-week aggregate                            | 26 / 28 | 27 / 28 |
| play-by-play v1 (fumbles from `turnover_type`) | 28 / 28 | 23 / 28 |
| play-by-play v2 (recovery events)              | 28 / 28 | **28 / 28** |

All five week-3 misses were exactly 2 points (one fumble recovery) low, and all
five were special-teams recoveries by the KICKING team: three kickoff-return
fumbles (Carolina, New Orleans, Giants) and two muffed punts (Arizona,
Pittsburgh). v1 could never see them. It read `turnover_type`, which the play
labeller sets for SCRIMMAGE snaps only (`model/nfl_play_archetypes._turnover_type`
-- deliberately, because nflverse books a punt muff against the wrong team's
row). DraftKings pays a defense/special-teams unit for these recoveries, so
v1 was structurally short every time one happened: 108 team-weeks in
2023-2026, against the aggregate.

v2 counts the recovery EVENT, in the NFL gamebook's own notation. The play
text writes a recovery that changes possession as upper-case
`RECOVERED by <TEAM>-` and one the fumbling team keeps as lower-case
`recovered by <TEAM>-`; checked on every special-teams recovery 2016-2026
(`punt ... MUFFS catch, RECOVERED by CHI-81` vs `MUFFS catch, recovered by
NYJ-37`). A recovery is credited to the team named when the loose ball
immediately before it came from `FUMBLES` or `MUFFS` -- not from a blocked
kick (scored as a block, not a recovery) and not from an onside kick (nobody
fumbled). Against the nflverse team aggregate's `fumble_recovery_opp`, which
nflverse derives from its structured recovery columns, this agrees on
**1,726 / 1,726** team-weeks (2023-2026); v1 agreed on 1,611.

**The earlier "wider text rule is worse" finding was misattributed.** v1's
notes said parsing `RECOVERED by` scored 23/26 in week 2 "because DraftKings
does not credit every such recovery". Reproduced: the text rule's week-2
misses were Denver (a strip-sack recovery on a play wiped by penalty, `- No
Play`) and New Orleans (a kickoff recovery the replay official REVERSED).
Both were recoveries that did not stand, read out of overturned text. Week 2
had no standing special-teams recovery at all, so it could not show the gap
week 3 exposed. DraftKings does pay kicking-team recoveries; it does not pay
rulings that were overturned.

Hence the second v2 rule, applied to every text-derived component: **score
only the ruling that stands.** When a replay review or challenge reverses a
play, the description carries the overturned ruling first and the standing
one after `REVERSED.`; only the text after the last `REVERSED.` is read. That
also removes 16 defensive touchdowns and 2 safeties v1 credited from
overturned text in 2023-2025 (for example an interception return touchdown
reversed to down-by-contact). A recovery on a play wiped by penalty (`- No
Play`) is not a recovery. A safety enforced on a penalty (`SAFETY - No Play`,
holding in the end zone) still is, and is still counted.

A defensive touchdown additionally requires that the DEFENSE held the ball
when it was scored: the last change of possession before `TOUCHDOWN` must be
an interception or an upper-case recovery by the defense. That stops an
interception fumbled back to the offense and returned for a score being paid
to the defense that threw it away (Arizona, 2025 week 5).

## What is derived here, and what is not

Derived from plays: sacks, interceptions, fumble recoveries, safeties,
blocked kicks, defensive touchdowns.

NOT derived here: **points allowed** and **special-teams return touchdowns**.
Points allowed needs the final score and the opponent's own defensive scores,
which the caller already resolves; special-teams return touchdowns are scored
to the returning team, which this table's `defteam` does not identify on a
kick. Both continue to come from the caller. This module returns only what it
can source honestly, so a missing component is visibly absent rather than
silently zero.

## Honest limits

DraftKings ground truth is 56 team-defense scores over two weeks. Two rare
classes follow the recovery definition and the structured aggregate but have
no DraftKings case yet: a fumble out of the end zone for a touchback (no one
recovers it, so no recovery is credited; v1 credited one, ~2 a season), and an
interception fumbled back to the original offense (that team recovered an
opponent's fumble and is credited; ~2 a season). Sacks and defensive
touchdowns still disagree with the aggregate: it records one sack fewer on 33
team-weeks in 2023-2026, and its `def_tds` omits fumble-return touchdowns by
construction (its total, 106, equals this module's interception-return
touchdowns exactly; the 51 remaining differences are all fumble returns).
DraftKings sided with the play record each time either was tested (Carolina
and Jacksonville sacks, New England's scoop-and-score), so the caller records
those disagreements rather than assuming either source is right.

Pure: no database access, no I/O.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping

from model.nfl_team_aliases import normalize_team

VERSION = "nfl-dst-components-pbp-v2"

#: Components this module sources from plays. Anything outside this set is the
#: caller's to supply; listing it explicitly keeps the boundary auditable.
DERIVED_COMPONENTS = (
    "sacks",
    "interceptions",
    "fumble_recoveries",
    "safeties",
    "blocked_kicks",
    "defensive_tds",
)

_TOUCHDOWN = re.compile(r"\bTOUCHDOWN\b", re.IGNORECASE)
_NULLIFIED = re.compile(r"NULLIFIED", re.IGNORECASE)
_SAFETY = re.compile(r"\bSAFETY\b", re.IGNORECASE)
_NO_PLAY = re.compile(r"-\s*No Play\b", re.IGNORECASE)

# Everything before the last of these is a ruling that was overturned.
_REVERSED = re.compile(r"\bREVERSED\.")

# Case-SENSITIVE on purpose: upper-case is the gamebook's change-of-possession
# recovery; lower-case is the fumbling team keeping its own ball.
_RECOVERED = re.compile(r"RECOVERED by ([A-Z]{2,3})-")

# What put the ball on the ground. Only a fumble or a muff makes the next
# recovery a fumble recovery; a blocked kick or an onside kick does not.
_LOOSE_BALL = re.compile(r"FUMBLES|MUFFS|BLOCKED|kicks onside")
_FUMBLED = ("FUMBLES", "MUFFS")

# A change of possession on a scrimmage snap. `INTERCEPTED by` names a player,
# not a team; the interceptor is always the snap's defense.
_POSSESSION_GAINED = re.compile(r"INTERCEPTED by|RECOVERED by ([A-Z]{2,3})-")


def _text(play: Mapping[str, Any], key: str) -> str:
    value = play.get(key)
    return "" if value is None else str(value)


def standing_ruling(description: str) -> str:
    """The part of a play description that stands after replay review.

    A reversed play is written overturned-ruling first, then `...and the play
    was REVERSED.`, then the ruling that counts. An upheld review keeps its
    only ruling, so nothing is cut.
    """
    return _REVERSED.split(description)[-1]


def fumble_recoveries(description: str) -> list[str]:
    """Teams credited with a fumble recovery on one play, in order.

    One entry per upper-case recovery of a fumbled or muffed ball in the
    standing ruling, on scrimmage and special-teams plays alike. A play wiped
    by penalty has none.
    """
    text = standing_ruling(description)
    if _NO_PLAY.search(text):
        return []
    credited = []
    for match in _RECOVERED.finditer(text):
        causes = _LOOSE_BALL.findall(text, 0, match.start())
        if causes and causes[-1] in _FUMBLED:
            team = normalize_team(match.group(1))
            if team:
                credited.append(team)
    return credited


def _scored_by_defense(text: str, defense: str) -> bool:
    """Was the defense holding the ball when the touchdown was scored?"""
    touchdown = _TOUCHDOWN.search(text)
    if not touchdown:
        return False
    gained = list(_POSSESSION_GAINED.finditer(text, 0, touchdown.start()))
    if not gained:
        return False
    last = gained[-1]
    if last.group(0).startswith("INTERCEPTED"):
        return True
    return normalize_team(last.group(1)) == defense


def derive_dst_components(plays: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    """Per-defense component counts, keyed by canonical team abbreviation.

    `plays` rows need `defteam`, `turnover_type`, `had_sack`, `st_outcome`
    and `description`. A play with no `defteam` is skipped: nobody is on
    defense on it, so crediting anyone would be an invention.

    Every team appearing on defense gets an entry, including an all-zero one,
    so a genuinely quiet defense is distinguishable from a team absent from
    the feed entirely. A fumble recovery is credited to the team that made it,
    which on a punt is the punting team -- the play's offense.
    """
    components: dict[str, dict[str, float]] = defaultdict(
        lambda: {name: 0.0 for name in DERIVED_COMPONENTS}
    )

    for play in plays:
        defense = normalize_team(play.get("defteam"))
        if not defense:
            continue
        counts = components[defense]

        standing = standing_ruling(_text(play, "description"))
        # A play wiped by penalty did not happen for scoring purposes.
        live = not _NULLIFIED.search(standing)

        if play.get("had_sack"):
            counts["sacks"] += 1.0

        turnover = play.get("turnover_type")
        if turnover == "interception":
            counts["interceptions"] += 1.0

        for team in fumble_recoveries(_text(play, "description")):
            components[team]["fumble_recoveries"] += 1.0

        if play.get("st_outcome") == "blocked":
            counts["blocked_kicks"] += 1.0

        if live and _SAFETY.search(standing):
            counts["safeties"] += 1.0

        # Paid only when the defense took the ball away on the same play and
        # still had it when the touchdown was scored.
        if live and turnover and _scored_by_defense(standing, defense):
            counts["defensive_tds"] += 1.0

    return dict(components)


def compare_components(
    derived: Mapping[str, float] | None,
    aggregate: Mapping[str, float] | None,
) -> dict[str, dict[str, float]]:
    """Components where the two sources disagree, as {name: {pbp, aggregate}}.

    Returned for the evidence ledger, never to decide which source wins. An
    empty dict means the two independent records agree, which is itself worth
    recording.
    """
    if not derived or not aggregate:
        return {}
    out: dict[str, dict[str, float]] = {}
    for name in DERIVED_COMPONENTS:
        a = float(derived.get(name) or 0.0)
        b = float(aggregate.get(name) or 0.0)
        if a != b:
            out[name] = {"play_by_play": a, "team_aggregate": b}
    return out
