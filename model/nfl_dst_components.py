"""DST scoring components derived from play-by-play rather than team aggregates.

## Why this exists

DST points were built from nflverse's team-week aggregate release
(`raw_team_stats`). That release drops events. Measured against DraftKings'
own published contest scoring for the 2026 week-2 main slate (contest
195648006, 26 team-defenses with a known DK score):

| component source | agrees with DraftKings |
|---|---|
| team-week aggregate (previous) | 24 / 26 |
| play-by-play (this module)     | **26 / 26** |

The two failures were not rounding. New England was credited 15.0 against
DK's 21.0 because a strip-sack fumble returned for a touchdown reached the
aggregate as `def_tds: 0`; Carolina 25.0 against 26.0 because a third sack
was recorded as two. Both events are present and unambiguous in the
play-by-play, which is the record the aggregate is built from.

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

## The two rules that were measured, not assumed

**Fumble recoveries come from `turnover_type`, not the play text.** Parsing
`RECOVERED by <TEAM>-` from the description looks more complete -- it also
catches a muffed kick return the kicking team recovers -- and it scored
23/26, worse than both alternatives. DraftKings does not credit every such
recovery. The narrower rule scored 26/26 and is therefore the one used.

**A defensive touchdown requires a turnover on the same play.** That
distinguishes a pick-six or scoop-and-score, which the defense is paid for,
from an offensive touchdown in a game the team happened to be defending.

Nullified plays are excluded everywhere: a touchdown wiped by penalty is not
a touchdown.

## Honest limits

One week of ground truth (26 team-games) is what this is validated on -- the
only week for which a DraftKings contest export is on hand. It is better than
the aggregate on that week and never worse on any team. Weeks 1 and 2 also
disagree with the aggregate on a handful of teams in the other direction
(Chicago and the Jets in week 1, where the aggregate records a fumble
recovery the play-by-play does not); those cannot be adjudicated without
another contest export, so the caller records the disagreement rather than
assuming this module is right.

Pure: no database access, no I/O.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping

from model.nfl_team_aliases import normalize_team

VERSION = "nfl-dst-components-pbp-v1"

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


def _text(play: Mapping[str, Any], key: str) -> str:
    value = play.get(key)
    return "" if value is None else str(value)


def derive_dst_components(plays: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    """Per-defense component counts, keyed by canonical team abbreviation.

    `plays` rows need `defteam`, `turnover_type`, `had_sack`, `st_outcome`
    and `description`. A play with no `defteam` is skipped: nobody is on
    defense on it, so crediting anyone would be an invention.

    Every team appearing on defense gets an entry, including an all-zero one,
    so a genuinely quiet defense is distinguishable from a team absent from
    the feed entirely.
    """
    components: dict[str, dict[str, float]] = defaultdict(
        lambda: {name: 0.0 for name in DERIVED_COMPONENTS}
    )

    for play in plays:
        defense = normalize_team(play.get("defteam"))
        if not defense:
            continue
        counts = components[defense]

        description = _text(play, "description")
        # A play wiped by penalty did not happen for scoring purposes.
        live = not _NULLIFIED.search(description)

        if play.get("had_sack"):
            counts["sacks"] += 1.0

        turnover = play.get("turnover_type")
        if turnover == "interception":
            counts["interceptions"] += 1.0
        if turnover == "fumble_lost":
            counts["fumble_recoveries"] += 1.0

        if play.get("st_outcome") == "blocked":
            counts["blocked_kicks"] += 1.0

        if live and _SAFETY.search(description):
            counts["safeties"] += 1.0

        # Paid only when the defense took the ball away on the same play.
        if live and turnover and _TOUCHDOWN.search(description):
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
