"""What the penalty erased -- recovering the football under a `no_play` row.

When a flag wipes a snap, nflverse records the penalty and ZEROES everything
else. Verified on 2025: across all 4,723 `no_play` rows, `yards_gained`,
`sack`, `interception`, `touchdown`, `complete_pass` and `fumble` are all
zero without exception. The football that happened survives in exactly one
place, the description string, and nowhere else in the release.

So a season loses, silently:

    49 touchdowns        87 sacks        26 interceptions
    23 fumbles           683 snaps that gained or lost real yardage

That is 21.3 erased snaps a team-season, well above the floor for a
team-level claim, and it distorts in a specific direction: an offence that
keeps moving the ball and keeps having it called back is indistinguishable
from one that cannot move it at all. A defence's takeaway rate is short 49
turnovers that nobody is charged with. A pass rusher's sack count is short 87.

WHAT THIS MODULE DOES NOT DO. It does not score the wiped yardage -- it did
not count, and the scoreboard is right. `wiped_*` is a parallel record of
what was erased, never mixed into `yards_gained` or any rate built on it.
The penalty remains the outcome of the down.

PRECEDENCE IS DOCUMENTED AND THE OVERLAPS ARE FLAGS, per this project's
standing rule. `wiped_event` is single-valued so it can carry a denominator;
`wiped_touchdown`, `wiped_turnover` and `wiped_sack` are independent booleans
so a strip-sack that was also erased loses neither half. A sack that fumbled
sets `wiped_event = "sack"` AND `wiped_turnover = True`.

THE YARDAGE RULE, stated because the naive reading is wrong. A description
can contain several "for N yards" and they do not all belong to the offence:

    INTERCEPTED by 42-A.Wingard at JAX 25. 42-A.Wingard for 75 yards, TOUCHDOWN

The 75 is the RETURN. The offence gained nothing. So the first yardage token
is taken only when it appears BEFORE any turnover marker; after one, the
offence's wiped gain is zero and the return is recorded separately. Reading
the first number unconditionally would credit an intercepted quarterback with
a 75-yard play.
"""
from __future__ import annotations

import re

import pandas as pd

VERSION = "nfl-wiped-plays-v1"

_YARDS = re.compile(r"for (-?\d+) yards?", re.I)
_NO_GAIN = re.compile(r"for no gain", re.I)
_TURNOVER_MARK = re.compile(r"INTERCEPTED|FUMBLES", re.I)
_INTERCEPTOR = re.compile(r"INTERCEPTED by \d+-([A-Z][\w.'\-]+)", re.I)
_SACKER = re.compile(r"sacked at [A-Z]{2,3} \d+ for -?\d+ yards? \((?:sack split by )?\d+-([A-Z][\w.'\-]+)", re.I)


def _first_yardage(desc: str) -> float | None:
    """Offensive yardage on the wiped snap, or None if the text does not say.

    Only counts a token appearing before a turnover marker -- see the module
    docstring on why the naive first-match is wrong.
    """
    turn = _TURNOVER_MARK.search(desc)
    window = desc[:turn.start()] if turn else desc
    match = _YARDS.search(window)
    if match:
        return float(match.group(1))
    if _NO_GAIN.search(window):
        return 0.0
    # A turnover before any yardage token means the offence gained nothing.
    return 0.0 if turn else None


def _event(desc: str) -> str | None:
    """Single-valued, so it can carry a denominator. Overlaps are flags."""
    if not desc:
        return None
    low = desc.lower()
    if "intercepted" in low:
        return "interception"
    if "sacked" in low:
        return "sack"
    if "fumbles" in low:
        return "fumble"
    if "scrambles" in low:
        return "scramble"
    if "incomplete" in low:
        return "incompletion"
    if " pass short" in low or " pass deep" in low:
        return "completion"
    if re.search(r"(left|right) (end|guard|tackle)|up the middle", low):
        return "run"
    if "punts" in low or "field goal" in low or "kicks" in low:
        return "kick"
    return None


def wiped(pbp: pd.DataFrame) -> pd.DataFrame:
    """One row per play, `wiped_*` populated only where a flag erased football.

    Returned aligned to `pbp.index` so it can be assigned column-wise onto the
    play frame without a join.
    """
    desc = pbp.get("desc", pd.Series("", index=pbp.index)).fillna("")
    is_no_play = pbp.get("play_type", pd.Series("", index=pbp.index)).astype(str).eq("no_play")

    event = pd.Series(None, index=pbp.index, dtype=object)
    yards = pd.Series(float("nan"), index=pbp.index, dtype="float64")
    for idx in pbp.index[is_no_play]:
        text = desc.at[idx]
        event.at[idx] = _event(text)
        found = _first_yardage(text)
        if found is not None:
            yards.at[idx] = found

    low = desc.str.lower()
    return pd.DataFrame({
        "wiped_event": event,
        "wiped_yards": yards,
        # Independent, so a precedence order cannot take either from the other.
        "wiped_touchdown": is_no_play & desc.str.contains("TOUCHDOWN", regex=False),
        "wiped_turnover": is_no_play & low.str.contains("intercepted|fumbles", regex=True),
        "wiped_sack": is_no_play & low.str.contains("sacked", regex=False),
        # Who the flag took the credit from. NULL where the text does not name
        # them -- a guessed defender is worse than an absent one.
        "wiped_defender": pd.Series(
            [(_INTERCEPTOR.search(t) or _SACKER.search(t) or [None])
             for t in desc.where(is_no_play, "")],
            index=pbp.index,
        ).map(lambda m: m.group(1) if hasattr(m, "group") else None),
    })
