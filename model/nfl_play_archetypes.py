"""Label every play with an archetype -- the TRANSITION layer.

A drive archetype is a terminal state: how the possession ended. A play
archetype is a transition: what this snap did to the down-distance-field-
position state. Both are needed for the state machine to be a state machine.

=============================================================================
THE TAXONOMY -- 12 archetypes, mutually exclusive and exhaustive
=============================================================================
Precedence runs top to bottom; the first match wins. The ordering is the
design, so it is stated rather than left implicit in the code.

  SPECIAL_TEAMS          Punt, field goal, kickoff, extra point. Not a
                         scrimmage transition.
  KNEEL_SPIKE            Victory formation or a clock stop. A deliberate
                         non-play; counting it as a failed run would slander
                         every offence that ever led late.
  PENALTY                A flag wiped the play out (`no_play`). The down did
                         not resolve, so no outcome label can apply.
  TURNOVER_PLAY          Interception or lost fumble. Ranked above SACK and
                         above the down labels because losing the ball is the
                         transition; the down it happened on is a modifier.
  SACK                   Kept separate from a stuffed run: it is a negative
                         play AND a quarterback/protection event, and the two
                         have different persistence.
  GOAL_LINE_PUNCH        A rush from inside the opponent's 5, any down. A
                         1-yard gain here is a near-touchdown, not the
                         "modest" gain the yardage alone suggests.

  LATE_DOWN_CONVERSION   3rd or 4th down, line to gain reached.
  LATE_DOWN_FAILURE      3rd or 4th down, short of it.

  EARLY_DOWN_EXPLOSIVE   1st or 2nd down, gain >= 20.
  EARLY_DOWN_SUCCESS     1st or 2nd down, meets the standard success
                         threshold (40% of the distance on 1st, 60% on 2nd).
  EARLY_DOWN_STUFF       1st or 2nd down, gain <= 0.
  EARLY_DOWN_MODEST      1st or 2nd down, positive but not successful. The
                         residual bucket, and named so it reads as one.

Late downs rank ABOVE goal line except for a rush inside the 5, because on
3rd and 4th down the conversion is the decision-relevant fact and field
position is already carried as a modifier. GOAL_LINE_PUNCH outranks both
because a 1-yard rush at the 2 is a different event from a 1-yard rush at
midfield on any down.

=============================================================================
MODIFIERS -- carried alongside, never folded into the archetype
=============================================================================
  down, ydstogo, yardline_100    raw state, so any cut can be recovered
  distance_bucket                short <=3 / medium 4-6 / long >=7
  success                        the standard down-weighted success criterion
  explosive                      gain >= 20
  shotgun, no_huddle             available in nflverse, carried untouched
  epa, wp                        nflverse's own, never recomputed here

SUCCESS is the conventional 40/60/100 percent-of-distance rule, not something
invented here. It is stated as a constant so a later screen cannot move it
quietly.
"""
from __future__ import annotations

import pandas as pd

VERSION = "nfl-play-archetype-v1"

EXPLOSIVE_PLAY_YARDS = 20
SHORT_DISTANCE = 3
LONG_DISTANCE = 7
GOAL_LINE_YARDLINE = 5
# Conventional success thresholds: share of the distance needed, by down.
SUCCESS_SHARE = {1: 0.40, 2: 0.60, 3: 1.00, 4: 1.00}

SPECIAL_TEAMS_PLAYS = ("punt", "field_goal", "kickoff", "extra_point")


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def label_plays(pbp: pd.DataFrame) -> pd.DataFrame:
    """One row per play, in game order, with archetype and modifiers."""
    frame = pbp[pbp["posteam"].notna()].copy()
    frame = frame.sort_values("play_id")

    down = _num(frame, "down")
    togo = _num(frame, "ydstogo")
    gain = _num(frame, "yards_gained")
    yardline = _num(frame, "yardline_100")
    play_type = frame.get("play_type", pd.Series("", index=frame.index)).astype(str)

    share = down.map(SUCCESS_SHARE)
    success = (gain >= (togo * share)) & down.notna() & togo.notna()
    explosive = gain >= EXPLOSIVE_PLAY_YARDS
    converted = (gain >= togo) & togo.notna()

    out = pd.DataFrame({
        "game_id": frame.get("game_id"),
        "play_id": _num(frame, "play_id").astype("Int64"),
        "drive": _num(frame, "fixed_drive").astype("Int64"),
        "team": frame["posteam"],
        "quarter": _num(frame, "qtr").astype("Int64"),
        "clock": frame.get("time"),
        "down": down.astype("Int64"),
        "ydstogo": togo.astype("Int64"),
        "yardline_100": yardline.astype("Int64"),
        "play_type": play_type,
        "yards_gained": gain,
        "distance_bucket": _distance_bucket(togo, down),
        "success": success.fillna(False),
        "explosive": explosive.fillna(False),
        "shotgun": _num(frame, "shotgun").fillna(0).astype(bool),
        "no_huddle": _num(frame, "no_huddle").fillna(0).astype(bool),
        "epa": _num(frame, "epa").round(3),
        "wp": _num(frame, "wp").round(4),
        "description": frame.get("desc"),
    })
    out["play_archetype"] = _archetypes(frame, down, togo, gain, yardline, play_type,
                                        success, explosive, converted)
    return out.reset_index(drop=True)


def _archetypes(frame, down, togo, gain, yardline, play_type,
                success, explosive, converted) -> pd.Series:
    """Precedence exactly as documented in the module docstring."""
    sack = _num(frame, "sack").fillna(0) == 1
    interception = _num(frame, "interception").fillna(0) == 1
    fumble_lost = _num(frame, "fumble_lost").fillna(0) == 1
    rush = play_type == "run"

    label = pd.Series("EARLY_DOWN_MODEST", index=frame.index, dtype=object)
    # Assigned in REVERSE precedence so that earlier rules overwrite later
    # ones -- the top of the documented list ends up winning.
    label[down.notna() & (down <= 2) & (gain <= 0)] = "EARLY_DOWN_STUFF"
    label[down.notna() & (down <= 2) & success] = "EARLY_DOWN_SUCCESS"
    label[down.notna() & (down <= 2) & explosive] = "EARLY_DOWN_EXPLOSIVE"
    label[down.notna() & (down >= 3) & ~converted] = "LATE_DOWN_FAILURE"
    label[down.notna() & (down >= 3) & converted] = "LATE_DOWN_CONVERSION"
    label[rush & yardline.notna() & (yardline <= GOAL_LINE_YARDLINE)] = "GOAL_LINE_PUNCH"
    label[sack] = "SACK"
    label[interception | fumble_lost] = "TURNOVER_PLAY"
    label[play_type == "no_play"] = "PENALTY"
    label[play_type.isin(("qb_kneel", "qb_spike"))] = "KNEEL_SPIKE"
    label[play_type.isin(SPECIAL_TEAMS_PLAYS)] = "SPECIAL_TEAMS"
    # A row with no down and no recognised play type is not a snap at all
    # (game start, end of quarter markers). Never call it an early-down play.
    label[down.isna() & ~play_type.isin(SPECIAL_TEAMS_PLAYS + ("qb_kneel", "qb_spike", "no_play"))] = "NON_PLAY"
    return label


def _distance_bucket(togo: pd.Series, down: pd.Series) -> pd.Series:
    """Distance to go, bucketed -- but ONLY where a line to gain exists.

    A kickoff carries `ydstogo = 0`, which a naive cut labels "short". There is
    no such thing as a short-distance kickoff; the play has no line to gain at
    all. Gating on `down` is what separates "0 yards to go" from "the concept
    does not apply here".
    """
    live = togo.notna() & down.notna()
    bucket = pd.Series("unknown", index=togo.index, dtype=object)
    bucket[live & (togo >= LONG_DISTANCE)] = "long"
    bucket[live & (togo < LONG_DISTANCE)] = "medium"
    bucket[live & (togo <= SHORT_DISTANCE)] = "short"
    return bucket
