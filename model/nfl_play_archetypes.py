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
  KNEEL                  Victory formation. Burns clock while ahead.
  SPIKE                  Clock stop. Preserves time while trying to score.
                         Split from a single KNEEL_SPIKE label because they are
                         opposite game states and nflverse distinguishes them
                         at source (2025: 433 kneels, 79 spikes), so collapsing
                         them discarded free information.
  PENALTY                A flag wiped the play out (`no_play`). The down did
                         not resolve, so no outcome label can apply.
  TURNOVER_PLAY          Interception or lost fumble. Ranked above SACK and
                         above the down labels because losing the ball is the
                         transition; the down it happened on is a modifier.
  SACK                   Kept separate from a stuffed run: it is a negative
                         play AND a quarterback/protection event, and the two
                         have different persistence.

  LATE_DOWN_CONVERSION   3rd or 4th down, line to gain reached.
  LATE_DOWN_FAILURE      3rd or 4th down, short of it.

  EARLY_DOWN_EXPLOSIVE   1st or 2nd down, gain >= 20.
  EARLY_DOWN_SUCCESS     1st or 2nd down, meets the standard success
                         threshold (40% of the distance on 1st, 60% on 2nd).
  EARLY_DOWN_FAILURE       1st or 2nd down, gain <= 0.
  EARLY_DOWN_MODEST      1st or 2nd down, positive but not successful. The
                         residual bucket, and named so it reads as one.

GOAL_LINE_PUNCH WAS REMOVED IN v3, and removing it made the taxonomy both
smaller and more truthful. It labelled a goal-line RUSH only, so 24.5
goal-line runs a team-season got a terminal label while 20.9 goal-line PASS
snaps scattered across seven labels that never mention the goal line. A
goal-line conversion rate could not be computed from the taxonomy at all,
because 46% of the denominator was not in it. Goal line is a field-position
fact about any snap, so it is now the `goal_line` modifier -- the same
terminal-state-versus-modifier rule the rest of the file follows, applied to
a label written before that rule existed.

=============================================================================
MODIFIERS -- carried alongside, never folded into the archetype
=============================================================================
  down, ydstogo, yardline_100    raw state, so any cut can be recovered
  distance_bucket                short <=3 / medium 4-6 / long >=7
  success                        the standard down-weighted success criterion
  explosive                      gain >= 20
  shotgun, no_huddle             available in nflverse, carried untouched

  PARTICIPATION MODIFIERS (v3) -- from nflverse's separate participation
  release, joined on (game_id, play_id). Present on 99.8% of scrimmage snaps
  and on none of the kickoffs, punts, kicks or kneels, which is structural
  rather than missing. They record what down-and-distance cannot: holding
  1st-and-10 fixed and letting personnel move, pass rate spans 55.6% in 11
  personnel to 26.2% in 22, and the box answers 6.05 to 6.89. That is the
  pre-snap conversation, and the taxonomy previously recorded neither half.

  formation                      SHOTGUN / SINGLEBACK / EMPTY / I_FORM / ...
  personnel_grouping             coach notation -- "11", "12", "21"
  defenders_in_box               count
  pass_rushers                   count; blitz is >= 5, heavy >= 6
  blitz, heavy_blitz             nullable booleans, NULL where unobserved
  pressure                       the quarterback was pressured. The largest
                                 outcome split in the data.
  coverage_type, man_zone        shell and man/zone, ~49% populated, NULL
                                 where unknown rather than guessed
  epa, wp                        nflverse's own, never recomputed here
  goal_line                      snap from inside the opponent's GOAL_LINE
                                 yardline, run or pass. Replaces the old
                                 rush-only GOAL_LINE_PUNCH label.
  penalty_type / penalty_team    what the flag was and who it was on.
                                 PENALTY carried 81 plays a team-season with
                                 no attributes at all.
  penalty_first_down             the flag moved the chains. The old docstring
                                 claimed "the down did not resolve, so no
                                 outcome label can apply" -- false 22.8 times
                                 a team-season, on defensive penalties that
                                 produced a first down.
  turnover_type                  interception / fumble_lost / None. The
                                 mechanism, kept off the terminal label so a
                                 play that is somehow both (4 in 2025: a pick
                                 the returner fumbles back) needs no arbitrary
                                 tiebreak.
  had_sack                       TRUE on a sack, INCLUDING one that also lost
                                 the ball. TURNOVER_PLAY outranks SACK, so
                                 without this flag every strip-sack disappears
                                 from the sack count -- 75 of 1,287 sacks in
                                 2025, i.e. a taxonomy-derived sack rate was
                                 5.8% low. Overlapping causes belong in flags,
                                 never in a precedence order.

SUCCESS is the conventional 40/60/100 percent-of-distance rule, not something
invented here. It is stated as a constant so a later screen cannot move it
quietly.
"""
from __future__ import annotations

import pandas as pd

VERSION = "nfl-play-archetype-v3"

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


def label_plays(pbp: pd.DataFrame, participation: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per play, in game order, with archetype and modifiers.

    `participation` is optional so the labeller still runs without it; the
    participation columns are then absent rather than silently False.
    """
    if participation is not None:
        from model.nfl_participation import attach
        pbp = attach(pbp, participation)
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
        "goal_line": (yardline.notna() & (yardline <= GOAL_LINE_YARDLINE)),
        "penalty_type": frame.get("penalty_type"),
        "penalty_team": frame.get("penalty_team"),
        "penalty_first_down": (_num(frame, "first_down_penalty").fillna(0) == 1),
        "turnover_type": _turnover_type(frame),
        "had_sack": (_num(frame, "sack").fillna(0) == 1),
        "shotgun": _num(frame, "shotgun").fillna(0).astype(bool),
        "no_huddle": _num(frame, "no_huddle").fillna(0).astype(bool),
        "epa": _num(frame, "epa").round(3),
        "wp": _num(frame, "wp").round(4),
        "description": frame.get("desc"),
    })
    out["play_archetype"] = _archetypes(frame, down, togo, gain, yardline, play_type,
                                        success, explosive, converted)
    for source, name in (
        ("offense_formation", "formation"), ("personnel_grouping", "personnel_grouping"),
        ("defenders_in_box", "defenders_in_box"), ("pass_rushers", "pass_rushers"),
        ("blitz", "blitz"), ("pressure", "pressure"),
        ("defense_coverage_type", "coverage_type"), ("defense_man_zone_type", "man_zone"),
    ):
        if source in frame:
            out[name] = frame[source].values
    return out.reset_index(drop=True)


def _archetypes(frame, down, togo, gain, yardline, play_type,
                success, explosive, converted) -> pd.Series:
    """Precedence exactly as documented in the module docstring."""
    sack = _num(frame, "sack").fillna(0) == 1
    interception = _num(frame, "interception").fillna(0) == 1
    fumble_lost = _num(frame, "fumble_lost").fillna(0) == 1

    label = pd.Series("EARLY_DOWN_MODEST", index=frame.index, dtype=object)
    # Assigned in REVERSE precedence so that earlier rules overwrite later
    # ones -- the top of the documented list ends up winning.
    label[down.notna() & (down <= 2) & (gain <= 0)] = "EARLY_DOWN_FAILURE"
    label[down.notna() & (down <= 2) & success] = "EARLY_DOWN_SUCCESS"
    label[down.notna() & (down <= 2) & explosive] = "EARLY_DOWN_EXPLOSIVE"
    label[down.notna() & (down >= 3) & ~converted] = "LATE_DOWN_FAILURE"
    label[down.notna() & (down >= 3) & converted] = "LATE_DOWN_CONVERSION"
    label[sack] = "SACK"
    label[interception | fumble_lost] = "TURNOVER_PLAY"
    label[play_type == "no_play"] = "PENALTY"
    label[play_type == "qb_spike"] = "SPIKE"
    label[play_type == "qb_kneel"] = "KNEEL"
    label[play_type.isin(SPECIAL_TEAMS_PLAYS)] = "SPECIAL_TEAMS"
    # A row with no down and no recognised play type is not a snap at all
    # (game start, end of quarter markers). Never call it an early-down play.
    label[down.isna() & ~play_type.isin(SPECIAL_TEAMS_PLAYS + ("qb_kneel", "qb_spike", "no_play"))] = "NON_PLAY"
    return label


def _turnover_type(frame: pd.DataFrame) -> pd.Series:
    """Mechanism of a lost possession, as a modifier rather than a label."""
    interception = _num(frame, "interception").fillna(0) == 1
    fumble = _num(frame, "fumble_lost").fillna(0) == 1
    out = pd.Series(None, index=frame.index, dtype=object)
    out[fumble] = "fumble_lost"
    out[interception] = "interception"   # 4 plays a season carry both flags
    return out


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
