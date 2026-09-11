"""Label every play with an archetype -- the TRANSITION layer.

A drive archetype is a terminal state: how the possession ended. A play
archetype is a transition: what this snap did to the down-distance-field-
position state. Both are needed for the state machine to be a state machine.

=============================================================================
THE TAXONOMY -- 14 archetypes, mutually exclusive and exhaustive
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
  TWO_POINT              A two-point conversion attempt. It has no down, so
                         v3 swept all 130 of them (4.1 a team-season) into
                         NON_PLAY -- "not a snap at all" -- when they are
                         run/pass plays with participation on every one and
                         128 of 130 inside the 3. That is the GOAL_LINE_PUNCH
                         error repeated: a real football event with nowhere
                         to live because the rule was written against a
                         different case. Ranked above the down labels because
                         it cannot reach them; there is no down.
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

  formation                      SHOTGUN / UNDER CENTER / PISTOL -- the three
                                 values this release actually ships. An
                                 earlier docstring listed SINGLEBACK, EMPTY
                                 and I_FORM, which belong to a different
                                 charting source and are not here. Not
                                 redundant with `shotgun`: PISTOL is coded
                                 shotgun=1 but runs 26.2% pass on 1st-and-10
                                 against SHOTGUN's 65.7%.
  personnel_grouping             coach notation -- "11", "12", "21"
  defenders_in_box               count
  pass_rushers                   count; blitz is >= 5, heavy >= 6
  blitz, heavy_blitz             nullable booleans, NULL where unobserved --
                                 and unobserved is 39% of snaps, because the
                                 source writes 0 rushers rather than a null on
                                 a non-dropback. See nfl_participation.
  pressure                       the quarterback was pressured. The largest
                                 outcome split in the data. Dropbacks only; a
                                 handoff has no quarterback to pressure.
  n_ol, n_wr                     line and receiver counts, because two-digit
                                 personnel notation cannot express a six-OL
                                 jumbo look and silently calls it 11 or 12.
  coverage_type, man_zone        shell and man/zone, ~48% and ~61% populated,
                                 NULL where unknown rather than guessed
  epa, wp                        nflverse's own, never recomputed here
  goal_line                      snap from inside the opponent's GOAL_LINE
                                 yardline, run or pass. Replaces the old
                                 rush-only GOAL_LINE_PUNCH label. Set to the
                                 2, after moving 5 -> 3 -> 2 across three
                                 measurements. TD rate is the only criterion
                                 that actually separates: .549 / .460 /
                                 .322 / .342 / .314 from the 1 out to the 5 --
                                 one break, between the 2 and the 3, with the
                                 3, 4 and 5 flat together. The "personnel
                                 cliff" cited for the 3 does not exist; heavy
                                 personnel declines smoothly (.75 / .63 / .54
                                 / .48 / .48), which is a gradient and
                                 adjudicates nothing. 19.1 snaps a
                                 team-season, above the usable floor.
  penalty_type / penalty_team    what the flag was and who it was on.
                                 PENALTY carried 81 plays a team-season with
                                 no attributes at all.
  converted                      reached the line to gain on a late down.
                                 THE numerator for a conversion rate --
                                 `outcome` cannot be, because CONVERSION and
                                 EXPLOSIVE compete there and one has to lose.
  scramble                       a called pass the quarterback ran. nflverse
                                 types it "run"; no coach does. 35.9 a
                                 team-season at +0.480 EPA against a designed
                                 run's -0.048, so a rushing rate that counts
                                 them is wrong one way.
  two_point_result               success / failure on a TWO_POINT snap. The
                                 label shipped result-blind -- the same
                                 opacity `st_outcome` was built to fix,
                                 reproduced in the commit that added it.
  outcome                        CONVERSION / EXPLOSIVE / SUCCESS / MODEST /
                                 FAILURE, computed for EVERY scrimmage snap
                                 with a down, whatever else happened on it.
                                 Its own axis, because the archetype label is
                                 outranked on late downs by SACK, TURNOVER,
                                 PENALTY, KNEEL and SPIKE -- all of which are
                                 third-down ATTEMPTS -- so a third-down rate
                                 read off the label alone was high by 4.6
                                 points, one-directionally, in both seasons
                                 checked. Denominators come from the
                                 single-valued axes; numerators may come from
                                 here.
  passer / rusher / receiver     who handled the ball. Full attribution --
                                 every tackler, rusher and defensive back --
                                 is in `nfl_play_participants`, long form,
                                 because a play has one passer and can have
                                 six tacklers.
  qb_hit                         the quarterback was hit. NOT zero-sum, which
                                 is why it is here and not left to a GROUP BY:
                                 a hit without a sack means the defence won
                                 the rep AND the offence survived the play,
                                 and both are true. 55 a team-season.
  injury_on_play                 somebody was hurt on this snap. Harms one
                                 side without being the other's gain, so it
                                 cannot be recovered by flipping any
                                 aggregation. 30 a team-season.
  penalty_side                   offense / defense, read from `penalty_team`
                                 rather than assumed.
  st_outcome                     kick outcome -- made/missed/blocked,
                                 touchback/fair_catch/downed/returned. Its own
                                 axis, NULL on scrimmage snaps. SPECIAL_TEAMS
                                 is 232.5 snaps a team-season and carried no
                                 result at all: a 22-yard field goal and a
                                 blocked 58-yarder were the same row.
  kick_distance, return_yards    magnitude for the above.
  season, week, season_type      which games these plays are. Without
                                 `season`, concatenated years are
                                 indistinguishable except by parsing game_id.
  defteam, home_team,            who it was against and where.
  posteam_type, div_game
  score_differential             up 21 and down 21 are different games; `wp`
                                 mixes score with time and cannot be unpicked.
  game_seconds_remaining,        real clock. `clock` is the string "2:31",
  half_seconds_remaining         which does not sort across quarters, so
                                 two-minute offence (128 snaps a team-season)
                                 could not be identified at all.
  roof, surface, temp, wind      conditions.
  spread_line, total_line        the market's pre-game view, carried as
                                 context only -- nothing here claims it is
                                 beatable, and no label is derived from it.
  qb_dropback                    the REAL pass/run split. `play_type == run`
                                 counts a scramble as a run and a sack as
                                 neither: 38 scrambles and 40 sacks a
                                 team-season, so a pass rate off `play_type`
                                 is wrong.
  first_down                     moved the chains. Invisible before: on early
                                 downs, 1st-and-10 for 12 and 1st-and-10 for 5
                                 were both EARLY_DOWN_SUCCESS.
  tackled_for_loss               EARLY_DOWN_FAILURE lumps negative plays with
                                 zero-gain incompletions, and the EPA gap
                                 between those two is larger than the gap
                                 between FAILURE and MODEST.
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

=============================================================================
THE ENFORCEMENT RULE -- read before adding anything to this module
=============================================================================
This taxonomy has now produced the same bug four times, in four places, and
the shape is always identical: a column claims to cover a population, a
precedence order quietly removes part of it, and a rate computed off that
column is wrong in the SAME DIRECTION every time. It was GOAL_LINE_PUNCH
(rush-only, so goal-line pass snaps had nowhere to live), then the late-down
labels (outranked by SACK/TURNOVER/PENALTY, all of which are third-down
ATTEMPTS, +4.59pp), then `outcome` itself (CONVERSION overwriting EXPLOSIVE,
-27% of explosive plays), then three-and-out at drive level (-4.24pp).

The rule, stated once so the fifth version of this bug has to be deliberate:

  DENOMINATORS come only from SINGLE-VALUED fields -- down, play_type,
  outcome, the archetype label. One row, one value, no overlap.

  NUMERATORS come only from FLAGS -- converted, explosive, success,
  first_down, had_sack, penalty_first_down, tackled_for_loss, scramble,
  goal_line, turnover_type. Each is independent. A play may set any number
  of them, and none can be taken by a precedence order.

Any new fact that can CO-OCCUR with an existing label is a flag. If it needs
a precedence order to coexist, that is the proof it is a flag, not a label.

SUCCESS is the conventional 40/60/100 percent-of-distance rule, not something
invented here. It is stated as a constant so a later screen cannot move it
quietly.
"""
from __future__ import annotations

import pandas as pd

VERSION = "nfl-play-archetype-v8"

EXPLOSIVE_PLAY_YARDS = 20
SHORT_DISTANCE = 3
LONG_DISTANCE = 7
GOAL_LINE_YARDLINE = 2
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
        # CONTEXT. Every one of these is in the source and none of them
        # survived into v4, so the frame could not answer who the opponent
        # was, which week it was, whether the team was home, or whether it was
        # up 21 or down 21. `wp` mixes score and time and cannot be unpicked
        # into either. Worst of the set is `season`: the loader guards
        # carefully against labelling one season's drives with another
        # season's plays, and then emitted a frame in which six concatenated
        # seasons are indistinguishable except by parsing `game_id`.
        "season": _num(frame, "season").astype("Int64"),
        "week": _num(frame, "week").astype("Int64"),
        "season_type": frame.get("season_type"),
        "defteam": frame.get("defteam"),
        "home_team": frame.get("home_team"),
        "posteam_type": frame.get("posteam_type"),
        "div_game": _num(frame, "div_game").fillna(0).astype(bool),
        "score_differential": _num(frame, "score_differential").astype("Int64"),
        # `clock` ships as the string "2:31", which does not even sort across
        # quarters, so two-minute offence -- 128 snaps a team-season -- could
        # not be identified at all.
        "game_seconds_remaining": _num(frame, "game_seconds_remaining").astype("Int64"),
        "half_seconds_remaining": _num(frame, "half_seconds_remaining").astype("Int64"),
        "roof": frame.get("roof"),
        "surface": frame.get("surface"),
        "temp": _num(frame, "temp"),
        "wind": _num(frame, "wind"),
        "spread_line": _num(frame, "spread_line"),
        "total_line": _num(frame, "total_line"),
        # `play_type == "run"` counts a scramble as a run and a sack as
        # neither, so any pass rate derived from it is wrong by roughly 38
        # scrambles and 40 sacks a team-season. Dropback is the real split.
        "qb_dropback": _num(frame, "qb_dropback").fillna(0).astype(bool),
        # Moving the chains was invisible on early downs: 1st-and-10 for 12
        # and 1st-and-10 for 5 are both EARLY_DOWN_SUCCESS.
        "first_down": _num(frame, "first_down").fillna(0).astype(bool),
        "tackled_for_loss": _num(frame, "tackled_for_loss").fillna(0).astype(bool),
        # The line to gain was reached on a late down. A FLAG, not a reading of
        # `outcome` -- see _outcome's note on why a single-valued column cannot
        # serve as the numerator for two overlapping facts.
        "converted": (down.notna() & (down >= 3) & converted
                      & ~play_type.isin(SPECIAL_TEAMS_PLAYS + ("qb_kneel", "qb_spike", "no_play"))),
        # nflverse types a scramble as play_type == "run". No coach has ever
        # called one a run: it is a called pass where protection or coverage
        # failed, the line was pass-setting and the receivers were running
        # routes. It is a dropback outcome, the way a sack is -- which this
        # taxonomy already gets right for sacks. 35.9 a team-season, 7.7% of
        # rows typed "run", and they carry +0.480 EPA against a designed run's
        # -0.048, so a rushing rate that includes them is wrong one way.
        # Recoverable since v5 as (qb_dropback & play_type == "run"), which
        # matches this column exactly; named here so nobody has to know that.
        "scramble": _num(frame, "qb_scramble").fillna(0).astype(bool),
        "two_point_result": frame.get("two_point_conv_result"),
        # THE BALL-HANDLERS. Full attribution -- including every defender --
        # lives in nfl_play_participants, one row per player per role, because
        # a play has one passer and can have six tacklers. These three are
        # carried here as well because they are 1:1 with the snap and are the
        # join key most analysis reaches for first.
        "passer": frame.get("passer_player_name"),
        "rusher": frame.get("rusher_player_name"),
        "receiver": frame.get("receiver_player_name"),
        # NOT ZERO-SUM. Everything else in this frame is: a conversion for the
        # offence IS a conversion allowed by the defence, so a defensive rate
        # is a GROUP BY on `defteam` away and needs no mirrored label. These
        # three are different -- no amount of flipping the aggregation
        # produces them.
        #
        # `qb_hit` without a sack is the clearest case: the defence won the
        # rep and the offence survived the play, and BOTH are true. 1,769 a
        # season, 55 a team-season. Today a pass rush that beats the line all
        # afternoon without finishing is indistinguishable from one that is
        # not there.
        "qb_hit": _num(frame, "qb_hit").fillna(0).astype(bool),
        "injury_on_play": frame.get("desc", pd.Series("", index=frame.index))
                          .fillna("").str.contains("was injured during the play"),
        # Which unit was flagged. `penalty_team` is stated in the source, so
        # it is read rather than inferred -- both units commit fouls.
        "penalty_side": _penalty_side(frame),
        # SPECIAL_TEAMS is the second-largest label in the taxonomy -- 232.5
        # snaps a team-season -- and was completely opaque: `play_type`
        # recovered punt/FG/kickoff/XP, but the RESULT of any of them was
        # nowhere. A 22-yard field goal and a blocked 58-yarder were the same
        # row. NULL on every scrimmage snap, because a kick outcome is not a
        # fact about a handoff.
        "st_outcome": _st_outcome(frame, play_type),
        "kick_distance": _num(frame, "kick_distance"),
        "return_yards": _num(frame, "return_yards").where(play_type.isin(SPECIAL_TEAMS_PLAYS)),
    })
    out["play_archetype"] = _archetypes(frame, down, togo, gain, yardline, play_type,
                                        success, explosive, converted)
    out["outcome"] = _outcome(frame, down, play_type, gain, success, explosive, converted)
    for source, name in (
        ("offense_formation", "formation"), ("personnel_grouping", "personnel_grouping"),
        ("defenders_in_box", "defenders_in_box"), ("pass_rushers", "pass_rushers"),
        ("blitz", "blitz"), ("heavy_blitz", "heavy_blitz"), ("pressure", "pressure"),
        ("n_ol", "n_ol"), ("n_wr", "n_wr"),
        ("defense_coverage_type", "coverage_type"), ("defense_man_zone_type", "man_zone"),
    ):
        if source in frame:
            out[name] = frame[source].values
    return out.reset_index(drop=True)


def _outcome(frame, down, play_type, gain, success, explosive, converted) -> pd.Series:
    """What the snap DID, computed for every scrimmage snap with a down --
    independently of whatever else happened on it.

    THIS FIXES A ONE-DIRECTIONAL BIAS, not a cosmetic gap. `play_archetype`
    is outranked on late downs by SACK, TURNOVER_PLAY, PENALTY, KNEEL and
    SPIKE, and every one of those is a third-down ATTEMPT. They leave the
    conversion count alone and walk off with a piece of the denominator, so a
    third-down rate read off the label was high EVERY time:

                      taxonomy   nflverse truth   bias
        3rd down 2025   .4411        .3952       +4.6 pp
        4th down 2025   .6083        .5497       +5.9 pp
        3rd down 2024   .4446        .3979       +4.7 pp

    The arithmetic is exact: on 4th down LATE_DOWN_CONVERSION matches
    nflverse's own `fourth_down_converted` to the play, while the failure side
    is missing precisely the sacks, turnovers and kneels the precedence order
    took. 153 late-down snaps a team-season sat outside the two labels that
    claim to cover late downs.

    This is the `had_sack` lesson finished rather than half-applied. That flag
    recovered the SACK and left the DOWN still swallowed; snap type and
    outcome are different facts and were sharing one column, so an event won
    and the outcome vanished. They are now separate axes.

    WHAT THIS COLUMN IS NOT. It is SINGLE-VALUED and therefore ORDERED, and an
    ordering among facts that are not mutually exclusive costs something: a
    third-down gain of 25 is both explosive and a conversion, and this column
    can only say one. CONVERSION wins, so 544 explosive plays -- 27% of them,
    17 a team-season -- do not appear as EXPLOSIVE here. An earlier version of
    this docstring said `outcome` was computed "whatever else happened on it"
    and told readers to take numerators from it. That was false for EXPLOSIVE,
    and it was the same precedence mistake this axis exists to undo, rebuilt
    one level down.

    So: the DENOMINATOR comes from the single-valued axes (down, play_type)
    and the NUMERATOR comes from the FLAGS -- `converted`, `explosive`,
    `success`, `first_down`, `had_sack`, `tackled_for_loss` -- each of which
    is independent and none of which can be stolen by a precedence order. A
    sack still lands in the third-down denominator automatically, because it
    still has a down. `outcome` is a readable one-word summary of a snap and
    a legitimate GROUPING key; it is not a numerator.

    Residual against nflverse's own converted/failed denominator is +0.51pp on
    third down and +0.24pp on fourth, and it is fully explained rather than
    slop: it is 61 third-down kneels and 9 spikes, which nflverse counts as
    third-down failures and this axis excludes. Zero unexplained plays on
    fourth down. Excluding a kneel from an attempt rate is the right side of
    that disagreement.
    """
    # `no_play` is excluded and that exclusion is load-bearing: a flag wiped
    # the snap out, so the down did not resolve and there was no attempt.
    # Including them overshot the correction to -12.8 pp on fourth down, in
    # the opposite direction to the bug being fixed -- which is why this is
    # graded against nflverse's own converted/failed denominator rather than
    # against "looks better than before".
    scrimmage = down.notna() & ~play_type.isin(
        SPECIAL_TEAMS_PLAYS + ("qb_kneel", "qb_spike", "no_play")
    )
    out = pd.Series(None, index=frame.index, dtype=object)
    out[scrimmage] = "MODEST"
    out[scrimmage & (gain <= 0)] = "FAILURE"
    out[scrimmage & success] = "SUCCESS"
    out[scrimmage & explosive] = "EXPLOSIVE"
    # On a late down the line to gain is the only question, so CONVERSION
    # outranks the gradations above it.
    out[scrimmage & (down >= 3) & converted] = "CONVERSION"
    out[scrimmage & (down >= 3) & ~converted] = "FAILURE"
    return out


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
    # Documented above PENALTY/TURNOVER_PLAY/SACK and implemented below all
    # three until v6, which had no effect in 2025 or 2024 -- no two-point try
    # was flagged, picked or sacked in either season -- but was a latent
    # disagreement between the docstring and the code.
    label[_num(frame, "two_point_attempt").fillna(0) == 1] = "TWO_POINT"
    label[play_type == "qb_spike"] = "SPIKE"
    label[play_type == "qb_kneel"] = "KNEEL"
    label[play_type.isin(SPECIAL_TEAMS_PLAYS)] = "SPECIAL_TEAMS"
    # A row with no down and no recognised play type is not a snap at all
    # (game start, end of quarter markers). Never call it an early-down play.
    two_point = _num(frame, "two_point_attempt").fillna(0) == 1
    label[down.isna() & ~two_point
          & ~play_type.isin(SPECIAL_TEAMS_PLAYS + ("qb_kneel", "qb_spike", "no_play"))] = "NON_PLAY"
    return label


def _penalty_side(frame: pd.DataFrame) -> pd.Series:
    """Which unit was flagged -- offence or defence. NULL when no flag."""
    team = frame.get("penalty_team")
    if team is None:
        return pd.Series(None, index=frame.index, dtype=object)
    out = pd.Series(None, index=frame.index, dtype=object)
    flagged = team.notna()
    out[flagged] = "defense"
    out[flagged & team.eq(frame.get("posteam"))] = "offense"
    return out


def _st_outcome(frame: pd.DataFrame, play_type: pd.Series) -> pd.Series:
    """Outcome of a kick, as its own axis. NULL on every scrimmage snap.

    Deliberately one value per kick and mutually exclusive, because a kick
    has exactly one fate. The source's punt columns are not exclusive --
    `punt_inside_twenty` co-occurs with downed, fair-catch and out-of-bounds,
    since it describes WHERE the ball stopped rather than HOW -- so it is
    carried as its own flag rather than folded in here, which would be a
    precedence order discarding one of two true facts.

    Six extra points in 2025 come back NULL, and that is right: each was
    wiped by a penalty and replayed, so nflverse nulls the result. A kick
    that did not count has no outcome.
    """
    out = pd.Series(None, index=frame.index, dtype=object)
    fg = frame.get("field_goal_result")
    xp = frame.get("extra_point_result")
    if fg is not None:
        out[play_type == "field_goal"] = fg[play_type == "field_goal"]
    if xp is not None:
        out[play_type == "extra_point"] = xp[play_type == "extra_point"]

    punt = play_type == "punt"
    ret = _num(frame, "return_yards").fillna(0)
    out[punt] = "returned"
    for flag, label in (("punt_fair_catch", "fair_catch"), ("punt_out_of_bounds", "out_of_bounds"),
                        ("punt_downed", "downed"), ("touchback", "touchback"),
                        ("punt_blocked", "blocked")):
        out[punt & (_num(frame, flag).fillna(0) == 1)] = label
    out[punt & (ret == 0) & out.isin(["returned"])] = "no_return"

    kick = play_type == "kickoff"
    out[kick] = "returned"
    out[kick & (_num(frame, "touchback").fillna(0) == 1)] = "touchback"
    out[kick & (_num(frame, "kickoff_out_of_bounds").fillna(0) == 1)] = "out_of_bounds"
    out[kick & (_num(frame, "kickoff_fair_catch").fillna(0) == 1)] = "fair_catch"
    return out


def _turnover_type(frame: pd.DataFrame) -> pd.Series:
    """Mechanism of a lost possession, as a modifier rather than a label.

    SCRIMMAGE SNAPS ONLY. nflverse records a punt returner's muff against the
    PUNTING team's row -- the team that gained the ball -- so 31 plays carried
    a turnover naming the wrong side entirely. This column is rendered on the
    web page, so the inversion was visible.
    """
    scrim = frame.get("play_type", pd.Series("", index=frame.index)).astype(str).isin(
        ("run", "pass", "no_play"))
    interception = (_num(frame, "interception").fillna(0) == 1) & scrim
    fumble = (_num(frame, "fumble_lost").fillna(0) == 1) & scrim
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
