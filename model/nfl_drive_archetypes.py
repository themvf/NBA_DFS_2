"""Label every NFL drive with a terminal archetype plus trajectory modifiers.

This is the LABELLING layer only. It describes what happened on a drive; it
makes no prediction and carries no betting claim. The screen that asks whether
these labels survive the closing line is a separate, pre-registered step.

Source is the nflverse play-by-play release the V2 fantasy pipeline already
downloads (`ingest/ff_v2_historical_context.py`), not a PFR scrape.

=============================================================================
THE TAXONOMY -- 12 archetypes, mutually exclusive and exhaustive
=============================================================================
Every drive gets exactly one. Terminal state decides the family; trajectory
only breaks ties inside a family.

SCORED (offense put points on the board)
  METHODICAL_TD        TD with >=3 first downs, no single play carrying the
                       drive. A sustained, converted possession.
  EXPLOSIVE_TD         TD where one gain of 20+ yards was >=50% of the net
                       yards. One play did the work.
  SHORT_FIELD_TD       TD with <3 first downs and no explosive play -- the
                       drive was handed good position rather than earning it.
  RED_ZONE_SETTLE_FG   FG after reaching inside the 20. Got there, didn't
                       finish. The archetype red-zone offences are judged on.
  LONG_FG              FG without reaching the 20. A kick, not a settle.

FAILED, NO GIVEAWAY (possession changes, no points either way)
  THREE_AND_OUT        <=3 plays, 0 first downs, punt.
  STALLED              Punt after at least one first down, or more than 3
                       plays. Moved the ball, ran out of downs.
  MISSED_FG            Reached scoring range and missed. Distinct from a
                       stall: the drive earned a scoring chance.

GAVE IT AWAY (possession lost, sometimes with points against)
  TURNOVER_GIVEAWAY    Interception or lost fumble. Variance.
  TURNOVER_ON_DOWNS    Failed 4th-down attempt. A decision, not variance --
                       kept separate because it is coaching aggression, and
                       it correlates with game script rather than with talent.
  SCORE_AGAINST        The possession ended with the OPPONENT scoring -- a
                       pick-six, scoop-and-score, punt or kick return TD, or a
                       safety. Folding this into a turnover loses the fact that
                       the possession had negative scoring value. Named
                       SCORE_AGAINST rather than DEFENSIVE_SCORE because a
                       return touchdown is a special-teams score, and because
                       the name must describe it from the POSSESSING team's
                       point of view -- the row belongs to the team that lost
                       the ball, never the team that scored.

CLOCK
  CLOCK_EXPIRED        A real possession that ran out of half or game clock.
                       IS a team trait and counts in the denominator. Splitting
                       this out was not cosmetic: of 210 clock-ended drives in
                       2025 only 35 were one-play kneels, while 112 ran 4+
                       plays and 51.8% of those died inside the opponent's 40.
                       Discarding them threw away real two-minute possessions
                       in scoring range from every rate.

  KNEEL_DOWN           Victory formation or a spike-out. <=2 plays on a clock
                       ending. NOT a team trait -- excluded from denominators.
                       See rule 1.

=============================================================================
MODIFIERS -- carried alongside, never folded into the archetype
=============================================================================
  explosive_dependence  bool. One gain of 20+ yards was >=50% of net yards.
                        RISES WHEN DRIVES ARE SHORT, so it conflates a
                        big-play offence with one that cannot sustain. Read
                        it as "did one play carry this drive", nothing more.
  explosive_plays       int. Count of gains of 20+ yards.
  explosive_frequency   float. explosive_plays / snaps. The rate measure --
                        independent of drive length, and the one a
                        "this offence generates big plays" claim needs.
  reached_red_zone      bool.
  start_bucket          short_field / normal / long_field.
  end_bucket            red_zone / scoring_range / midfield / own_territory --
                        where the possession DIED. Added because the terminal
                        label alone is a genuine mixture: 53.7% of
                        TURNOVER_ON_DOWNS and 37.3% of TURNOVER_GIVEAWAY end
                        inside the opponent's 40, and those are worth several
                        expected points more than the same label at midfield.
                        Carried as a modifier rather than as new terminal
                        labels, per rule 2 -- `TURNOVER_GIVEAWAY x red_zone` is
                        conditionable without doubling the taxonomy.
  garbage_time          bool, from win probability at drive start.

  had_sack              a sack occurred on this drive
  had_penalty           a flag wiped out a play on this drive
  failed_short          the drive's last snap was 3rd/4th down with <= 2 to go
  turnover_type         interception / fumble_lost / None

                        These four are INDEPENDENT FLAGS, deliberately not a
                        single `stall_cause` field, and deliberately not new
                        terminal labels. Subdividing STALLED by cause was
                        considered and rejected on measurement: the causes
                        OVERLAP (100 of 791 stalled drives in 2025 had both a
                        sack and a penalty), one proposed cell is far too thin
                        to be a team trait (a short-yardage stop happens 1.3
                        times per team per SEASON), and the outcomes barely
                        separate (drive EPA -1.64 to -1.22 across all four).
                        A single-valued cause field would merely relocate the
                        arbitrary precedence instead of removing it, so a drive
                        is simply STALLED + had_sack + had_penalty at once, all
                        three true, none of them ranked.

=============================================================================
FOUR DEFINITIONAL RULES -- frozen so a later screen cannot quietly move them
=============================================================================
1. CLOCK-CENSORED DRIVES ARE NOT STALLS. A drive that dies on the half or game
   clock is its own terminal state. Folding it into "three and out" inflates
   the stall rate of whichever team happened to receive last, and receiving
   order is decided by a coin toss -- pure noise entering the feature.
2. TERMINAL STATE AND TRAJECTORY ARE SEPARATE. "Explosive" is a modifier, not
   a competing terminal label; a drive can be explosive and still punt.
   Collapsing them makes the mix impossible to condition on later, which is
   the whole point of the exercise.
3. FIELD POSITION IS PART OF THE LABEL, NOT A CONFOUND TO IGNORE. A drive
   starting on the opponent's 30 scores at a high rate regardless of the
   offence. `start_bucket` is carried so a rate can be conditioned on it
   rather than crediting the offence for the defence's and special teams'
   work.
4. GARBAGE TIME IS FLAGGED, NEVER SILENTLY DROPPED. The threshold is frozen
   below. Choosing it after seeing a result is the totals-mirage failure mode.
5. A DRIVE BELONGS TO A (TEAM, QUARTERBACK) PAIR, NOT A TEAM. A mid-game QB
   injury reassigns the rest of the game to a different player, and a team-level
   rate then describes a backup while claiming to describe the team. The 2026
   opener is the worked example: Sam Darnold was hurt on a sack at 12:27 of the
   first quarter and 9 of Seattle's 10 drives were Drew Lock's. Carrying that
   week's "Seattle" mix forward would feed the wrong quarterback into the next
   week's feature -- the same attribution error this project already recorded
   for `mlb_matchups.our_prob_home` and `mlb_bets.event_commence`.

   Note the asymmetry that makes this worse than a pregame injury: a pregame
   injury is IN the closing line, so the market prices it. A mid-game injury is
   in neither the line nor any pregame feature, so for a residual-against-the-
   close screen it is irreducible noise in the dependent variable. It does not
   bias the estimate; it destroys power, and it is not rare.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

VERSION = "nfl-drive-archetype-v5"

# --- frozen thresholds -------------------------------------------------------
EXPLOSIVE_PLAY_YARDS = 20      # a single scrimmage gain of at least this many
EXPLOSIVE_SHARE = 0.50         # ...carrying at least this share of net yards
METHODICAL_FIRST_DOWNS = 3     # first downs that mark a drive as sustained
THREE_AND_OUT_PLAYS = 3        # plays at or under this, with no first down
SHORT_FIELD_YARDLINE = 60      # start inside opponent's 60 (yardline_100 <= 60)
LONG_FIELD_YARDLINE = 85       # start behind own 15 (yardline_100 >= 85)
RED_ZONE_YARDLINE = 20         # end inside opponent's 20
SCORING_RANGE_YARDLINE = 40    # end inside opponent's 40 (rough FG range)
MIDFIELD_YARDLINE = 60         # end past own 40
KNEEL_MAX_PLAYS = 2            # plays at or under this on a clock ending
SHORT_YARDAGE = 2              # yards to go that count as short on a late down
GARBAGE_WP = 0.05              # win prob outside [wp, 1-wp] at drive start
MIN_QB_ATTEMPTS = 2            # attempts before a passer counts as a QB, not a
                               # gadget-play thrower. `passer_player_name`
                               # records ANYONE who throws, so a receiver on a
                               # trick play otherwise enters the QB roster: at
                               # >=1 attempt 21.5% of 2025 team-games look like
                               # they used two QBs, at >=2 12.1%, at >=5 6.8%.
                               # Set at 2 because a starter hurt early may throw
                               # very few passes -- Darnold threw 3 in the 2026
                               # opener before leaving -- so a high bar would
                               # discard exactly the case this field exists for.

SCRIMMAGE = ("run", "pass")

# nflverse `fixed_drive_result` vocabulary, verified against a full season
# (2025: Touchdown, Punt, Field goal, Turnover, Turnover on downs, End of half,
#  Missed field goal, Opp touchdown, Safety).
SCORED_TD = "Touchdown"
SCORED_FG = "Field goal"
CENSORED = ("End of half", "End of game")
AGAINST = ("Opp touchdown", "Safety")

PBP_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
    "play_by_play_{season}.parquet"
)


def load_pbp(season: int, cache: Path | None = None) -> pd.DataFrame:
    if cache and cache.exists():
        cached = pd.read_parquet(cache)
        # A cache file is for ONE season, but the path is a plain argument with
        # no season in it. Returning it for whichever season was asked would
        # label one season's games against another season's plays and report
        # the rest as "absent from the release" -- silently wrong, and it takes
        # a mismatched cache to notice. Check rather than trust.
        seasons = set(pd.to_numeric(cached.get("season"), errors="coerce").dropna().astype(int))
        if seasons and seasons != {season}:
            raise SystemExit(
                f"--cache holds season(s) {sorted(seasons)} but season {season} was "
                f"requested; refusing to label one season against another's plays"
            )
        return cached
    frame = pd.read_parquet(PBP_URL.format(season=season))
    if cache:
        frame.to_parquet(cache)
    return frame


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _first(frame: pd.DataFrame, column: str, default=None):
    if column not in frame or frame[column].isna().all():
        return default
    return frame[column].dropna().iloc[0]


def label_drives(pbp: pd.DataFrame) -> pd.DataFrame:
    """One row per offensive drive, with archetype and modifiers."""
    drive_col = "fixed_drive" if "fixed_drive" in pbp else "drive"
    frame = pbp[pbp["posteam"].notna()].dropna(subset=[drive_col]).copy()

    quarterbacks = _quarterbacks(frame)
    starters = _starters(frame, quarterbacks)

    rows: list[dict] = []
    for (game_id, team, drive_no), group in frame.groupby(
        ["game_id", "posteam", drive_col], sort=True
    ):
        ordered = group.sort_values("play_id")
        # Field position comes from SNAPS only. nflverse groups the kickoff into
        # the receiving team's drive, and a kickoff row's `yardline_100` is the
        # kicking spot -- reading it as the drive start puts every post-kickoff
        # drive on a fake short field with negative net yards.
        snaps = ordered[_num(ordered, "down").notna()]
        scrimmage = snaps[snaps["play_type"].isin(SCRIMMAGE)]

        snap_yl = _num(snaps, "yardline_100").dropna()
        if snap_yl.empty:
            start_yl = end_yl = float("nan")
        else:
            start_yl = float(snap_yl.iloc[0])
            # The last snap's spot is where it STARTED; add its gain to get the
            # drive's true end, or a scoring play reads as zero yards gained.
            last_gain = _num(snaps.loc[[snap_yl.index[-1]]], "yards_gained").fillna(0.0).iloc[0]
            end_yl = float(snap_yl.iloc[-1]) - float(last_gain)

        if scrimmage.empty and snaps.empty:
            # Not a drive. `posteam` flips mid-drive on a return touchdown's
            # conversion attempt and on some kickoff rows, so grouping by
            # (game, posteam, drive) invents a 0-snap possession for the team
            # that did NOT have the ball. Across 2025 that is 311 rows, 68 of
            # them labelled SCORE_AGAINST -- i.e. crediting the team that
            # SCORED with a points-against archetype, the exact inversion of
            # what the label means.
            continue

        qb = _drive_qb(ordered, quarterbacks.get(team, frozenset()))

        gains = _num(scrimmage, "yards_gained").dropna()
        max_gain = float(gains.max()) if not gains.empty else 0.0
        explosive_plays = int((gains >= EXPLOSIVE_PLAY_YARDS).sum())
        net_yards = (
            float(start_yl - end_yl) if pd.notna(start_yl) and pd.notna(end_yl) else 0.0
        )

        result = str(_first(ordered, "fixed_drive_result", "Unknown"))
        plays = _first(ordered, "drive_play_count")
        plays = int(plays) if plays is not None else int(len(scrimmage))
        first_downs = int(_first(ordered, "drive_first_downs", 0) or 0)
        red_zone = bool(_first(ordered, "drive_inside20", 0) or 0)
        sacked = bool((_num(ordered, "sack").fillna(0) == 1).any())
        penalised = bool((ordered.get("play_type", pd.Series(dtype=object)) == "no_play").any())
        last_snap = snaps.iloc[-1] if not snaps.empty else None
        failed_short = bool(
            last_snap is not None
            and pd.notna(last_snap.get("down"))
            and float(last_snap["down"]) >= 3
            and pd.notna(last_snap.get("ydstogo"))
            and float(last_snap["ydstogo"]) <= SHORT_YARDAGE
        )
        interception = bool((_num(ordered, "interception").fillna(0) == 1).any())
        fumble = bool((_num(ordered, "fumble_lost").fillna(0) == 1).any())

        types = ordered.get("play_type", pd.Series(dtype=object)).astype(str)
        knelt = bool(types.isin(("qb_kneel", "qb_spike")).any())
        mechanism = _score_against_mechanism(ordered, result)

        wp = _first(ordered, "wp")
        wp = float(wp) if wp is not None else float("nan")

        dependence = (
            explosive_plays > 0
            and net_yards > 0
            and (max_gain / net_yards) >= EXPLOSIVE_SHARE
        )
        n_snaps = int(len(scrimmage))

        rows.append({
            "game_id": game_id,
            "team": team,
            # CONTEXT, carried for the same reason as at play level: the frame
            # could not say which season or week it was, who the opponent was,
            # whether the team was home, or what the score was. `wp_at_start`
            # is not a substitute -- it mixes score with time remaining.
            "season": _first(ordered, "season"),
            "week": _first(ordered, "week"),
            "season_type": _first(ordered, "season_type"),
            "defteam": _first(ordered, "defteam"),
            "posteam_type": _first(ordered, "posteam_type"),
            "score_differential_start": _first(ordered, "score_differential"),
            "game_seconds_remaining_start": _first(ordered, "game_seconds_remaining"),
            "roof": _first(ordered, "roof"),
            "qb": qb,
            "drive": int(drive_no),
            "quarter": int(_first(ordered, "qtr", 0) or 0),
            "start_yardline_100": start_yl,
            "start_bucket": _start_bucket(start_yl),
            "end_yardline_100": end_yl,
            "end_bucket": _end_bucket(end_yl),
            "start_transition": _first(ordered, "drive_start_transition"),
            "plays": plays,
            "snaps": n_snaps,
            "net_yards": net_yards,
            "first_downs": first_downs,
            "max_play": max_gain,
            "explosive_plays": explosive_plays,
            "explosive_dependence": dependence,
            "explosive_frequency": round(explosive_plays / n_snaps, 3) if n_snaps else 0.0,
            "reached_red_zone": red_zone,
            "result": result,
            "epa": round(float(_num(ordered, "epa").sum()), 2),
            "wp_at_start": wp,
            "garbage_time": bool(pd.notna(wp) and (wp < GARBAGE_WP or wp > 1 - GARBAGE_WP)),
            "had_sack": sacked,
            "had_penalty": penalised,
            "failed_short": failed_short,
            "turnover_type": "interception" if interception else ("fumble_lost" if fumble else None),
            "archetype": _archetype(result, plays, first_downs, red_zone, dependence,
                                    kneeled=knelt),
            "score_against_mechanism": mechanism,
        })

    out = pd.DataFrame(rows).sort_values(["game_id", "team", "drive"])
    # A drive with no pass and no QB carry (a one-play goal-line rush) has no
    # identified quarterback. Carry the team's most recent known QB forward and
    # FLAG it, rather than leaving a null that splits the team's summary row.
    out["qb_inferred"] = out["qb"].isna()
    out["qb"] = out.groupby(["game_id", "team"])["qb"].ffill().bfill()
    # AFTER the fill, never before. Computing this per drive inside the loop
    # stamped a carry-forward drive False, and `summarise` aggregates with
    # .all(), so one null drive flipped an entire team's starter flag.
    out["qb_is_starter"] = [
        qb is not None and qb == starters.get((game, team))
        for game, team, qb in zip(out["game_id"], out["team"], out["qb"])
    ]
    return out.sort_values(["game_id", "drive"]).reset_index(drop=True)


def _archetype(
    result: str, plays: int, first_downs: int, red_zone: bool, dependence: bool,
    kneeled: bool = True,
) -> str:
    """Terminal state first. Trajectory only breaks ties within a terminal state."""
    if result in CENSORED:
        # A kneel and a two-minute drive that died on the opponent's 25 are
        # both "the clock ended it" and are not remotely the same event.
        #
        # `plays <= 2` alone was the wrong test, because it asks how SHORT the
        # possession was rather than whether anybody knelt. 62 of 230
        # KNEEL_DOWN drives contained no kneel and no spike -- mean +5.9 net
        # yards, four of them dying inside the opponent's 40 -- and
        # `summarise` drops KNEEL_DOWN from every denominator, so those were
        # real possessions excluded from every rate. The play labeller already
        # identifies KNEEL and SPIKE; gate on their presence.
        if plays <= KNEEL_MAX_PLAYS and kneeled:
            return "KNEEL_DOWN"
        return "CLOCK_EXPIRED"
    if result in AGAINST:
        return "SCORE_AGAINST"                  # points AGAINST -- not a plain turnover
    if result == SCORED_TD:
        if dependence:
            return "EXPLOSIVE_TD"
        return "METHODICAL_TD" if first_downs >= METHODICAL_FIRST_DOWNS else "SHORT_FIELD_TD"
    if result == SCORED_FG:
        return "RED_ZONE_SETTLE_FG" if red_zone else "LONG_FG"
    if result == "Missed field goal":
        return "MISSED_FG"
    if result == "Turnover on downs":
        return "TURNOVER_ON_DOWNS"
    if result == "Turnover":
        return "TURNOVER_GIVEAWAY"
    if plays <= THREE_AND_OUT_PLAYS and first_downs == 0:
        return "THREE_AND_OUT"
    return "STALLED"


def _quarterbacks(frame: pd.DataFrame) -> dict[str, frozenset[str]]:
    """Everyone who threw a pass for a team. Used so a kneel-only or
    scramble-only drive can still be attributed to the right quarterback."""
    if "passer_player_name" not in frame:
        return {}
    passers = frame.dropna(subset=["passer_player_name"])
    counts = passers.groupby(["posteam", "passer_player_name"]).size()
    counts = counts[counts >= MIN_QB_ATTEMPTS]
    return {
        team: frozenset(group.index.get_level_values("passer_player_name"))
        for team, group in counts.groupby(level="posteam")
    }


def _starters(frame: pd.DataFrame, quarterbacks: dict[str, frozenset[str]]) -> dict:
    """The quarterback on a team's FIRST snap of the game -- not the one with
    the most snaps. A starter hurt early is still the starter, and the market
    priced the game on him."""
    out: dict[tuple[str, str], str | None] = {}
    for (game_id, team), group in frame.groupby(["game_id", "posteam"]):
        ordered = group.sort_values("play_id")
        # FIRST, not most frequent. _drive_qb takes the mode, which would hand
        # the label to whoever played most -- i.e. the backup, in exactly the
        # games where this field matters.
        first_drive = ordered[ordered["fixed_drive"] == ordered["fixed_drive"].min()]
        out[(game_id, team)] = _drive_qb(first_drive, quarterbacks.get(team, frozenset()))
    return out


def _drive_qb(ordered: pd.DataFrame, roster: frozenset[str]) -> str | None:
    """The passer on the drive; falls back to a rusher who is a known passer
    for that team, so a kneel-down or scramble-only drive is still attributed."""
    if "passer_player_name" in ordered:
        passers = ordered["passer_player_name"].dropna()
        if not passers.empty:
            return str(passers.mode().iloc[0])
    if "rusher_player_name" in ordered:
        for name in ordered["rusher_player_name"].dropna():
            if name in roster:
                return str(name)
    return None


def in_game_injuries(pbp: pd.DataFrame) -> pd.DataFrame:
    """Injury and return events the play-by-play states outright. Free, and the
    only in-game availability signal in this project -- `docs/NFL Injury.md`
    and `ff_player_injury_observations` cover PREGAME roster status only."""
    desc = pbp["desc"].astype(str)
    hurt = desc.str.extract(r"([A-Z]{2,3})-([\w.\'-]+) was injured during the play")
    back = desc.str.extract(r"Injury Update: ([A-Z]{2,3})-([\w.\'-]+) has returned")
    rows = []
    for frame, event in ((hurt, "injured"), (back, "returned")):
        hit = frame.dropna()
        for idx in hit.index:
            play = pbp.loc[idx]
            rows.append({
                "game_id": play.get("game_id"), "qtr": play.get("qtr"),
                "clock": play.get("time"), "team": hit.loc[idx, 0],
                "player": hit.loc[idx, 1], "event": event,
                "drive": play.get("fixed_drive"),
            })
    out = pd.DataFrame(rows)
    return out.sort_values(["game_id", "drive"]).reset_index(drop=True) if not out.empty else out


def reconcile(pbp: pd.DataFrame, drives: pd.DataFrame) -> pd.DataFrame:
    """Do the labelled scoring drives add up to the real final score?

    This has caught nothing yet because it was done by hand each time. Doing it
    by hand does not scale to six seasons, and a labelling bug that changes the
    points attributed to a team is exactly the class of defect that produces
    plausible numbers rather than an error.

    Points from the possessing team's own drives only. SCORE_AGAINST points go
    to the OPPONENT and are added back to them, which is what makes this a real
    check on the archetype rather than a restatement of the box score.
    """
    value = {"Touchdown": 7, "Field goal": 3}
    rows = []
    kick_return = _kick_return_tds(pbp)
    for game_id, game in drives.groupby("game_id"):
        plays = pbp[pbp["game_id"] == game_id]
        last = plays.sort_values("play_id").iloc[-1]
        actual = {
            str(last.get("home_team")): float(last.get("total_home_score") or 0),
            str(last.get("away_team")): float(last.get("total_away_score") or 0),
        }
        labelled = {team: 0.0 for team in actual}
        for _, drive in game.iterrows():
            team = str(drive["team"])
            if drive["archetype"] == "SCORE_AGAINST":
                other = next((t for t in actual if t != team), None)
                if other:
                    # A safety is 2, not 7. Getting this wrong put exactly 11
                    # of 544 team-games at a clean +5 -- a constant offset,
                    # which is what a miscoded scoring rule looks like as
                    # opposed to noise.
                    labelled[other] += 2 if str(drive["result"]) == "Safety" else 7
            elif team in labelled:
                labelled[team] += value.get(str(drive["result"]), 0)
        for team, points in labelled.items():
            returns = kick_return.get((game_id, team), 0)
            rows.append({
                "game_id": game_id, "team": team,
                "labelled": points, "kick_return_td": returns,
                "actual": actual.get(team),
                # Kickoff-return touchdowns belong to NO drive -- the receiving
                # team never snapped the ball, so no drive result covers them.
                # They are reported as their own column rather than folded into
                # `labelled`, because this is a check and a check that absorbs
                # its own residuals stops being one.
                "delta": points + returns * 7 - (actual.get(team) or 0),
            })
    return pd.DataFrame(rows)


def _score_against_mechanism(ordered: pd.DataFrame, result: str) -> str | None:
    """HOW the opponent scored on this possession -- a modifier, not a label.

    SCORE_AGAINST is a mixture of four different events. In 2025: 48 pick-sixes
    and fumble returns, 17 punt-return or blocked-punt touchdowns, 3 blocked
    field-goal returns and 12 safeties. Thirty-two of 81 are not offensive
    giveaways at all, and none of them carried anything to tell them apart --
    so `summarise().giveaway_rate`, which is keyed on the QUARTERBACK, was
    charging him for blocked punts.
    """
    if result not in AGAINST:
        return None
    types = ordered.get("play_type", pd.Series(dtype=object)).astype(str)
    if (_num(ordered, "safety").fillna(0) == 1).any():
        return "safety"
    kick = types.isin(("punt", "field_goal", "extra_point"))
    if bool((kick & (_num(ordered, "touchdown").fillna(0) == 1)).any()):
        return "punt" if bool((types == "punt").any()) else "field_goal"
    if (_num(ordered, "interception").fillna(0) == 1).any():
        return "pass_return"
    if (_num(ordered, "fumble_lost").fillna(0) == 1).any():
        return "fumble_return"
    return None


def _kick_return_tds(pbp: pd.DataFrame) -> dict[tuple[str, str], int]:
    """Kickoff touchdowns, keyed by the team that ACTUALLY SCORED.

    Keyed on `td_team`, not `posteam`. Assuming the receiving team scored is
    wrong whenever the kicking team recovers a muff in the end zone -- once in
    2025, which put both teams in that game off by a mirrored 6 and -7.
    """
    if "play_type" not in pbp:
        return {}
    kicks = pbp[
        (pbp["play_type"] == "kickoff")
        & (pd.to_numeric(pbp.get("sp"), errors="coerce") == 1)
        & pbp["desc"].astype(str).str.contains("TOUCHDOWN", na=False)
    ]
    team = "td_team" if "td_team" in kicks else "posteam"
    return kicks.dropna(subset=[team]).groupby(["game_id", team]).size().to_dict()


def _start_bucket(yardline_100: float) -> str:
    if pd.isna(yardline_100):
        return "unknown"
    if yardline_100 <= SHORT_FIELD_YARDLINE:
        return "short_field"
    if yardline_100 >= LONG_FIELD_YARDLINE:
        return "long_field"
    return "normal"


def _end_bucket(yardline_100: float) -> str:
    """Where the possession died. See `end_bucket` in the module docstring."""
    if pd.isna(yardline_100):
        return "unknown"
    if yardline_100 <= RED_ZONE_YARDLINE:
        return "red_zone"
    if yardline_100 <= SCORING_RANGE_YARDLINE:
        return "scoring_range"
    if yardline_100 <= MIDFIELD_YARDLINE:
        return "midfield"
    return "own_territory"


def summarise(drives: pd.DataFrame) -> pd.DataFrame:
    """Mix per (team, QUARTERBACK) -- see rule 5. Keying this on team alone is
    how a backup's game gets recorded as a team trait. Only KNEEL_DOWN is
    excluded from the denominator; CLOCK_EXPIRED is a real possession."""
    live = drives[drives["archetype"] != "KNEEL_DOWN"]
    rows = []
    for (team, qb), group in live.groupby(["team", "qb"], dropna=False):
        n = len(group)
        snaps = int(group["snaps"].sum())
        counts = group["archetype"].value_counts()
        rows.append({
            "team": team,
            "qb": qb,
            "starter": bool(group["qb_is_starter"].all()),
            "live_drives": n,
            # the two explosiveness measures, deliberately side by side
            "expl_dependence_rate": round(group["explosive_dependence"].mean(), 3),
            "expl_per_snap": round(int(group["explosive_plays"].sum()) / snaps, 3) if snaps else 0.0,
            "three_and_out_rate": round(counts.get("THREE_AND_OUT", 0) / n, 3),
            "sack_drive_rate": round(group["had_sack"].mean(), 3),
            "penalty_drive_rate": round(group["had_penalty"].mean(), 3),
            # OFFENSIVE giveaways only. SCORE_AGAINST also holds punt-return
            # and blocked-kick touchdowns and safeties -- 32 of 81 in 2025,
            # one a team-season -- and this row is keyed on the QUARTERBACK,
            # who did not block that punt. `score_against_mechanism` keeps the
            # rest visible rather than discarding them.
            "giveaway_rate": round((
                counts.get("TURNOVER_GIVEAWAY", 0)
                + int((group["archetype"].eq("SCORE_AGAINST")
                       & group["score_against_mechanism"].isin(
                           ["pass_return", "fumble_return"])).sum())
            ) / n, 3),
            "red_zone_trip_rate": round(group["reached_red_zone"].mean(), 3),
            # a possession that reached scoring range and produced nothing
            "wasted_range_rate": round((
                group["archetype"].isin(["TURNOVER_GIVEAWAY", "TURNOVER_ON_DOWNS",
                                         "MISSED_FG", "CLOCK_EXPIRED"])
                & group["end_bucket"].isin(["red_zone", "scoring_range"])
            ).mean(), 3),
            "score_rate": round(group["result"].isin([SCORED_TD, SCORED_FG]).mean(), 3),
            "plays_per_drive": round(group["plays"].mean(), 2),
            "yards_per_drive": round(group["net_yards"].mean(), 1),
            "epa_per_drive": round(group["epa"].mean(), 2),
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Label NFL drives with archetypes.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--game", help="game_id substring, e.g. NE_SEA")
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()

    pbp = load_pbp(args.season, args.cache)
    if args.game:
        pbp = pbp[pbp["game_id"].str.contains(args.game, case=False, na=False)]
    if pbp.empty:
        raise SystemExit("no plays matched")

    drives = label_drives(pbp)
    cols = ["drive", "team", "qb", "quarter", "start_bucket", "plays",
            "net_yards", "first_downs", "max_play", "explosive_plays",
            "explosive_dependence", "end_bucket", "result", "epa", "archetype"]
    print(f"{VERSION}  |  {sorted(pbp['game_id'].unique())}\n")
    print(drives[cols].to_string(index=False))
    print("\n--- archetype counts ---\n")
    print(drives.groupby(["team", "archetype"]).size().unstack(fill_value=0).to_string())
    injuries = in_game_injuries(pbp)
    if not injuries.empty:
        print("\n--- in-game injury events (from play-by-play text) ---\n")
        print(injuries.to_string(index=False))
    print("\n--- mix per (team, QB) (kneel-downs excluded) ---\n")
    print(summarise(drives).to_string(index=False))
    check = reconcile(pbp, drives)
    print("\n--- reconciliation vs actual final score ---")
    print("(delta is expected to be non-zero: two-point conversions, missed")
    print(" extra points and return-TD values are not modelled. A LARGE or")
    print(" one-sided delta is the signal, not a delta of zero.)\n")
    print(check.to_string(index=False))


if __name__ == "__main__":
    main()
