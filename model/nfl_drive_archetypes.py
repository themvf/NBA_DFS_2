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
  DEFENSIVE_SCORE      Pick-six, scoop-and-score, or safety. Points for the
                       OPPONENT. Folding this into a turnover loses the fact
                       that the drive had negative scoring value.

NOT A TEAM TRAIT
  CENSORED_CLOCK       Ended on the half or game clock. See rule 1.

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
  garbage_time          bool, from win probability at drive start.

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
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

VERSION = "nfl-drive-archetype-v2"

# --- frozen thresholds -------------------------------------------------------
EXPLOSIVE_PLAY_YARDS = 20      # a single scrimmage gain of at least this many
EXPLOSIVE_SHARE = 0.50         # ...carrying at least this share of net yards
METHODICAL_FIRST_DOWNS = 3     # first downs that mark a drive as sustained
THREE_AND_OUT_PLAYS = 3        # plays at or under this, with no first down
SHORT_FIELD_YARDLINE = 60      # start inside opponent's 60 (yardline_100 <= 60)
LONG_FIELD_YARDLINE = 85       # start behind own 15 (yardline_100 >= 85)
GARBAGE_WP = 0.05              # win prob outside [wp, 1-wp] at drive start

SCRIMMAGE = ("run", "pass")

# nflverse `fixed_drive_result` vocabulary, verified against a full season
# (2025: Touchdown, Punt, Field goal, Turnover, Turnover on downs, End of half,
#  Missed field goal, Opp touchdown, Safety).
SCORED_TD = "Touchdown"
SCORED_FG = "Field goal"
CENSORED = ("End of half", "End of game")
DEFENSIVE = ("Opp touchdown", "Safety")

PBP_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
    "play_by_play_{season}.parquet"
)


def load_pbp(season: int, cache: Path | None = None) -> pd.DataFrame:
    if cache and cache.exists():
        return pd.read_parquet(cache)
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
            "drive": int(drive_no),
            "quarter": int(_first(ordered, "qtr", 0) or 0),
            "start_yardline_100": start_yl,
            "start_bucket": _start_bucket(start_yl),
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
            "archetype": _archetype(result, plays, first_downs, red_zone, dependence),
        })

    return pd.DataFrame(rows).sort_values(["game_id", "drive"]).reset_index(drop=True)


def _archetype(
    result: str, plays: int, first_downs: int, red_zone: bool, dependence: bool
) -> str:
    """Terminal state first. Trajectory only breaks ties within a terminal state."""
    if result in CENSORED:
        return "CENSORED_CLOCK"                 # rule 1: never a stall
    if result in DEFENSIVE:
        return "DEFENSIVE_SCORE"                # points AGAINST -- not a plain turnover
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


def _start_bucket(yardline_100: float) -> str:
    if pd.isna(yardline_100):
        return "unknown"
    if yardline_100 <= SHORT_FIELD_YARDLINE:
        return "short_field"
    if yardline_100 >= LONG_FIELD_YARDLINE:
        return "long_field"
    return "normal"


def summarise(drives: pd.DataFrame) -> pd.DataFrame:
    """Team-level mix. Censored drives are excluded from the denominator --
    they are an artefact of the clock, not a team trait."""
    live = drives[drives["archetype"] != "CENSORED_CLOCK"]
    rows = []
    for team, group in live.groupby("team"):
        n = len(group)
        snaps = int(group["snaps"].sum())
        counts = group["archetype"].value_counts()
        rows.append({
            "team": team,
            "live_drives": n,
            "censored": int((drives["team"] == team).sum() - n),
            # the two explosiveness measures, deliberately side by side
            "expl_dependence_rate": round(group["explosive_dependence"].mean(), 3),
            "expl_per_snap": round(int(group["explosive_plays"].sum()) / snaps, 3) if snaps else 0.0,
            "three_and_out_rate": round(counts.get("THREE_AND_OUT", 0) / n, 3),
            "giveaway_rate": round(
                (counts.get("TURNOVER_GIVEAWAY", 0) + counts.get("DEFENSIVE_SCORE", 0)) / n, 3
            ),
            "red_zone_trip_rate": round(group["reached_red_zone"].mean(), 3),
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
    cols = ["drive", "team", "quarter", "start_yardline_100", "start_bucket", "plays",
            "net_yards", "first_downs", "max_play", "explosive_plays",
            "explosive_dependence", "result", "epa", "archetype"]
    print(f"{VERSION}  |  {sorted(pbp['game_id'].unique())}\n")
    print(drives[cols].to_string(index=False))
    print("\n--- archetype counts ---\n")
    print(drives.groupby(["team", "archetype"]).size().unstack(fill_value=0).to_string())
    print("\n--- team mix (censored drives excluded) ---\n")
    print(summarise(drives).to_string(index=False))


if __name__ == "__main__":
    main()
