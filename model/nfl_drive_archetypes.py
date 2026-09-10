"""Label every NFL drive with a terminal archetype plus trajectory modifiers.

This is the LABELLING layer only. It describes what happened on a drive; it
makes no prediction and carries no betting claim. The screen that asks whether
these labels survive the closing line is a separate, pre-registered step.

Source is the nflverse play-by-play release the V2 fantasy pipeline already
downloads (`ingest/ff_v2_historical_context.py`), not a PFR scrape.

Four definitional rules, fixed here so a later screen cannot quietly move them:

1. CLOCK-CENSORED DRIVES ARE NOT STALLS. A drive that dies on the half or game
   clock is its own terminal state. Folding it into "three and out" inflates the
   stall rate of whichever team happened to receive last, and receiving order is
   decided by a coin toss -- i.e. pure noise entering the feature.
2. TERMINAL STATE AND TRAJECTORY ARE SEPARATE. "Explosive" is a modifier on a
   drive, not a competing terminal label; a drive can be explosive and still
   punt. Collapsing them into one label makes the mix impossible to condition on
   later, which is the whole point of the exercise.
3. FIELD POSITION IS PART OF THE LABEL, NOT A CONFOUND TO IGNORE. A drive that
   starts on the opponent's 30 scores at a high rate regardless of the offense.
   `start_bucket` is carried so a rate can be conditioned on it rather than
   attributing the defense's and special teams' work to the offense.
4. GARBAGE TIME IS FLAGGED, NEVER SILENTLY DROPPED. The threshold is frozen
   below. Choosing it after seeing a result is the totals-mirage failure mode.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

VERSION = "nfl-drive-archetype-v1"

# --- frozen thresholds -------------------------------------------------------
EXPLOSIVE_PLAY_YARDS = 20      # a single scrimmage gain of at least this many
EXPLOSIVE_SHARE = 0.50         # ...contributing at least this share of net yards
METHODICAL_FIRST_DOWNS = 3     # first downs that mark a drive as sustained
THREE_AND_OUT_PLAYS = 3        # plays at or under this, with no first down
SHORT_FIELD_YARDLINE = 60      # start inside opponent's 60 (yardline_100 <= 60)
LONG_FIELD_YARDLINE = 85       # start behind own 15 (yardline_100 >= 85)
GARBAGE_WP = 0.05              # win prob outside [wp, 1-wp] at drive start

SCRIMMAGE = ("run", "pass")

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


def label_drives(pbp: pd.DataFrame) -> pd.DataFrame:
    """One row per offensive drive, with archetype and modifiers."""
    drive_col = "fixed_drive" if "fixed_drive" in pbp else "drive"
    frame = pbp[pbp["posteam"].notna()].dropna(subset=[drive_col]).copy()

    rows: list[dict] = []
    keys = ["game_id", "posteam", drive_col]
    for (game_id, team, drive_no), group in frame.groupby(keys, sort=True):
        ordered = group.sort_values("play_id")
        # Field position comes from SNAPS only. nflverse groups the kickoff into
        # the receiving team's drive, and a kickoff row's `yardline_100` is the
        # kicking spot -- reading it as the drive start puts every post-kickoff
        # drive on a fake short field with negative net yards.
        snaps = ordered[_num(ordered, "down").notna()]
        scrimmage = snaps[snaps["play_type"].isin(SCRIMMAGE)]

        snap_yl = _num(snaps, "yardline_100").dropna()
        start_yl = float(snap_yl.iloc[0]) if not snap_yl.empty else float("nan")
        if snap_yl.empty:
            end_yl = float("nan")
        else:
            # The last snap's spot is where it STARTED; add its gain to get the
            # drive's true end, or a scoring play reads as zero yards gained.
            last = snaps.loc[snap_yl.index[-1]]
            last_gain = pd.to_numeric(pd.Series([last.get("yards_gained")]), errors="coerce").fillna(0.0).iloc[0]
            end_yl = float(snap_yl.iloc[-1]) - float(last_gain)

        gains = _num(scrimmage, "yards_gained").dropna()
        max_gain = float(gains.max()) if not gains.empty else 0.0
        net_yards = float(start_yl - end_yl) if pd.notna(start_yl) and pd.notna(end_yl) else 0.0

        result = str(ordered["fixed_drive_result"].dropna().iloc[0]) if ordered["fixed_drive_result"].notna().any() else "Unknown"
        plays = _num(ordered, "drive_play_count").dropna()
        plays = int(plays.iloc[0]) if not plays.empty else int(len(scrimmage))
        first_downs = _num(ordered, "drive_first_downs").dropna()
        first_downs = int(first_downs.iloc[0]) if not first_downs.empty else 0
        inside20 = _num(ordered, "drive_inside20").dropna()
        inside20 = bool(inside20.iloc[0]) if not inside20.empty else False

        wp = _num(ordered, "wp").dropna()
        wp = float(wp.iloc[0]) if not wp.empty else float("nan")

        explosive = (
            max_gain >= EXPLOSIVE_PLAY_YARDS
            and net_yards > 0
            and (max_gain / net_yards) >= EXPLOSIVE_SHARE
        )

        rows.append({
            "game_id": game_id,
            "team": team,
            "drive": int(drive_no),
            "quarter": int(_num(ordered, "qtr").dropna().iloc[0]) if ordered["qtr"].notna().any() else None,
            "start_yardline_100": start_yl,
            "start_transition": str(ordered["drive_start_transition"].dropna().iloc[0]) if ordered["drive_start_transition"].notna().any() else None,
            "plays": plays,
            "net_yards": net_yards,
            "first_downs": first_downs,
            "max_play": max_gain,
            "explosive": explosive,
            "reached_red_zone": inside20,
            "result": result,
            "epa": round(float(_num(ordered, "epa").sum()), 2),
            "wp_at_start": wp,
            "archetype": _archetype(result, plays, first_downs, inside20, explosive),
            "start_bucket": _start_bucket(start_yl),
            "garbage_time": bool(pd.notna(wp) and (wp < GARBAGE_WP or wp > 1 - GARBAGE_WP)),
        })

    out = pd.DataFrame(rows)
    return out.sort_values(["game_id", "drive"]).reset_index(drop=True)


def _archetype(result: str, plays: int, first_downs: int, inside20: bool, explosive: bool) -> str:
    """Terminal state first. Trajectory only breaks ties within a terminal state."""
    if result in ("End of half", "End of game"):
        return "CENSORED_CLOCK"          # rule 1: never a stall
    if result == "Touchdown":
        if explosive:
            return "EXPLOSIVE_TD"
        return "METHODICAL_TD" if first_downs >= METHODICAL_FIRST_DOWNS else "SHORT_FIELD_TD"
    if result == "Field goal":
        return "RED_ZONE_SETTLE_FG" if inside20 else "LONG_FG"
    if result in ("Turnover", "Turnover on downs", "Safety", "Opp touchdown"):
        return "TURNOVER"
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
    """Team-level archetype mix. Censored drives are excluded from the
    denominator -- they are an artefact of the clock, not a team trait."""
    live = drives[drives["archetype"] != "CENSORED_CLOCK"]
    rows = []
    for team, group in live.groupby("team"):
        n = len(group)
        counts = group["archetype"].value_counts()
        rows.append({
            "team": team,
            "live_drives": n,
            "censored": int((drives["team"] == team).sum() - n),
            "explosive_rate": round(group["explosive"].mean(), 3),
            "three_and_out_rate": round(counts.get("THREE_AND_OUT", 0) / n, 3),
            "red_zone_trip_rate": round(group["reached_red_zone"].mean(), 3),
            "score_rate": round(group["result"].isin(["Touchdown", "Field goal"]).mean(), 3),
            "plays_per_drive": round(group["plays"].mean(), 2),
            "yards_per_drive": round(group["net_yards"].mean(), 1),
            "epa_per_drive": round(group["epa"].mean(), 2),
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
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
            "net_yards", "first_downs", "max_play", "explosive", "result", "epa", "archetype"]
    print(f"{VERSION}  |  {sorted(pbp['game_id'].unique())}\n")
    print(drives[cols].to_string(index=False))
    print("\n--- team archetype mix (censored drives excluded) ---\n")
    print(summarise(drives).to_string(index=False))


if __name__ == "__main__":
    main()
