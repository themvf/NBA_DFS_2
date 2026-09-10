"""nflverse participation: formation, personnel grouping, box and rush counts.

A separate nflverse release from the play-by-play, joined on
(game_id, play_id). It is the only source in this project for what was
actually on the field, which down-and-distance cannot tell you: 2nd-and-7 in
11 personnel against a six-man box is a different play from 2nd-and-7 in 12
personnel against eight, and the taxonomy could not previously separate them.

COVERAGE IS STRUCTURAL, NOT PARTIAL. Measured on 2025: 99.8% of scrimmage
snaps (34,632 run/pass plays) carry participation, and 0% of kickoffs, punts,
field goals, extra points and kneels do — nflverse simply does not record
personnel for those. So a missing value means "this was not a scrimmage
snap", not "the data failed". Penalties (`no_play`) sit at 31.8%, which is
the one genuinely ragged case and is reported rather than filled.

THE RELEASE CARRIES MORE THAN THIS MODULE CURRENTLY READS. It has 26
columns, including `was_pressure` (100% populated), `defense_coverage_type`
(COVER_0 through COVER_9, 48.8%), `defense_man_zone_type`, `route`,
`time_to_throw` and `ngs_air_yards`. Those were initially recorded elsewhere
in this project as data that does not exist, which was wrong: the check was
run against the base play-by-play and the absence generalised to every
source. Pressure IS loaded, because it is the largest outcome split anywhere in the
data (EPA -0.535 against +0.242) and it is 100% populated. Coverage shell and
man/zone are loaded too but are roughly half populated, so they are carried
as NULL-where-unknown rather than filled. `route` and `time_to_throw` are
left for later: their per-play semantics need checking before they are worth
storing.

PERSONNEL IS TRANSLATED INTO COACH NOTATION. The source ships a verbose
roster string; "11 personnel" is what anyone in football actually says, and
it is derived here as (running backs)(tight ends). A fullback counts as a
back, per the standard convention — so 1 FB + 1 RB + 1 TE is 21, not 11.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

VERSION = "nfl-participation-v1"

PARTICIPATION_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/"
    "pbp_participation_{season}.parquet"
)

# Five or more rushers is the conventional blitz threshold: four is the
# standard rush, so a fifth is by definition an extra defender sent.
BLITZ_RUSHERS = 5
HEAVY_BLITZ_RUSHERS = 6

_COUNT = re.compile(r"(\d+)\s+([A-Z]+)")


def load_participation(season: int, cache: Path | None = None) -> pd.DataFrame:
    if cache and cache.exists():
        frame = pd.read_parquet(cache)
    else:
        frame = pd.read_parquet(PARTICIPATION_URL.format(season=season))
    return frame.rename(columns={"nflverse_game_id": "game_id"})


def personnel_grouping(personnel: str | None) -> str | None:
    """"1 C, 2 G, 1 QB, 1 RB, 2 T, 1 TE, 3 WR" -> "11".

    Backs then tight ends, the way it is said out loud. A fullback is a back:
    1 FB + 1 RB + 1 TE is 21 personnel, not 11.
    """
    if not isinstance(personnel, str) or not personnel.strip():
        return None
    counts = {position: int(number) for number, position in _COUNT.findall(personnel)}
    backs = counts.get("RB", 0) + counts.get("FB", 0)
    ends = counts.get("TE", 0)
    if backs == 0 and ends == 0 and counts.get("WR", 0) == 0:
        return None
    return f"{backs}{ends}"


def attach(pbp: pd.DataFrame, participation: pd.DataFrame) -> pd.DataFrame:
    """Join participation onto play-by-play, one row in, one row out.

    `validate="one_to_one"` is load-bearing: a play-level join that fans out
    silently multiplies drive-level aggregates, which has already happened
    once in this project's analysis and was caught only because four groups
    reported identical means.
    """
    fields = ["game_id", "play_id", "offense_formation", "offense_personnel",
              "defense_personnel", "defenders_in_box", "number_of_pass_rushers",
              "was_pressure", "defense_coverage_type", "defense_man_zone_type"]
    available = [f for f in fields if f in participation.columns]
    merged = pbp.merge(
        participation[available].drop_duplicates(subset=["game_id", "play_id"]),
        on=["game_id", "play_id"], how="left", validate="one_to_one",
    )
    if len(merged) != len(pbp):
        raise RuntimeError(f"participation join changed row count {len(pbp)} -> {len(merged)}")

    # `offense_formation` is participation's own signal that a row is an
    # offensive snap, and it is the ONLY trustworthy gate here. Kickoffs carry
    # a populated `offense_personnel` -- but it describes the KICK COVERAGE
    # UNIT ("1 DE, 1 FS, 3 ILB, 1 RB, 1 SS, 3 TE, 1 WR"), which a personnel
    # parser happily reads as "13 personnel" on every kickoff in the league.
    # A field being populated is not a field being applicable.
    snap = merged.get("offense_formation").notna() if "offense_formation" in merged else False

    box = pd.to_numeric(merged.get("defenders_in_box"), errors="coerce").where(snap)
    rushers = pd.to_numeric(merged.get("number_of_pass_rushers"), errors="coerce").where(snap)
    merged["personnel_grouping"] = merged.get("offense_personnel").map(personnel_grouping).where(snap)
    merged["defenders_in_box"] = box
    merged["pass_rushers"] = rushers
    for column in ("defense_coverage_type", "defense_man_zone_type"):
        if column in merged:
            merged[column] = merged[column].where(snap)
    # NULL where unknown, never False: "we did not observe a blitz" and "there
    # was no blitz" are different claims and only one of them is supported.
    merged["blitz"] = rushers.where(rushers.isna(), rushers >= BLITZ_RUSHERS).astype("boolean")
    merged["heavy_blitz"] = rushers.where(rushers.isna(), rushers >= HEAVY_BLITZ_RUSHERS).astype("boolean")
    if "was_pressure" in merged:
        merged["pressure"] = merged["was_pressure"].astype("boolean").where(snap)
    return merged
