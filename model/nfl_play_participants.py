"""Who was on the play -- the ATTRIBUTION layer, both sides of the ball.

The play and drive taxonomies describe WHAT happened. Neither describes WHO
did it, and the omission is not symmetric between the sides: every label in
those modules is written from the offence's chair (`team` is always
`posteam`), so a defence appears only as the thing that failed to stop
something.

TWO SEPARATE PROBLEMS LIVE INSIDE "THE DEFENCE HAS NO STORY", and they have
different answers. Conflating them produces twice the taxonomy and no new
information.

  1. THE OUTCOME IS ZERO-SUM, AND IS ALREADY TWO-SIDED. A conversion for the
     offence IS a conversion allowed by the defence. Measured on 2025, the
     league offensive success rate (.4764) and the league defence-allowed
     rate (.4783) are the same number counted from opposite ends. Since
     `defteam` is carried on every row, a defensive rate is a GROUP BY away
     and needs no new label. Mirroring the archetypes would be duplication --
     two labels that always co-occur are one label with two names.

  2. ATTRIBUTION IS NOT ZERO-SUM AND IS ENTIRELY ABSENT. A sack is in the
     data; who made it is not. The source carries 26 defensive-credit columns
     and the taxonomy carried none of them, so the pass rusher, the tackler
     and the man who forced the fumble were all anonymous -- as were the
     passer, rusher and receiver on the other side. That is this module.

ONE ROW PER PLAYER PER ROLE PER PLAY, which is the shape the question
actually has. A play has one passer but can have six tacklers, so a wide
column per role would either truncate or leave a ragged tail of
`solo_tackle_2`, `assist_tackle_4` columns that every consumer has to know to
coalesce. Long form also makes the multi-description property structural
rather than a convention: "every snap where this man was the tackler" is a
WHERE clause, and a player who both caught the ball and fumbled it gets two
honest rows instead of one row that has to choose.

SIDE IS RESOLVED FROM THE DATA, NEVER ASSUMED. Most defensive credit belongs
to `defteam`, but two roles genuinely can fall either way: a fumble recovery
(`fumble_recovery_1_team`) and a penalty (`penalty_team`) -- an offence
recovers its own fumble often, and both units commit fouls. Those two carry
their own team column in the source and it is used rather than inferred.
"""
from __future__ import annotations

import pandas as pd

VERSION = "nfl-play-participants-v1"

# (source prefix, role, side). `side` None means "read the team column named
# below" rather than assume. Roles are named for what the player DID, so they
# read the same way in a group-by as they do out loud.
OFFENSE = (
    ("passer", "passer"),
    ("rusher", "rusher"),
    ("receiver", "receiver"),
    ("fumbled_1", "fumbled"),
)
DEFENSE = (
    ("sack", "sack"),
    ("half_sack_1", "half_sack"),
    ("half_sack_2", "half_sack"),
    ("qb_hit_1", "qb_hit"),
    ("qb_hit_2", "qb_hit"),
    ("tackle_for_loss_1", "tackle_for_loss"),
    ("solo_tackle_1", "solo_tackle"),
    ("solo_tackle_2", "solo_tackle"),
    ("assist_tackle_1", "assist_tackle"),
    ("assist_tackle_2", "assist_tackle"),
    ("tackle_with_assist_1", "tackle_with_assist"),
    ("interception", "interception"),
    ("forced_fumble_player_1", "forced_fumble"),
    ("pass_defense_1", "pass_defense"),
    ("pass_defense_2", "pass_defense"),
)
KICKING = (
    ("kicker", "kicker"),
    ("punter", "punter"),
)
RETURNING = (
    ("kickoff_returner", "kickoff_returner"),
    ("punt_returner", "punt_returner"),
)
# Roles whose side is not fixed -- read the named team column instead.
TEAM_RESOLVED = (
    ("fumble_recovery_1", "fumble_recovery", "fumble_recovery_1_team"),
    ("penalty", "penalty", "penalty_team"),
    ("td", "touchdown", "td_team"),
)


def _rows(frame: pd.DataFrame, prefix: str, role: str,
          team: pd.Series, side: str) -> pd.DataFrame | None:
    name = frame.get(f"{prefix}_player_name")
    if name is None:
        return None
    keep = name.notna()
    if not keep.any():
        return None
    ident = frame.get(f"{prefix}_player_id")
    return pd.DataFrame({
        "game_id": frame["game_id"][keep],
        "play_id": pd.to_numeric(frame["play_id"], errors="coerce")[keep].astype("Int64"),
        "season": pd.to_numeric(frame.get("season"), errors="coerce")[keep].astype("Int64"),
        "week": pd.to_numeric(frame.get("week"), errors="coerce")[keep].astype("Int64"),
        "team": team[keep],
        "side": side if isinstance(side, str) else side[keep],
        "role": role,
        "player_name": name[keep],
        "player_id": (ident[keep] if ident is not None else None),
    })


def participants(pbp: pd.DataFrame) -> pd.DataFrame:
    """One row per (play, player, role). Long, not wide -- see module docstring."""
    frame = pbp[pbp["game_id"].notna()].copy()
    posteam = frame.get("posteam")
    defteam = frame.get("defteam")
    out: list[pd.DataFrame] = []

    for prefix, role in OFFENSE:
        out.append(_rows(frame, prefix, role, posteam, "offense"))
    for prefix, role in DEFENSE:
        out.append(_rows(frame, prefix, role, defteam, "defense"))
    # A kicking specialist belongs to the possessing team; a returner to the
    # other one. Neither is "offense" in the sense the rest of this file means
    # it, and calling them that would put a punter in a dropback denominator.
    for prefix, role in KICKING:
        out.append(_rows(frame, prefix, role, posteam, "kicking"))
    for prefix, role in RETURNING:
        out.append(_rows(frame, prefix, role, defteam, "returning"))
    for prefix, role, team_col in TEAM_RESOLVED:
        team = frame.get(team_col)
        if team is None:
            continue
        # The recovering/penalised/scoring team is stated in the data. Infer
        # the SIDE from it rather than the other way round: an offence
        # recovers its own fumble often, and both units commit fouls.
        side = pd.Series("defense", index=frame.index)
        side[team.eq(posteam)] = "offense"
        side[team.isna()] = None
        out.append(_rows(frame, prefix, role, team, side))

    rows = [r for r in out if r is not None]
    if not rows:
        return pd.DataFrame(columns=["game_id", "play_id", "season", "week",
                                     "team", "side", "role", "player_name", "player_id"])
    joined = pd.concat(rows, ignore_index=True)
    # A man credited twice in the same role on one play is a source artifact,
    # not two events. Two DIFFERENT roles on one play is real (a receiver who
    # fumbles) and is preserved.
    joined = joined.drop_duplicates(subset=["game_id", "play_id", "role", "player_name"])
    return joined.sort_values(["game_id", "play_id", "side", "role", "player_name"]).reset_index(drop=True)


def summarise(part: pd.DataFrame) -> pd.DataFrame:
    """Snaps credited per (player, team, role) -- the shape player analysis wants."""
    grouped = part.groupby(["team", "side", "role", "player_name"], dropna=False).size()
    return grouped.reset_index(name="plays").sort_values("plays", ascending=False)
