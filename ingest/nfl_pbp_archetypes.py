"""Write play-and-drive archetypes for NFL games into `nfl_pbp_archetypes`.

Python owns this table; the web app reads it and never writes -- the same
single-writer rule this project applies to `mlb_matchups` and
`ff_player_week_stats`.

Source is the nflverse play-by-play release, which the V2 fantasy pipeline
already downloads. Labels come from `model.nfl_play_archetypes` (transition)
and `model.nfl_drive_archetypes` (terminal); neither is reimplemented here,
so the page and any future screen cannot drift apart from each other.

Re-running a game REPLACES its rows. These are derived labels, not
observations: there is no audit value in retaining a superseded labelling,
and the labeller versions are stamped per row so two labellings stay
comparable.

`--relabel-stale` makes that self-healing. Bumping a labeller version used to
leave the table on the old labelling until somebody remembered to dispatch
the workflow -- and since the web deploy is instant while the ingest is not,
that opened a window where the page expected columns the stored rows did not
have. Stale mode asks the table which games disagree with the CURRENT
versions and relabels exactly those, so a version bump repairs itself and no
season list has to be maintained anywhere.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_batch

from config import load_config
from db.database import DatabaseManager
from model.nfl_drive_archetypes import VERSION as DRIVE_VERSION, label_drives, load_pbp
from model.nfl_participation import VERSION as PARTICIPATION_VERSION, load_participation
from model.nfl_play_archetypes import VERSION as PLAY_VERSION, label_plays
from model.nfl_play_participants import VERSION as PARTICIPANTS_VERSION, participants

COLUMNS = (
    "game_id", "play_id", "passer", "rusher", "receiver", "qb_hit", "injury_on_play", "penalty_side", "outcome", "converted", "scramble", "two_point_result", "drive_no_first_down", "drive_score_against_mechanism", "season", "week", "season_type", "home_team", "away_team",
    "posteam", "drive", "quarter", "clock", "down", "ydstogo", "yardline_100",
    "play_type", "yards_gained", "play_archetype", "turnover_type", "had_sack",
    "goal_line", "penalty_type", "penalty_team", "penalty_first_down",
    "formation", "personnel_grouping", "defenders_in_box", "pass_rushers",
    "blitz", "heavy_blitz", "n_ol", "n_wr", "pressure", "coverage_type", "man_zone",
    "defteam", "posteam_type", "div_game", "score_differential", "game_seconds_remaining", "half_seconds_remaining", "roof", "surface", "temp", "wind", "spread_line", "total_line", "qb_dropback", "first_down", "tackled_for_loss", "st_outcome", "kick_distance", "return_yards",
    "distance_bucket", "success",
    "explosive", "shotgun", "no_huddle", "epa", "wp", "description",
    "drive_archetype", "drive_qb", "drive_qb_is_starter", "drive_start_bucket",
    "drive_end_bucket", "drive_result", "drive_turnover_type", "drive_had_sack",
    "drive_had_penalty", "drive_failed_short", "drive_plays", "drive_net_yards", "drive_epa",
    "play_labeller_version", "drive_labeller_version", "participation_labeller_version",
)


def stale_games(db: DatabaseManager) -> dict[int, list[str]]:
    """Games whose stored labelling disagrees with the current versions.

    Grouped by season so each season's play-by-play is downloaded once, not
    once per game. An empty result means the table is already current --
    which is the common case, and must be a clean no-op rather than a
    full rebuild.
    """
    rows = db.execute(
        """SELECT season, game_id FROM nfl_pbp_archetypes
           WHERE play_labeller_version <> %s OR drive_labeller_version <> %s
              OR participation_labeller_version IS DISTINCT FROM %s
           GROUP BY season, game_id ORDER BY season, game_id""",
        (PLAY_VERSION, DRIVE_VERSION, PARTICIPATION_VERSION),
    )
    out: dict[int, list[str]] = {}
    for row in rows:
        out.setdefault(int(row["season"]), []).append(str(row["game_id"]))
    return out


def build_rows(pbp: pd.DataFrame, participation: pd.DataFrame | None = None) -> list[tuple]:
    """Join the two label layers onto one row per play."""
    # NULL when participation was not attached, NOT the current version. A
    # game labelled while nflverse had not yet published participation (2026
    # 404s today) must stay stale, so the next run picks it up once the
    # release lands. Stamping the version on a row that never saw the data
    # would mark it permanently done -- the same "ran fine, found nothing,
    # structurally could not have" failure the detector-health work exists to
    # catch.
    participation_version = None if participation is None else PARTICIPATION_VERSION
    plays = label_plays(pbp, participation)
    drives = label_drives(pbp).set_index(["game_id", "team", "drive"])

    meta = pbp.drop_duplicates("game_id").set_index("game_id")
    rows: list[tuple] = []
    for play in plays.itertuples(index=False):
        game = meta.loc[play.game_id]
        key = (play.game_id, play.team, play.drive)
        # A play on a possession the drive labeller dropped (a 0-snap phantom)
        # still belongs in the table -- the play happened. Its drive columns
        # are NULL, which is the honest answer, not a guessed archetype.
        drive = drives.loc[key] if key in drives.index else None
        rows.append((
            play.game_id, _int(play.play_id),
            _text(getattr(play, "passer", None)),
            _text(getattr(play, "rusher", None)),
            _text(getattr(play, "receiver", None)),
            _bool(getattr(play, "qb_hit", None)),
            _bool(getattr(play, "injury_on_play", None)),
            _text(getattr(play, "penalty_side", None)),
            _text(play.outcome),
            bool(play.converted), bool(play.scramble), _text(getattr(play, 'two_point_result', None)),
            _bool(_get(drive, 'no_first_down')),
            _text(_get(drive, "score_against_mechanism")), _int(game.get("season")),
            _int(game.get("week")), _text(game.get("season_type")),
            _text(game.get("home_team")), _text(game.get("away_team")),
            play.team, _int(play.drive), _int(play.quarter), _text(play.clock),
            _int(play.down), _int(play.ydstogo), _int(play.yardline_100),
            _text(play.play_type), _float(play.yards_gained), play.play_archetype,
            _text(play.turnover_type), bool(play.had_sack),
            bool(play.goal_line), _text(getattr(play, "penalty_type", None)),
            _text(getattr(play, "penalty_team", None)), bool(play.penalty_first_down),
            _text(getattr(play, "formation", None)), _text(getattr(play, "personnel_grouping", None)),
            _float(getattr(play, "defenders_in_box", None)), _float(getattr(play, "pass_rushers", None)),
            _bool(getattr(play, "blitz", None)), _bool(getattr(play, "heavy_blitz", None)),
            _float(getattr(play, "n_ol", None)), _float(getattr(play, "n_wr", None)),
            _bool(getattr(play, "pressure", None)),
            _text(getattr(play, "coverage_type", None)), _text(getattr(play, "man_zone", None)),
            _text(getattr(play, "defteam", None)),
            _text(getattr(play, "posteam_type", None)),
            _bool(getattr(play, "div_game", None)),
            _int(getattr(play, "score_differential", None)),
            _int(getattr(play, "game_seconds_remaining", None)),
            _int(getattr(play, "half_seconds_remaining", None)),
            _text(getattr(play, "roof", None)),
            _text(getattr(play, "surface", None)),
            _float(getattr(play, "temp", None)),
            _float(getattr(play, "wind", None)),
            _float(getattr(play, "spread_line", None)),
            _float(getattr(play, "total_line", None)),
            _bool(getattr(play, "qb_dropback", None)),
            _bool(getattr(play, "first_down", None)),
            _bool(getattr(play, "tackled_for_loss", None)),
            _text(getattr(play, "st_outcome", None)),
            _float(getattr(play, "kick_distance", None)),
            _float(getattr(play, "return_yards", None)),
            play.distance_bucket, bool(play.success), bool(play.explosive),
            bool(play.shotgun), bool(play.no_huddle), _float(play.epa),
            _float(play.wp), _text(play.description),
            _text(_get(drive, "archetype")), _text(_get(drive, "qb")),
            _bool(_get(drive, "qb_is_starter")), _text(_get(drive, "start_bucket")),
            _text(_get(drive, "end_bucket")), _text(_get(drive, "result")),
            _text(_get(drive, "turnover_type")), _bool(_get(drive, "had_sack")),
            _bool(_get(drive, "had_penalty")), _bool(_get(drive, "failed_short")),
            _int(_get(drive, "plays")), _float(_get(drive, "net_yards")),
            _float(_get(drive, "epa")),
            PLAY_VERSION, DRIVE_VERSION, participation_version,
        ))
    return rows


def _participation(season: int) -> pd.DataFrame | None:
    """Participation is enrichment, not a dependency.

    A failure here must not stop the archetypes being written: the labels are
    computable without it and the columns are simply absent, which the schema
    allows. Silently substituting False would be worse than a missing column.
    """
    try:
        return load_participation(season)
    except Exception as exc:  # noqa: BLE001 - any transport failure degrades the same way
        print(f"  WARNING season {season}: participation unavailable ({exc}); "
              f"formation/personnel/pressure columns will be NULL")
        return None


def _get(drive, field):
    return None if drive is None else drive.get(field)


def _int(value):
    return None if value is None or pd.isna(value) else int(value)


def _float(value):
    return None if value is None or pd.isna(value) else float(value)


def _bool(value):
    return None if value is None or pd.isna(value) else bool(value)


def _text(value):
    return None if value is None or (not isinstance(value, str) and pd.isna(value)) else str(value)


PARTICIPANT_COLUMNS = ("game_id", "play_id", "season", "week", "team", "side",
                       "role", "player_id", "player_name", "participants_version")


def participant_rows(pbp: pd.DataFrame) -> list[tuple]:
    """Long-form attribution for the same games -- see model.nfl_play_participants."""
    frame = participants(pbp)
    return [
        (r.game_id, _int(r.play_id), _int(r.season), _int(r.week), _text(r.team),
         _text(r.side), _text(r.role), _text(r.player_id), _text(r.player_name),
         PARTICIPANTS_VERSION)
        for r in frame.itertuples(index=False)
    ]


def write_participants(db: DatabaseManager, rows: list[tuple]) -> int:
    """Replace each game's attribution wholesale, for the same reason as write()."""
    if not rows:
        return 0
    games = sorted({row[0] for row in rows})
    placeholders = ",".join(["%s"] * len(PARTICIPANT_COLUMNS))
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM nfl_pbp_play_participants WHERE game_id = ANY(%s)", (games,))
            execute_batch(cursor, (
                f"INSERT INTO nfl_pbp_play_participants ({','.join(PARTICIPANT_COLUMNS)}) "
                f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            ), rows, page_size=1000)
        connection.commit()
    return len(rows)


def write(db: DatabaseManager, rows: list[tuple]) -> int:
    if not rows:
        return 0
    games = sorted({row[0] for row in rows})
    placeholders = ",".join(["%s"] * len(COLUMNS))
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in COLUMNS if c not in ("game_id", "play_id"))
    with db.connect() as connection:
        with connection.cursor() as cursor:
            # Replace the game wholesale: a re-label that produced FEWER plays
            # would otherwise leave the surplus behind as stale rows that no
            # longer correspond to any labelling.
            cursor.execute("DELETE FROM nfl_pbp_archetypes WHERE game_id = ANY(%s)", (games,))
            execute_batch(cursor, (
                f"INSERT INTO nfl_pbp_archetypes ({','.join(COLUMNS)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT (game_id, play_id) DO UPDATE SET {updates}, labelled_at=NOW()"
            ), rows, page_size=500)
        connection.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int)
    parser.add_argument("--game", help="game_id substring, e.g. NE_SEA. Omit for the whole season.")
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--database-url")
    parser.add_argument(
        "--relabel-stale", action="store_true",
        help="Relabel every game whose stored labeller version is not the current one.",
    )
    args = parser.parse_args()
    if not args.relabel_stale and args.season is None:
        parser.error("--season is required unless --relabel-stale is given")

    url = args.database_url or load_config().database_url
    if not url:
        raise SystemExit("no DATABASE_URL configured")
    db = DatabaseManager(url)

    if args.relabel_stale:
        stale = stale_games(db)
        if not stale:
            print(f"{PLAY_VERSION} + {DRIVE_VERSION}: nothing stale, no work to do")
            return
        total = 0
        for season, game_ids in stale.items():
            pbp = load_pbp(season, args.cache)
            pbp = pbp[pbp["game_id"].isin(game_ids)]
            if pbp.empty:
                # The rows name a game the current release no longer carries.
                # Say so rather than deleting evidence or silently skipping.
                print(f"  WARNING season {season}: {len(game_ids)} stale games "
                      f"absent from the nflverse release; left as-is")
                continue
            total += write(db, build_rows(pbp, _participation(season)))
            write_participants(db, participant_rows(pbp))
            print(f"  season {season}: relabelled {len(game_ids)} games")
        print(f"{PLAY_VERSION} + {DRIVE_VERSION}: relabelled {total} plays")
        return

    pbp = load_pbp(args.season, args.cache)
    if args.game:
        pbp = pbp[pbp["game_id"].str.contains(args.game, case=False, na=False)]
    if pbp.empty:
        raise SystemExit("no plays matched")

    rows = build_rows(pbp, _participation(args.season))
    written = write(db, rows)
    credited = write_participants(db, participant_rows(pbp))
    print(f"{PLAY_VERSION} + {DRIVE_VERSION}: wrote {written} plays "
          f"across {len(set(r[0] for r in rows))} games")
    print(f"{PARTICIPANTS_VERSION}: wrote {credited} player credits")


if __name__ == "__main__":
    main()
