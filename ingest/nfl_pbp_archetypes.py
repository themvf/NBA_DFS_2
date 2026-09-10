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
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_batch

from config import load_config
from db.database import DatabaseManager
from model.nfl_drive_archetypes import VERSION as DRIVE_VERSION, label_drives, load_pbp
from model.nfl_play_archetypes import VERSION as PLAY_VERSION, label_plays

COLUMNS = (
    "game_id", "play_id", "season", "week", "season_type", "home_team", "away_team",
    "posteam", "drive", "quarter", "clock", "down", "ydstogo", "yardline_100",
    "play_type", "yards_gained", "play_archetype", "distance_bucket", "success",
    "explosive", "shotgun", "no_huddle", "epa", "wp", "description",
    "drive_archetype", "drive_qb", "drive_qb_is_starter", "drive_start_bucket",
    "drive_end_bucket", "drive_result", "drive_plays", "drive_net_yards", "drive_epa",
    "play_labeller_version", "drive_labeller_version",
)


def build_rows(pbp: pd.DataFrame) -> list[tuple]:
    """Join the two label layers onto one row per play."""
    plays = label_plays(pbp)
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
            play.game_id, _int(play.play_id), _int(game.get("season")),
            _int(game.get("week")), _text(game.get("season_type")),
            _text(game.get("home_team")), _text(game.get("away_team")),
            play.team, _int(play.drive), _int(play.quarter), _text(play.clock),
            _int(play.down), _int(play.ydstogo), _int(play.yardline_100),
            _text(play.play_type), _float(play.yards_gained), play.play_archetype,
            play.distance_bucket, bool(play.success), bool(play.explosive),
            bool(play.shotgun), bool(play.no_huddle), _float(play.epa),
            _float(play.wp), _text(play.description),
            _text(_get(drive, "archetype")), _text(_get(drive, "qb")),
            _bool(_get(drive, "qb_is_starter")), _text(_get(drive, "start_bucket")),
            _text(_get(drive, "end_bucket")), _text(_get(drive, "result")),
            _int(_get(drive, "plays")), _float(_get(drive, "net_yards")),
            _float(_get(drive, "epa")),
            PLAY_VERSION, DRIVE_VERSION,
        ))
    return rows


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
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--game", help="game_id substring, e.g. NE_SEA. Omit for the whole season.")
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--database-url")
    args = parser.parse_args()

    pbp = load_pbp(args.season, args.cache)
    if args.game:
        pbp = pbp[pbp["game_id"].str.contains(args.game, case=False, na=False)]
    if pbp.empty:
        raise SystemExit("no plays matched")

    url = args.database_url or load_config().database_url
    if not url:
        raise SystemExit("no DATABASE_URL configured")

    rows = build_rows(pbp)
    db = DatabaseManager(url)
    written = write(db, rows)
    print(f"{PLAY_VERSION} + {DRIVE_VERSION}: wrote {written} plays "
          f"across {len(set(r[0] for r in rows))} games")


if __name__ == "__main__":
    main()
