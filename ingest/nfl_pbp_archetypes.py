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
season list has to be maintained anywhere. It also compares the release
itself: each labelled game's release content is digested into
`nfl_pbp_source_digests`, and a game nflverse has corrected since it was
labelled is relabelled. The current NFL season is always in scope, so a new
season is picked up without a manual `--season` run, and a release that
cannot be read once games have been played fails the run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_batch

from config import load_config
from db.database import DatabaseManager
from ingest.nfl_dfs_weekly import target_season
from model.nfl_drive_archetypes import VERSION as DRIVE_VERSION, label_drives, load_pbp
from model.nfl_participation import VERSION as PARTICIPATION_VERSION, load_participation
from model.nfl_play_archetypes import VERSION as PLAY_VERSION, label_plays
from model.nfl_play_participants import VERSION as PARTICIPANTS_VERSION, participants

COLUMNS = (
    "game_id", "play_id", "wiped_event", "wiped_yards", "wiped_touchdown", "wiped_turnover", "wiped_sack", "wiped_defender", "drive_injuries", "penalty_yards", "air_yards", "yards_after_catch", "xyac_mean_yardage", "pass_length", "pass_location", "run_location", "run_gap", "cp", "cpoe", "xpass", "pass_oe", "series", "series_success", "series_result", "goal_to_go", "out_of_bounds", "timeout_team", "home_coach", "away_coach", "drive_time_of_possession", "drive_yards_penalized", "drive_inside_twenty", "passer", "rusher", "receiver", "qb_hit", "injury_on_play", "penalty_side", "outcome", "converted", "scramble", "two_point_result", "drive_no_first_down", "drive_score_against_mechanism", "season", "week", "season_type", "home_team", "away_team",
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

    PARTICIPATION IS DELIBERATELY NOT CHECKED HERE; see
    `participation_stale`. A game labelled while nflverse had not published
    participation is not WRONG, it is as good as that game can currently be,
    and treating it as stale made every run relabel the whole season forever
    -- so the "nothing to do" fast path never fired and a busy log became
    indistinguishable from a log doing real work.
    """
    rows = db.execute(
        """SELECT season, game_id FROM nfl_pbp_archetypes
           WHERE play_labeller_version <> %s OR drive_labeller_version <> %s
           GROUP BY season, game_id ORDER BY season, game_id""",
        (PLAY_VERSION, DRIVE_VERSION),
    )
    out: dict[int, list[str]] = {}
    for row in rows:
        out.setdefault(int(row["season"]), []).append(str(row["game_id"]))
    return out


def participation_stale(db: DatabaseManager, season: int) -> list[str]:
    """Games in `season` that predate participation, now that it EXISTS.

    Only the caller knows whether the release is available -- it has just
    tried to fetch it -- so this is asked only in that case. The distinction
    is the point: "we have not attached participation" and "participation is
    attachable and we have not attached it" are different states, and only
    the second is work.
    """
    rows = db.execute(
        """SELECT game_id FROM nfl_pbp_archetypes
           WHERE season = %s AND participation_labeller_version IS DISTINCT FROM %s
           GROUP BY game_id ORDER BY game_id""",
        (season, PARTICIPATION_VERSION),
    )
    return [str(r["game_id"]) for r in rows]


def seasons_present(db: DatabaseManager) -> list[int]:
    """Seasons the table already carries. The scope for picking up new games."""
    rows = db.execute("SELECT DISTINCT season FROM nfl_pbp_archetypes ORDER BY season")
    return [int(r["season"]) for r in rows if r["season"] is not None]


def scoped_seasons(db: DatabaseManager, now: datetime | None = None) -> list[int]:
    """Seasons whose release is compared with the table on every stale pass.

    The latest season the table carries AND the current NFL season. Only the
    first used to be checked, so a new season could never be picked up: in
    September the table's latest season is still last year's, and every new
    game sat outside it forever.
    """
    present = seasons_present(db)
    return sorted(set(present[-1:]) | {target_season(None, now or datetime.now(timezone.utc))})


def completed_games(db: DatabaseManager, season: int) -> int:
    row = db.execute_one(
        """SELECT COUNT(*)::int AS n FROM nfl_season_games
           WHERE season = %s AND game_type = 'REG' AND completed""",
        (season,),
    )
    return int(row["n"]) if row else 0


def load_release(db: DatabaseManager, season: int, cache: Path | None = None) -> pd.DataFrame | None:
    """The season's nflverse release, or None when it cannot exist yet.

    A missing release is only benign before the season has a completed game.
    Once games have been played, a download failure is a failure: it used to
    print a warning and then "nothing stale or missing, no work to do", and
    exit 0 -- the same message a healthy, current table produces.
    """
    try:
        return load_pbp(season, cache)
    except Exception as exc:  # noqa: BLE001 - classified below, never swallowed
        played = completed_games(db, season)
        if played:
            raise RuntimeError(
                f"season {season}: the nflverse play-by-play release could not be read ({exc}) although "
                f"{played} regular-season games are completed; nothing was checked or labelled"
            ) from exc
        print(f"  season {season}: no release yet ({type(exc).__name__}) and no completed games; nothing to label")
        return None


def _digest_value(value):
    """A JSON-stable form of one release cell."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (list, tuple)) or type(value).__name__ == "ndarray":
        return [_digest_value(v) for v in list(value)]
    if isinstance(value, dict):
        return {str(k): _digest_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):  # numpy scalar
        value = value.item()
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        if value.is_integer():
            # A column's dtype depends on the rest of the season (one NaN turns
            # an int column float); the same play must digest the same either way.
            return int(value)
    return value if isinstance(value, (int, float, bool, str)) else str(value)


def game_digests(pbp: pd.DataFrame) -> dict[str, tuple[str, int]]:
    """Per game: (digest of its release content, play count)."""
    columns = sorted(pbp.columns)
    out: dict[str, tuple[str, int]] = {}
    for game_id, frame in pbp.groupby("game_id", sort=True):
        ordered = frame.sort_values("play_id", kind="mergesort")[columns]
        records = [[_digest_value(v) for v in row] for row in ordered.itertuples(index=False, name=None)]
        payload = json.dumps([columns, records], separators=(",", ":"), allow_nan=False, default=str)
        out[str(game_id)] = (hashlib.sha256(payload.encode("utf-8")).hexdigest(), len(ordered))
    return out


def stored_digests(db: DatabaseManager, season: int) -> dict[str, str]:
    rows = db.execute("SELECT game_id, source_digest FROM nfl_pbp_source_digests WHERE season = %s", (season,))
    return {str(r["game_id"]): str(r["source_digest"]) for r in rows}


def record_digests(db: DatabaseManager, season: int, digests: dict[str, tuple[str, int]]) -> int:
    if not digests:
        return 0
    with db.connect() as connection:
        with connection.cursor() as cursor:
            execute_batch(cursor, """
                INSERT INTO nfl_pbp_source_digests (game_id, season, source_digest, play_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET season = EXCLUDED.season,
                    source_digest = EXCLUDED.source_digest, play_count = EXCLUDED.play_count,
                    recorded_at = NOW()
                WHERE nfl_pbp_source_digests.source_digest IS DISTINCT FROM EXCLUDED.source_digest""",
                [(game_id, season, digest, plays) for game_id, (digest, plays) in sorted(digests.items())],
                page_size=500)
    return len(digests)


def release_changes(labelled: set[str], stored: dict[str, str],
                    current: dict[str, tuple[str, int]]) -> dict[str, list[str]]:
    """Compare one season's release with the table. Pure.

    - `new`: played and never labelled;
    - `corrected`: labelled from content that nflverse has since changed;
    - `baseline`: labelled before digests were recorded. Their digest is
      recorded without relabelling: a relabel replaces the game's rows and
      resets `labelled_at`, which point-in-time readers
      (`labelled_at <= as_of`, ingest/nfl_matchup_context.py) would see as the
      whole season disappearing from every earlier replay.
    """
    released = set(current)
    return {
        "new": sorted(released - labelled),
        "corrected": sorted(g for g in released & labelled if g in stored and stored[g] != current[g][0]),
        "baseline": sorted(g for g in released & labelled if g not in stored),
    }


def missing_games(db: DatabaseManager, cache: Path | None = None,
                  now: datetime | None = None) -> tuple[dict[int, list[str]], dict[int, pd.DataFrame]]:
    """Games the release carries that the table has never labelled, or has
    labelled from content nflverse has since corrected.

    WHY THIS EXISTS. `stale_games` refreshes games already in the table, and
    that is all the merge-triggered path could ever do -- so a week of real
    football could be played and the pipeline had no way to notice. Week 1 of
    2026 is the case that exposed it: the Wednesday opener was ingested by a
    one-off season run when it was the only game played, and the thirteen
    Sunday games and Monday night then sat outside the table with every
    automatic path reporting "nothing stale, no work to do" -- which was true,
    and useless. A pipeline that only heals what it already knows about is
    half a pipeline. The same held for corrections: nflverse revises a game's
    play-by-play after it is first published, and a game labelled once was
    never looked at again.

    SCOPED TO `scoped_seasons` (the latest season present plus the current
    season). Keeping the current season complete is routine; backfilling a
    historical one is a deliberate act with a real cost, and conflating them
    would mean a single game labelled once for a test quietly triggering a
    285-game rebuild on the next unrelated merge. Older seasons stay the job
    of an explicit `--season` run.

    Returns the games to label per season and the releases already loaded, so
    the caller does not download them twice.
    """
    found: dict[int, list[str]] = {}
    releases: dict[int, pd.DataFrame] = {}
    for season in scoped_seasons(db, now):
        released = load_release(db, season, cache)
        if released is None:
            continue
        releases[season] = released
        rows = db.execute(
            "SELECT DISTINCT game_id FROM nfl_pbp_archetypes WHERE season = %s",
            (season,),
        )
        current = game_digests(released[released["game_id"].notna()])
        changes = release_changes({str(r["game_id"]) for r in rows}, stored_digests(db, season), current)
        if changes["baseline"]:
            record_digests(db, season, {g: current[g] for g in changes["baseline"]})
            print(f"  season {season}: recorded the release digest of {len(changes['baseline'])} "
                  f"already-labelled game(s) (first check; not relabelled)")
        if changes["new"]:
            print(f"  season {season}: {len(changes['new'])} newly-played game(s) not yet labelled")
        if changes["corrected"]:
            print(f"  season {season}: {len(changes['corrected'])} game(s) corrected by nflverse since they "
                  f"were labelled: {', '.join(changes['corrected'])}")
        todo = sorted(set(changes["new"]) | set(changes["corrected"]))
        if todo:
            found[season] = todo
    return found, releases


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
            _text(getattr(play, "wiped_event", None)),
            _float(getattr(play, "wiped_yards", None)),
            _bool(getattr(play, "wiped_touchdown", None)),
            _bool(getattr(play, "wiped_turnover", None)),
            _bool(getattr(play, "wiped_sack", None)),
            _text(getattr(play, "wiped_defender", None)),
            _int(_get(drive, "injuries")),
            _float(getattr(play, "penalty_yards", None)),
            _float(getattr(play, "air_yards", None)),
            _float(getattr(play, "yards_after_catch", None)),
            _float(getattr(play, "xyac_mean_yardage", None)),
            _text(getattr(play, "pass_length", None)),
            _text(getattr(play, "pass_location", None)),
            _text(getattr(play, "run_location", None)),
            _text(getattr(play, "run_gap", None)),
            _float(getattr(play, "cp", None)),
            _float(getattr(play, "cpoe", None)),
            _float(getattr(play, "xpass", None)),
            _float(getattr(play, "pass_oe", None)),
            _int(getattr(play, "series", None)),
            _bool(getattr(play, "series_success", None)),
            _text(getattr(play, "series_result", None)),
            _bool(getattr(play, "goal_to_go", None)),
            _bool(getattr(play, "out_of_bounds", None)),
            _text(getattr(play, "timeout_team", None)),
            _text(getattr(play, "home_coach", None)),
            _text(getattr(play, "away_coach", None)),
            _text(_get(drive, "time_of_possession")),
            _float(_get(drive, "yards_penalized")),
            _float(_get(drive, "inside_twenty")),
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
        help="Label every game the table disagrees with the release about: a stale "
             "labeller version, or a game that has been played and never labelled.",
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
        # Newly-played and nflverse-corrected games are handled on the same
        # pass: all are "the table disagrees with the release", and splitting
        # them into separate flags meant the automatic path silently covered
        # only some of them. A release that cannot be read for a season with
        # completed games raises here rather than reading as "nothing to do".
        fresh, releases = missing_games(db, args.cache)
        for season, ids in fresh.items():
            stale[season] = sorted(set(stale.get(season, [])) | set(ids))
        if not stale:
            print(f"{PLAY_VERSION} + {DRIVE_VERSION}: nothing stale, missing or corrected in "
                  f"{sorted(releases) or 'no published'} release(s); no work to do")
            return
        total = 0
        for season in sorted(set(stale) | set(seasons_present(db))):
            game_ids = stale.get(season, [])
            # Fetching participation first is what lets the two "stale" ideas
            # stay separate: only once the release is in hand can a game
            # labelled without it be called out of date.
            part = _participation(season)
            if part is not None:
                extra = [g for g in participation_stale(db, season) if g not in game_ids]
                if extra:
                    print(f"  season {season}: {len(extra)} game(s) predate participation, "
                          f"which is now published")
                    game_ids = sorted(set(game_ids) | set(extra))
            if not game_ids:
                continue
            pbp = releases[season] if season in releases else load_pbp(season, args.cache)
            pbp = pbp[pbp["game_id"].isin(game_ids)]
            if pbp.empty:
                # The rows name a game the current release no longer carries.
                # Say so rather than deleting evidence or silently skipping.
                print(f"  WARNING season {season}: {len(game_ids)} stale games "
                      f"absent from the nflverse release; left as-is")
                continue
            digests = game_digests(pbp)
            total += write(db, build_rows(pbp, part))
            write_participants(db, participant_rows(pbp))
            # Recorded after the labels commit: a failed write leaves the old
            # digest, so the next pass retries the game.
            record_digests(db, season, digests)
            print(f"  season {season}: labelled {len(game_ids)} games")
        print(f"{PLAY_VERSION} + {DRIVE_VERSION}: wrote {total} plays")
        return

    pbp = load_pbp(args.season, args.cache)
    if args.game:
        pbp = pbp[pbp["game_id"].str.contains(args.game, case=False, na=False)]
    if pbp.empty:
        raise SystemExit("no plays matched")

    digests = game_digests(pbp)
    rows = build_rows(pbp, _participation(args.season))
    written = write(db, rows)
    credited = write_participants(db, participant_rows(pbp))
    record_digests(db, args.season, digests)
    print(f"{PLAY_VERSION} + {DRIVE_VERSION}: wrote {written} plays "
          f"across {len(set(r[0] for r in rows))} games")
    print(f"{PARTICIPANTS_VERSION}: wrote {credited} player credits")


if __name__ == "__main__":
    main()
