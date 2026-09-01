"""Database query helpers for NBA DFS v2."""

from __future__ import annotations

import json
import logging

from db.database import DatabaseManager

logger = logging.getLogger(__name__)


def _execute_values_batch(db: DatabaseManager, sql: str, rows: list[tuple], page_size: int = 1000) -> int:
    """Bulk insert/update rows efficiently with psycopg2 execute_values."""
    if not rows:
        return 0

    from psycopg2.extras import execute_values

    with db.connect() as conn:
        cur = conn.cursor()
        execute_values(cur, sql, rows, page_size=page_size)

    return len(rows)


# ── Team helpers ──────────────────────────────────────────────────────────────

def build_team_abbrev_cache(db: DatabaseManager) -> dict[str, int]:
    """Return {ABBREV_UPPER: team_id} for all 30 teams in a single query."""
    rows = db.execute("SELECT team_id, abbreviation FROM teams")
    return {r["abbreviation"].upper(): r["team_id"] for r in rows}


def upsert_nba_team(
    db: DatabaseManager,
    name: str,
    abbreviation: str,
    conference: str = "",
    division: str = "",
    logo_url: str = "",
) -> int:
    row = db.execute_one(
        """
        INSERT INTO teams (name, abbreviation, conference, division, logo_url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (abbreviation) DO UPDATE SET
            name = EXCLUDED.name,
            conference = EXCLUDED.conference,
            division = EXCLUDED.division,
            logo_url = EXCLUDED.logo_url
        RETURNING team_id
        """,
        (name, abbreviation, conference, division, logo_url),
    )
    return row["team_id"] if row else 0


# ── NBA stats upserts ─────────────────────────────────────────────────────────

def upsert_nba_team_stats(
    db: DatabaseManager,
    team_id: int,
    season: str,
    pace: float | None,
    off_rtg: float | None,
    def_rtg: float | None,
) -> None:
    db.execute(
        """
        INSERT INTO nba_team_stats (team_id, season, pace, off_rtg, def_rtg)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (team_id, season) DO UPDATE SET
            pace = EXCLUDED.pace,
            off_rtg = EXCLUDED.off_rtg,
            def_rtg = EXCLUDED.def_rtg,
            fetched_at = NOW()
        """,
        (team_id, season, pace, off_rtg, def_rtg),
    )


def upsert_nba_player_stats(
    db: DatabaseManager,
    player_id: int,
    season: str,
    team_id: int | None,
    name: str,
    position: str | None,
    games: int,
    avg_minutes: float,
    ppg: float,
    rpg: float,
    apg: float,
    spg: float,
    bpg: float,
    tovpg: float,
    threefgm_pg: float,
    usage_rate: float,
    dd_rate: float,
    fpts_std: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO nba_player_stats (
            player_id, season, team_id, name, position, games,
            avg_minutes, ppg, rpg, apg, spg, bpg, tovpg,
            threefgm_pg, usage_rate, dd_rate, fpts_std
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_id, season) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            games = EXCLUDED.games,
            avg_minutes = EXCLUDED.avg_minutes,
            ppg = EXCLUDED.ppg,
            rpg = EXCLUDED.rpg,
            apg = EXCLUDED.apg,
            spg = EXCLUDED.spg,
            bpg = EXCLUDED.bpg,
            tovpg = EXCLUDED.tovpg,
            threefgm_pg = EXCLUDED.threefgm_pg,
            usage_rate = EXCLUDED.usage_rate,
            dd_rate = EXCLUDED.dd_rate,
            fpts_std = EXCLUDED.fpts_std,
            fetched_at = NOW()
        """,
        (
            player_id, season, team_id, name, position, games,
            avg_minutes, ppg, rpg, apg, spg, bpg, tovpg,
            threefgm_pg, usage_rate, dd_rate, fpts_std,
        ),
    )


def upsert_nba_player_game_logs(db: DatabaseManager, rows: list[dict]) -> int:
    tuples = [
        (
            row["season"],
            row["season_type"],
            row["player_id"],
            row["name"],
            row.get("team_id"),
            row.get("opponent_team_id"),
            row["game_id"],
            row.get("game_date"),
            row.get("matchup"),
            row.get("team_abbreviation"),
            row.get("opponent_abbreviation"),
            row.get("is_home"),
            row.get("win_loss"),
            row.get("minutes"),
            row.get("points"),
            row.get("rebounds"),
            row.get("assists"),
            row.get("steals"),
            row.get("blocks"),
            row.get("turnovers"),
            row.get("fgm"),
            row.get("fga"),
            row.get("fg3m"),
            row.get("fg3a"),
            row.get("ftm"),
            row.get("fta"),
            row.get("plus_minus"),
        )
        for row in rows
    ]

    return _execute_values_batch(
        db,
        """
        INSERT INTO nba_player_game_logs (
            season, season_type, player_id, name, team_id, opponent_team_id,
            game_id, game_date, matchup, team_abbreviation, opponent_abbreviation,
            is_home, win_loss, minutes, points, rebounds, assists, steals,
            blocks, turnovers, fgm, fga, fg3m, fg3a, ftm, fta, plus_minus
        ) VALUES %s
        ON CONFLICT (season, season_type, player_id, game_id) DO UPDATE SET
            name = EXCLUDED.name,
            team_id = EXCLUDED.team_id,
            opponent_team_id = EXCLUDED.opponent_team_id,
            game_date = EXCLUDED.game_date,
            matchup = EXCLUDED.matchup,
            team_abbreviation = EXCLUDED.team_abbreviation,
            opponent_abbreviation = EXCLUDED.opponent_abbreviation,
            is_home = EXCLUDED.is_home,
            win_loss = EXCLUDED.win_loss,
            minutes = EXCLUDED.minutes,
            points = EXCLUDED.points,
            rebounds = EXCLUDED.rebounds,
            assists = EXCLUDED.assists,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            turnovers = EXCLUDED.turnovers,
            fgm = EXCLUDED.fgm,
            fga = EXCLUDED.fga,
            fg3m = EXCLUDED.fg3m,
            fg3a = EXCLUDED.fg3a,
            ftm = EXCLUDED.ftm,
            fta = EXCLUDED.fta,
            plus_minus = EXCLUDED.plus_minus,
            fetched_at = NOW()
        """,
        tuples,
    )


def upsert_nba_team_game_logs(db: DatabaseManager, rows: list[dict]) -> int:
    tuples = [
        (
            row["season"],
            row["season_type"],
            row["team_id"],
            row.get("opponent_team_id"),
            row["team_name"],
            row.get("team_abbreviation"),
            row.get("opponent_abbreviation"),
            row["game_id"],
            row.get("game_date"),
            row.get("matchup"),
            row.get("is_home"),
            row.get("win_loss"),
            row.get("fg3m"),
            row.get("fg3a"),
            row.get("opp_fg3m"),
            row.get("opp_fg3a"),
            row.get("pts"),
            row.get("opp_pts"),
            row.get("ast"),
            row.get("reb"),
            row.get("opp_ast"),
            row.get("opp_reb"),
            row.get("fga"),
            row.get("fta"),
            row.get("oreb"),
            row.get("tov"),
            row.get("opp_fga"),
            row.get("opp_fta"),
            row.get("opp_oreb"),
            row.get("opp_tov"),
            row.get("plus_minus"),
        )
        for row in rows
    ]

    return _execute_values_batch(
        db,
        """
        INSERT INTO nba_team_game_logs (
            season, season_type, team_id, opponent_team_id, team_name,
            team_abbreviation, opponent_abbreviation, game_id, game_date,
            matchup, is_home, win_loss, fg3m, fg3a, opp_fg3m, opp_fg3a,
            pts, opp_pts, ast, reb, opp_ast, opp_reb, fga, fta, oreb, tov,
            opp_fga, opp_fta, opp_oreb, opp_tov, plus_minus
        ) VALUES %s
        ON CONFLICT (season, season_type, team_id, game_id) DO UPDATE SET
            opponent_team_id = EXCLUDED.opponent_team_id,
            team_name = EXCLUDED.team_name,
            team_abbreviation = EXCLUDED.team_abbreviation,
            opponent_abbreviation = EXCLUDED.opponent_abbreviation,
            game_date = EXCLUDED.game_date,
            matchup = EXCLUDED.matchup,
            is_home = EXCLUDED.is_home,
            win_loss = EXCLUDED.win_loss,
            fg3m = EXCLUDED.fg3m,
            fg3a = EXCLUDED.fg3a,
            opp_fg3m = EXCLUDED.opp_fg3m,
            opp_fg3a = EXCLUDED.opp_fg3a,
            pts = EXCLUDED.pts,
            opp_pts = EXCLUDED.opp_pts,
            ast = EXCLUDED.ast,
            reb = EXCLUDED.reb,
            opp_ast = EXCLUDED.opp_ast,
            opp_reb = EXCLUDED.opp_reb,
            fga = EXCLUDED.fga,
            fta = EXCLUDED.fta,
            oreb = EXCLUDED.oreb,
            tov = EXCLUDED.tov,
            opp_fga = EXCLUDED.opp_fga,
            opp_fta = EXCLUDED.opp_fta,
            opp_oreb = EXCLUDED.opp_oreb,
            opp_tov = EXCLUDED.opp_tov,
            plus_minus = EXCLUDED.plus_minus,
            fetched_at = NOW()
        """,
        tuples,
    )


def upsert_nba_matchup(
    db: DatabaseManager,
    game_date: str,
    game_id: str,
    home_team_id: int | None,
    away_team_id: int | None,
    vegas_total: float | None = None,
    home_ml: int | None = None,
    away_ml: int | None = None,
    home_spread: float | None = None,
    vegas_prob_home: float | None = None,
    home_implied: float | None = None,
    away_implied: float | None = None,
    home_score: int | None = None,
    away_score: int | None = None,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO nba_matchups (
            game_date, game_id, home_team_id, away_team_id,
            vegas_total, home_ml, away_ml, home_spread, vegas_prob_home,
            home_implied, away_implied, home_score, away_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
            game_id = COALESCE(EXCLUDED.game_id, nba_matchups.game_id),
            vegas_total = EXCLUDED.vegas_total,
            home_ml = EXCLUDED.home_ml,
            away_ml = EXCLUDED.away_ml,
            home_spread = EXCLUDED.home_spread,
            vegas_prob_home = EXCLUDED.vegas_prob_home,
            home_implied = COALESCE(EXCLUDED.home_implied, nba_matchups.home_implied),
            away_implied = COALESCE(EXCLUDED.away_implied, nba_matchups.away_implied),
            home_score = COALESCE(EXCLUDED.home_score, nba_matchups.home_score),
            away_score = COALESCE(EXCLUDED.away_score, nba_matchups.away_score),
            fetched_at = NOW()
        RETURNING id
        """,
        (game_date, game_id, home_team_id, away_team_id,
         vegas_total, home_ml, away_ml, home_spread, vegas_prob_home,
         home_implied, away_implied, home_score, away_score),
    )
    return row["id"] if row else 0


def insert_game_odds_history_rows(db: DatabaseManager, rows: list[dict]) -> int:
    tuples = [
        (
            row["sport"],
            row["matchup_id"],
            row.get("event_id"),
            row["game_date"],
            row.get("home_team_id"),
            row.get("away_team_id"),
            row.get("home_team_name"),
            row.get("away_team_name"),
            row.get("bookmaker_count", 0),
            row.get("home_ml"),
            row.get("away_ml"),
            row.get("home_spread"),
            row.get("vegas_total"),
            row.get("vegas_prob_home"),
            row.get("home_implied"),
            row.get("away_implied"),
            row["capture_key"],
            row.get("captured_at"),
            json.dumps(row["books"]) if row.get("books") else None,
            row.get("vegas_total_raw"),
            row.get("draw_ml"),
        )
        for row in rows
    ]

    return _execute_values_batch(
        db,
        """
        INSERT INTO game_odds_history (
            sport, matchup_id, event_id, game_date, home_team_id, away_team_id,
            home_team_name, away_team_name, bookmaker_count, home_ml, away_ml,
            home_spread, vegas_total, vegas_prob_home, home_implied, away_implied,
            capture_key, captured_at, books, vegas_total_raw, draw_ml
        ) VALUES %s
        ON CONFLICT (sport, matchup_id, capture_key) DO NOTHING
        """,
        tuples,
    )


def insert_player_prop_history_rows(db: DatabaseManager, rows: list[dict]) -> int:
    tuples = [
        (
            row["sport"],
            row.get("slate_id"),
            row["dk_player_id"],
            row["player_name"],
            row.get("team_id"),
            row.get("event_id"),
            row["market_key"],
            row.get("line"),
            row.get("price"),
            row.get("bookmaker_key"),
            row.get("bookmaker_title"),
            row.get("book_count", 0),
            row["capture_key"],
            row.get("captured_at"),
        )
        for row in rows
    ]

    return _execute_values_batch(
        db,
        """
        INSERT INTO player_prop_history (
            sport, slate_id, dk_player_id, player_name, team_id, event_id, market_key,
            line, price, bookmaker_key, bookmaker_title, book_count, capture_key, captured_at
        ) VALUES %s
        ON CONFLICT (sport, slate_id, dk_player_id, market_key, capture_key) DO NOTHING
        """,
        tuples,
    )


# ── DK slate / player upserts ─────────────────────────────────────────────────

def upsert_dk_slate(
    db: DatabaseManager,
    slate_date: str,
    game_count: int = 0,
    dk_draft_group_id: int | None = None,
    contest_type: str = "main",
    contest_format: str = "gpp",
    sport: str = "nba",
) -> int:
    row = db.execute_one(
        """
        INSERT INTO dk_slates (slate_date, game_count, dk_draft_group_id, contest_type, contest_format, sport)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (slate_date, contest_type, contest_format, sport) DO UPDATE SET
            game_count = EXCLUDED.game_count,
            dk_draft_group_id = COALESCE(EXCLUDED.dk_draft_group_id, dk_slates.dk_draft_group_id)
        RETURNING id
        """,
        (slate_date, game_count, dk_draft_group_id, contest_type, contest_format, sport),
    )
    return row["id"] if row else 0


def upsert_dk_player(db: DatabaseManager, slate_id: int, player: dict) -> None:
    db.execute(
        """
        INSERT INTO dk_players (
            slate_id, dk_player_id, name, team_abbrev, team_id, mlb_team_id, matchup_id,
            eligible_positions, salary, game_info, avg_fpts_dk,
            linestar_proj, linestar_own_pct, proj_own_pct, our_proj, expected_hr, hr_prob_1plus, our_own_pct, our_leverage,
            proj_floor, proj_ceiling, boom_rate,
            dk_in_starting_lineup, dk_starting_lineup_order, dk_team_lineup_confirmed,
            dk_status, is_out
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (slate_id, dk_player_id) DO UPDATE SET
            name = EXCLUDED.name,
            team_abbrev = EXCLUDED.team_abbrev,
            team_id = EXCLUDED.team_id,
            mlb_team_id = EXCLUDED.mlb_team_id,
            matchup_id = EXCLUDED.matchup_id,
            eligible_positions = EXCLUDED.eligible_positions,
            salary = EXCLUDED.salary,
            game_info = EXCLUDED.game_info,
            avg_fpts_dk = EXCLUDED.avg_fpts_dk,
            linestar_proj = EXCLUDED.linestar_proj,
            linestar_own_pct = EXCLUDED.linestar_own_pct,
            proj_own_pct = EXCLUDED.proj_own_pct,
            our_proj = EXCLUDED.our_proj,
            expected_hr = EXCLUDED.expected_hr,
            hr_prob_1plus = EXCLUDED.hr_prob_1plus,
            our_own_pct = EXCLUDED.our_own_pct,
            our_leverage = EXCLUDED.our_leverage,
            proj_floor = EXCLUDED.proj_floor,
            proj_ceiling = EXCLUDED.proj_ceiling,
            boom_rate = EXCLUDED.boom_rate,
            dk_in_starting_lineup = EXCLUDED.dk_in_starting_lineup,
            dk_starting_lineup_order = EXCLUDED.dk_starting_lineup_order,
            dk_team_lineup_confirmed = EXCLUDED.dk_team_lineup_confirmed,
            dk_status = EXCLUDED.dk_status,
            is_out = EXCLUDED.is_out
        """,
        (
            slate_id,
            player["dk_player_id"],
            player["name"],
            player["team_abbrev"],
            player.get("team_id"),
            player.get("mlb_team_id"),
            player.get("matchup_id"),
            player["eligible_positions"],
            player["salary"],
            player.get("game_info"),
            player.get("avg_fpts_dk"),
            player.get("linestar_proj"),
            player.get("linestar_own_pct"),
            player.get("proj_own_pct"),
            player.get("our_proj"),
            player.get("expected_hr"),
            player.get("hr_prob_1plus"),
            player.get("our_own_pct"),
            player.get("our_leverage"),
            player.get("proj_floor"),
            player.get("proj_ceiling"),
            player.get("boom_rate"),
            player.get("dk_in_starting_lineup"),
            player.get("dk_starting_lineup_order"),
            player.get("dk_team_lineup_confirmed"),
            player.get("dk_status"),
            player.get("is_out", False),
        ),
    )


# ── NFL team + matchup helpers ───────────────────────────────────────────────

def build_nfl_team_name_cache(db: DatabaseManager) -> dict[str, int]:
    """Return exact Odds API team-name -> internal team ID."""
    rows = db.execute("SELECT team_id, odds_api_name FROM nfl_teams WHERE active")
    return {str(row["odds_api_name"]): int(row["team_id"]) for row in rows}


def upsert_nfl_team(
    db: DatabaseManager,
    *,
    name: str,
    abbreviation: str,
    odds_api_name: str,
    city: str | None = None,
    conference: str | None = None,
    division: str | None = None,
    active: bool = True,
    logo_url: str = "",
) -> int:
    row = db.execute_one(
        """
        INSERT INTO nfl_teams (
            name, abbreviation, odds_api_name, city, conference, division,
            active, logo_url, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (odds_api_name) DO UPDATE SET
            name = EXCLUDED.name,
            abbreviation = EXCLUDED.abbreviation,
            city = EXCLUDED.city,
            conference = EXCLUDED.conference,
            division = EXCLUDED.division,
            active = EXCLUDED.active,
            logo_url = EXCLUDED.logo_url,
            updated_at = NOW()
        RETURNING team_id
        """,
        (name, abbreviation, odds_api_name, city, conference, division, active, logo_url),
    )
    return int(row["team_id"]) if row else 0


def upsert_nfl_matchup(
    db: DatabaseManager,
    *,
    event_id: str,
    game_date: str,
    commence_time,
    home_team_id: int,
    away_team_id: int,
    season: int | None = None,
    season_type: str | None = None,
    week: int | None = None,
    game_status: str | None = None,
) -> int:
    """Upsert an NFL event by provider event ID.

    Schedule refreshes may move kickoff/date/team assignment for an upcoming
    event, but must never erase scores or completion state already observed.
    """
    row = db.execute_one(
        """
        INSERT INTO nfl_matchups (
            event_id, season, season_type, week, game_date, commence_time,
            home_team_id, away_team_id, game_status, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            season = COALESCE(EXCLUDED.season, nfl_matchups.season),
            season_type = COALESCE(EXCLUDED.season_type, nfl_matchups.season_type),
            week = COALESCE(EXCLUDED.week, nfl_matchups.week),
            game_date = EXCLUDED.game_date,
            commence_time = EXCLUDED.commence_time,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            game_status = CASE
                WHEN nfl_matchups.completed THEN nfl_matchups.game_status
                ELSE COALESCE(EXCLUDED.game_status, nfl_matchups.game_status)
            END,
            fetched_at = NOW()
        RETURNING id
        """,
        (event_id, season, season_type, week, game_date, commence_time,
         home_team_id, away_team_id, game_status),
    )
    return int(row["id"]) if row else 0


# ── College football team + schedule helpers ────────────────────────────────

def upsert_cfb_team(
    db: DatabaseManager,
    *,
    cfbd_team_id: int,
    name: str,
    conference: str | None = None,
    classification: str | None = None,
    abbreviation: str | None = None,
    logo_url: str = "",
) -> int:
    row = db.execute_one(
        """
        INSERT INTO cfb_teams (
            cfbd_team_id, name, abbreviation, conference, classification,
            logo_url, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (cfbd_team_id) DO UPDATE SET
            name = EXCLUDED.name,
            abbreviation = COALESCE(EXCLUDED.abbreviation, cfb_teams.abbreviation),
            conference = EXCLUDED.conference,
            classification = EXCLUDED.classification,
            logo_url = COALESCE(NULLIF(EXCLUDED.logo_url, ''), cfb_teams.logo_url),
            active = TRUE,
            updated_at = NOW()
        RETURNING team_id
        """,
        (cfbd_team_id, name, abbreviation, conference, classification, logo_url),
    )
    return int(row["team_id"]) if row else 0


def upsert_cfb_team_alias(
    db: DatabaseManager, *, provider: str, alias: str, team_id: int, reviewed: bool,
) -> None:
    db.execute(
        """
        INSERT INTO cfb_team_aliases (provider, alias, team_id, reviewed)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (provider, alias) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            reviewed = cfb_team_aliases.reviewed OR EXCLUDED.reviewed
        """,
        (provider, alias, team_id, reviewed),
    )


def build_cfb_team_name_cache(db: DatabaseManager, provider: str = "odds_api") -> dict[str, int]:
    """Return exact canonical names plus reviewed provider aliases.

    Keys intentionally remain human-readable and case-sensitive. Normalization
    is handled once by the ingest boundary; this function never fuzzy-matches.
    """
    rows = db.execute(
        """
        SELECT t.team_id, t.name, a.alias
        FROM cfb_teams t
        LEFT JOIN cfb_team_aliases a
          ON a.team_id=t.team_id AND a.provider=%s AND a.reviewed=TRUE
        WHERE t.active=TRUE
        """,
        (provider,),
    )
    cache: dict[str, int] = {}
    for row in rows:
        cache[str(row["name"])] = int(row["team_id"])
        if row.get("alias"):
            cache[str(row["alias"])] = int(row["team_id"])
    return cache


def upsert_cfb_venue(
    db: DatabaseManager, *, cfbd_venue_id: int | None, name: str,
) -> int:
    if cfbd_venue_id is None:
        row = db.execute_one(
            """
            SELECT venue_id FROM cfb_venues WHERE name=%s
            ORDER BY venue_id LIMIT 1
            """,
            (name,),
        )
        if row:
            return int(row["venue_id"])
        row = db.execute_one(
            "INSERT INTO cfb_venues (name) VALUES (%s) RETURNING venue_id",
            (name,),
        )
        return int(row["venue_id"]) if row else 0
    row = db.execute_one(
        """
        INSERT INTO cfb_venues (cfbd_venue_id, name, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (cfbd_venue_id) DO UPDATE SET
            name=EXCLUDED.name, updated_at=NOW()
        RETURNING venue_id
        """,
        (cfbd_venue_id, name),
    )
    return int(row["venue_id"]) if row else 0


def upsert_cfb_matchup(
    db: DatabaseManager,
    *,
    cfbd_game_id: int,
    season: int,
    season_type: str,
    week: int,
    game_date: str,
    commence_time,
    start_time_tbd: bool,
    home_team_id: int,
    away_team_id: int,
    venue_id: int | None,
    neutral_site: bool,
    conference_game: bool,
    network: str | None,
    completed: bool,
    home_score: int | None,
    away_score: int | None,
    home_line_scores: list | None,
    away_line_scores: list | None,
    game_status: str | None = None,
) -> int:
    overtime_periods = max(
        len(home_line_scores or []) - 4,
        len(away_line_scores or []) - 4,
        0,
    )
    row = db.execute_one(
        """
        INSERT INTO cfb_matchups (
            cfbd_game_id, season, season_type, week, game_date, commence_time,
            start_time_tbd, home_team_id, away_team_id, venue_id, neutral_site,
            conference_game, network, game_status, completed, home_score,
            away_score, home_line_scores, away_line_scores, went_to_overtime,
            overtime_periods, fetched_at, score_fetched_at, final_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, NOW(),
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END
        )
        ON CONFLICT (cfbd_game_id) DO UPDATE SET
            season=EXCLUDED.season,
            season_type=EXCLUDED.season_type,
            week=EXCLUDED.week,
            game_date=EXCLUDED.game_date,
            commence_time=EXCLUDED.commence_time,
            start_time_tbd=EXCLUDED.start_time_tbd,
            home_team_id=EXCLUDED.home_team_id,
            away_team_id=EXCLUDED.away_team_id,
            venue_id=EXCLUDED.venue_id,
            neutral_site=EXCLUDED.neutral_site,
            conference_game=EXCLUDED.conference_game,
            network=COALESCE(EXCLUDED.network, cfb_matchups.network),
            game_status=COALESCE(EXCLUDED.game_status, cfb_matchups.game_status),
            completed=EXCLUDED.completed,
            home_score=COALESCE(EXCLUDED.home_score, cfb_matchups.home_score),
            away_score=COALESCE(EXCLUDED.away_score, cfb_matchups.away_score),
            home_line_scores=COALESCE(EXCLUDED.home_line_scores, cfb_matchups.home_line_scores),
            away_line_scores=COALESCE(EXCLUDED.away_line_scores, cfb_matchups.away_line_scores),
            went_to_overtime=EXCLUDED.went_to_overtime,
            overtime_periods=EXCLUDED.overtime_periods,
            fetched_at=NOW(),
            score_fetched_at=CASE WHEN EXCLUDED.completed THEN NOW() ELSE cfb_matchups.score_fetched_at END,
            final_at=CASE WHEN EXCLUDED.completed THEN COALESCE(cfb_matchups.final_at, NOW()) ELSE cfb_matchups.final_at END
        RETURNING id
        """,
        (
            cfbd_game_id, season, season_type, week, game_date, commence_time,
            start_time_tbd, home_team_id, away_team_id, venue_id, neutral_site,
            conference_game, network, game_status, completed, home_score,
            away_score, json.dumps(home_line_scores) if home_line_scores is not None else None,
            json.dumps(away_line_scores) if away_line_scores is not None else None,
            overtime_periods > 0, overtime_periods, completed, completed,
        ),
    )
    return int(row["id"]) if row else 0


def map_cfb_odds_event(db: DatabaseManager, *, matchup_id: int, event_id: str) -> None:
    row = db.execute_one(
        "SELECT odds_event_id FROM cfb_matchups WHERE id=%s",
        (matchup_id,),
    )
    if not row:
        raise ValueError(f"unknown CFB matchup id {matchup_id}")
    existing = row.get("odds_event_id")
    if existing and str(existing) != event_id:
        raise ValueError(
            f"CFB matchup {matchup_id} already maps to odds event {existing}"
        )
    db.execute(
        "UPDATE cfb_matchups SET odds_event_id=%s, fetched_at=NOW() WHERE id=%s",
        (event_id, matchup_id),
    )


def quarantine_cfb_event(
    db: DatabaseManager, *, event_id: str, home_name: str | None,
    away_name: str | None, commence_time, reason: str, raw_json: dict,
) -> None:
    db.execute(
        """
        INSERT INTO cfb_unmapped_events (
            provider, provider_event_id, home_name, away_name, commence_time,
            reason, raw_json
        ) VALUES ('odds_api', %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (provider, provider_event_id) DO UPDATE SET
            home_name=EXCLUDED.home_name,
            away_name=EXCLUDED.away_name,
            commence_time=EXCLUDED.commence_time,
            reason=EXCLUDED.reason,
            raw_json=EXCLUDED.raw_json,
            last_seen_at=NOW(),
            occurrences=cfb_unmapped_events.occurrences + 1
        """,
        (event_id, home_name, away_name, commence_time, reason, json.dumps(raw_json)),
    )


# ── MLB team helpers ──────────────────────────────────────────────────────────

def build_mlb_team_abbrev_cache(db: DatabaseManager) -> dict[str, int]:
    """Return {ABBREV_UPPER: team_id} for all MLB teams in a single query."""
    rows = db.execute("SELECT team_id, abbreviation FROM mlb_teams")
    return {r["abbreviation"].upper(): r["team_id"] for r in rows}


def build_mlb_dk_abbrev_cache(db: DatabaseManager) -> dict[str, int]:
    """Return {DK_ABBREV_UPPER: team_id} for MLB teams that have a dk_abbrev override.

    Falls back to abbreviation if dk_abbrev is NULL.
    """
    rows = db.execute("SELECT team_id, abbreviation, dk_abbrev FROM mlb_teams")
    return {
        (r["dk_abbrev"] or r["abbreviation"]).upper(): r["team_id"]
        for r in rows
    }


def upsert_mlb_team(
    db: DatabaseManager,
    name: str,
    abbreviation: str,
    dk_abbrev: str | None = None,
    ballpark: str | None = None,
    city: str | None = None,
    division: str | None = None,
    mlb_id: int | None = None,
    logo_url: str = "",
) -> int:
    row = db.execute_one(
        """
        INSERT INTO mlb_teams (name, abbreviation, dk_abbrev, ballpark, city, division, mlb_id, logo_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (abbreviation) DO UPDATE SET
            name = EXCLUDED.name,
            dk_abbrev = EXCLUDED.dk_abbrev,
            ballpark = EXCLUDED.ballpark,
            city = EXCLUDED.city,
            division = EXCLUDED.division,
            mlb_id = EXCLUDED.mlb_id,
            logo_url = EXCLUDED.logo_url
        RETURNING team_id
        """,
        (name, abbreviation, dk_abbrev, ballpark, city, division, mlb_id, logo_url),
    )
    return row["team_id"] if row else 0


# ── MLB stats upserts ─────────────────────────────────────────────────────────

def upsert_mlb_park_factors(
    db: DatabaseManager,
    team_id: int,
    season: str,
    runs_factor: float = 1.0,
    hr_factor: float = 1.0,
) -> None:
    db.execute(
        """
        INSERT INTO mlb_park_factors (team_id, season, runs_factor, hr_factor)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (team_id, season) DO UPDATE SET
            runs_factor = EXCLUDED.runs_factor,
            hr_factor = EXCLUDED.hr_factor
        """,
        (team_id, season, runs_factor, hr_factor),
    )


def upsert_mlb_team_stats(
    db: DatabaseManager,
    team_id: int,
    season: str,
    team_wrc_plus: float | None = None,
    team_k_pct: float | None = None,
    team_bb_pct: float | None = None,
    team_iso: float | None = None,
    team_ops: float | None = None,
    bullpen_era: float | None = None,
    bullpen_fip: float | None = None,
    staff_k_pct: float | None = None,
    staff_bb_pct: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO mlb_team_stats (
            team_id, season, team_wrc_plus, team_k_pct, team_bb_pct,
            team_iso, team_ops, bullpen_era, bullpen_fip, staff_k_pct, staff_bb_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_id, season) DO UPDATE SET
            team_wrc_plus = EXCLUDED.team_wrc_plus,
            team_k_pct    = EXCLUDED.team_k_pct,
            team_bb_pct   = EXCLUDED.team_bb_pct,
            team_iso      = EXCLUDED.team_iso,
            team_ops      = EXCLUDED.team_ops,
            bullpen_era   = EXCLUDED.bullpen_era,
            bullpen_fip   = EXCLUDED.bullpen_fip,
            staff_k_pct   = EXCLUDED.staff_k_pct,
            staff_bb_pct  = EXCLUDED.staff_bb_pct,
            fetched_at    = NOW()
        """,
        (
            team_id, season, team_wrc_plus, team_k_pct, team_bb_pct,
            team_iso, team_ops, bullpen_era, bullpen_fip, staff_k_pct, staff_bb_pct,
        ),
    )


def upsert_mlb_batter_stats(
    db: DatabaseManager,
    player_id: int,
    season: str,
    team_id: int | None,
    name: str,
    batting_order: int | None = None,
    games: int = 0,
    pa_pg: float | None = None,
    avg: float | None = None,
    obp: float | None = None,
    slg: float | None = None,
    iso: float | None = None,
    babip: float | None = None,
    wrc_plus: float | None = None,
    k_pct: float | None = None,
    bb_pct: float | None = None,
    hr_pg: float | None = None,
    singles_pg: float | None = None,
    doubles_pg: float | None = None,
    triples_pg: float | None = None,
    rbi_pg: float | None = None,
    runs_pg: float | None = None,
    sb_pg: float | None = None,
    hbp_pg: float | None = None,
    wrc_plus_vs_l: float | None = None,
    wrc_plus_vs_r: float | None = None,
    avg_fpts_pg: float | None = None,
    fpts_std: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO mlb_batter_stats (
            player_id, season, team_id, name, batting_order, games,
            pa_pg, avg, obp, slg, iso, babip, wrc_plus, k_pct, bb_pct,
            hr_pg, singles_pg, doubles_pg, triples_pg,
            rbi_pg, runs_pg, sb_pg, hbp_pg,
            wrc_plus_vs_l, wrc_plus_vs_r, avg_fpts_pg, fpts_std
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (player_id, season) DO UPDATE SET
            team_id       = EXCLUDED.team_id,
            name          = EXCLUDED.name,
            batting_order = EXCLUDED.batting_order,
            games         = EXCLUDED.games,
            pa_pg         = EXCLUDED.pa_pg,
            avg           = EXCLUDED.avg,
            obp           = EXCLUDED.obp,
            slg           = EXCLUDED.slg,
            iso           = EXCLUDED.iso,
            babip         = EXCLUDED.babip,
            wrc_plus      = COALESCE(EXCLUDED.wrc_plus, mlb_batter_stats.wrc_plus),
            k_pct         = EXCLUDED.k_pct,
            bb_pct        = EXCLUDED.bb_pct,
            hr_pg         = EXCLUDED.hr_pg,
            singles_pg    = EXCLUDED.singles_pg,
            doubles_pg    = EXCLUDED.doubles_pg,
            triples_pg    = EXCLUDED.triples_pg,
            rbi_pg        = EXCLUDED.rbi_pg,
            runs_pg       = EXCLUDED.runs_pg,
            sb_pg         = EXCLUDED.sb_pg,
            hbp_pg        = EXCLUDED.hbp_pg,
            wrc_plus_vs_l = COALESCE(EXCLUDED.wrc_plus_vs_l, mlb_batter_stats.wrc_plus_vs_l),
            wrc_plus_vs_r = COALESCE(EXCLUDED.wrc_plus_vs_r, mlb_batter_stats.wrc_plus_vs_r),
            avg_fpts_pg   = EXCLUDED.avg_fpts_pg,
            fpts_std      = EXCLUDED.fpts_std,
            fetched_at    = NOW()
        """,
        (
            player_id, season, team_id, name, batting_order, games,
            pa_pg, avg, obp, slg, iso, babip, wrc_plus, k_pct, bb_pct,
            hr_pg, singles_pg, doubles_pg, triples_pg,
            rbi_pg, runs_pg, sb_pg, hbp_pg,
            wrc_plus_vs_l, wrc_plus_vs_r, avg_fpts_pg, fpts_std,
        ),
    )


def upsert_mlb_pitcher_stats(
    db: DatabaseManager,
    player_id: int,
    season: str,
    team_id: int | None,
    name: str,
    hand: str | None = None,
    games: int = 0,
    ip_pg: float | None = None,
    era: float | None = None,
    fip: float | None = None,
    xfip: float | None = None,
    k_per_9: float | None = None,
    bb_per_9: float | None = None,
    hr_per_9: float | None = None,
    k_pct: float | None = None,
    bb_pct: float | None = None,
    hr_fb_pct: float | None = None,
    whip: float | None = None,
    avg_fpts_pg: float | None = None,
    fpts_std: float | None = None,
    win_pct: float | None = None,
    qs_pct: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO mlb_pitcher_stats (
            player_id, season, team_id, name, hand, games,
            ip_pg, era, fip, xfip, k_per_9, bb_per_9, hr_per_9,
            k_pct, bb_pct, hr_fb_pct, whip,
            avg_fpts_pg, fpts_std, win_pct, qs_pct
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (player_id, season) DO UPDATE SET
            team_id    = EXCLUDED.team_id,
            name       = EXCLUDED.name,
            hand       = EXCLUDED.hand,
            games      = EXCLUDED.games,
            ip_pg      = EXCLUDED.ip_pg,
            era        = EXCLUDED.era,
            fip        = EXCLUDED.fip,
            xfip       = EXCLUDED.xfip,
            k_per_9    = EXCLUDED.k_per_9,
            bb_per_9   = EXCLUDED.bb_per_9,
            hr_per_9   = EXCLUDED.hr_per_9,
            k_pct      = EXCLUDED.k_pct,
            bb_pct     = EXCLUDED.bb_pct,
            hr_fb_pct  = EXCLUDED.hr_fb_pct,
            whip       = EXCLUDED.whip,
            avg_fpts_pg = EXCLUDED.avg_fpts_pg,
            fpts_std   = EXCLUDED.fpts_std,
            win_pct    = EXCLUDED.win_pct,
            qs_pct     = EXCLUDED.qs_pct,
            fetched_at = NOW()
        """,
        (
            player_id, season, team_id, name, hand, games,
            ip_pg, era, fip, xfip, k_per_9, bb_per_9, hr_per_9,
            k_pct, bb_pct, hr_fb_pct, whip,
            avg_fpts_pg, fpts_std, win_pct, qs_pct,
        ),
    )


def insert_mlb_team_stats_snapshot(
    db: DatabaseManager,
    team_id: int,
    season: str,
    snapshot_date: str,
    team_wrc_plus: float | None = None,
    team_k_pct: float | None = None,
    team_bb_pct: float | None = None,
    team_iso: float | None = None,
    team_ops: float | None = None,
    bullpen_era: float | None = None,
    bullpen_fip: float | None = None,
    staff_k_pct: float | None = None,
    staff_bb_pct: float | None = None,
    source: str = "pybaseball_fangraphs",
    available_at=None,
    stats_through_at=None,
    sample_size: int | None = None,
    window_label: str | None = None,
    transformation_version: str = "mlb-stats-history-v2",
    raw_checksum: str | None = None,
) -> None:
    """Append a dated snapshot alongside the current-state mlb_team_stats row.

    Point-in-time history for betting models — see CLAUDE.md "MLB Moneyline —
    Point-in-Time Leak Finding" (2026-07-05). Idempotent per (team, season,
    Each source capture is inserted as a new immutable row. Re-runs never
    rewrite what the system knew at an earlier availability timestamp.
    """
    db.execute(
        """
        INSERT INTO mlb_team_stats_history (
            team_id, season, snapshot_date, team_wrc_plus, team_k_pct,
            team_bb_pct, team_iso, team_ops, bullpen_era, bullpen_fip,
            staff_k_pct, staff_bb_pct, source, available_at,
            stats_through_at, sample_size, window_label,
            transformation_version, raw_checksum
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, COALESCE(%s, NOW()), %s, %s, %s, %s, %s)
        """,
        (
            team_id, season, snapshot_date, team_wrc_plus, team_k_pct,
            team_bb_pct, team_iso, team_ops, bullpen_era, bullpen_fip,
            staff_k_pct, staff_bb_pct, source, available_at,
            stats_through_at, sample_size, window_label,
            transformation_version, raw_checksum,
        ),
    )


def insert_mlb_pitcher_stats_snapshot(
    db: DatabaseManager,
    player_id: int,
    season: str,
    snapshot_date: str,
    team_id: int | None,
    name: str,
    hand: str | None = None,
    games: int | None = None,
    games_started: int | None = None,
    innings_pitched: float | None = None,
    ip_per_start: float | None = None,
    k_per_9: float | None = None,
    bb_per_9: float | None = None,
    fip: float | None = None,
    xfip: float | None = None,
    era: float | None = None,
    source: str = "pybaseball_fangraphs",
    available_at=None,
    stats_through_at=None,
    sample_size: int | None = None,
    window_label: str | None = None,
    transformation_version: str = "mlb-stats-history-v2",
    raw_checksum: str | None = None,
) -> None:
    """Append a dated snapshot alongside the current-state mlb_pitcher_stats
    row. Only carries the fields model/mlb_moneyline_model.py actually reads
    (k_per_9, xfip, era) — see CLAUDE.md "MLB Moneyline — Point-in-Time Leak
    Finding" (2026-07-05).
    """
    db.execute(
        """
        INSERT INTO mlb_pitcher_stats_history (
            player_id, season, snapshot_date, team_id, name, hand, games,
            games_started, innings_pitched, ip_per_start, k_per_9, bb_per_9,
            fip, xfip, era,
            source, available_at, stats_through_at, sample_size, window_label,
            transformation_version, raw_checksum
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, COALESCE(%s, NOW()), %s, %s, %s, %s, %s)
        """,
        (
            player_id, season, snapshot_date, team_id, name, hand, games,
            games_started, innings_pitched, ip_per_start, k_per_9, bb_per_9,
            fip, xfip, era,
            source, available_at, stats_through_at, sample_size, window_label,
            transformation_version, raw_checksum,
        ),
    )


def upsert_mlb_beat_article(
    db: DatabaseManager,
    source: str,
    team_id: int | None,
    url: str,
    title: str,
    published_at: str | None,
    raw_text: str,
) -> int:
    """Insert a scraped beat-writer article; returns its id.

    Idempotent on url (UNIQUE) -- re-scraping the same listing page is safe.
    published_at is the SITE'S OWN displayed timestamp, not scrape time --
    see CLAUDE.md "MLB Beat-Writer Information-Latency Pilot" (2026-07-05).
    """
    return db.execute_insert(
        """
        INSERT INTO mlb_beat_articles (source, team_id, url, title, published_at, raw_text)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO UPDATE SET
            title        = EXCLUDED.title,
            published_at = EXCLUDED.published_at,
            raw_text     = EXCLUDED.raw_text
        RETURNING id
        """,
        (source, team_id, url, title, published_at, raw_text),
    )


def get_mlb_beat_articles_without_facts(db: DatabaseManager, model_version: str) -> list[dict]:
    """Articles that have not yet been run through extraction for this model_version.

    A prior model_version's extraction (if any) does not count -- bumping
    model_version means every article is eligible for re-extraction under
    the new version, same as the star-rating ledgers elsewhere in this project.
    """
    return db.execute(
        """
        SELECT a.id, a.source, a.team_id, a.url, a.title, a.published_at, a.raw_text
        FROM mlb_beat_articles a
        WHERE NOT EXISTS (
            SELECT 1 FROM mlb_beat_facts f
            WHERE f.article_id = a.id AND f.model_version = %s
        )
        ORDER BY a.published_at ASC NULLS LAST
        """,
        (model_version,),
    )


def insert_mlb_beat_fact(
    db: DatabaseManager,
    article_id: int,
    fact_type: str,
    team_id: int | None,
    player_name: str | None,
    description: str,
    quote: str,
    model_version: str,
) -> None:
    """Append one extracted fact. Not upserted -- an article can yield zero,
    one, or several facts per extraction run, and re-running under a new
    model_version should not silently overwrite the old version's rows."""
    db.execute(
        """
        INSERT INTO mlb_beat_facts (
            article_id, fact_type, team_id, player_name, description, quote, model_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (article_id, fact_type, team_id, player_name, description, quote, model_version),
    )


def mark_mlb_beat_article_extracted_empty(db: DatabaseManager, article_id: int, model_version: str) -> None:
    """Record that an article was processed and yielded zero facts.

    Without this, get_mlb_beat_articles_without_facts() would re-select (and
    re-pay DeepSeek for) the same fact-less article on every run, since a
    NOT EXISTS check against zero rows can't distinguish "not yet processed"
    from "processed, nothing found." Stored as a fact_type='_none' sentinel
    row rather than a new table/column -- kept internal to this module.
    """
    db.execute(
        """
        INSERT INTO mlb_beat_facts (article_id, fact_type, team_id, description, quote, model_version)
        VALUES (%s, '_none', NULL, 'no qualifying facts found', 'n/a', %s)
        """,
        (article_id, model_version),
    )


def get_active_youtube_pick_channels(db: DatabaseManager) -> list[dict]:
    """Channels the pipeline should scrape. Written from the web app's
    "Add Channel" action; read here by the ingest script each run."""
    return db.execute(
        "SELECT id, channel_id, channel_name, handle FROM youtube_pick_channels "
        "WHERE active ORDER BY added_at ASC"
    )


def upsert_youtube_pick_channel(
    db: DatabaseManager,
    channel_id: str,
    channel_name: str,
    handle: str | None = None,
) -> int:
    """Register a channel for tracking; returns its id. Idempotent on
    channel_id -- re-adding an existing channel just updates its name/handle
    and leaves `active` alone."""
    return db.execute_insert(
        """
        INSERT INTO youtube_pick_channels (channel_id, channel_name, handle)
        VALUES (%s, %s, %s)
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            handle       = EXCLUDED.handle
        RETURNING id
        """,
        (channel_id, channel_name, handle),
    )


def upsert_youtube_pick_video(
    db: DatabaseManager,
    channel_id: str,
    channel_name: str,
    video_id: str,
    title: str,
    published_at: str | None,
    transcript_text: str,
) -> int:
    """Insert a scraped picks-channel video; returns its id.

    Idempotent on video_id (UNIQUE) -- re-checking the RSS feed is safe.
    """
    return db.execute_insert(
        """
        INSERT INTO youtube_pick_videos (channel_id, channel_name, video_id, title, published_at, transcript_text)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            title           = EXCLUDED.title,
            published_at    = EXCLUDED.published_at,
            transcript_text = EXCLUDED.transcript_text
        RETURNING id
        """,
        (channel_id, channel_name, video_id, title, published_at, transcript_text),
    )


def get_youtube_pick_videos_without_picks(db: DatabaseManager, model_version: str) -> list[dict]:
    """Videos not yet run through pick extraction for this model_version."""
    return db.execute(
        """
        SELECT id, channel_id, channel_name, video_id, title, published_at, transcript_text
        FROM youtube_pick_videos v
        WHERE transcript_text IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM youtube_picks p
              WHERE p.video_id = v.id AND p.model_version = %s
          )
        ORDER BY published_at ASC NULLS LAST
        """,
        (model_version,),
    )


def insert_youtube_pick(
    db: DatabaseManager,
    video_id: int,
    sport: str,
    bet_type: str,
    subject: str,
    opponent: str | None,
    selection: str,
    odds_american: int | None,
    game_context: str | None,
    confidence_label: str | None,
    quote: str,
    model_version: str,
) -> None:
    """Append one extracted pick. ON CONFLICT DO NOTHING against the
    (video_id, sport, bet_type, subject, selection) unique index so two
    overlapping extraction passes over the same not-yet-extracted video
    can't double-insert the same pick (a real race that inflated records
    2-4x before the index existed)."""
    db.execute(
        """
        INSERT INTO youtube_picks (
            video_id, sport, bet_type, subject, opponent, selection,
            odds_american, game_context, confidence_label, quote, model_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id, sport, bet_type, subject, selection) DO NOTHING
        """,
        (video_id, sport, bet_type, subject, opponent, selection,
         odds_american, game_context, confidence_label, quote, model_version),
    )


def mark_youtube_pick_video_extracted_empty(db: DatabaseManager, video_id: int, model_version: str) -> None:
    """Record that a video was processed and yielded zero picks -- same
    sentinel-row pattern as mark_mlb_beat_article_extracted_empty(), and for
    the same reason (avoid re-paying DeepSeek for the same fact-less video)."""
    db.execute(
        """
        INSERT INTO youtube_picks (video_id, sport, bet_type, subject, selection, quote, model_version, status)
        VALUES (%s, '_none', '_none', 'n/a', 'n/a', 'n/a', %s, '_none')
        ON CONFLICT (video_id, sport, bet_type, subject, selection) DO NOTHING
        """,
        (video_id, model_version),
    )


def mark_youtube_picks_unsettleable(db: DatabaseManager, allowed_pairs: tuple) -> int:
    """One-time classification: picks whose (sport, bet_type) is outside the
    supported set are marked 'unsettleable' rather than left ambiguously
    'pending' forever -- see CLAUDE.md "YouTube Picks Settlement". Only
    touches rows still 'pending' (never the '_none' quote sentinel)."""
    rows = db.execute(
        """
        UPDATE youtube_picks
        SET status = 'unsettleable'
        WHERE status = 'pending'
          AND sport != '_none'
          AND (sport, bet_type) NOT IN %s
        RETURNING id
        """,
        (allowed_pairs,),
    )
    return len(rows)


def get_youtube_picks_for_subgame_check(db: DatabaseManager) -> list[dict]:
    """Picks that could be a partial-game market (first 5 innings, first
    half, etc.) which we have no data to grade -- returned for a Python-side
    text check. Includes already-settled rows so a pick mis-graded under the
    old moneyline-only logic (e.g. an F5 moneyline scored on the final) can
    be corrected back to 'unsettleable'."""
    return db.execute(
        "SELECT id, selection, game_context, status FROM youtube_picks "
        "WHERE status IN ('pending', 'won', 'lost', 'push')"
    )


def mark_youtube_pick_unsettleable(db: DatabaseManager, pick_id: int) -> None:
    """Force a single pick to 'unsettleable', clearing any resolution/result
    left over from a prior (incorrect) grade."""
    db.execute(
        "UPDATE youtube_picks SET status = 'unsettleable', matchup_ref = NULL, "
        "result_detail = NULL, settled_at = NULL WHERE id = %s",
        (pick_id,),
    )


def get_unsettleable_in_scope_youtube_picks(db: DatabaseManager, allowed_pairs: tuple) -> list[dict]:
    """Picks marked 'unsettleable' whose (sport, bet_type) is actually in
    scope now -- e.g. totals/spreads a previous, narrower settlement pass
    (moneyline-only) wrote off. Returned so a Python-side check can re-open
    the non-partial-game ones. Makes settlement self-healing when scope
    widens instead of stranding old rows."""
    return db.execute(
        "SELECT id, selection, game_context FROM youtube_picks "
        "WHERE status = 'unsettleable' AND (sport, bet_type) IN %s",
        (allowed_pairs,),
    )


def reopen_youtube_pick(db: DatabaseManager, pick_id: int) -> None:
    """Return a pick to 'pending' for (re)resolution + grading, clearing any
    stale resolution/result."""
    db.execute(
        "UPDATE youtube_picks SET status = 'pending', matchup_ref = NULL, "
        "result_detail = NULL, settled_at = NULL WHERE id = %s",
        (pick_id,),
    )


def get_resolvable_youtube_picks(db: DatabaseManager, allowed_pairs: tuple) -> list[dict]:
    """Pending picks eligible for game resolution -- supported (sport,
    bet_type), no matchup_ref yet."""
    return db.execute(
        """
        SELECT p.id, p.sport, p.bet_type, p.subject, p.opponent, p.selection, p.game_context,
               v.published_at
        FROM youtube_picks p
        JOIN youtube_pick_videos v ON v.id = p.video_id
        WHERE p.status = 'pending' AND p.matchup_ref IS NULL
          AND (p.sport, p.bet_type) IN %s
        """,
        (allowed_pairs,),
    )


def set_youtube_pick_matchup_ref(db: DatabaseManager, pick_id: int, matchup_ref: str) -> None:
    db.execute("UPDATE youtube_picks SET matchup_ref = %s WHERE id = %s", (matchup_ref, pick_id))


def get_resolved_pending_youtube_picks(db: DatabaseManager) -> list[dict]:
    """Pending picks that have already been resolved to a game -- candidates
    for grading once that game is final."""
    return db.execute(
        "SELECT id, sport, bet_type, subject, selection, matchup_ref FROM youtube_picks "
        "WHERE status = 'pending' AND matchup_ref IS NOT NULL"
    )


def settle_youtube_pick(db: DatabaseManager, pick_id: int, status: str, result_detail: str) -> None:
    db.execute(
        "UPDATE youtube_picks SET status = %s, result_detail = %s, settled_at = NOW() WHERE id = %s",
        (status, result_detail, pick_id),
    )


def upsert_mlb_matchup(
    db: DatabaseManager,
    game_date: str,
    game_id: str | None,
    home_team_id: int | None,
    away_team_id: int | None,
    home_sp_id: int | None = None,
    home_sp_name: str | None = None,
    away_sp_id: int | None = None,
    away_sp_name: str | None = None,
    vegas_total: float | None = None,
    home_ml: int | None = None,
    away_ml: int | None = None,
    vegas_prob_home: float | None = None,
    home_implied: float | None = None,
    away_implied: float | None = None,
    ballpark: str | None = None,
    weather_temp: int | None = None,
    wind_speed: int | None = None,
    wind_direction: str | None = None,
    commence_time=None,
) -> int:
    """Upsert with game_id-first row identity (2026-07-07 doubleheader fix).

    One row per GAME (MLB gamePk), not per (date, teams) slot — the old
    slot-unique constraint could not represent a split doubleheader (two games,
    same date + teams) and crashed the whole pipeline when a postponed game's
    gamePk reappeared on its makeup date (STL/MIL, gamePk 823062).

    Resolution order:
      1. gamePk already owned by a row → UPDATE it. game_date/commence_time
         move WITH the game (a reschedule relocates the row); other fields
         keep existing values when the incoming value is NULL.
      2. No gamePk owner, but a single game_id-less "orphan" row occupies the
         (date, teams) slot (created by the odds ingest, which has no gamePk)
         → adopt it: attach the gamePk and update. With two feed games in one
         slot (split DH), game 1 adopts the orphan and game 2 falls through
         to INSERT — each game ends with its own row.
      3. Otherwise INSERT a new row.
    Callers without a game_id resolve to the single slot row when unambiguous
    (or the orphan), and INSERT an orphan row when the slot is empty.
    """
    field_updates = """
            home_sp_id     = COALESCE(%s, mlb_matchups.home_sp_id),
            home_sp_name   = COALESCE(%s, mlb_matchups.home_sp_name),
            away_sp_id     = COALESCE(%s, mlb_matchups.away_sp_id),
            away_sp_name   = COALESCE(%s, mlb_matchups.away_sp_name),
            vegas_total    = COALESCE(%s, mlb_matchups.vegas_total),
            home_ml        = COALESCE(%s, mlb_matchups.home_ml),
            away_ml        = COALESCE(%s, mlb_matchups.away_ml),
            vegas_prob_home = COALESCE(%s, mlb_matchups.vegas_prob_home),
            home_implied   = COALESCE(%s, mlb_matchups.home_implied),
            away_implied   = COALESCE(%s, mlb_matchups.away_implied),
            ballpark       = COALESCE(%s, mlb_matchups.ballpark),
            weather_temp   = COALESCE(%s, mlb_matchups.weather_temp),
            wind_speed     = COALESCE(%s, mlb_matchups.wind_speed),
            wind_direction = COALESCE(%s, mlb_matchups.wind_direction),
            commence_time  = COALESCE(%s, mlb_matchups.commence_time),
            fetched_at     = NOW()
    """
    field_params = (
        home_sp_id, home_sp_name, away_sp_id, away_sp_name,
        vegas_total, home_ml, away_ml, vegas_prob_home,
        home_implied, away_implied, ballpark,
        weather_temp, wind_speed, wind_direction, commence_time,
    )

    if game_id is not None:
        # 1. Own row by gamePk — the game itself may have moved dates.
        owner = db.execute_one(
            "SELECT id, game_date FROM mlb_matchups WHERE game_id = %s", (game_id,)
        )
        if owner:
            if str(owner["game_date"]) != str(game_date):
                logger.info(
                    "mlb_matchups: gamePk %s moved %s -> %s (rescheduled/makeup game)",
                    game_id, owner["game_date"], game_date,
                )
            row = db.execute_one(
                f"""UPDATE mlb_matchups SET
                    game_date = %s,
                    home_team_id = COALESCE(%s, mlb_matchups.home_team_id),
                    away_team_id = COALESCE(%s, mlb_matchups.away_team_id),
                    {field_updates}
                WHERE id = %s RETURNING id""",
                (game_date, home_team_id, away_team_id, *field_params, owner["id"]),
            )
            return row["id"] if row else 0
        # 2. Adopt a single game_id-less orphan in this slot (odds-ingest row).
        orphans = db.execute(
            "SELECT id FROM mlb_matchups WHERE game_date = %s AND home_team_id = %s "
            "AND away_team_id = %s AND game_id IS NULL",
            (game_date, home_team_id, away_team_id),
        )
        if len(orphans) == 1:
            row = db.execute_one(
                f"UPDATE mlb_matchups SET game_id = %s, {field_updates} WHERE id = %s RETURNING id",
                (game_id, *field_params, orphans[0]["id"]),
            )
            return row["id"] if row else 0
    else:
        # No gamePk (odds-only caller): update the single slot row when
        # unambiguous; prefer the orphan when a DH makes the slot ambiguous.
        slot_rows = db.execute(
            "SELECT id, game_id FROM mlb_matchups WHERE game_date = %s AND home_team_id = %s "
            "AND away_team_id = %s ORDER BY commence_time NULLS LAST, id",
            (game_date, home_team_id, away_team_id),
        )
        if len(slot_rows) == 1:
            target = slot_rows[0]["id"]
        elif len(slot_rows) > 1:
            orphan = next((r for r in slot_rows if r["game_id"] is None), None)
            target = orphan["id"] if orphan else slot_rows[0]["id"]
            logger.warning(
                "mlb_matchups: game_id-less upsert into a %d-row slot (%s doubleheader?) — "
                "updating row %s", len(slot_rows), game_date, target,
            )
        else:
            target = None
        if target is not None:
            row = db.execute_one(
                f"UPDATE mlb_matchups SET {field_updates} WHERE id = %s RETURNING id",
                (*field_params, target),
            )
            return row["id"] if row else 0

    # 3. New game — INSERT (ON CONFLICT (game_id) as a concurrency backstop).
    row = db.execute_one(
        """
        INSERT INTO mlb_matchups (
            game_date, game_id, home_team_id, away_team_id,
            home_sp_id, home_sp_name, away_sp_id, away_sp_name,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark,
            weather_temp, wind_speed, wind_direction, commence_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET fetched_at = NOW()
        RETURNING id
        """,
        (
            game_date, game_id, home_team_id, away_team_id,
            home_sp_id, home_sp_name, away_sp_id, away_sp_name,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark,
            weather_temp, wind_speed, wind_direction, commence_time,
        ),
    )
    return row["id"] if row else 0


def insert_mlb_schedule_revision(
    db: DatabaseManager,
    *,
    matchup_id: int,
    game_id: str,
    revision_hash: str,
    game_date: str,
    commence_time,
    home_team_id: int,
    away_team_id: int,
    venue_id: int | None,
    venue_name: str | None,
    home_sp_id: int | None,
    home_sp_name: str | None,
    home_sp_status: str,
    away_sp_id: int | None,
    away_sp_name: str | None,
    away_sp_status: str,
    game_status: str | None,
    source_available_at,
    raw_json: dict,
) -> int:
    """Insert one immutable official-schedule revision, deduplicated by hash."""
    row = db.execute_one(
        """
        INSERT INTO mlb_schedule_revisions (
            matchup_id, game_id, revision_hash, game_date, commence_time,
            home_team_id, away_team_id, venue_id, venue_name,
            home_sp_id, home_sp_name, home_sp_status,
            away_sp_id, away_sp_name, away_sp_status, game_status,
            source, source_available_at, raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, 'mlb_stats_api_schedule', %s, %s::jsonb)
        ON CONFLICT (game_id, revision_hash) DO NOTHING
        RETURNING id
        """,
        (
            matchup_id, game_id, revision_hash, game_date, commence_time,
            home_team_id, away_team_id, venue_id, venue_name,
            home_sp_id, home_sp_name, home_sp_status,
            away_sp_id, away_sp_name, away_sp_status, game_status,
            source_available_at, json.dumps(raw_json, sort_keys=True),
        ),
    )
    return row["id"] if row else 0


def insert_mlb_starter_workload_snapshot(
    db: DatabaseManager,
    *,
    matchup_id: int,
    side: str,
    pitcher_id: int,
    pitcher_name: str | None,
    event_commence,
    last_start_date: str | None,
    days_rest: int | None,
    starts_sample: int,
    pitches_last_start: int | None,
    avg_pitches_last_3: float | None,
    avg_innings_last_3: float | None,
    season_ip_per_start: float | None,
    expected_innings: float | None,
    stats_through_at,
    available_at,
    raw_checksum: str,
    raw_json: dict,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO mlb_starter_workload_snapshots (
            matchup_id, side, pitcher_id, pitcher_name, event_commence,
            last_start_date, days_rest, starts_sample, pitches_last_start,
            avg_pitches_last_3, avg_innings_last_3, season_ip_per_start,
            expected_innings, source, stats_through_at, available_at,
            transformation_version, raw_checksum, raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'mlb_stats_api_game_log', %s, %s, 'mlb-starter-workload-v1',
                %s, %s::jsonb)
        ON CONFLICT (matchup_id, side, raw_checksum) DO NOTHING
        RETURNING id
        """,
        (
            matchup_id, side, pitcher_id, pitcher_name, event_commence,
            last_start_date, days_rest, starts_sample, pitches_last_start,
            avg_pitches_last_3, avg_innings_last_3, season_ip_per_start,
            expected_innings, stats_through_at, available_at, raw_checksum,
            json.dumps(raw_json, sort_keys=True),
        ),
    )
    return row["id"] if row else 0


def insert_mlb_team_offense_split_snapshot(
    db: DatabaseManager,
    *,
    team_id: int,
    season: str,
    wrc_plus_vs_l: float | None,
    wrc_plus_vs_r: float | None,
    players_vs_l: int,
    players_vs_r: int,
    pa_weight_vs_l: float | None,
    pa_weight_vs_r: float | None,
    stats_through_at,
    available_at,
    raw_checksum: str,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO mlb_team_offense_split_snapshots (
            team_id, season, wrc_plus_vs_l, wrc_plus_vs_r,
            players_vs_l, players_vs_r, pa_weight_vs_l, pa_weight_vs_r,
            source, stats_through_at, available_at, transformation_version,
            raw_checksum
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                'fangraphs_batter_splits_weighted', %s, %s,
                'mlb-team-offense-splits-v1', %s)
        ON CONFLICT (team_id, season, raw_checksum) DO NOTHING
        RETURNING id
        """,
        (
            team_id, season, wrc_plus_vs_l, wrc_plus_vs_r,
            players_vs_l, players_vs_r, pa_weight_vs_l, pa_weight_vs_r,
            stats_through_at, available_at, raw_checksum,
        ),
    )
    return row["id"] if row else 0


def insert_mlb_weather_forecast_snapshot(
    db: DatabaseManager,
    *,
    matchup_id: int,
    event_commence,
    venue_name: str | None,
    latitude: float,
    longitude: float,
    provider: str,
    provider_model: str | None,
    provider_issued_at,
    valid_at,
    available_at,
    temperature_f: float | None,
    relative_humidity_pct: float | None,
    precipitation_probability_pct: float | None,
    wind_speed_mph: float | None,
    wind_direction: str | None,
    roof_capability: str,
    roof_state: str,
    roof_source: str,
    source_status: str,
    raw_checksum: str,
    raw_json: dict,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO mlb_weather_forecast_snapshots (
          matchup_id, event_commence, venue_name, latitude, longitude, provider,
          provider_model, provider_issued_at, valid_at, available_at,
          temperature_f, relative_humidity_pct, precipitation_probability_pct,
          wind_speed_mph, wind_direction, roof_capability, roof_state,
          roof_source, source_status, raw_checksum, raw_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (matchup_id, provider, raw_checksum) DO NOTHING
        RETURNING id
        """,
        (
            matchup_id, event_commence, venue_name, latitude, longitude, provider,
            provider_model, provider_issued_at, valid_at, available_at,
            temperature_f, relative_humidity_pct, precipitation_probability_pct,
            wind_speed_mph, wind_direction, roof_capability, roof_state,
            roof_source, source_status, raw_checksum,
            json.dumps(raw_json, sort_keys=True),
        ),
    )
    return row["id"] if row else 0


# ── Soccer / World Cup queries ─────────────────────────────────────────────────

def build_soccer_team_name_cache(db: DatabaseManager) -> dict[str, int]:
    """Return {name: team_id} for all soccer teams.

    Callers normalize names (accents, casing) before lookup — the raw canonical
    name is stored here so it stays aligned with the odds feed's naming.
    """
    rows = db.execute("SELECT team_id, name FROM soccer_teams")
    return {r["name"]: r["team_id"] for r in rows}


def upsert_soccer_team(
    db: DatabaseManager,
    name: str,
    abbreviation: str | None = None,
    dk_abbrev: str | None = None,
    confederation: str | None = None,
    logo_url: str = "",
) -> int:
    """Upsert a national team keyed on canonical name.

    Name is the conflict key (not abbreviation) because odds feeds are the
    source of truth for naming and a few nations qualify late.  Existing
    non-NULL metadata is preserved when the new value is NULL.
    """
    row = db.execute_one(
        """
        INSERT INTO soccer_teams (name, abbreviation, dk_abbrev, confederation, logo_url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET
            abbreviation  = COALESCE(EXCLUDED.abbreviation, soccer_teams.abbreviation),
            dk_abbrev     = COALESCE(EXCLUDED.dk_abbrev, soccer_teams.dk_abbrev),
            confederation = COALESCE(EXCLUDED.confederation, soccer_teams.confederation),
            logo_url      = CASE WHEN EXCLUDED.logo_url <> '' THEN EXCLUDED.logo_url
                                 ELSE soccer_teams.logo_url END
        RETURNING team_id
        """,
        (name, abbreviation, dk_abbrev, confederation, logo_url),
    )
    return row["team_id"] if row else 0


def upsert_soccer_matchup(
    db: DatabaseManager,
    game_date: str,
    game_id: str | None,
    home_team_id: int | None,
    away_team_id: int | None,
    commence_time=None,
    stage: str | None = None,
    vegas_total: float | None = None,
    home_ml: int | None = None,
    draw_ml: int | None = None,
    away_ml: int | None = None,
    vegas_prob_home: float | None = None,
    vegas_prob_draw: float | None = None,
    vegas_prob_away: float | None = None,
    home_implied: float | None = None,
    away_implied: float | None = None,
    over_odds: int | None = None,
    under_odds: int | None = None,
    pinnacle_prob_home: float | None = None,
    pinnacle_prob_draw: float | None = None,
    pinnacle_prob_away: float | None = None,
    dk_dnb_home_ml: int | None = None,
    dk_dnb_away_ml: int | None = None,
    dnb_home_prob: float | None = None,
    dnb_away_prob: float | None = None,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO soccer_matchups (
            game_date, game_id, commence_time, home_team_id, away_team_id, stage,
            vegas_total, home_ml, draw_ml, away_ml,
            vegas_prob_home, vegas_prob_draw, vegas_prob_away,
            home_implied, away_implied, over_odds, under_odds,
            pinnacle_prob_home, pinnacle_prob_draw, pinnacle_prob_away,
            dk_dnb_home_ml, dk_dnb_away_ml, dnb_home_prob, dnb_away_prob
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
            game_id              = COALESCE(EXCLUDED.game_id, soccer_matchups.game_id),
            commence_time        = COALESCE(EXCLUDED.commence_time, soccer_matchups.commence_time),
            stage                = COALESCE(EXCLUDED.stage, soccer_matchups.stage),
            vegas_total          = COALESCE(EXCLUDED.vegas_total, soccer_matchups.vegas_total),
            home_ml              = COALESCE(EXCLUDED.home_ml, soccer_matchups.home_ml),
            draw_ml              = COALESCE(EXCLUDED.draw_ml, soccer_matchups.draw_ml),
            away_ml              = COALESCE(EXCLUDED.away_ml, soccer_matchups.away_ml),
            vegas_prob_home      = COALESCE(EXCLUDED.vegas_prob_home, soccer_matchups.vegas_prob_home),
            vegas_prob_draw      = COALESCE(EXCLUDED.vegas_prob_draw, soccer_matchups.vegas_prob_draw),
            vegas_prob_away      = COALESCE(EXCLUDED.vegas_prob_away, soccer_matchups.vegas_prob_away),
            home_implied         = COALESCE(EXCLUDED.home_implied, soccer_matchups.home_implied),
            away_implied         = COALESCE(EXCLUDED.away_implied, soccer_matchups.away_implied),
            over_odds            = COALESCE(EXCLUDED.over_odds, soccer_matchups.over_odds),
            under_odds           = COALESCE(EXCLUDED.under_odds, soccer_matchups.under_odds),
            pinnacle_prob_home   = COALESCE(EXCLUDED.pinnacle_prob_home, soccer_matchups.pinnacle_prob_home),
            pinnacle_prob_draw   = COALESCE(EXCLUDED.pinnacle_prob_draw, soccer_matchups.pinnacle_prob_draw),
            pinnacle_prob_away   = COALESCE(EXCLUDED.pinnacle_prob_away, soccer_matchups.pinnacle_prob_away),
            dk_dnb_home_ml       = COALESCE(EXCLUDED.dk_dnb_home_ml, soccer_matchups.dk_dnb_home_ml),
            dk_dnb_away_ml       = COALESCE(EXCLUDED.dk_dnb_away_ml, soccer_matchups.dk_dnb_away_ml),
            dnb_home_prob        = COALESCE(EXCLUDED.dnb_home_prob, soccer_matchups.dnb_home_prob),
            dnb_away_prob        = COALESCE(EXCLUDED.dnb_away_prob, soccer_matchups.dnb_away_prob),
            fetched_at           = NOW()
        RETURNING id
        """,
        (
            game_date, game_id, commence_time, home_team_id, away_team_id, stage,
            vegas_total, home_ml, draw_ml, away_ml,
            vegas_prob_home, vegas_prob_draw, vegas_prob_away,
            home_implied, away_implied, over_odds, under_odds,
            pinnacle_prob_home, pinnacle_prob_draw, pinnacle_prob_away,
            dk_dnb_home_ml, dk_dnb_away_ml, dnb_home_prob, dnb_away_prob,
        ),
    )
    return row["id"] if row else 0


def upsert_soccer_model_params(
    db: DatabaseManager,
    mu: float,
    home_adv: float,
    n_matches: int | None = None,
    trained_at: str | None = None,
) -> None:
    """Store the singleton global Poisson params (id=1)."""
    db.execute(
        """
        INSERT INTO soccer_model_params (id, mu, home_adv, n_matches, trained_at, updated_at)
        VALUES (1, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            mu         = EXCLUDED.mu,
            home_adv   = EXCLUDED.home_adv,
            n_matches  = EXCLUDED.n_matches,
            trained_at = EXCLUDED.trained_at,
            updated_at = NOW()
        """,
        (mu, home_adv, n_matches, trained_at),
    )


def get_soccer_model_params(db: DatabaseManager) -> dict | None:
    """Return {mu, home_adv, n_matches, trained_at} or None if never trained."""
    return db.execute_one("SELECT mu, home_adv, n_matches, trained_at FROM soccer_model_params WHERE id = 1")


def upsert_soccer_team_rating(
    db: DatabaseManager,
    team_id: int,
    elo: float | None = None,
    attack: float | None = None,
    defense: float | None = None,
    matches: int = 0,
    rating_date: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO soccer_team_ratings (team_id, elo, attack, defense, matches, rating_date, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (team_id) DO UPDATE SET
            elo         = EXCLUDED.elo,
            attack      = EXCLUDED.attack,
            defense     = EXCLUDED.defense,
            matches     = EXCLUDED.matches,
            rating_date = EXCLUDED.rating_date,
            updated_at  = NOW()
        """,
        (team_id, elo, attack, defense, matches, rating_date),
    )


def upsert_soccer_player_stat(
    db: DatabaseManager,
    *,
    player_name: str,
    normalized_name: str,
    team_id: int | None,
    team_name: str | None,
    season: str,
    position: str | None,
    matches: int,
    minutes_played: float,
    goals: int,
    shots: int,
    shots_on_target: int,
    xg: float,
    npxg: float,
    goals_per_90: float | None,
    shots_per_90: float | None,
    xg_per_90: float | None,
    npxg_per_90: float | None,
    is_penalty_taker: bool = False,
    first_scorer_matches: int = 0,
    early_goals: int = 0,
    early_goal_rate: float | None = None,
    first_scorer_rate: float | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO soccer_player_stats (
            player_name, normalized_name, team_id, team_name, season, position,
            matches, minutes_played, goals, shots, shots_on_target,
            xg, npxg, goals_per_90, shots_per_90, xg_per_90, npxg_per_90,
            is_penalty_taker, first_scorer_matches, early_goals,
            early_goal_rate, first_scorer_rate, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (player_name, season) DO UPDATE SET
            normalized_name       = EXCLUDED.normalized_name,
            team_id               = EXCLUDED.team_id,
            team_name             = EXCLUDED.team_name,
            position              = EXCLUDED.position,
            matches               = EXCLUDED.matches,
            minutes_played        = EXCLUDED.minutes_played,
            goals                 = EXCLUDED.goals,
            shots                 = EXCLUDED.shots,
            shots_on_target       = EXCLUDED.shots_on_target,
            xg                    = EXCLUDED.xg,
            npxg                  = EXCLUDED.npxg,
            goals_per_90          = EXCLUDED.goals_per_90,
            shots_per_90          = EXCLUDED.shots_per_90,
            xg_per_90             = EXCLUDED.xg_per_90,
            npxg_per_90           = EXCLUDED.npxg_per_90,
            is_penalty_taker      = EXCLUDED.is_penalty_taker,
            first_scorer_matches  = EXCLUDED.first_scorer_matches,
            early_goals           = EXCLUDED.early_goals,
            early_goal_rate       = EXCLUDED.early_goal_rate,
            first_scorer_rate     = EXCLUDED.first_scorer_rate,
            updated_at            = NOW()
        """,
        (
            player_name, normalized_name, team_id, team_name, season, position,
            matches, minutes_played, goals, shots, shots_on_target,
            xg, npxg, goals_per_90, shots_per_90, xg_per_90, npxg_per_90,
            is_penalty_taker, first_scorer_matches, early_goals,
            early_goal_rate, first_scorer_rate,
        ),
    )


def upsert_soccer_match_scorer(
    db: DatabaseManager,
    *,
    game_id: str,
    game_date: str,
    scorer_name: str,
    scorer_team: str | None,
    goal_minute: int | None,
    tsdb_event_id: str | None,
    source: str = "thesportsdb",
) -> None:
    db.execute(
        """
        INSERT INTO soccer_match_scorers
            (game_id, game_date, scorer_name, scorer_team, goal_minute, tsdb_event_id, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
            scorer_name    = EXCLUDED.scorer_name,
            scorer_team    = EXCLUDED.scorer_team,
            goal_minute    = EXCLUDED.goal_minute,
            tsdb_event_id  = EXCLUDED.tsdb_event_id,
            source         = EXCLUDED.source
        """,
        (game_id, game_date, scorer_name, scorer_team, goal_minute, tsdb_event_id, source),
    )


def upsert_soccer_match_goal(
    db: DatabaseManager,
    *,
    game_id: str,
    game_date: str,
    player_name: str,
    player_team: str | None,
    assist_name: str | None = None,
    goal_minute: int | None,
    is_first_goal: bool = False,
    source: str = "thesportsdb",
) -> None:
    db.execute(
        """
        INSERT INTO soccer_match_goals
            (game_id, game_date, player_name, player_team, assist_name, goal_minute, is_first_goal, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id, player_name, goal_minute) DO UPDATE SET
            player_team    = EXCLUDED.player_team,
            assist_name    = EXCLUDED.assist_name,
            is_first_goal  = EXCLUDED.is_first_goal,
            source         = EXCLUDED.source
        """,
        (game_id, game_date, player_name, player_team, assist_name, goal_minute, is_first_goal, source),
    )


def get_all_soccer_player_stats(
    db: DatabaseManager,
    season: str = "combined",
) -> dict[str, dict]:
    """Return {normalized_name: row} for all players in a given season."""
    rows = db.execute(
        "SELECT * FROM soccer_player_stats WHERE season = %s", (season,)
    )
    return {r["normalized_name"]: r for r in rows}


MLB_HOMERUN_TRAINING_COLUMNS = [
    "season",
    "game_date",
    "game_id",
    "hitter_mlb_id",
    "hitter_name",
    "hitter_team_id",
    "hitter_team_abbrev",
    "opponent_team_id",
    "opponent_team_abbrev",
    "is_home",
    "ballpark",
    "batting_order",
    "plate_appearances",
    "at_bats",
    "opposing_sp_mlb_id",
    "opposing_sp_name",
    "opposing_sp_hand",
    "hitter_games",
    "hitter_pa_pg",
    "hitter_hr_pg",
    "hitter_iso",
    "hitter_slg",
    "hitter_wrc_plus",
    "hitter_split_wrc_plus",
    "pitcher_games",
    "pitcher_ip_pg",
    "pitcher_hr_per_9",
    "pitcher_hr_fb_pct",
    "pitcher_xfip",
    "pitcher_fip",
    "pitcher_k_per_9",
    "pitcher_bb_per_9",
    "pitcher_whip",
    "pitcher_era",
    "park_runs_factor",
    "park_hr_factor",
    "weather_temp",
    "wind_speed",
    "wind_direction",
    "actual_hr",
    "hit_hr_1plus",
    "feature_source",
    "source",
]


def upsert_mlb_homerun_training_rows(db: DatabaseManager, rows: list[dict]) -> int:
    """Bulk upsert baseball-only HR training rows.

    Rows are keyed by actual MLB game id and hitter MLBAM id. They intentionally
    contain no DraftKings or market-odds fields.
    """
    if not rows:
        return 0

    values = [tuple(row.get(column) for column in MLB_HOMERUN_TRAINING_COLUMNS) for row in rows]
    column_sql = ", ".join(MLB_HOMERUN_TRAINING_COLUMNS)
    update_sql = ",\n            ".join(
        f"{column} = EXCLUDED.{column}"
        for column in MLB_HOMERUN_TRAINING_COLUMNS
        if column not in {"season", "game_date", "game_id", "hitter_mlb_id"}
    )

    return _execute_values_batch(
        db,
        f"""
        INSERT INTO mlb_homerun_training_games ({column_sql})
        VALUES %s
        ON CONFLICT (game_id, hitter_mlb_id) DO UPDATE SET
            {update_sql},
            fetched_at = NOW()
        """,
        values,
    )
