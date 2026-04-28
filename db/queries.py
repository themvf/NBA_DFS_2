"""Database query helpers for NBA DFS v2."""

from __future__ import annotations

from db.database import DatabaseManager


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
            capture_key, captured_at
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
) -> int:
    row = db.execute_one(
        """
        INSERT INTO mlb_matchups (
            game_date, game_id, home_team_id, away_team_id,
            home_sp_id, home_sp_name, away_sp_id, away_sp_name,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
            game_id        = COALESCE(EXCLUDED.game_id, mlb_matchups.game_id),
            home_sp_id     = COALESCE(EXCLUDED.home_sp_id, mlb_matchups.home_sp_id),
            home_sp_name   = COALESCE(EXCLUDED.home_sp_name, mlb_matchups.home_sp_name),
            away_sp_id     = COALESCE(EXCLUDED.away_sp_id, mlb_matchups.away_sp_id),
            away_sp_name   = COALESCE(EXCLUDED.away_sp_name, mlb_matchups.away_sp_name),
            vegas_total    = COALESCE(EXCLUDED.vegas_total, mlb_matchups.vegas_total),
            home_ml        = COALESCE(EXCLUDED.home_ml, mlb_matchups.home_ml),
            away_ml        = COALESCE(EXCLUDED.away_ml, mlb_matchups.away_ml),
            vegas_prob_home = COALESCE(EXCLUDED.vegas_prob_home, mlb_matchups.vegas_prob_home),
            home_implied   = COALESCE(EXCLUDED.home_implied, mlb_matchups.home_implied),
            away_implied   = COALESCE(EXCLUDED.away_implied, mlb_matchups.away_implied),
            ballpark       = COALESCE(EXCLUDED.ballpark, mlb_matchups.ballpark),
            fetched_at     = NOW()
        RETURNING id
        """,
        (
            game_date, game_id, home_team_id, away_team_id,
            home_sp_id, home_sp_name, away_sp_id, away_sp_name,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark,
        ),
    )
    return row["id"] if row else 0


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
