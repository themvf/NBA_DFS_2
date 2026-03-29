"""Database query helpers for NBA DFS v2."""

from __future__ import annotations

from db.database import DatabaseManager


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


def upsert_nba_matchup(
    db: DatabaseManager,
    game_date: str,
    game_id: str,
    home_team_id: int | None,
    away_team_id: int | None,
    vegas_total: float | None = None,
    home_ml: int | None = None,
    away_ml: int | None = None,
    vegas_prob_home: float | None = None,
) -> int:
    row = db.execute_one(
        """
        INSERT INTO nba_matchups (
            game_date, game_id, home_team_id, away_team_id,
            vegas_total, home_ml, away_ml, vegas_prob_home
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
            game_id = COALESCE(EXCLUDED.game_id, nba_matchups.game_id),
            vegas_total = EXCLUDED.vegas_total,
            home_ml = EXCLUDED.home_ml,
            away_ml = EXCLUDED.away_ml,
            vegas_prob_home = EXCLUDED.vegas_prob_home,
            fetched_at = NOW()
        RETURNING id
        """,
        (game_date, game_id, home_team_id, away_team_id,
         vegas_total, home_ml, away_ml, vegas_prob_home),
    )
    return row["id"] if row else 0


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
            slate_id, dk_player_id, name, team_abbrev, team_id, matchup_id,
            eligible_positions, salary, game_info, avg_fpts_dk,
            linestar_proj, proj_own_pct, our_proj, our_leverage,
            proj_floor, proj_ceiling, boom_rate, is_out
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (slate_id, dk_player_id) DO UPDATE SET
            name = EXCLUDED.name,
            team_abbrev = EXCLUDED.team_abbrev,
            team_id = EXCLUDED.team_id,
            matchup_id = EXCLUDED.matchup_id,
            eligible_positions = EXCLUDED.eligible_positions,
            salary = EXCLUDED.salary,
            game_info = EXCLUDED.game_info,
            avg_fpts_dk = EXCLUDED.avg_fpts_dk,
            linestar_proj = EXCLUDED.linestar_proj,
            proj_own_pct = EXCLUDED.proj_own_pct,
            our_proj = EXCLUDED.our_proj,
            our_leverage = EXCLUDED.our_leverage,
            proj_floor = EXCLUDED.proj_floor,
            proj_ceiling = EXCLUDED.proj_ceiling,
            boom_rate = EXCLUDED.boom_rate,
            is_out = EXCLUDED.is_out
        """,
        (
            slate_id,
            player["dk_player_id"],
            player["name"],
            player["team_abbrev"],
            player.get("team_id"),
            player.get("matchup_id"),
            player["eligible_positions"],
            player["salary"],
            player.get("game_info"),
            player.get("avg_fpts_dk"),
            player.get("linestar_proj"),
            player.get("proj_own_pct"),
            player.get("our_proj"),
            player.get("our_leverage"),
            player.get("proj_floor"),
            player.get("proj_ceiling"),
            player.get("boom_rate"),
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
            wrc_plus      = EXCLUDED.wrc_plus,
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
            wrc_plus_vs_l = EXCLUDED.wrc_plus_vs_l,
            wrc_plus_vs_r = EXCLUDED.wrc_plus_vs_r,
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
    away_sp_id: int | None = None,
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
            home_sp_id, away_sp_id,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
            game_id        = COALESCE(EXCLUDED.game_id, mlb_matchups.game_id),
            home_sp_id     = COALESCE(EXCLUDED.home_sp_id, mlb_matchups.home_sp_id),
            away_sp_id     = COALESCE(EXCLUDED.away_sp_id, mlb_matchups.away_sp_id),
            vegas_total    = EXCLUDED.vegas_total,
            home_ml        = EXCLUDED.home_ml,
            away_ml        = EXCLUDED.away_ml,
            vegas_prob_home = EXCLUDED.vegas_prob_home,
            home_implied   = EXCLUDED.home_implied,
            away_implied   = EXCLUDED.away_implied,
            ballpark       = COALESCE(EXCLUDED.ballpark, mlb_matchups.ballpark),
            fetched_at     = NOW()
        RETURNING id
        """,
        (
            game_date, game_id, home_team_id, away_team_id,
            home_sp_id, away_sp_id,
            vegas_total, home_ml, away_ml, vegas_prob_home,
            home_implied, away_implied, ballpark,
        ),
    )
    return row["id"] if row else 0
