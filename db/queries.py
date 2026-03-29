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
) -> int:
    row = db.execute_one(
        """
        INSERT INTO dk_slates (slate_date, game_count, dk_draft_group_id, contest_type, contest_format)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (slate_date, contest_type, contest_format) DO UPDATE SET
            game_count = EXCLUDED.game_count,
            dk_draft_group_id = COALESCE(EXCLUDED.dk_draft_group_id, dk_slates.dk_draft_group_id)
        RETURNING id
        """,
        (slate_date, game_count, dk_draft_group_id, contest_type, contest_format),
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
