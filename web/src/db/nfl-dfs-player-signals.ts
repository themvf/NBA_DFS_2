import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { NflPlayerSignalEvidence } from "@/lib/nfl-dfs/player-signals";
import type { NflAirDefenseEvidence } from "@/lib/nfl-dfs/player-signals";
import type { NflAirTeamEvidence, NflAirMarketEvidence } from "@/lib/nfl-dfs/air-matchup-evidence";
import { canonicalNflTeam } from "@/lib/nfl-dfs/dk-salary-csv";

/** Participant GSIS ids are the player join; game/play ids are the play join. */
export async function readNflDfsPlayerSignalEvidence(
  season: number, week: number, asOf: Date,
): Promise<Map<string, NflPlayerSignalEvidence>> {
  if (!Number.isInteger(season) || !Number.isInteger(week) || week < 1 || !Number.isFinite(asOf.getTime())) {
    return new Map();
  }
  const result = await db.execute(sql`
    WITH role_plays AS (
      SELECT DISTINCT pp.player_id, pp.role, p.game_id, p.play_id,
        p.play_type, p.yardline_100, p.air_yards, p.yards_after_catch,
        p.xyac_mean_yardage, p.qb_dropback
      FROM nfl_pbp_play_participants pp
      JOIN nfl_pbp_archetypes p ON p.game_id = pp.game_id AND p.play_id = pp.play_id
      WHERE p.season = ${season} AND p.week >= ${Math.max(1, week - 3)}
        AND p.week < ${week} AND p.season_type = 'REG'
        AND p.labelled_at <= ${asOf} AND pp.player_id IS NOT NULL
        AND pp.role IN ('receiver', 'rusher')
    )
    SELECT player_id AS "playerId", COUNT(DISTINCT game_id)::int AS games,
      COUNT(*) FILTER (WHERE role = 'receiver' AND play_type = 'pass')::int AS targets,
      COUNT(*) FILTER (WHERE role = 'receiver' AND play_type = 'pass' AND yards_after_catch IS NOT NULL)::int AS catches,
      COALESCE(SUM(air_yards) FILTER (WHERE role = 'receiver' AND play_type = 'pass'), 0)::float8 AS "targetAirYards",
      COALESCE(SUM(air_yards) FILTER (WHERE role = 'receiver' AND play_type = 'pass' AND yards_after_catch IS NOT NULL), 0)::float8 AS "caughtAirYards",
      COUNT(*) FILTER (WHERE role = 'receiver' AND play_type = 'pass' AND air_yards >= 20)::int AS "deepTargets",
      COALESCE(SUM(yards_after_catch) FILTER (WHERE role = 'receiver' AND play_type = 'pass'), 0)::float8 AS "yardsAfterCatch",
      COALESCE(SUM(xyac_mean_yardage) FILTER (WHERE role = 'receiver' AND play_type = 'pass' AND yards_after_catch IS NOT NULL), 0)::float8 AS "expectedYac",
      COUNT(*) FILTER (WHERE role = 'rusher' AND play_type = 'run' AND qb_dropback IS NOT TRUE)::int AS carries,
      COUNT(*) FILTER (WHERE role = 'rusher' AND play_type = 'run' AND qb_dropback IS NOT TRUE AND yardline_100 <= 5)::int AS "carriesInsideFive",
      COUNT(*) FILTER (WHERE role = 'receiver' AND play_type = 'pass' AND yardline_100 <= 10)::int AS "targetsInsideTen"
    FROM role_plays GROUP BY player_id`);
  return new Map(result.rows.map(raw => {
    const row = raw as Record<string, unknown>;
    const metric = (key: string) => Number(row[key] ?? 0);
    return [String(row.playerId), {
      games: metric("games"), targets: metric("targets"), catches: metric("catches"),
      targetAirYards: metric("targetAirYards"), caughtAirYards: metric("caughtAirYards"),
      deepTargets: metric("deepTargets"), yardsAfterCatch: metric("yardsAfterCatch"),
      expectedYac: metric("expectedYac"), carries: metric("carries"),
      carriesInsideFive: metric("carriesInsideFive"), targetsInsideTen: metric("targetsInsideTen"),
    } satisfies NflPlayerSignalEvidence] as const;
  }));
}

/** Unique pass plays only: target air yards include incompletions. */
export async function readNflAirDefenseEvidence(
  season: number, week: number, asOf: Date,
): Promise<Map<string, NflAirDefenseEvidence>> {
  if (!Number.isInteger(season) || !Number.isInteger(week) || week < 1 || !Number.isFinite(asOf.getTime())) return new Map();
  const result = await db.execute(sql`
    SELECT p.defteam AS team, COUNT(DISTINCT p.game_id)::int AS games,
      COUNT(*)::int AS targets, SUM(p.air_yards)::float8 AS "targetAirYards"
    FROM nfl_pbp_archetypes p
    WHERE p.season = ${season} AND p.week >= ${Math.max(1, week - 3)} AND p.week < ${week}
      AND p.season_type = 'REG' AND p.labelled_at <= ${asOf}
      AND p.play_type = 'pass' AND p.air_yards IS NOT NULL AND p.defteam IS NOT NULL
    GROUP BY p.defteam`);
  return new Map(result.rows.map(raw => {
    const row = raw as Record<string, unknown>;
    return [canonicalNflTeam(String(row.team)), {
      games: Number(row.games), targets: Number(row.targets), targetAirYards: Number(row.targetAirYards),
    }] as const;
  }));
}

export async function readNflAirTeamEvidence(season: number, week: number, asOf: Date): Promise<Map<string, NflAirTeamEvidence>> {
  if (!Number.isInteger(season) || !Number.isInteger(week) || week < 1 || !Number.isFinite(asOf.getTime())) return new Map();
  const result = await db.execute(sql`
    WITH targets AS (
      SELECT DISTINCT p.posteam, p.game_id, p.play_id, p.air_yards
      FROM nfl_pbp_archetypes p
      JOIN nfl_pbp_play_participants pp ON pp.game_id = p.game_id AND pp.play_id = p.play_id
      WHERE p.season = ${season} AND p.week >= ${Math.max(1, week - 3)} AND p.week < ${week}
        AND p.season_type = 'REG' AND p.labelled_at <= ${asOf}
        AND p.play_type = 'pass' AND p.posteam IS NOT NULL AND pp.role = 'receiver'
    )
    SELECT posteam AS team, COUNT(DISTINCT game_id)::int AS games,
      COUNT(*)::int AS targets, COALESCE(SUM(air_yards), 0)::float8 AS "targetAirYards"
    FROM targets GROUP BY posteam`);
  return new Map(result.rows.map(raw => {
    const row = raw as Record<string, unknown>;
    return [canonicalNflTeam(String(row.team)), { games: Number(row.games), targets: Number(row.targets), targetAirYards: Number(row.targetAirYards) }] as const;
  }));
}

export async function readNflAirMarketEvidence(season: number, week: number, asOf: Date): Promise<Map<string, NflAirMarketEvidence>> {
  if (!Number.isInteger(season) || !Number.isInteger(week) || week < 1 || !Number.isFinite(asOf.getTime())) return new Map();
  const result = await db.execute(sql`
    SELECT g.nflverse_game_id AS "gameId", g.kickoff,
      home.abbreviation AS "homeTeam", away.abbreviation AS "awayTeam",
      quote.id AS "quoteId", quote.captured_at AS "capturedAt",
      quote.home_spread AS spread, quote.vegas_total AS total,
      quote.home_ml AS "homeMl", quote.away_ml AS "awayMl"
    FROM nfl_season_games g
    JOIN nfl_teams home ON home.team_id = g.home_team_id
    JOIN nfl_teams away ON away.team_id = g.away_team_id
    LEFT JOIN LATERAL (
      SELECT o.id, o.captured_at, o.home_spread, o.vegas_total, o.home_ml, o.away_ml
      FROM game_odds_history o
      WHERE o.sport = 'nfl' AND o.matchup_id = g.matchup_id
        AND o.captured_at <= ${asOf} AND o.captured_at < g.kickoff
      ORDER BY o.captured_at DESC, o.id DESC LIMIT 1
    ) quote ON TRUE
    WHERE g.season = ${season} AND g.week = ${week} AND g.game_type = 'REG'`);
  const out = new Map<string, NflAirMarketEvidence>();
  for (const raw of result.rows) {
    const row = raw as Record<string, unknown>;
    const home = canonicalNflTeam(String(row.homeTeam)), away = canonicalNflTeam(String(row.awayTeam));
    const number = (value: unknown) => value == null ? null : Number(value);
    const iso = (value: unknown) => value == null ? null : new Date(String(value)).toISOString();
    const base = { gameId: String(row.gameId), kickoff: iso(row.kickoff), quoteId: number(row.quoteId), capturedAt: iso(row.capturedAt), total: number(row.total) };
    out.set(home, { ...base, opponent: away, spread: number(row.spread), moneyline: number(row.homeMl) });
    out.set(away, { ...base, opponent: home, spread: number(row.spread) == null ? null : -Number(row.spread), moneyline: number(row.awayMl) });
  }
  return out;
}
