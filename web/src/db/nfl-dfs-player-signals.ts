import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { NflPlayerSignalEvidence } from "@/lib/nfl-dfs/player-signals";
import type { NflAirDefenseEvidence } from "@/lib/nfl-dfs/player-signals";
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
