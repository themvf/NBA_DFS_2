/**
 * SQL for the experimental projection sources, kept free of the database client
 * so tests can render it. Both read the newest capture AT OR BEFORE a saved
 * slate's projection cutoff (`asOf`): a capture from after that decision time
 * must never replace the one the slate was built against.
 */
import { sql } from 'drizzle-orm';

export const calibratedSnapshotsQuery = (studyId: string, season: number, week: number, asOf: Date) => sql`SELECT DISTINCT ON (player_id)
    id::text,player_id,season,week,captured_at,kickoff,payload
    FROM nfl_dfs_shadow_predictions
    WHERE study_run_id=${studyId} AND season=${season} AND week=${week}
      AND captured_at <= ${asOf.toISOString()}
    ORDER BY player_id,captured_at DESC,id DESC`;

export const volumeShareRunQuery = (season: number, week: number, asOf: Date) => sql`SELECT run_digest,as_of_at,payload
    FROM nfl_dfs_volume_share_runs
    WHERE season=${season} AND week=${week} AND as_of_at <= ${asOf.toISOString()}
    ORDER BY as_of_at DESC,run_digest DESC LIMIT 1`;

/** The first run for the week captured after the cutoff, to say why none applies yet. */
export const volumeShareLaterRunQuery = (season: number, week: number, asOf: Date) => sql`SELECT min(as_of_at) AS first_after
    FROM nfl_dfs_volume_share_runs
    WHERE season=${season} AND week=${week} AND as_of_at > ${asOf.toISOString()}`;
