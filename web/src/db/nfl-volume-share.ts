import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { WorkloadReport } from "@/lib/nfl-dfs/workload-projection";
import { volumeShareLaterRunQuery, volumeShareRunQuery } from "@/lib/nfl-dfs/source-queries";

export type VolumeShareLoad = { report: WorkloadReport | null; runDigest: string | null; reason: string | null };

const utc = (value: unknown) => new Date(String(value)).toISOString().slice(0, 16).replace("T", " ") + " UTC";

async function tableExists(): Promise<boolean> {
  const result = await db.execute(sql`SELECT to_regclass('public.nfl_dfs_volume_share_runs') IS NOT NULL AS ok`);
  return result.rows[0]?.ok === true;
}

/**
 * The WR volume-share run a saved slate may use: the newest one for its week
 * captured at or before its projection cutoff. Written by the research
 * workflow (ingest/nfl_dfs_target_share.py) after each production refresh, so a
 * run normally lands AFTER the projection run it follows and applies to slates
 * on the next projection run. The reason says which case applies.
 */
export async function getVolumeShareReport(season: number, week: number, asOf: Date): Promise<VolumeShareLoad> {
  if (!(await tableExists())) return { report: null, runDigest: null,
    reason: "No WR volume-share run has been recorded yet. The research workflow writes one after each projection refresh." };
  const rows = await db.execute(volumeShareRunQuery(season, week, asOf));
  const row = rows.rows[0];
  if (row) return { report: row.payload as WorkloadReport, runDigest: String(row.run_digest), reason: null };
  const later = (await db.execute(volumeShareLaterRunQuery(season, week, asOf))).rows[0]?.first_after;
  return { report: null, runDigest: null, reason: later
    ? `The first WR volume-share run for ${season} week ${week} was captured ${utc(later)}, after this slate's projection cutoff (${utc(asOf)}). It applies once the slate moves to a newer projection run.`
    : `No WR volume-share run exists for ${season} week ${week}. The research workflow writes one after each projection refresh.` };
}

/** Newest run of any week, for the model page. */
export async function getLatestVolumeShareReport(): Promise<{ report: WorkloadReport; runDigest: string } | null> {
  if (!(await tableExists())) return null;
  const rows = await db.execute(sql`SELECT run_digest,payload FROM nfl_dfs_volume_share_runs ORDER BY as_of_at DESC,run_digest DESC LIMIT 1`);
  const row = rows.rows[0];
  return row ? { report: row.payload as WorkloadReport, runDigest: String(row.run_digest) } : null;
}
