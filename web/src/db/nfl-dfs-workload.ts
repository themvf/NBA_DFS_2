import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { WorkloadReport } from "@/lib/nfl-dfs/workload";
export async function getNflWorkload(): Promise<{
  digest: string;
  report: WorkloadReport;
} | null> {
  const rows = await db.execute(
    sql`SELECT run_digest,payload FROM nfl_dfs_workload_runs ORDER BY as_of_at DESC,run_digest DESC LIMIT 1`,
  );
  const row = rows.rows[0];
  if (!row) return null;
  const report = row.payload as WorkloadReport;
  if (
    report.version !== "nfl-dfs-workload-v1" ||
    !Array.isArray(report.forecasts)
  )
    throw new Error("Unsupported NFL workload contract");
  return { digest: String(row.run_digest), report };
}
