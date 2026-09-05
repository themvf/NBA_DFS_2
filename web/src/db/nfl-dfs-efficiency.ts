import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { EfficiencyReport } from "@/lib/nfl-dfs/efficiency";

export async function getNflEfficiency(
  workloadRunDigest: string,
): Promise<{ digest: string; report: EfficiencyReport } | null> {
  const rows = await db.execute(sql`
    SELECT run_digest,payload
    FROM nfl_dfs_efficiency_runs
    WHERE workload_run_digest=${workloadRunDigest}
    ORDER BY as_of_at DESC,run_digest DESC
    LIMIT 1
  `);
  const row = rows.rows[0];
  if (!row) return null;
  const report = row.payload as EfficiencyReport;
  if (
    report.version !== "nfl-dfs-efficiency-v3" ||
    !Array.isArray(report.forecasts)
  ) {
    throw new Error("Unsupported NFL efficiency contract");
  }
  return { digest: String(row.run_digest), report };
}
