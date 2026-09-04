import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { FeatureAudit } from "@/lib/nfl-dfs/feature-audit";

export async function getNflFeatureAudit(digest?: string): Promise<{ digest: string; report: FeatureAudit } | null> {
  // No source payload/evidence, model computation, or DDL in the web request.
  const result = digest
    ? await db.execute(sql`SELECT audit_digest,payload FROM nfl_dfs_feature_audits WHERE audit_digest=${digest} LIMIT 1`)
    : await db.execute(sql`SELECT audit_digest,payload FROM nfl_dfs_feature_audits ORDER BY created_at DESC,audit_digest DESC LIMIT 1`);
  const row = result.rows[0];
  if (!row) return null;
  const report = row.payload as FeatureAudit;
  if (report.version !== "nfl-dfs-feature-audit-v1" || !Array.isArray(report.datasets) || !Array.isArray(report.fields)) {
    throw new Error("Unsupported NFL feature audit contract");
  }
  return { digest: String(row.audit_digest), report };
}
