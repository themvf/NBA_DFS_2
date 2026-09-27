import "server-only";
import { sql } from "drizzle-orm";
import { db } from ".";

/** Hosted, append-only report reader. No dependency on worker-local artifacts. */
export async function getNflMatchupResearch(uploadId?: string) {
  if(uploadId && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(uploadId))
    return {status:"unavailable" as const,reason:"The requested salary upload ID is invalid."};
  try {
    const rows=await db.execute(sql`SELECT report_id,upload_id::text,baseline_run_id::text,comparison_digest,published_at,payload
      FROM nfl_matchup_research_reports WHERE (${uploadId ?? null}::uuid IS NULL OR upload_id=${uploadId ?? null}::uuid)
      ORDER BY captured_at DESC,published_at DESC,report_id DESC LIMIT 1`);
    const row=rows.rows[0];
    const p=row?.payload as any;
    if(!p || p.version!=="nfl-matchup-published-report-v1" || p.productionChanged!==false ||
        p.reportId!==row.report_id || p.uploadId!==row.upload_id || p.baselineRunId!==row.baseline_run_id || p.comparisonDigest!==row.comparison_digest)
      return {status:"unavailable" as const,reason:"No compatible published report is available for this salary upload."};
    return {...p,status:"available" as const,publishedAt:String(row.published_at),
      players:p.players as Array<{name:string;position:string;team:string;salary:number;baseline:number|null;candidate:number|null;delta:number;status:string;reason:string;ledger:any[]}>,
      missing:p.missing as string[]};
  } catch {
    return {status:"unavailable" as const,reason:"The published research report is unavailable. The regular saved forecasts remain available in the NFL workspace."};
  }
}
