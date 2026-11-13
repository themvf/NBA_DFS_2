import {isMissingRelation} from "@/lib/database-errors";
import "server-only";
import {sql} from "drizzle-orm";
import {db} from "@/db";
import {matchAvailability,type AvailabilityEvidence,type InjuryState,type Identity} from "@/lib/nba-availability";
const iso=(v:unknown)=>v instanceof Date?v.toISOString():String(v);
export async function getNbaAvailability(date:string):Promise<AvailabilityEvidence|null>{
 if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return null;
 try{
  const [reports,snaps]=await Promise.all([
   db.execute(sql`SELECT report_sha256,published_at,observed_at,report->>'source_url' AS source_url,states FROM nba_injury_observations
    WHERE observed_at<=clock_timestamp() AND published_at<=observed_at
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date'=${date})
    ORDER BY published_at DESC,observed_at DESC LIMIT 1`),
   db.execute(sql`SELECT snapshot->'candidates' AS candidates FROM nba_ghost_snapshots WHERE game_date=${date}::date AND version='nba-points-ghost-v2' AND mode='prospective' ORDER BY created_at LIMIT 1`)
  ]);
  const r=reports.rows[0];if(!r)return null;
  return {sha:String(r.report_sha256),publishedAt:iso(r.published_at),observedAt:iso(r.observed_at),sourceUrl:String(r.source_url),states:matchAvailability(date,(snaps.rows[0]?.candidates??[]) as Identity[],r.states as InjuryState[])};
 }catch(e){if(isMissingRelation(e))return null;throw e;}
}
/**
 * The newest official report observed for a date, whole: every player row and each team's
 * filing state. The contest builder matches it to tonight's quoted players itself
 * (lib/nba-contest-injury.ts), so it does not depend on a ghost snapshot existing.
 */
export async function getNbaInjuryReport(date:string):Promise<import("@/lib/nba-contest-injury").InjuryReport|null>{
 if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return null;
 try{
  const r=(await db.execute(sql`SELECT report_sha256,published_at,observed_at,report->>'source_url' AS source_url,states,report->'team_submissions' AS submissions
    FROM nba_injury_observations
    WHERE observed_at<=clock_timestamp() AND published_at<=observed_at
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(report->'team_submissions') t WHERE t->>'game_date'=${date})
    ORDER BY published_at DESC,observed_at DESC LIMIT 1`)).rows[0];
  if(!r)return null;
  const list=<T,>(v:unknown):T[]=>(typeof v==="string"?JSON.parse(v):v??[]) as T[];
  return {sha:String(r.report_sha256),publishedAt:iso(r.published_at),observedAt:iso(r.observed_at),sourceUrl:String(r.source_url),
   states:list(r.states),submissions:list(r.submissions)};
 }catch(e){if(isMissingRelation(e))return null;throw e;}
}
