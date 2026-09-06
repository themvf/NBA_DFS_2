import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { calibratedRelease, type CalibrationSnapshot } from "@/lib/nfl-dfs/calibrated-projection";

/** Read-only: existing daily shadow job owns immutable forecast snapshots. */
export async function getCalibratedSnapshots(season: number, week: number): Promise<CalibrationSnapshot[]> {
  const result = await db.execute(sql`SELECT DISTINCT ON (player_id)
    id::text,player_id,season,week,captured_at,kickoff,payload
    FROM nfl_dfs_shadow_predictions
    WHERE study_run_id=${calibratedRelease.studyId} AND season=${season} AND week=${week}
    ORDER BY player_id,captured_at DESC,id DESC`);
  type Recipe={features:string[];center:number[];scale:number[];coefficients:number[]};
  let recipes:Record<string,{recipe:Recipe}>={};
  try {
    const study=await db.execute(sql`SELECT report FROM nfl_dfs_research_runs WHERE run_id=${calibratedRelease.studyId}`);
    const report=study.rows[0]?.report as {output_digest?:string;candidates?:typeof recipes}|undefined;
    if(report?.output_digest===calibratedRelease.studyDigest)recipes=report.candidates??{};
  }catch {/* Explanations degrade independently; the pinned forecast remains usable. */}
  return result.rows.map(r => {
    const p=r.payload as Record<string,unknown>,recipe=recipes[`${p.position}:opportunity`]?.recipe;
    let explanationTerms:CalibrationSnapshot['explanationTerms'];
    if(recipe&&Array.isArray(recipe.features)&&Array.isArray(recipe.coefficients)&&Array.isArray(recipe.center)&&Array.isArray(recipe.scale)) {
      explanationTerms=[{name:'Model intercept',input:1,center:0,scale:1,coefficient:recipe.coefficients[0],points:recipe.coefficients[0]},...recipe.features.map((name,i)=>({name,input:Number(p[name]),center:recipe.center[i],scale:recipe.scale[i],coefficient:recipe.coefficients[i+1],points:(Number(p[name])-recipe.center[i])/recipe.scale[i]*recipe.coefficients[i+1]}))];
    }
    return { id: String(r.id), playerId: Number(r.player_id), season: Number(r.season), week: Number(r.week), capturedAt: new Date(r.captured_at as string).toISOString(), kickoff: new Date(r.kickoff as string).toISOString(), payload:r.payload,explanationTerms };
  });
}
