import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { calibratedRelease, type CalibrationSnapshot } from "@/lib/nfl-dfs/calibrated-projection";
import { calibratedSnapshotsQuery } from "@/lib/nfl-dfs/source-queries";

/**
 * Read-only: existing daily shadow job owns immutable forecast snapshots.
 *
 * The newest capture per player AT OR BEFORE `asOf` (a saved slate's projection
 * cutoff). Without the bound a later freeze replaced the slate's candidate with
 * one captured after its decision time, which the reader then refused, so every
 * saved slate lost its candidates at the next daily freeze.
 */
type Recipe={features:string[];center:number[];scale:number[];coefficients:number[]};
type Recipes=Record<string,{recipe:Recipe}>;
// A research run's report is immutable and ~570 KB; the page read fetched it
// on every open just for the explanation recipes. Cached per server instance,
// keyed by the pinned study and digest; a failed read is not cached.
const recipesByStudy=new Map<string,Promise<Recipes>>();
function studyRecipes(studyId:string,digest:string):Promise<Recipes> {
  const key=`${studyId}:${digest}`;
  let pending=recipesByStudy.get(key);
  if(!pending) {
    pending=db.execute(sql`SELECT report FROM nfl_dfs_research_runs WHERE run_id=${studyId}`).then(study=>{
      const report=study.rows[0]?.report as {output_digest?:string;candidates?:Recipes}|undefined;
      return report?.output_digest===digest?report.candidates??{}:{};
    });
    pending.catch(()=>recipesByStudy.delete(key));
    recipesByStudy.set(key,pending);
  }
  return pending;
}

export async function getCalibratedSnapshots(season: number, week: number, asOf: Date): Promise<CalibrationSnapshot[]> {
  const [result,recipes]=await Promise.all([
    db.execute(calibratedSnapshotsQuery(calibratedRelease.studyId, season, week, asOf)),
    studyRecipes(calibratedRelease.studyId,calibratedRelease.studyDigest)
      .catch(():Recipes=>({})), /* Explanations degrade independently; the pinned forecast remains usable. */
  ]);
  return result.rows.map(r => {
    const p=r.payload as Record<string,unknown>,recipe=recipes[`${p.position}:opportunity`]?.recipe;
    let explanationTerms:CalibrationSnapshot['explanationTerms'];
    if(recipe&&Array.isArray(recipe.features)&&Array.isArray(recipe.coefficients)&&Array.isArray(recipe.center)&&Array.isArray(recipe.scale)) {
      explanationTerms=[{name:'Model intercept',input:1,center:0,scale:1,coefficient:recipe.coefficients[0],points:recipe.coefficients[0]},...recipe.features.map((name,i)=>({name,input:Number(p[name]),center:recipe.center[i],scale:recipe.scale[i],coefficient:recipe.coefficients[i+1],points:(Number(p[name])-recipe.center[i])/recipe.scale[i]*recipe.coefficients[i+1]}))];
    }
    return { id: String(r.id), playerId: Number(r.player_id), season: Number(r.season), week: Number(r.week), capturedAt: new Date(r.captured_at as string).toISOString(), kickoff: new Date(r.kickoff as string).toISOString(), payload:r.payload,explanationTerms };
  });
}
