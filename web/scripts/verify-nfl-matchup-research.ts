import assert from "node:assert/strict";
import { sql } from "drizzle-orm";
import { db } from "../src/db";
import { getNflMatchupResearch } from "../src/db/nfl-matchup-research";

async function main() {
  const report=await getNflMatchupResearch(process.argv[2]);
  assert.equal(report.status,"available");
  if(report.status!=="available") throw new Error(report.reason);
  assert.ok(report.players.length>0);
  assert.equal(report.productionChanged,false);
  if(report.coherent) assert.equal(report.coherent.manifest.sources.comparison_digest,report.comparisonDigest);
  if(report.portfolios) assert.equal(report.portfolios.comparisonDigest,report.comparisonBytesDigest);
  if(report.archived) assert.equal(report.archived.forecast_inputs_allowed,false);
  const exact=await getNflMatchupResearch(report.uploadId);
  assert.equal(exact.status,"available");
  assert.equal(exact.reportId,report.reportId);
  assert.equal((await getNflMatchupResearch("not-a-uuid")).status,"unavailable");
  assert.equal((await getNflMatchupResearch("00000000-0000-0000-0000-000000000000")).status,"unavailable");
  const count=await db.execute(sql`SELECT COUNT(*)::int n FROM nfl_matchup_research_reports WHERE report_id=${report.reportId}`);
  assert.equal(count.rows[0].n,1);
  console.log(JSON.stringify({reportId:report.reportId,uploadId:report.uploadId,players:report.players.length,
    coherent:!!report.coherent,portfolios:!!report.portfolios,archivedContests:report.archived?.contests.length??0,localFilesRequired:false}));
}
main().catch(e=>{console.error(e);process.exitCode=1;});
