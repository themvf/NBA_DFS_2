import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { getPickemEvidence } from "../src/db/pickem-evidence";
import { getNflPickemSlate } from "../src/db/queries";
import { compareContestCards, EMPTY_POOL_CONFIG } from "../src/lib/nfl/pickem-contest";
import { DEFAULT_FIELD } from "../src/lib/nfl/pickem-policy";
async function main() {
const evidence = await getPickemEvidence(2026);
const slate = await getNflPickemSlate(2026, evidence);
const upcoming = slate.games.filter(g=>g.week===3 && Date.parse(g.kickoff!)>Date.now());
const summaries = upcoming.map(g=> {
  const e=evidence.games[g.gameId], f=e?.matchup;
  if (f?.candidate) {
    assert.equal(f.candidate.tie,f.baseline!.tie);
    assert.ok(Math.abs(f.candidate.home+f.candidate.away+f.candidate.tie-1)<1e-10);
    if (f.status!=='qualified') assert.equal(g.provenance,e.latest?.pHome!=null?'market_ml_novig':g.provenance);
  }
  const sourceIds=e?.pfr?.flatMap(t=>t.games.filter(p=>p.capturedAt).map(p=>{
    assert.ok(p.manifest.snapshotId); assert.ok(p.manifest.recordedAt); return p.manifest.snapshotId;
  })) ?? [];
  return {gameId:g.gameId,matchup:`${g.awayAbbrev}@${g.homeAbbrev}`,activeHomeConditional:g.pHome,
    status:f?.status??'unavailable',baseline:f?.baseline,candidate:f?.candidate,
    reasons:f?.reasons??[],pfrSnapshotIds:[...new Set(sourceIds)]};
});
const fallback=compareContestCards(upcoming.map(g=>({...g,fieldHomePct:null})),[],EMPTY_POOL_CONFIG,DEFAULT_FIELD);
assert.equal(fallback.candidates.length,0);assert.equal(fallback.baseline.weeklyPayout,null);
await writeFile('../artifacts/nfl-matchup-implementation/2026-09-27/pickem-ui-verification.json',JSON.stringify({capturedAt:evidence.loadedAt,
  games:summaries,missingPoolInputsFallback:fallback,warnings:evidence.warnings},null,2));
console.log(JSON.stringify({upcoming:upcoming.length,forecasts:summaries.filter(g=>g.candidate).length,
  sourceManifests:summaries.filter(g=>g.pfrSnapshotIds.length).length,unqualifiedActiveChanges:0,warnings:evidence.warnings}));
}
main().catch(e=>{console.error(e);process.exitCode=1;});
