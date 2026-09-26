import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {basename,resolve} from 'node:path';

async function main(){
  const input=resolve(process.argv[2]??'../DKSalaries.csv');
  const bytes=await readFile(input);
  const form=new FormData();
  form.set('file',new File([bytes],basename(input),{type:'text/csv'}));
  const {loadNflSalaryCsv,explainNflPlayerProjection}=await import('../src/app/dfs/nfl/actions');
  const slate=await loadNflSalaryCsv(form);
  assert(slate.projectionRunId,'salary slate must pin a projection run');
  assert.match(slate.platformEligibilityManifestDigest??'',/^[0-9a-f]{64}$/);
  assert.equal(slate.availabilityResolution?.legacyPlayers,0,'new projection run may not fall back to mutable availability');
  assert((slate.availabilityResolution?.pinnedPlayers??0)>0);
  assert(slate.availabilityHealth?.state_counts,'run-level availability health must be persisted');
  for(const player of slate.players){
    assert.equal(player.platformEligibility?.manifestDigest,slate.platformEligibilityManifestDigest);
    assert.equal(player.availabilityEvidence?.platformManifestDigest,slate.platformEligibilityManifestDigest);
    if(player.ffPlayerId!=null){
      assert.equal(player.availability?.pinned,true,`${player.name} must present the projection run's pinned decision`);
      assert.equal(player.availabilityEvidence?.gameDecisionId,player.availability?.decisionId);
    }
  }
  const matched=slate.players.find(player=>player.ffPlayerId!=null);
  assert(matched,'fixture needs at least one model-matched player');
  const drawer=await explainNflPlayerProjection(slate.uploadId,matched.id);
  assert.equal(drawer.ok,true);
  if(drawer.ok){
    assert.equal(drawer.availabilityDecisionId,matched.availabilityEvidence?.gameDecisionId);
    assert.equal(drawer.platformEligibilityManifestDigest,slate.platformEligibilityManifestDigest);
    assert.deepEqual(drawer.availabilityEvidence,matched.availabilityEvidence);
  }
  console.log(JSON.stringify({uploadId:slate.uploadId,projectionRunId:slate.projectionRunId,
    players:slate.players.length,matchedPlayers:slate.players.filter(p=>p.ffPlayerId!=null).length,
    gameDecisionId:matched.availabilityEvidence?.gameDecisionId,
    platformEligibilityManifestDigest:slate.platformEligibilityManifestDigest,
    availabilityHealth:slate.availabilityHealth},null,2));
}

main().catch(error=>{console.error(error);process.exitCode=1;});
