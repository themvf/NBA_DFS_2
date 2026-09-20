/** Read-only unless --refresh is explicitly provided. Uses DATABASE_URL. */
import assert from 'node:assert/strict';
import { eq } from 'drizzle-orm';
import { db } from '../src/db';
import { nflDfsSlateUploads, nflDfsSlatePlayers } from '../src/db/schema';
import { loadSavedNflWorkspace, refreshNflSlateProjections } from '../src/app/dfs/nfl/actions';

async function main() {
  const uploadId = process.argv[2];
  if (!uploadId) throw new Error('Supply a saved upload UUID, optionally --refresh.');
  const original = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId,uploadId));
  const oldRows = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId,uploadId));
  const before = await loadSavedNflWorkspace(uploadId);
  console.log(JSON.stringify({uploadId,players:before.slate.players.length,run:before.slate.projectionRunId,
    refreshAvailable:before.slate.refreshAvailable,refreshMessage:before.slate.refreshMessage}));
  assert.equal(before.slate.refreshMessage,null,'All salary games must match the stored schedule');
  if (!process.argv.includes('--refresh')) return;
  const next = await refreshNflSlateProjections(uploadId);
  assert.equal(next.players.length,before.slate.players.length);
  assert.notEqual(next.projectionRunId,before.slate.projectionRunId,'Expected a newer compatible run');
  assert.notEqual(next.uploadId,uploadId,'Refresh must create a distinct snapshot');
  assert.equal(next.refreshAvailable,false);
  assert.deepEqual(await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId,uploadId)),original);
  assert.deepEqual(await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId,uploadId)),oldRows);
  const freshRows = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId,next.uploadId));
  for (const old of oldRows) {
    const fresh = freshRows.find(r=>r.dkPlayerId===old.dkPlayerId)!;
    for (const key of ['salary','captainSalary','captainDkPlayerId','gameInfo','fantasyprosProj','linestarProj','linestarOwnPct','customProj','comparisonEvidence'] as const)
      assert.deepEqual(fresh[key],old[key],key);
  }
  const repeat = await refreshNflSlateProjections(uploadId);
  assert.equal(repeat.uploadId,next.uploadId,'Repeated refresh should reuse the same salary/run snapshot');
  console.log(JSON.stringify({refreshedUpload:next.uploadId,run:next.projectionRunId,players:next.players.length,
    oldSnapshotUnchanged:true,salariesAndComparisonsPreserved:true,pools:next.redistribution?.pools}));
}
main().catch(error=>{console.error(error instanceof Error ? error.message : 'Refresh verification failed');process.exitCode=1;});
