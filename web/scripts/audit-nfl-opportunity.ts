/** Read-only saved-slate evidence. No credentials are written to the artifact. */
import { writeFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import { desc, eq } from 'drizzle-orm';
import { db } from '../src/db';
import { nflDfsSlateUploads, nflDfsSlatePlayers, nflDfsProjectionRuns, nflDfsPlayerProjections } from '../src/db/schema';
import { getNflRosterEvidence } from '../src/db/nfl-dfs-availability';
import { getNflWorkload } from '../src/db/nfl-dfs-workload';
import { loadSavedNflWorkspace, explainNflPlayerProjection } from '../src/app/dfs/nfl/actions';
import { optimizeNflLineups } from '../src/app/dfs/nfl/nfl-optimizer';

async function main() {
  const uploads = await db.select().from(nflDfsSlateUploads).orderBy(desc(nflDfsSlateUploads.createdAt)).limit(12);
  if (!process.argv[2]) { console.log(JSON.stringify(uploads, null, 2)); return; }
  const upload = uploads.find(u => u.uploadId === process.argv[2]) ??
    (await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, process.argv[2])))[0];
  if (!upload?.projectionRunId) throw new Error('Missing upload/run');
  const runs = await db.select().from(nflDfsProjectionRuns).where(eq(nflDfsProjectionRuns.runId, upload.projectionRunId));
  const [rows, projections, roster, workspace] = await Promise.all([
    db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, upload.uploadId)),
    db.select().from(nflDfsPlayerProjections).where(eq(nflDfsPlayerProjections.runId, upload.projectionRunId)),
    getNflRosterEvidence(runs[0].season, runs[0].week), loadSavedNflWorkspace(upload.uploadId),
  ]);
  const team = rows.find(r => /Aaron Jones/i.test(r.name))?.team;
  if (!team) throw new Error('Jones missing');
  const selected = rows.filter(r => r.team === team);
  const research = await getNflWorkload();
  const researchTeam = research?.report.forecasts.find(f => f.team === team);
  const researchShares = researchTeam ? { digest: research!.digest, version: research!.report.version,
    scenario: 'research_only_not_historical_optimizer', forecast: researchTeam,
    reconciliation: Object.entries(researchTeam.budgets).map(([unit,budget]) => {
      const assigned = researchTeam.players.reduce((sum,p) => sum + (p.components[unit]?.mean ?? 0), 0);
      const reserve = budget ? budget.mean * (budget.unallocated_share ?? 1) : null;
      return {unit, budget:budget?.mean ?? null, assigned, reserve,
        mismatch:budget && reserve !== null ? assigned + reserve - budget.mean : null};
    }) } : null;
  const jones = selected.find(r => /Aaron Jones/i.test(r.name))!;
  const explanation = await explainNflPlayerProjection(upload.uploadId, jones.id);
  const resolvedJones = workspace.slate.players.find(p => p.id === jones.id)!;
  assert(explanation.ok);
  assert.equal(explanation.projection, resolvedJones.ourProj);
  assert.deepEqual(explanation.statMeans, resolvedJones.statMeans);
  assert.equal(explanation.floor, resolvedJones.floorFpts);
  assert.equal(explanation.ceiling, resolvedJones.ceilingFpts);
  assert.equal(explanation.boomRate, resolvedJones.boomRate);
  // Pure local solve, never a saved optimizer run or contest submission.
  const solve = optimizeNflLineups(workspace.slate.players, {
    format: workspace.slate.format, mode: 'cash', projectionSource: 'our', allowDkFallback: false,
    nLineups: 1, minSalary: 0, maxExposure: 1, minUnique: 1, stackPassCatchers: 0,
    bringBack: false, randomness: 0, lockedPlayerIds: [jones.dkPlayerId], excludedPlayerIds: [],
    minExposureByPlayer: {}, maxExposureByPlayer: {},
  });
  const slot = solve.lineups[0]?.slots.find(s => s.player.dkPlayerId === jones.dkPlayerId);
  assert(slot, 'Jones must appear in the local verification solve');
  assert.equal(slot.projection / slot.multiplier, explanation.projection);
  assert.deepEqual(await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, upload.uploadId)), rows);
  const result = { capturedAt: new Date().toISOString(), upload, run: runs[0],
    players: selected.map(row => ({row, projection: projections.find(p => p.playerId === row.ffPlayerId),
      roster: roster.get(row.ffPlayerId ?? -1), resolved: workspace.slate.players.find(p => p.id === row.id)})),
    redistribution: workspace.slate.redistribution, researchShares,
    explanation, verification: { poolDrawerOptimizerParity: true, salarySnapshotUnchanged: true,
      optimizerJonesPoints: slot.projection / slot.multiplier, savedOptimizerRun: false } };
  const path = process.argv[3];
  if (path) { writeFileSync(path, JSON.stringify(result, null, 2) + '\n'); console.log(`Saved audit: ${path}`); }
  else console.log(JSON.stringify(result, null, 2));
}
main().catch(error => { console.error(error instanceof Error ? error.message : 'Audit failed'); process.exitCode = 1; });
