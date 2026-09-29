/**
 * Read-only: what each experimental projection source says about a saved slate.
 *
 * Usage: verify-nfl-source-availability.ts <upload-id> [simulated request ISO]
 * Prints the server's verdict now, and (optionally) the same rule re-run at an
 * earlier request time, with the newest run as it stood then. Every write
 * statement is refused before it reaches the database.
 */
import assert from 'node:assert/strict';
import { sql, type SQL } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import { db } from '../src/db';

const WRITE = /^\s*(insert|update|delete|create|alter|drop|truncate|grant|with\b[\s\S]*\b(insert|update|delete)\b)/i;
const dialect = new PgDialect();
const guarded = db as unknown as Record<string, unknown>;
for (const method of ['insert', 'update', 'delete', 'batch', 'transaction']) guarded[method] = () => { throw new Error(`read-only verification: db.${method} refused`); };
const execute = db.execute.bind(db);
guarded.execute = ((query: Parameters<typeof db.execute>[0]) => {
  const text = typeof query === 'string' ? query : dialect.sqlToQuery(query as SQL).sql;
  if (WRITE.test(text)) throw new Error(`read-only verification: write statement refused: ${text.slice(0, 80)}`);
  return execute(query);
}) as typeof db.execute;

const [uploadId, simulated] = process.argv.slice(2);
if (!/^[0-9a-f-]{36}$/.test(uploadId ?? '')) throw new Error('Provide a saved slate upload id.');

async function main() {
  const { loadSavedNflWorkspace } = await import('../src/app/dfs/nfl/actions');
  const { computeSourceAvailability } = await import('../src/lib/nfl-dfs/source-availability');
  const { calibratedRelease } = await import('../src/lib/nfl-dfs/calibrated-projection');
  const { getVolumeShareReport } = await import('../src/db/nfl-volume-share');
  const { getCalibratedSnapshots } = await import('../src/db/nfl-dfs-calibrated');
  const { slate } = await loadSavedNflWorkspace(uploadId);
  assert.ok(slate.sourceAvailability, 'workspace must carry a server-computed source verdict');
  const show = (label: string, a: NonNullable<typeof slate.sourceAvailability>) => {
    console.log(`\n== ${label} (request ${a.evaluatedAt}; decision ${a.decisionAt}; newest run: ${a.onNewestRun})`);
    for (const source of ['workload', 'calibrated'] as const) {
      console.log(`${source}: ${a[source].usable ? 'USABLE' : 'UNAVAILABLE'} -- ${a[source].reason}`);
      for (const [p, v] of Object.entries(a[source].positions)) console.log(`   ${p}: ${v.usable ? `${v.count} usable` : 'unavailable'} -- ${v.reason}`);
    }
  };
  console.log(JSON.stringify({ uploadId, format: slate.format, teams: slate.teams, projectionRunId: slate.projectionRunId, modelAsOf: slate.modelAsOf, refreshAvailable: slate.refreshAvailable, onNewestRun: slate.onNewestRun, release: { version: calibratedRelease.version, studyId: calibratedRelease.studyId } }, null, 2));
  show('Server verdict now', slate.sourceAvailability!);

  const run = (await db.execute(sql`SELECT season,week,as_of_at FROM nfl_dfs_projection_runs WHERE run_id=${slate.projectionRunId}`)).rows[0];
  const asOf = new Date(String(run.as_of_at)), season = Number(run.season), week = Number(run.week);
  // Snapshot as-of bound on real data: how many players' NEWEST capture is after the cutoff (the old query picked those, and the reader then refused them).
  const counts = (await db.execute(sql`SELECT count(DISTINCT player_id)::int AS players,
      count(DISTINCT player_id) FILTER (WHERE captured_at <= ${asOf.toISOString()})::int AS at_or_before,
      (SELECT count(*)::int FROM (SELECT DISTINCT ON (player_id) captured_at FROM nfl_dfs_shadow_predictions
         WHERE study_run_id=${calibratedRelease.studyId} AND season=${season} AND week=${week}
         ORDER BY player_id,captured_at DESC,id DESC) newest WHERE captured_at > ${asOf.toISOString()}) AS newest_after_cutoff
    FROM nfl_dfs_shadow_predictions WHERE study_run_id=${calibratedRelease.studyId} AND season=${season} AND week=${week}`)).rows[0];
  const bounded = await getCalibratedSnapshots(season, week, asOf);
  assert.ok(bounded.every((s) => Date.parse(s.capturedAt) <= asOf.getTime()), 'every returned snapshot is at or before the cutoff');
  console.log(`\nShadow study ${calibratedRelease.studyId.slice(0, 8)} week ${week}: ${counts.players} players captured; ${counts.at_or_before} have a capture at or before the cutoff; for ${counts.newest_after_cutoff} the newest capture is after it. Bounded read returns ${bounded.length}.`);

  if (simulated) {
    const now = Date.parse(simulated);
    const newest = (await db.execute(sql`SELECT run_id FROM nfl_dfs_projection_runs WHERE season=${season} AND week=${week} AND as_of_at <= ${new Date(now).toISOString()} ORDER BY as_of_at DESC LIMIT 1`)).rows[0];
    const volume = await getVolumeShareReport(season, week, asOf);
    const verdict = computeSourceAvailability(slate.players, { now, decisionAt: slate.modelAsOf, onNewestRun: newest?.run_id === slate.projectionRunId }, {
      release: calibratedRelease, slateReason: null, volumeShareReason: volume.report ? null : volume.reason,
      calibratedReason: bounded.length ? null : `Shadow study ${calibratedRelease.studyId.slice(0, 8)} froze no forecasts for ${season} week ${week} at or before this slate's projection cutoff.` });
    show(`Same rule at ${simulated} (newest run then: ${newest?.run_id ?? 'none'})`, verdict);
    // The 60-second fix on real rows: the shared workload pool under the decision clock vs the pre-B4 live rule.
    const { workloadPoolEligible } = await import('../src/lib/nfl-dfs/workload-projection');
    const clock = { now, decisionAt: slate.modelAsOf, onNewestRun: newest?.run_id === slate.projectionRunId };
    const byPosition = (eligible: (p: typeof slate.players[number]) => boolean) => Object.fromEntries(['QB', 'RB', 'WR', 'TE', 'DST'].map((pos) => [pos, slate.players.filter((p) => p.position === pos && eligible(p)).length]));
    console.log(`\nWorkload pool at ${simulated}: decision-clock rule ${JSON.stringify(byPosition((p) => workloadPoolEligible(p, clock)))}; pre-B4 60-second rule ${JSON.stringify(byPosition((p) => workloadPoolEligible(p, now)))}.`);
  }
}
main().catch((error) => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
