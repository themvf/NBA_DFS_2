/**
 * Ghost-lineup dry run against the live database (read-mostly):
 * finds the gradable slates in the next N hours, builds ONE system slate
 * through the workspace read (an origin='system' upload, invisible to every
 * owner-facing reader), freezes its house inputs in memory, and builds the
 * house and one design arm in process. It saves no run and no freeze, and it
 * deletes the system upload it created unless --keep is passed.
 *
 *   npx tsx --conditions=react-server scripts/research-nfl-ghost-dryrun.ts [hours] [--keep]
 */
import { config } from "dotenv";
config({ path: ".env.local", quiet: true });
process.env.CRON_SECRET ??= "local-dry-run";

(async () => {
  const hours = Number(process.argv.find(a => /^\d+$/.test(a)) ?? 60);
  const keep = process.argv.includes("--keep");
  const { gradableSlates } = await import("../src/lib/nfl-dfs/ghost/service");
  const { ghostFreezeSlate } = await import("../src/app/dfs/nfl/actions");
  const { houseSettings } = await import("../src/lib/nfl-dfs/ghost/house");
  const { buildArm } = await import("../src/lib/nfl-dfs/ghost/arm");
  const { ghostDesign } = await import("../src/lib/nfl-dfs/ghost/design");
  const { optimizeNflLineups } = await import("../src/app/dfs/nfl/nfl-optimizer");
  const { db } = await import("../src/db"); const { sql } = await import("drizzle-orm");
  const rows = (r: unknown) => (r as { rows: Record<string, unknown>[] }).rows;

  const { slates, skipped } = await gradableSlates(new Date(), hours);
  console.log(`gradable in the next ${hours}h:`);
  for (const s of slates) console.log(`  ${s.draftGroupId} ${s.format} ${s.kind} first kickoff ${s.firstKickoff} games ${s.games.map(g => `${g.away}@${g.home}`).join(" ")}`);
  console.log(`skipped: ${skipped.length} (${[...new Set(skipped.map(s => s.reason))].join("; ")})`);
  const pick = slates.find(s => s.format === (process.argv.includes("--showdown") ? "showdown" : "classic")) ?? slates[0];
  if (!pick) return;
  const snap = rows(await db.execute(sql`SELECT snapshot_id FROM nfl_dfs_dk_pool_snapshots WHERE draft_group_id=${pick.draftGroupId} ORDER BY captured_at DESC LIMIT 1`))[0];
  const poolRows = rows(await db.execute(sql`SELECT dk_pid, name, position, team, opponent, salary, status, is_disabled, ppg
    FROM nfl_dfs_dk_pool_player_status WHERE snapshot_id=${String(snap.snapshot_id)}::uuid`))
    .map(r => ({ ...r, dk_pid: Number(r.dk_pid), salary: r.salary == null ? null : Number(r.salary), ppg: r.ppg == null ? null : Number(r.ppg) }));
  const t0 = Date.now();
  const frozen = await ghostFreezeSlate(process.env.CRON_SECRET!, { format: pick.format, poolRows: poolRows as never, games: pick.games,
    fileName: `system:dry-run:${pick.draftGroupId}:${snap.snapshot_id}`, digest: `dry-run-${Date.now()}`, house: houseSettings(pick.format, { favoriteTeam: null, underdogTeam: null }) });
  console.log(`\nfroze ${pick.draftGroupId}: ${frozen.inputSnapshot.length} players in ${Date.now() - t0} ms; projections ${frozen.modelVersion} as of ${frozen.modelAsOf}; ownership ${frozen.ownership.capability}, leverage ${frozen.ownership.leverage}`);
  console.log(`  snapshot size ${(JSON.stringify(frozen.inputSnapshot).length / 1e6).toFixed(1)} MB; anchored ${frozen.inputSnapshot.filter(p => p.marketAnchor).length}, absence-committed ${frozen.inputSnapshot.filter(p => p.absenceCommit).length}, with prior opportunity ${frozen.inputSnapshot.filter(p => p.priorOpportunity).length}, no DK average ${frozen.inputSnapshot.filter(p => p.dkAvg == null).length}`);
  console.log(`  warnings: ${frozen.warnings.slice(0, 4).join(" | ")}`);
  const design = ghostDesign(pick.format);
  for (const arm of [design.arms[0], design.arms[1]]) {
    const b = buildArm(pick.format, frozen.inputSnapshot, frozen.resolvedHouse, arm.levels);
    const t = Date.now(); const r = optimizeNflLineups(b.players, b.settings);
    console.log(`  arm ${arm.arm}${arm.house ? " (house)" : ""}: ${r.lineups.length}/${b.settings.nLineups} lineups in ${((Date.now() - t) / 1000).toFixed(1)}s; ${r.warnings.find(w => /Stopped/.test(w)) ?? "complete"}`);
  }
  // The system upload must be invisible to the owner's slate list and the pinned research selectors.
  const visible = rows(await db.execute(sql`SELECT count(*)::int n FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id WHERE u.upload_id=${frozen.uploadId}::uuid`))[0];
  const origin = rows(await db.execute(sql`SELECT origin, projection_run_id FROM nfl_dfs_slate_uploads WHERE upload_id=${frozen.uploadId}::uuid`))[0];
  console.log(`  system upload ${frozen.uploadId}: origin ${origin.origin}, projection link ${origin.projection_run_id ?? "none"}, visible to run-joined readers: ${visible.n}`);
  if (!keep) {
    await db.execute(sql`DELETE FROM nfl_dfs_slate_uploads WHERE upload_id=${frozen.uploadId}::uuid AND origin='system'`);
    console.log("  deleted the dry-run system upload");
  }
})().catch((e) => { console.error(e); process.exit(1); });
