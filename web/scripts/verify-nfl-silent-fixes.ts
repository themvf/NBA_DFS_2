/**
 * Read-only check of the 2026-09-29 silent-failure fixes against live data.
 * Only SELECTs: it reads saved slates, runs and captures through the same code
 * the page uses and runs the optimizer in memory. It never saves a run, records
 * a Slate Check (the build is forced to "local"), dispatches a workflow, or
 * calls GitHub (the dispatch token is dropped).
 *
 *   npm run verify:nfl-silent-fixes
 */
import { sql } from "drizzle-orm";

for (const key of ["VERCEL_GIT_COMMIT_SHA", "NFL_POOL_CODE_REVISION", "NEXT_PUBLIC_COMMIT_SHA", "GITHUB_DISPATCH_TOKEN"]) delete process.env[key];

async function main() {
  const { db } = await import("../src/db");
  const { loadSavedNflLineups, loadSavedNflWorkspace } = await import("../src/app/dfs/nfl/actions");
  const { currentPoolForQa, runNflPreExportQa, savedRunQaEvidence, nflOverlapCap } = await import("../src/lib/nfl-dfs/pre-export-qa");
  const { optimizeNflLineups } = await import("../src/app/dfs/nfl/nfl-optimizer");
  const { readDefensiveCaptures } = await import("../src/db/nfl-defensive-projections");
  const { resolveDefensiveForecast } = await import("../src/lib/nfl-dfs/defensive-projection");
  const { getNflInjuryCoverage } = await import("../src/db/nfl-dfs-availability");
  const { showdownGame, nflTeamKey } = await import("../src/lib/nfl-dfs/availability");
  const { projectionError } = await import("../src/lib/nfl-dfs/slate-results");
  const { normalizeName } = await import("../src/lib/nfl-dfs/field-audit");
  type Slate = Awaited<ReturnType<typeof loadSavedNflWorkspace>>["slate"];

  const cache = new Map<string, Slate>();
  const slateFor = async (uploadId: string, starters: Record<string, number> = {}) => {
    const key = `${uploadId}|${JSON.stringify(starters)}`;
    if (!cache.has(key)) cache.set(key, (await loadSavedNflWorkspace(uploadId, starters)).slate);
    return cache.get(key)!;
  };
  const newestUpload = async (uploadId: string) => {
    const rows = await db.execute(sql`SELECT u2.upload_id, u2.player_count,
        (SELECT count(*)::int FROM nfl_dfs_slate_players p WHERE p.upload_id=u2.upload_id) AS stored
      FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_slate_uploads u2 ON (u2.slate_signature=u.slate_signature OR u2.file_digest=u.file_digest)
      WHERE u.upload_id=${uploadId}::uuid ORDER BY u2.created_at DESC`);
    return String(rows.rows.find((r) => Number(r.stored) >= Number(r.player_count))?.upload_id ?? uploadId);
  };

  // --- Finding 1 (+3): every saved run re-checked against the CURRENT pool ---
  console.log("\n[1] Saved runs re-checked against the current pool (run's own confirmed starters)");
  const runs = await db.execute(sql`SELECT run_id, upload_id, created_at, status, generated_lineups, requested_lineups
    FROM nfl_dfs_optimizer_runs WHERE status IN ('complete','partial') AND generated_lineups > 0 ORDER BY created_at`);
  let blockedByPool = 0, blockedByRole = 0, noEvidence = 0, partial = 0;
  for (const run of runs.rows) {
    const saved = await loadSavedNflLineups(String(run.upload_id), String(run.run_id));
    const settings = saved.settings;
    const slate = await slateFor(await newestUpload(String(run.upload_id)), settings.confirmedStartingQbs ?? {});
    const qa = runNflPreExportQa({
      format: settings.format, requestedLineups: Number(run.requested_lineups),
      lineups: saved.lineups.map((l) => ({ lineupNumber: l.lineupNumber, playerIds: l.playerIds, totalSalary: l.totalSalary,
        slots: l.slots.map((s) => ({ slot: s.slot, playerId: s.player.dkPlayerId, salary: s.salary, player: s.player })), archetype: l.archetype ?? null })),
      ...savedRunQaEvidence(saved.evidence, settings),
      overlapCap: nflOverlapCap(settings.format, settings.minUnique, settings.maxPairwiseOverlap),
      currentPool: currentPoolForQa(slate.players),
    });
    const pool = qa.checks.find((c) => c.id === "current_pool")!;
    const role = qa.checks.find((c) => c.id === "current_pool_role");
    if (!saved.evidence) noEvidence++;
    if (Number(run.generated_lineups) < Number(run.requested_lineups)) partial++;
    const label = `${String(run.run_id).slice(0, 8)} ${new Date(String(run.created_at)).toISOString().slice(0, 16)} ${slate.games.join(",").slice(0, 30)}`;
    if (!pool.passed) { blockedByPool++; console.log(`  OUT NOW  ${label}: ${pool.detail.slice(0, 220)}`); }
    if (role) { blockedByRole++; console.log(`  BACKUP   ${label}: ${role.detail.slice(0, 160)}`); }
  }
  console.log(`  ${runs.rows.length} runs; ${blockedByPool} blocked outright (a lineup player is out now); ${blockedByRole} need an override or rebuild (a lineup QB is now listed as a backup); ${noEvidence} without saved evidence now read "can't be checked"; ${partial} partial runs now need an override.`);

  // --- Finding 2: week-3 classic, defensive on, DK-average fallback ---
  console.log("\n[2] Week-3 classic, defensive experimental + DK-average fallback");
  const classicId = "d5d97cc7-0574-491b-ae89-efad4b836b37";
  const classic = await slateFor(classicId);
  const captures = await readDefensiveCaptures(classicId, classic.projectionRunId!, "pfr-efficiency", new Date());
  const withBundles = classic.players.map((p) => ({ ...p, defensiveForecast: resolveDefensiveForecast(p, classic.projectionRunId!, { mode: "experimental", profile: "pfr-efficiency" }, captures.get(p.ffPlayerId ?? -1) ?? null) }));
  const base = { format: "classic" as const, mode: "gpp" as const, projectionSource: "our" as const, nLineups: 1, minSalary: 45000, minPlayerSalary: 1000,
    requireObservedHistory: false, maxExposure: 1, minUnique: 1, stackPassCatchers: 0 as const, bringBack: false, randomness: 0,
    lockedPlayerIds: [], excludedPlayerIds: [], minExposureByPlayer: {}, maxExposureByPlayer: {}, defensiveAdjustments: { mode: "experimental" as const, profile: "pfr-efficiency" as const } };
  const on = optimizeNflLineups(withBundles, { ...base, allowDkFallback: true });
  const off = optimizeNflLineups(withBundles, { ...base, allowDkFallback: false });
  const fallbackIds = new Set(on.eligibility!.filter((e) => e.eligible).map((e) => e.dkPlayerId));
  const kept = off.eligibility!.filter((e) => !e.eligible && e.reasonCode === "NO_PROJECTED_OPPORTUNITY" && fallbackIds.has(e.dkPlayerId));
  console.log(`  fallback on: ${on.warnings.find((w) => /DK Avg fallback/.test(w)) ?? "no DK-average warning"}; players kept only by the fallback: ${kept.length} (${kept.slice(0, 6).map((e) => e.name).join(", ")}${kept.length > 6 ? ", …" : ""})`);

  // --- Finding 4: PHI@CHI, a captain range on the blocked backup ---
  console.log("\n[4] PHI@CHI, captain range on Tyson Bagent without confirming him");
  const phiChi = await slateFor("e455e66a-aebc-40e8-bf91-aeadea2747ab");
  const bagent = phiChi.players.find((p) => p.name === "Tyson Bagent")!;
  console.log(`  Bagent in pool: isOut=${bagent.isOut}, ${bagent.availability?.blockedReason ?? bagent.availability?.role}`);
  try {
    optimizeNflLineups(phiChi.players, { ...base, format: "showdown", defensiveAdjustments: undefined, allowDkFallback: false, minSalary: 0, nLineups: 3,
      exposurePolicies: [{ playerId: bagent.dkPlayerId, overall: { minPct: null, maxPct: 0.6 }, captain: { minPct: 0.15, maxPct: 0.3 }, flex: { minPct: null, maxPct: null }, exactTargetMode: false }] });
    console.log("  NOT FIXED: no error");
  } catch (error) { console.log(`  error: ${error instanceof Error ? error.message : error}`); }

  // --- Finding 8: WAS codes ---
  console.log("\n[8] Team codes");
  const was = classic.players.filter((p) => p.team === "WAS");
  console.log(`  week-3 classic WAS players: ${was.length}; teamSeasonGames ${[...new Set(was.map((p) => p.teamSeasonGames))].join("/")} (was 0 before the fix)`);
  const calibratedWas = was.filter((p) => p.calibrationReason === "Candidate matchup mismatch.").length;
  console.log(`  WAS players refused as "Candidate matchup mismatch.": ${calibratedWas}`);
  const games = await db.execute(sql`SELECT h.abbreviation AS home, a.abbreviation AS away, g.week,
      COALESCE(g.market_home_ml, g.quoted_home_ml) AS home_ml, COALESCE(g.market_away_ml, g.quoted_away_ml) AS away_ml
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
    WHERE g.season=2026 AND g.game_type='REG' AND NOT g.completed ORDER BY g.kickoff ASC`);
  const indWas = showdownGame(games.rows as never, ["IND", "WAS"]);
  console.log(`  IND@WAS Showdown game found: ${indWas ? `${indWas.away}@${indWas.home} ML ${indWas.away_ml}/${indWas.home_ml}` : "none"} (string match would find: ${games.rows.some((r) => [r.home, r.away].includes("WAS")) ? "yes" : "no"})`);
  console.log(`  nflTeamKey(WAS)=${nflTeamKey("WAS")}, nflTeamKey(WSH)=${nflTeamKey("WSH")}`);

  // --- Finding 13: injury coverage bounded by the run cutoff ---
  console.log("\n[13] Injury coverage as of the run cutoff vs as of now");
  const run = await db.execute(sql`SELECT season, week, as_of_at FROM nfl_dfs_projection_runs WHERE run_id=${classic.projectionRunId}`);
  const r = run.rows[0];
  const atCutoff = await getNflInjuryCoverage(Number(r.season), Number(r.week), new Date(String(r.as_of_at)));
  const atNow = await getNflInjuryCoverage(Number(r.season), Number(r.week), null);
  console.log(`  cutoff ${new Date(String(r.as_of_at)).toISOString()}: snapshot ${atCutoff?.snapshotId} captured ${atCutoff?.capturedAt}; now: snapshot ${atNow?.snapshotId} captured ${atNow?.capturedAt}`);

  // --- Finding 12: results grading on the page's projections ---
  console.log("\n[12] Position error: page projections vs stored rows");
  const contest = await db.execute(sql`SELECT c.contest_id, c.slate_upload_id FROM nfl_dfs_field_contests c WHERE c.slate_upload_id IS NOT NULL ORDER BY imported_at DESC LIMIT 1`);
  if (contest.rows[0]) {
    const uploadId = String(contest.rows[0].slate_upload_id);
    const owned = await db.execute(sql`SELECT normalized_name, fpts FROM nfl_dfs_field_ownership WHERE contest_id=${String(contest.rows[0].contest_id)} AND fpts IS NOT NULL`);
    const fpts = new Map(owned.rows.map((o) => [String(o.normalized_name), Number(o.fpts)]));
    const page = await slateFor(uploadId);
    const stored = await db.execute(sql`SELECT name, position, our_proj, is_out, projection_status FROM nfl_dfs_slate_players WHERE upload_id=${uploadId}::uuid`);
    const before = projectionError(stored.rows.map((p) => ({ name: String(p.name), position: String(p.position), ourProj: p.our_proj == null ? null : Number(p.our_proj), isOut: Boolean(p.is_out) })), fpts, normalizeName);
    const after = projectionError(page.players.map((p) => ({ name: p.name, position: p.position, ourProj: p.ourProj, isOut: p.isOut || p.projectionStatus === "out" })), fpts, normalizeName);
    const all = (x: typeof before) => x.find((e) => e.position === "All");
    console.log(`  ${page.games.join(",")}: stored n=${all(before)?.n} MAE ${all(before)?.mae}; page n=${all(after)?.n} MAE ${all(after)?.mae}`);
  }

  // --- Findings 6, 7, 10, 14, 16, 17, 19: the Slate Check and live status on recent slates ---
  console.log("\n[6/7/10/14/16/17/19] Recent slates");
  for (const uploadId of ["e455e66a-aebc-40e8-bf91-aeadea2747ab", classicId, "81544667-eb2d-4444-bb2e-e42a673adf94"]) {
    const s = await slateFor(uploadId);
    const stale = s.players.filter((p) => p.availabilityState === "stale" && p.salary < 3000).length;
    const qbOut = s.players.filter((p) => p.position === "QB" && p.ruledOut).map((p) => `${p.name} (${p.dkStatus ?? p.availability?.status})`);
    console.log(`  ${s.games.join(",").slice(0, 50)}: live lastPollOk=${s.liveDkStatus?.lastPollOk} lastSuccessful=${s.liveDkStatus?.lastSuccessfulPollAt}; cheap players with stale role evidence: ${stale}; QBs ruled out: ${qbOut.join(", ") || "none"}`);
    console.log(`    opponent adjustments: ${(s.opponentAdjustments ?? []).map((o) => `${o.label} ${o.applied}/${o.eligible}${o.error ? ` error: ${o.error}` : ""}`).join("; ")}`);
    for (const item of s.slateCheck?.items ?? []) if (["projections", "roster", "live-dk", "ownership", "opponent-error", "record"].includes(item.id) || (item.id.startsWith("qb:") && item.id !== "qb:starters")) console.log(`    ${item.level}: ${item.text}`);
  }
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
