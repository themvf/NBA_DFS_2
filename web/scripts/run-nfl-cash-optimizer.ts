/**
 * Run the NFL Classic optimizer against a SAVED salary slate and print the result.
 *
 * Exists so the optimizer can be run from a phone: the browser flow needs a file
 * upload and a session, this needs a `workflow_dispatch` tap. The salary slate is
 * already persisted (`nfl_dfs_slate_uploads` / `nfl_dfs_slate_players`), so nothing
 * is re-uploaded here -- this reads the stored slate and reruns the same solver the
 * page uses.
 *
 * READ-ONLY BY DESIGN. The page's `runNflOptimizer` also WRITES an optimizer run and
 * its lineups to the ledger. This script deliberately calls `optimizeNflLineups`
 * directly instead, so a report run from CI cannot add rows that look like decisions
 * somebody made. If you want a persisted run, use the page.
 *
 * It is the same solver, the same settings shape and the same guards, so the printed
 * lineup matches what the page would generate for identical settings.
 *
 * Run: npm run optimize:nfl -- --mode cash --lineups 1
 */
import { optimizeNflLineups, type NflOptimizerSettings, type NflGeneratedLineup, type NflLineupSlot }
  from "../src/app/dfs/nfl/nfl-optimizer";

const arg = (flag: string) => {
  const index = process.argv.indexOf(flag);
  return index === -1 ? null : process.argv[index + 1] ?? null;
};
const num = (flag: string, fallback: number) => {
  const raw = arg(flag);
  if (raw === null) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`${flag} must be a number; received ${raw}`);
  return value;
};

const money = (value: number) => `$${value.toLocaleString("en-US")}`;

/**
 * Narrows saved slates to the Classic ones by their display label.
 *
 * listSavedNflSlates() exposes only uploadId and label, so this is the only way to
 * avoid loading every upload. It is a HEURISTIC over a display string -- the
 * authoritative `format` field is still checked after the slate loads. Tested against
 * savedSlateLabel() itself so a label change cannot silently stop matching.
 */
export const CLASSIC_LABEL = /·\s*Classic\s*·/i;

/**
 * Whether a slot's cash score came from a MODELLED floor or from the flat fallback.
 *
 * `objective()` resolves the cash base as p10/floor `?? projection * 0.74`. That 0.74
 * is a constant, not a distribution, so a lineup built mostly on it is a lineup whose
 * floors are asserted rather than estimated -- which matters more in cash than
 * anywhere else. Mirrors the optimizer's own branching; keep the two in step.
 */
export function floorProvenance(slot: NflLineupSlot): { modelled: boolean; detail: string } {
  const player = slot.player;
  const finite = (value: number | null | undefined) => (typeof value === "number" && Number.isFinite(value) ? value : null);
  if (slot.projectionSource === "workload") {
    const p10 = finite(player.workload?.p10);
    return { modelled: p10 !== null, detail: p10 === null ? "workload p10 missing" : `workload p10 ${p10.toFixed(1)}` };
  }
  if (slot.projectionSource === "calibrated") {
    const p10 = finite(player.calibrated?.p10);
    return { modelled: p10 !== null, detail: p10 === null ? "calibrated p10 missing" : `calibrated p10 ${p10.toFixed(1)}` };
  }
  if (slot.projectionSource === "our" || slot.projectionSource === "our_fallback") {
    const floor = finite(player.floorFpts);
    return { modelled: floor !== null, detail: floor === null ? "no historical floor" : `historical floor ${floor.toFixed(1)}` };
  }
  return { modelled: false, detail: `${slot.projectionSource} has no floor estimate` };
}

function printLineup(lineup: NflGeneratedLineup, mode: string) {
  console.log(`\n--- Lineup ${lineup.lineupNumber} ---`);
  console.log("SLOT  PLAYER                     TEAM  OPP   SALARY    SCORE  SOURCE            FLOOR BASIS");
  for (const slot of lineup.slots) {
    const provenance = floorProvenance(slot);
    const flag = mode === "cash" && !provenance.modelled ? " <- FALLBACK" : "";
    console.log(
      `${slot.slot.padEnd(6)}${slot.player.name.slice(0, 26).padEnd(27)}` +
      `${slot.player.team.padEnd(6)}${(slot.player.opponent ?? "-").padEnd(6)}` +
      `${money(slot.salary).padStart(8)}${slot.projection.toFixed(1).padStart(8)}  ` +
      `${slot.projectionSource.padEnd(18)}${provenance.detail}${flag}`,
    );
  }
  console.log(
    `TOTAL salary ${money(lineup.totalSalary)} of $50,000 (${money(50000 - lineup.totalSalary)} unused) · ` +
    `projected ${lineup.projectedFpts.toFixed(1)} · floor sum ${lineup.floorFpts.toFixed(1)} · ceiling sum ${lineup.ceilingFpts.toFixed(1)}`,
  );
  if (mode === "cash") {
    const fallback = lineup.slots.filter(slot => !floorProvenance(slot).modelled);
    console.log(fallback.length
      ? `CAUTION ${fallback.length}/${lineup.slots.length} slots scored on the flat projection*0.74 fallback, not a modelled floor: ${fallback.map(s => s.player.name).join(", ")}`
      : "All slots scored on a modelled floor.");
  }
}

export async function main() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is not set; this script reads the saved slate from the database.");
  const mode = (arg("--mode") ?? "cash") as "cash" | "gpp";
  if (mode !== "cash" && mode !== "gpp") throw new Error(`--mode must be cash or gpp; received ${mode}`);
  // Resolve every numeric argument before any database work, so a typo fails in a
  // second rather than after a slate read.
  const nLineups = num("--lineups", 1);
  if (!Number.isInteger(nLineups) || nLineups < 1 || nLineups > 150) throw new Error(`--lineups must be an integer 1-150; received ${nLineups}`);
  const randomness = num("--randomness", 0);
  if (randomness < 0 || randomness > 0.25) throw new Error(`--randomness must be between 0 and 0.25; received ${randomness}`);
  const minSalary = num("--min-salary", 49000);
  const maxExposure = num("--max-exposure", 0.6);
  const minUnique = num("--min-unique", 2);

  // Deferred so the server-only stub and env are in place before the module graph loads.
  const { listSavedNflSlates, loadSavedNflWorkspace } = await import("../src/app/dfs/nfl/actions");
  const saved = await listSavedNflSlates(); // newest first
  if (!saved.length) throw new Error("No saved NFL salary slate was found. Upload one on /dfs/nfl first.");

  if (process.argv.includes("--list")) {
    console.log("Saved slates, newest first:");
    for (const entry of saved) console.log(`  ${entry.uploadId}  ${entry.label}`);
    return;
  }

  const requested = arg("--upload");
  let slate;
  if (requested) {
    slate = (await loadSavedNflWorkspace(requested)).slate;
    if (slate.format !== "classic") throw new Error(`Slate ${requested} is ${slate.format}; this runner is Classic-only.`);
  } else {
    // Take the newest CLASSIC slate, not merely the newest slate. Showdown and Classic
    // uploads share one table, so "most recent upload" is regularly a Showdown file and
    // a Classic-only runner that grabbed it would refuse a slate that is sitting right
    // there. The label is a display string, so it only narrows the candidates -- the
    // authoritative `format` is checked after loading.
    const candidates = saved.filter(entry => CLASSIC_LABEL.test(entry.label));
    for (const entry of candidates.length ? candidates : saved) {
      const loaded = (await loadSavedNflWorkspace(entry.uploadId)).slate;
      if (loaded.format === "classic") { slate = loaded; break; }
    }
    if (!slate) {
      throw new Error(`No saved Classic slate found among ${saved.length} upload(s). Available:\n` +
        saved.map(entry => `  ${entry.uploadId}  ${entry.label}`).join("\n"));
    }
  }

  // Same refusal as the page: an unverified legacy match can put the wrong human in a lineup.
  if (slate.players.some(p => p.identityMethod === "exact_name_position")) {
    throw new Error("This salary upload contains legacy player matches without team verification. Reload its salary CSV before optimizing.");
  }

  const settings: NflOptimizerSettings = {
    format: "classic",
    mode,
    projectionSource: "our",
    allowDkFallback: true,
    nLineups,
    minSalary,
    maxExposure,
    minUnique,
    // Ignored by the solver outside GPP, set explicitly so the intent is visible.
    stackPassCatchers: 0,
    bringBack: false,
    // Zero by default: the jitter seed is a hardcoded constant, so randomness is the
    // only thing that makes repeat runs differ, and in cash it only perturbs the floor
    // the objective is trying to maximise.
    randomness,
    lockedPlayerIds: [],
    excludedPlayerIds: [],
    minExposureByPlayer: {},
    maxExposureByPlayer: {},
  };

  console.log(`Slate ${slate.uploadId} · ${slate.format} · ${slate.games.length} games · ${slate.teams.length} teams · ${slate.fileName}`);
  console.log(`Model ${slate.modelVersion ?? "unknown"} · projection run ${slate.projectionRunId ?? "none"} · as of ${slate.modelAsOf ?? "unknown"}`);
  for (const warning of slate.warnings) console.log(`SLATE WARNING ${warning}`);
  // Availability freshness is the thing most likely to be stale on a game-day run.
  console.log(slate.injuryCoverage
    ? `Injury coverage: snapshot ${slate.injuryCoverage.snapshotId ?? "none"}`
    : "Injury coverage: no snapshot linked to this slate.");
  console.log(`Settings: mode=${settings.mode} source=${settings.projectionSource} lineups=${settings.nLineups} ` +
    `minSalary=${money(settings.minSalary)} randomness=${settings.randomness} maxExposure=${settings.maxExposure} minUnique=${settings.minUnique}`);
  console.log(`Pool: ${slate.players.length} players, ${slate.players.filter(p => p.isOut).length} flagged OUT`);

  const result = optimizeNflLineups(slate.players, settings);

  // The page refuses a calibrated player whose game already kicked off; a pregame
  // decision built on an in-progress game is not a pregame decision.
  const started = result.lineups.flatMap(l => l.slots).find(
    s => s.projectionSource === "calibrated" && s.player.calibrated && Date.parse(s.player.calibrated.kickoff) <= Date.now());
  if (started) throw new Error(`${started.player.name}'s game has already started. Refresh the slate before trusting this lineup.`);

  console.log(`\nSource coverage: ${result.sourceCoverage.direct} direct, ${result.sourceCoverage.fallback} fallback, ${result.sourceCoverage.excluded} excluded of ${result.sourceCoverage.requested} requested.`);
  for (const warning of result.warnings) console.log(`WARNING ${warning}`);
  if (!result.lineups.length) throw new Error("No feasible lineup was produced. Check salary, exposure and exclusion settings.");

  for (const lineup of result.lineups) printLineup(lineup, settings.mode);

  if (settings.mode === "cash" && result.lineups.length > 1) {
    console.log("\nNote: cash maximises the p10 floor and lineup 1 is solved without uniqueness constraints, " +
      "so it is the optimum. Later lineups are strictly worse on that objective and exist only to diversify.");
  }
  console.log("\nRead-only run: no optimizer run or lineup rows were written.");
}

if (process.argv[1]?.includes("run-nfl-cash-optimizer")) main().catch(error => { console.error(`\nFAILED: ${error instanceof Error ? error.message : error}`); process.exit(1); });
