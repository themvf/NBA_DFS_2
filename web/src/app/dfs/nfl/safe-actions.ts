"use server";

/**
 * Error-preserving wrappers for every NFL DFS server action the browser calls.
 * See `@/lib/nfl-dfs/safe-result` for why: a thrown message is replaced by a
 * generic digest in production. Components import the same names from
 * `./client-actions`, which unwraps these, so call sites are unchanged.
 * Scripts and server code keep calling `./actions` directly.
 */
import type { SafeResult } from "@/lib/nfl-dfs/safe-result";
import * as actions from "./actions";
import * as dataUpdates from "./data-update-actions";
import { captureCurrentPool } from "./pool-review/actions";
import { loadPlayerHistory } from "./review/actions";

async function run<T>(label: string, fn: () => Promise<T>): Promise<SafeResult<T>> {
  try {
    return { ok: true, value: await fn() };
  } catch (error) {
    console.error(`NFL DFS action failed: ${label}`, error);
    return { ok: false, error: error instanceof Error && error.message ? error.message : `${label} failed.` };
  }
}

type Args<F extends (...args: never[]) => unknown> = Parameters<F>;

export async function safeStartNflDataUpdate(...args: Args<typeof dataUpdates.startNflDataUpdate>) { return run("Starting the data update", () => dataUpdates.startNflDataUpdate(...args)); }
export async function safeReadNflDataUpdate(...args: Args<typeof dataUpdates.readNflDataUpdate>) { return run("Checking the data update", () => dataUpdates.readNflDataUpdate(...args)); }
export async function safeRefreshNflSlateProjections(...args: Args<typeof actions.refreshNflSlateProjections>) { return run("Projection refresh", () => actions.refreshNflSlateProjections(...args)); }
export async function safeListSavedNflSlates(...args: Args<typeof actions.listSavedNflSlates>) { return run("Listing saved slates", () => actions.listSavedNflSlates(...args)); }
export async function safeLoadSavedNflWorkspace(...args: Args<typeof actions.loadSavedNflWorkspace>) { return run("Loading the slate", () => actions.loadSavedNflWorkspace(...args)); }
export async function safeLoadSavedNflLineups(...args: Args<typeof actions.loadSavedNflLineups>) { return run("Loading saved lineups", () => actions.loadSavedNflLineups(...args)); }
export async function safeReadNflOptimizerAudit(...args: Args<typeof actions.readNflOptimizerAudit>) { return run("Reading the lineup audit", () => actions.readNflOptimizerAudit(...args)); }
export async function safeExportSavedNflDefensiveEntries(...args: Args<typeof actions.exportSavedNflDefensiveEntries>) { return run("Exporting entries", () => actions.exportSavedNflDefensiveEntries(...args)); }
export async function safeApplyNflComparison(...args: Args<typeof actions.applyNflComparison>) { return run("Importing comparison projections", () => actions.applyNflComparison(...args)); }
export async function safeLoadNflSalaryCsv(...args: Args<typeof actions.loadNflSalaryCsv>) { return run("Uploading salaries", () => actions.loadNflSalaryCsv(...args)); }
export async function safeSearchNflStarterNews(...args: Args<typeof actions.searchNflStarterNews>) { return run("Searching starter news", () => actions.searchNflStarterNews(...args)); }
export async function safeExplainNflPlayerProjection(...args: Args<typeof actions.explainNflPlayerProjection>) { return run("Explaining the projection", () => actions.explainNflPlayerProjection(...args)); }
export async function safeImportNflContestResults(...args: Args<typeof actions.importNflContestResults>) { return run("Importing contest results", () => actions.importNflContestResults(...args)); }
export async function safeReadNflFieldAudit(...args: Args<typeof actions.readNflFieldAudit>) { return run("Reading the field audit", () => actions.readNflFieldAudit(...args)); }
export async function safeReadNflSlateResults(...args: Args<typeof actions.readNflSlateResults>) { return run("Reading slate results", () => actions.readNflSlateResults(...args)); }
export async function safeCompareNflWorkload(...args: Args<typeof actions.compareNflWorkload>) { return run("Comparing workload projections", () => actions.compareNflWorkload(...args)); }
export async function safeLoadNflBenchmarks(...args: Args<typeof actions.loadNflBenchmarks>) { return run("Loading benchmarks", () => actions.loadNflBenchmarks(...args)); }
export async function safeFreezeNflBenchmark(...args: Args<typeof actions.freezeNflBenchmark>) { return run("Freezing the benchmark", () => actions.freezeNflBenchmark(...args)); }
export async function safePreviewNflTargetRedistribution(...args: Args<typeof actions.previewNflTargetRedistribution>) { return run("Previewing target redistribution", () => actions.previewNflTargetRedistribution(...args)); }
export async function safePreviewNflAbsence(...args: Args<typeof actions.previewNflAbsence>) { return run("Previewing the absence", () => actions.previewNflAbsence(...args)); }
export async function safeSaveNflBuildDraft(...args: Args<typeof actions.saveNflBuildDraft>) { return run("Saving your build settings", () => actions.saveNflBuildDraft(...args)); }
export async function safeReadNflBuildDraft(...args: Args<typeof actions.readNflBuildDraft>) { return run("Reading your build settings", () => actions.readNflBuildDraft(...args)); }
export async function safeCaptureCurrentPool(...args: Args<typeof captureCurrentPool>) { return run("Capturing the pool", () => captureCurrentPool(...args)); }
export async function safeLoadPlayerHistory(...args: Args<typeof loadPlayerHistory>) { return run("Loading player history", () => loadPlayerHistory(...args)); }
