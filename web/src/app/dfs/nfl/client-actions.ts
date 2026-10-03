/**
 * The NFL DFS server actions, as the browser should call them.
 *
 * Same names and signatures as `./actions`, but routed through
 * `./safe-actions` so a failure arrives with its real reason instead of
 * production's generic "Server Components render" digest. Import from here
 * in client components; server code and scripts keep using `./actions`.
 */
import { unwrap } from "@/lib/nfl-dfs/safe-result";
import * as safe from "./safe-actions";

export type {
  NflComparisonSource, NflProjectionExplanation, NflSlateResults, NflWorkspacePlayer, NflWorkspaceSlate,
} from "./actions";
export { generateNflLineups } from "./actions";

type Args<F extends (...args: never[]) => unknown> = Parameters<F>;

export const checkNflSlateFreshness = async (...a: Args<typeof safe.safeCheckNflSlateFreshness>) => unwrap(await safe.safeCheckNflSlateFreshness(...a));
export const refreshNflSlateProjections = async (...a: Args<typeof safe.safeRefreshNflSlateProjections>) => unwrap(await safe.safeRefreshNflSlateProjections(...a));
export const listSavedNflSlates = async (...a: Args<typeof safe.safeListSavedNflSlates>) => unwrap(await safe.safeListSavedNflSlates(...a));
export const loadSavedNflWorkspace = async (...a: Args<typeof safe.safeLoadSavedNflWorkspace>) => unwrap(await safe.safeLoadSavedNflWorkspace(...a));
export const loadSavedNflLineups = async (...a: Args<typeof safe.safeLoadSavedNflLineups>) => unwrap(await safe.safeLoadSavedNflLineups(...a));
export const readNflOptimizerAudit = async (...a: Args<typeof safe.safeReadNflOptimizerAudit>) => unwrap(await safe.safeReadNflOptimizerAudit(...a));
export const exportSavedNflDefensiveEntries = async (...a: Args<typeof safe.safeExportSavedNflDefensiveEntries>) => unwrap(await safe.safeExportSavedNflDefensiveEntries(...a));
export const exportSavedNflEntries = async (...a: Args<typeof safe.safeExportSavedNflEntries>) => unwrap(await safe.safeExportSavedNflEntries(...a));
export const applyNflComparison = async (...a: Args<typeof safe.safeApplyNflComparison>) => unwrap(await safe.safeApplyNflComparison(...a));
export const loadNflSalaryCsv = async (...a: Args<typeof safe.safeLoadNflSalaryCsv>) => unwrap(await safe.safeLoadNflSalaryCsv(...a));
export const searchNflStarterNews = async (...a: Args<typeof safe.safeSearchNflStarterNews>) => unwrap(await safe.safeSearchNflStarterNews(...a));
export const explainNflPlayerProjection = async (...a: Args<typeof safe.safeExplainNflPlayerProjection>) => unwrap(await safe.safeExplainNflPlayerProjection(...a));
export const importNflContestResults = async (...a: Args<typeof safe.safeImportNflContestResults>) => unwrap(await safe.safeImportNflContestResults(...a));
export const readNflFieldAudit = async (...a: Args<typeof safe.safeReadNflFieldAudit>) => unwrap(await safe.safeReadNflFieldAudit(...a));
export const readNflSlateResults = async (...a: Args<typeof safe.safeReadNflSlateResults>) => unwrap(await safe.safeReadNflSlateResults(...a));
export const compareNflWorkload = async (...a: Args<typeof safe.safeCompareNflWorkload>) => unwrap(await safe.safeCompareNflWorkload(...a));
export const loadNflBenchmarks = async (...a: Args<typeof safe.safeLoadNflBenchmarks>) => unwrap(await safe.safeLoadNflBenchmarks(...a));
export const freezeNflBenchmark = async (...a: Args<typeof safe.safeFreezeNflBenchmark>) => unwrap(await safe.safeFreezeNflBenchmark(...a));
export const previewNflTargetRedistribution = async (...a: Args<typeof safe.safePreviewNflTargetRedistribution>) => unwrap(await safe.safePreviewNflTargetRedistribution(...a));
export const previewNflAbsence = async (...a: Args<typeof safe.safePreviewNflAbsence>) => unwrap(await safe.safePreviewNflAbsence(...a));
export const startNflDataUpdate = async (...a: Args<typeof safe.safeStartNflDataUpdate>) => unwrap(await safe.safeStartNflDataUpdate(...a));
export const readNflDataUpdate = async (...a: Args<typeof safe.safeReadNflDataUpdate>) => unwrap(await safe.safeReadNflDataUpdate(...a));
export type { NflDataUpdateResult } from "./data-update-actions";
export const saveNflBuildDraft = async (...a: Args<typeof safe.safeSaveNflBuildDraft>) => unwrap(await safe.safeSaveNflBuildDraft(...a));
export const readNflBuildDraft = async (...a: Args<typeof safe.safeReadNflBuildDraft>) => unwrap(await safe.safeReadNflBuildDraft(...a));
export const captureCurrentPool = async (...a: Args<typeof safe.safeCaptureCurrentPool>) => unwrap(await safe.safeCaptureCurrentPool(...a));
export const loadPlayerHistory = async (...a: Args<typeof safe.safeLoadPlayerHistory>) => unwrap(await safe.safeLoadPlayerHistory(...a));
