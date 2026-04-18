/**
 * Cached wrappers for analytics queries.
 *
 * IMPORTANT: unstable_cache wrappers MUST be created at module level (once),
 * not inside a per-call factory function.  Creating the wrapper inside a
 * function recreates it on every call, so Next.js sees a new cache entry
 * each time and caching never works.
 *
 * unstable_cache(fn, keyParts, opts) includes both keyParts AND the serialised
 * function arguments in the cache key, so passing `sport` at call-time
 * correctly produces separate NBA / MLB entries.
 *
 * Call revalidateTag(ANALYTICS_CACHE_TAG, {}) after every results upload so
 * the next /analytics page load gets fresh data.
 */
import { unstable_cache } from "next/cache";

import {
  getCrossSlateAccuracy,
  getGameTotalModelAccuracy,
  getLeverageCalibration,
  getMlbBattingOrderCalibration,
  getMlbBlowupCandidateReport,
  getMlbRunEnvironmentReport,
  getMlbOwnershipModelReport,
  getMlbPitcherLineupReport,
  getMlbPerfectLineupAnalytics,
  getNbaPerfectLineupAnalytics,
  getOwnershipVsTeamTotal,
  getPositionAccuracy,
  getProjectionSourceBreakdown,
  getSalaryTierAccuracy,
  getStatLevelAccuracy,
} from "./queries";
import type { OwnershipDetailSort, Sport } from "./queries";

export const ANALYTICS_CACHE_TAG = "analytics";
const REVALIDATE = 3600; // 1 hour

// ---------------------------------------------------------------------------
// Cross-slate accuracy
// ---------------------------------------------------------------------------
export const getCachedCrossSlateAccuracy = unstable_cache(
  (sport: Sport) => getCrossSlateAccuracy(sport),
  ["analytics-cross-slate"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Position accuracy
// ---------------------------------------------------------------------------
export const getCachedPositionAccuracy = unstable_cache(
  (sport: Sport) => getPositionAccuracy(sport),
  ["analytics-position-accuracy"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Salary tier accuracy
// ---------------------------------------------------------------------------
export const getCachedSalaryTierAccuracy = unstable_cache(
  (sport: Sport) => getSalaryTierAccuracy(sport),
  ["analytics-salary-tier"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Leverage calibration
// ---------------------------------------------------------------------------
export const getCachedLeverageCalibration = unstable_cache(
  (sport: Sport) => getLeverageCalibration(sport),
  ["analytics-leverage"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Ownership vs team total
// ---------------------------------------------------------------------------
export const getCachedOwnershipVsTeamTotal = unstable_cache(
  (sport: Sport) => getOwnershipVsTeamTotal(sport),
  ["analytics-own-vs-total"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// MLB batting order calibration  (no sport arg)
// ---------------------------------------------------------------------------
export const getCachedMlbBattingOrderCalibration = unstable_cache(
  () => getMlbBattingOrderCalibration(),
  ["analytics-mlb-batting-order-v2"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

export const getCachedMlbOwnershipModelReport = unstable_cache(
  (selectedSlateId: number | null, sortBy: OwnershipDetailSort) => getMlbOwnershipModelReport(selectedSlateId, sortBy),
  ["analytics-mlb-ownership-model-v2"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

export const getCachedMlbRunEnvironmentReport = unstable_cache(
  () => getMlbRunEnvironmentReport(),
  ["analytics-mlb-run-environment"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

export const getCachedMlbPitcherLineupReport = unstable_cache(
  () => getMlbPitcherLineupReport(),
  ["analytics-mlb-pitcher-lineup"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

export const getCachedMlbBlowupCandidateReport = unstable_cache(
  () => getMlbBlowupCandidateReport(),
  ["analytics-mlb-blowup-candidates"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Projection source breakdown
// ---------------------------------------------------------------------------
export const getCachedProjectionSourceBreakdown = unstable_cache(
  (sport: Sport) => getProjectionSourceBreakdown(sport),
  ["analytics-proj-source"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Stat-level accuracy (requires actual_pts / actual_reb / … columns)
// ---------------------------------------------------------------------------
export const getCachedStatLevelAccuracy = unstable_cache(
  (sport: Sport) => getStatLevelAccuracy(sport),
  ["analytics-stat-level"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Game total model accuracy (NBA only, no args)
// ---------------------------------------------------------------------------
export const getCachedGameTotalModelAccuracy = unstable_cache(
  () => getGameTotalModelAccuracy(),
  ["analytics-game-total-model"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

// ---------------------------------------------------------------------------
// Perfect lineup LP-solver results (expensive — runs once per sport per hour)
// ---------------------------------------------------------------------------
export const getCachedNbaPerfectLineupAnalytics = unstable_cache(
  () => getNbaPerfectLineupAnalytics(),
  ["analytics-perfect-lineup-nba"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);

export const getCachedMlbPerfectLineupAnalytics = unstable_cache(
  () => getMlbPerfectLineupAnalytics(),
  ["analytics-perfect-lineup-mlb-v2"],
  { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
);
