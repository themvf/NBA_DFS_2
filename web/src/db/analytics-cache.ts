/**
 * Cached wrappers for analytics queries.
 *
 * All analytics data is historical and changes only when new results are
 * uploaded.  Caching at 1 h prevents repeated cold-start + LP-solver runs
 * from blowing Vercel's 10 s Hobby function timeout.
 *
 * Call `revalidateTag(ANALYTICS_CACHE_TAG)` after every results upload so the
 * next page load gets fresh data.
 */
import { unstable_cache } from "next/cache";

import {
  getCrossSlateAccuracy,
  getLeverageCalibration,
  getMlbBattingOrderCalibration,
  getMlbPerfectLineupAnalytics,
  getNbaPerfectLineupAnalytics,
  getOwnershipVsTeamTotal,
  getPositionAccuracy,
  getProjectionSourceBreakdown,
  getSalaryTierAccuracy,
  getStatLevelAccuracy,
  getGameTotalModelAccuracy,
} from "./queries";
import type { Sport } from "./queries";

export const ANALYTICS_CACHE_TAG = "analytics";
const REVALIDATE = 3600; // 1 hour

// ---------------------------------------------------------------------------
// Cross-slate accuracy
// ---------------------------------------------------------------------------
export const getCachedCrossSlateAccuracy = (sport: Sport) =>
  unstable_cache(
    () => getCrossSlateAccuracy(sport),
    [`analytics-cross-slate-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Position accuracy
// ---------------------------------------------------------------------------
export const getCachedPositionAccuracy = (sport: Sport) =>
  unstable_cache(
    () => getPositionAccuracy(sport),
    [`analytics-position-accuracy-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Salary tier accuracy
// ---------------------------------------------------------------------------
export const getCachedSalaryTierAccuracy = (sport: Sport) =>
  unstable_cache(
    () => getSalaryTierAccuracy(sport),
    [`analytics-salary-tier-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Leverage calibration
// ---------------------------------------------------------------------------
export const getCachedLeverageCalibration = (sport: Sport) =>
  unstable_cache(
    () => getLeverageCalibration(sport),
    [`analytics-leverage-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Ownership vs team total
// ---------------------------------------------------------------------------
export const getCachedOwnershipVsTeamTotal = (sport: Sport) =>
  unstable_cache(
    () => getOwnershipVsTeamTotal(sport),
    [`analytics-own-vs-total-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// MLB batting order calibration
// ---------------------------------------------------------------------------
export const getCachedMlbBattingOrderCalibration = () =>
  unstable_cache(
    () => getMlbBattingOrderCalibration(),
    ["analytics-mlb-batting-order"],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Projection source breakdown
// ---------------------------------------------------------------------------
export const getCachedProjectionSourceBreakdown = (sport: Sport) =>
  unstable_cache(
    () => getProjectionSourceBreakdown(sport),
    [`analytics-proj-source-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Stat-level accuracy (requires actual_pts / actual_reb / … columns)
// ---------------------------------------------------------------------------
export const getCachedStatLevelAccuracy = (sport: Sport) =>
  unstable_cache(
    () => getStatLevelAccuracy(sport),
    [`analytics-stat-level-${sport}`],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Game total model accuracy (NBA only)
// ---------------------------------------------------------------------------
export const getCachedGameTotalModelAccuracy = () =>
  unstable_cache(
    () => getGameTotalModelAccuracy(),
    ["analytics-game-total-model"],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

// ---------------------------------------------------------------------------
// Perfect lineup LP-solver results (expensive — runs once per sport per hour)
// ---------------------------------------------------------------------------
export const getCachedNbaPerfectLineupAnalytics = () =>
  unstable_cache(
    () => getNbaPerfectLineupAnalytics(),
    ["analytics-perfect-lineup-nba"],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();

export const getCachedMlbPerfectLineupAnalytics = () =>
  unstable_cache(
    () => getMlbPerfectLineupAnalytics(),
    ["analytics-perfect-lineup-mlb"],
    { revalidate: REVALIDATE, tags: [ANALYTICS_CACHE_TAG] },
  )();
