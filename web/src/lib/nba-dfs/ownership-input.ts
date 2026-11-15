/**
 * The ownership and leverage the NBA DraftKings optimizer reads
 * (`nba-ownership-input-v1`, 2026-11-14).
 *
 * Why this exists: the 2026-11-14 winners study (21 nights of stat-api
 * DraftKings Classic fields) found that this season the optimizer's ownership
 * rules never fired. LineStar was not connected, so it read our internal
 * estimate, which spreads 800% evenly (top player 14.5% on Oct 24, against
 * 75% real) and ranks players poorly (Spearman 0.42 vs stat-api's 0.93 on the
 * same players). It also found that leverage gave a player with no DraftKings
 * average a "field edge" equal to his whole projection.
 *
 * Order of ownership sources, per player:
 *   1. stat-api's projected GPP ownership (`statapi_own_pct`), when it covers
 *      80% of the slate's playable players and, if today's refresh failed, is
 *      under 90 minutes old. Not in `gpp_ls`, which is the LineStar mode.
 *   2. Whatever the pool already carries (calibrated LineStar, then the live
 *      blend, then our internal estimate). It counts as LineStar only when
 *      `linestar_own_pct` is present.
 *
 * Leverage is recomputed from the ownership actually used, with no field
 * edge when the player has no field projection.
 *
 * Pure: no database, no network. Tested in scripts/test-nba-ownership-input.ts.
 */

export const NBA_OWNERSHIP_INPUT_VERSION = "nba-ownership-input-v1";

/** A slate counts as covered when stat-api has this share of its playable players. */
export const STATAPI_MIN_COVERAGE = 0.8;

/**
 * When today's stat-api refresh failed, stored ownership older than this is
 * not used (it predates late news); the old chain runs and the warning says
 * why. Stated prior.
 */
export const STATAPI_MAX_STALE_MINUTES = 90;

export type NbaOwnershipSource = "statapi" | "linestar" | "internal" | "none";

export type NbaOwnershipInputPlayer = {
  id: number;
  salary: number;
  isOut: boolean | null;
  ourProj: number | null;
  ourLeverage: number | null;
  projOwnPct: number | null;
  avgFptsDk: number | null;
  linestarProj: number | null;
  rawLsProjOwnPct?: number | null;
  /**
   * LineStar's own ownership column (`linestar_own_pct`), written only by a
   * LineStar load. `proj_own_pct` cannot tell LineStar apart: the Python slate
   * loader writes a baseline estimate there when LineStar is missing.
   */
  linestarOwnPct?: number | null;
  statapiOwnPct?: number | null;
  /** When `statapiOwnPct` was stored (ISO). */
  statapiOwnAt?: string | null;
  spg?: number | null;
  bpg?: number | null;
  /** Projected field-average lineup ownership for the slate, stamped on every player. */
  fieldOwnershipSum?: number | null;
};

/** What the stat-api refresh reported for this run (null when it was not attempted). */
export type NbaStatApiPullStatus = { ok: boolean; skipped?: boolean; message: string } | null;

export type NbaOwnershipInputSummary = {
  version: string;
  source: NbaOwnershipSource;
  /** Playable players (not out, positive projection). */
  playable: number;
  /** Playable players with a stat-api ownership number. */
  statapiCovered: number;
  /** Minutes since the newest stored stat-api ownership; null when none is stored. */
  statapiAgeMinutes: number | null;
  /** Highest ownership any playable player carries after this step. */
  maxOwnPct: number | null;
  /** Projected field-average lineup ownership, sum(own^2)/100; null when the source is not calibrated. */
  fieldOwnershipSum: number | null;
  warning: string | null;
};

function finite(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

/**
 * Leverage = field edge x (1 - ownership)^0.7 x ceiling bonus.
 * Unlike the shared `computeLeverage` (still used by MLB), a player with no
 * field projection gets no edge: the old fallback made the edge his whole
 * projection, which handed deep-bench players with no DraftKings average the
 * biggest leverage on the slate.
 */
export function computeNbaLeverage(
  projection: number,
  ownPct: number,
  fieldProj: number | null,
  spg = 0,
  bpg = 0,
): number {
  // A DraftKings average of 0 means DraftKings has no games for the player,
  // which is the same as having no field projection.
  if (fieldProj == null || !Number.isFinite(fieldProj) || fieldProj <= 0) return 0;
  const edge = projection - fieldProj;
  const ownFraction = Math.max(0, Math.min(1, ownPct / 100));
  const ceilingBonus = 1 + (finite(spg) ?? 0) * 0.05 + (finite(bpg) ?? 0) * 0.04;
  return Math.round(edge * Math.pow(1 - ownFraction, 0.7) * ceilingBonus * 1000) / 1000;
}

/**
 * The field's average lineup ownership, forecast before lock: each lineup
 * holds player i with probability own_i/100 and his ownership is own_i, so the
 * expected sum is sum(own_i^2)/100. On the study's flagships it tracked the
 * real field average with correlation 0.92 (Oct 24: stat-api 222, actual 216).
 */
export function projectedFieldOwnershipSum(ownPcts: Array<number | null | undefined>): number {
  let total = 0;
  for (const own of ownPcts) {
    const value = finite(own);
    if (value != null && value > 0) total += (value * value) / 100;
  }
  return Math.round(total * 10) / 10;
}

/**
 * The field projection leverage measures against: DraftKings' average when it
 * is above 0, else LineStar's projection, else none. One rule for the slate
 * page (applyNbaOwnershipModels) and the optimizer, so they cannot disagree.
 */
export function nbaFieldProjection(avgFptsDk: number | null | undefined, linestarProj: number | null | undefined): number | null {
  const dk = finite(avgFptsDk);
  return dk != null && dk > 0 ? dk : finite(linestarProj);
}

function isPlayable(player: NbaOwnershipInputPlayer): boolean {
  return !player.isOut && (finite(player.ourProj) ?? 0) > 0 && player.salary > 0;
}

type StatApiProjectionRow = {
  player?: { operator_player_export_id?: string | number | null };
  ownership?: Array<{
    contest_kind?: string;
    size_tier?: string;
    multi_entry_per_user_band?: string;
    projected_ownership?: string | number | null;
  }>;
};

/**
 * stat-api's projected GPP ownership in percent, keyed by DraftKings
 * draftable id (`operator_player_export_id`, our `dk_player_id`), from a
 * `/slates/{id}/projections` response. Reads the large multi-entry GPP profile
 * (10k-100k entries, 20+ entries per user), the one the winners study
 * validated; any other GPP profile only when that one is missing.
 */
export function parseStatApiNbaOwnership(projections: unknown): Map<number, number> {
  const rows = ((projections as { projections?: StatApiProjectionRow[] })?.projections ?? []);
  const result = new Map<number, number>();
  for (const row of rows) {
    const dkId = Number(row.player?.operator_player_export_id);
    if (!Number.isFinite(dkId) || dkId <= 0) continue;
    const shareOf = (o: { projected_ownership?: string | number | null }) => {
      if (o.projected_ownership == null || o.projected_ownership === "") return null;
      const share = Number(o.projected_ownership);
      return Number.isFinite(share) && share >= 0 && share <= 1 ? share : null;
    };
    const profiles = (row.ownership ?? []).filter((o) => o.contest_kind === "gpp" && shareOf(o) != null);
    const preferred = profiles.find((o) => o.size_tier === "10k-100k" && o.multi_entry_per_user_band === "20+")
      ?? profiles[0];
    if (!preferred) continue;
    result.set(dkId, Math.round((shareOf(preferred) as number) * 1000) / 10);
  }
  return result;
}

/**
 * The run's ownership warning: the summary's own warning, plus what happened
 * to the stat-api refresh when it failed. `gpp_ls` never reads stat-api, so a
 * gpp_ls run gets no stat-api note.
 */
export function describeNbaOwnershipInput(
  summary: NbaOwnershipInputSummary,
  statApiPull: NbaStatApiPullStatus,
  mode: string,
): string | undefined {
  const parts: string[] = [];
  if (summary.warning) parts.push(summary.warning);
  if (mode !== "gpp_ls" && statApiPull && !statApiPull.ok) {
    parts.push(summary.source === "statapi" && summary.statapiAgeMinutes != null
      ? `stat-api refresh failed (${statApiPull.message}); using stat-api ownership stored ${summary.statapiAgeMinutes} minutes ago, which may miss late news.`
      : `stat-api: ${statApiPull.message}`);
  }
  return parts.length ? parts.join(" ") : undefined;
}

export function applyNbaOwnershipInput<T extends NbaOwnershipInputPlayer>(
  pool: T[],
  options: { mode: string; tournament: boolean; statApiPull?: NbaStatApiPullStatus; now?: number },
): { pool: T[]; summary: NbaOwnershipInputSummary } {
  const isLsMode = options.mode === "gpp_ls";
  const playable = pool.filter(isPlayable);
  const statapiCovered = playable.filter((p) => finite(p.statapiOwnPct) != null).length;
  const storedTimes = playable
    .map((p) => (p.statapiOwnAt ? Date.parse(p.statapiOwnAt) : NaN))
    .filter((v) => Number.isFinite(v));
  const now = options.now ?? Date.now();
  const statapiAgeMinutes = storedTimes.length
    ? Math.max(0, Math.round((now - Math.max(...storedTimes)) / 60_000))
    : null;
  const pullFailed = Boolean(options.statApiPull && !options.statApiPull.ok);
  const staleAfterFailure = pullFailed && statapiAgeMinutes != null && statapiAgeMinutes > STATAPI_MAX_STALE_MINUTES;
  const coverageOk = playable.length > 0 && statapiCovered / playable.length >= STATAPI_MIN_COVERAGE;
  const statapiUsable = !isLsMode && coverageOk && !staleAfterFailure;
  const hasLinestar = playable.some((p) => finite(p.linestarOwnPct) != null);
  const source: NbaOwnershipSource = statapiUsable
    ? "statapi"
    : hasLinestar
      ? "linestar"
      : playable.some((p) => finite(p.projOwnPct) != null) ? "internal" : "none";

  const withOwnership = pool.map((player) => {
    const ownership = player.isOut
      ? 0
      : statapiUsable
        // stat-api lists every player on the DraftKings slate, so a playable
        // player it does not list is one nobody expects to roster.
        ? finite(player.statapiOwnPct) ?? 0
        : finite(player.projOwnPct);
    const projection = finite(player.ourProj);
    const fieldProj = nbaFieldProjection(player.avgFptsDk, player.linestarProj);
    const leverage = !player.isOut && projection != null && projection > 0 && ownership != null
      ? computeNbaLeverage(projection, ownership, fieldProj, player.spg ?? 0, player.bpg ?? 0)
      : null;
    return { ...player, projOwnPct: ownership, ourLeverage: leverage };
  });

  // Callers pass the whole slate (before any game filter): contest ownership
  // does not change because the build uses fewer games.
  const calibrated = source === "statapi" || source === "linestar";
  const fieldOwnershipSum = calibrated
    ? projectedFieldOwnershipSum(withOwnership.filter(isPlayable).map((p) => p.projOwnPct))
    : null;
  const stamped = withOwnership.map((player) => ({ ...player, fieldOwnershipSum }));

  const owns = stamped.filter(isPlayable).map((p) => finite(p.projOwnPct)).filter((v): v is number => v != null);
  const maxOwnPct = owns.length ? Math.max(...owns) : null;
  const loadHint = isLsMode ? "Load LineStar ownership." : "Load stat-api ownership (needs STAT_API_KEY) or LineStar.";

  let warning: string | null = null;
  if (options.tournament && staleAfterFailure && !isLsMode) {
    warning = `stat-api refresh failed and the stored stat-api ownership is ${statapiAgeMinutes} minutes old (limit ${STATAPI_MAX_STALE_MINUTES}), so it was not used.`;
  }
  if (options.tournament && !calibrated) {
    const flat = source === "none"
      ? `No ownership numbers for this slate, so the tournament ownership rules are off. ${loadHint}`
      : `Ownership is our internal estimate (top player ${maxOwnPct?.toFixed(1) ?? "?"}%), far flatter than real ownership, so the tournament ownership rules barely act. ${loadHint}`;
    warning = warning ? `${warning} ${flat}` : flat;
  } else if (options.tournament && !isLsMode && !coverageOk && statapiCovered > 0) {
    warning = `stat-api ownership covers only ${statapiCovered} of ${playable.length} playable players, under the ${Math.round(STATAPI_MIN_COVERAGE * 100)}% needed, so it was not used.`;
  }

  return {
    pool: stamped,
    summary: {
      version: NBA_OWNERSHIP_INPUT_VERSION,
      source,
      playable: playable.length,
      statapiCovered,
      statapiAgeMinutes,
      maxOwnPct,
      fieldOwnershipSum,
      warning,
    },
  };
}
