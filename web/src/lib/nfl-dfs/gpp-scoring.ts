import type { NflOptimizerSettings } from '@/app/dfs/nfl/nfl-optimizer';
import type { PlayerExposurePolicy } from './exposure-plan';

/**
 * How the build form scores a GPP lineup, and which way each format starts
 * (`nfl-gpp-scoring-default-v1`, 2026-11-11).
 *
 * The optimizer knows two scoring modes, `draws` and `ceiling`. The form adds
 * a third choice that is `ceiling` with its two knobs turned down:
 *
 *   plain projection = gppScoring "ceiling", maxCeilingMultiple 1, boomWeight 0
 *
 * so a player is credited `min(P90, projection)`, his projection, plus the
 * random nudge (`randomness x projection`) that makes lineups differ. No
 * optimizer code changes for it.
 *
 * Why Classic GPP starts there. Rebuilt on six 2026 main slates (weeks 3-8)
 * with today's projection steps and paid out of six real contests a week with
 * their real prize tables, each payout capped at 50x the fee:
 *
 *   per $1                              capped return   cash   top 10%
 *   plain projection, page settings          0.60       22.0%    9.9%
 *   simulated games, page settings           0.41       15.9%    5.9%
 *   whole field                              0.60       22.9%   10.0%
 *   heavy entrants (prior record only)       0.70       27.3%   12.5%
 *
 * Paired by week, six reseeded builds each: +0.18 per $1 (90% t interval
 * -0.05 to +0.42, ahead in 4 of 6 weeks), +5.7 real points a lineup in all six
 * weeks. Simulated games and player ceilings both reward a wide range, and the
 * range is widest (P90 about 2.5x the projection) on players the field owns
 * under 2%: 17% of the simulated-games roster spots went to them against the
 * field's 7%, and 15% to players the field owned 20%+ against the field's 24%.
 *
 * What it does not show: any gain in the top 1% of finishes (six weeks cannot
 * see it), or that 0.25 is the right amount of noise (0.25 against 0.08 was
 * +0.07 with an interval across zero). The un-anchored 0.70 scale and the
 * range calibration were fitted on these same weeks; without them the gain is
 * +0.08 (-0.08 to +0.25, 3 of 6 weeks). It brings a build to about the field's
 * return, not above it. Showdown was not tested and keeps simulated games.
 */
export const GPP_SCORING_DEFAULT_VERSION = 'nfl-gpp-scoring-default-v1';

export type GppScoringKind = 'draws' | 'ceiling' | 'mean';

/**
 * The optimizer's own defaults for the two ceiling knobs, restated here so the
 * build form (a client component) does not import the optimizer module. A test
 * holds them equal to `DEFAULT_MAX_CEILING_MULTIPLE` and `DEFAULT_BOOM_WEIGHT`.
 */
export const DEFAULT_MAX_CEILING_MULTIPLE = 2.5;
export const DEFAULT_BOOM_WEIGHT = 2;

/** Noise for a plain-projection build: the level the six-week rebuild used. */
export const PLAIN_RANDOMNESS = 0.25;
/** Noise for the other two modes (unused by simulated games). */
export const STANDARD_RANDOMNESS = 0.08;

export type GppScoringFields = Pick<NflOptimizerSettings, 'gppScoring' | 'maxCeilingMultiple' | 'boomWeight'>;

/** The settings each choice stands for. Picking a choice sets all four. */
export function scoringPreset(kind: GppScoringKind): Required<GppScoringFields> & { randomness: number } {
  if (kind === 'mean') return { gppScoring: 'ceiling', maxCeilingMultiple: 1, boomWeight: 0, randomness: PLAIN_RANDOMNESS };
  return { gppScoring: kind, maxCeilingMultiple: DEFAULT_MAX_CEILING_MULTIPLE, boomWeight: DEFAULT_BOOM_WEIGHT, randomness: STANDARD_RANDOMNESS };
}

/** Which choice a set of settings is. Absent knobs mean the optimizer's defaults (2.5 and 2). */
export function scoringKindOf(settings: GppScoringFields): GppScoringKind {
  if (settings.gppScoring === 'draws') return 'draws';
  return (settings.maxCeilingMultiple ?? DEFAULT_MAX_CEILING_MULTIPLE) <= 1 && (settings.boomWeight ?? DEFAULT_BOOM_WEIGHT) === 0 ? 'mean' : 'ceiling';
}

/**
 * The scoring a new build starts on. Classic GPP: plain projection. Everything
 * else: simulated games, as before (cash ignores GPP scoring; the Diversified
 * shape runs its own search and must keep the optimizer's ceiling defaults).
 */
export function defaultGppScoringKind(format: 'classic' | 'showdown', mode: 'cash' | 'gpp', shape: string | undefined): GppScoringKind {
  return format === 'classic' && mode === 'gpp' && shape !== 'diversified' ? 'mean' : 'draws';
}

/**
 * A defense's cap in a plain-projection Classic build.
 *
 * Scoring on projection alone, the best-projected defense averaged 35% to 54%
 * of lineups across the six rebuilt weeks (at the flat 54% cap in three); the
 * field's most-owned defense was 10% to 32%. A defense's score is mostly turnovers and
 * touchdowns, so half a portfolio on one is a concentration the projection
 * does not earn. 30% is a stated limit near the top of the field's range.
 *
 * Rebuilt on the same six weeks with the cap (six reseeded builds a week), it
 * moved nothing that could be measured: capped return +0.01 per $1 (90% t
 * interval -0.05 to +0.06), cash rate -0.2 points, top 10% -0.3 points. It is
 * a limit on concentration, not a source of return.
 */
export const PLAIN_DST_MAX_EXPOSURE = 0.30;

/**
 * Add the defense cap to a plain-projection Classic GPP build.
 *
 * It is a per-player policy marked as not the user's, so it never shows up in
 * the form as a typed range, and it yields to a lock or to any range the user
 * set on that defense. Skipped when the cap would round to zero lineups.
 */
export function withPlainDstCap(settings: NflOptimizerSettings, dstIds: readonly number[]): NflOptimizerSettings {
  if (settings.format !== 'classic' || settings.mode !== 'gpp' || settings.buildShape === 'diversified') return settings;
  if (scoringKindOf(settings) !== 'mean' || Math.floor(PLAIN_DST_MAX_EXPOSURE * settings.nLineups + 1e-9) < 1) return settings;
  const taken = new Set((settings.exposurePolicies ?? []).map((policy) => policy.playerId));
  const locked = new Set(settings.lockedPlayerIds);
  const added: PlayerExposurePolicy[] = [...new Set(dstIds)].sort((a, b) => a - b)
    .filter((id) => !taken.has(id) && !locked.has(id)
      && settings.maxExposureByPlayer[String(id)] == null && settings.minExposureByPlayer[String(id)] == null)
    .map((id) => ({ playerId: id, overall: { minPct: null, maxPct: PLAIN_DST_MAX_EXPOSURE }, captain: { minPct: null, maxPct: null },
      flex: { minPct: null, maxPct: null }, exactTargetMode: false, overallFromUser: false }));
  return added.length ? { ...settings, exposurePolicies: [...(settings.exposurePolicies ?? []), ...added] } : settings;
}

/**
 * The Diversified shape scores with the optimizer's own ceiling defaults; the
 * form's plain-projection knobs must not reach it.
 */
export function withoutScoringKnobsForDiversified<T extends GppScoringFields & { buildShape?: string }>(settings: T): T {
  return settings.buildShape === 'diversified' ? { ...settings, maxCeilingMultiple: undefined, boomWeight: undefined } : settings;
}

/** A starter-sized model projection (points, before the un-anchored scale). */
export const UNLINED_MIN_MODEL_POINTS = 10;

export interface UnlinedPlayer { name: string; position: string; team: string; modelPoints: number; usedPoints: number; ownPct: number | null }

/**
 * Starters a plain-projection build will mostly skip for want of betting lines.
 *
 * An RB, WR or TE without a full set of prop lines keeps the model's number
 * times 0.70. Scored on projection, that usually takes him out of the build,
 * and in weeks 7 and 8 the player it happened to (lines not posted when the
 * build ran) was owned 20% by the field and scored 22 and 26. Lines mostly
 * post by Sunday morning, so the fix is to rebuild then or set his range.
 */
export function unlinedStarters(players: ReadonlyArray<{
  name: string; position: string; team: string; isOut?: boolean; projectionStatus?: string; ourProj: number | null; ownPct?: number | null;
  projectionCorrection?: { rule: string; before: { mean: number } } | null;
}>): UnlinedPlayer[] {
  return players.flatMap((player) => {
    const correction = player.projectionCorrection;
    if (player.isOut || player.projectionStatus === 'out' || correction?.rule !== 'unanchored_skill_shrink') return [];
    if (!(correction.before.mean >= UNLINED_MIN_MODEL_POINTS)) return [];
    return [{ name: player.name, position: player.position, team: player.team, modelPoints: correction.before.mean,
      usedPoints: player.ourProj ?? 0, ownPct: player.ownPct ?? null }];
  }).sort((a, b) => b.modelPoints - a.modelPoints);
}
