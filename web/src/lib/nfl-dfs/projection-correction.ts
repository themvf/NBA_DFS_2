/**
 * Projection corrections (`nfl-projection-correction-v1`, owner's decision
 * 2026-11-03): three fixes read off the pregame pool captures of 2026 weeks
 * 2-8 (63 slates, 3,933 served projections against DraftKings results) and
 * applied in the workspace read AFTER the market anchor and BEFORE the
 * long-memory ranges, the ownership model and the optimizer, so every
 * downstream number builds from the corrected projection. Each changed row
 * keeps the number it replaced in `before`, and the pool capture records the
 * row, so every week can be graded corrected-versus-uncorrected.
 *
 * 1. `unanchored_skill_shrink` (Classic): an RB, WR or TE with games of his
 *    own, no FULL set of prop lines and no absence commit, projected at 3+
 *    points, is multiplied by 0.70. Served unanchored skill players were
 *    over-projected by a quarter on the post-anchor weeks (bias -1.90, 7.62
 *    projected vs 5.72 scored, n=275); fitted on each week and checked on the
 *    others, the factor held every time: held-out MAE 5.38 -> 4.64 (week 6, k
 *    0.70), 3.57 -> 3.40 (week 7, 0.55), 4.65 -> 3.00 (week 8, 0.70). TE and
 *    WR carry it (0.50-0.80); RB barely needs it (0.75-0.95). A player
 *    anchored on the anytime-TD line ALONE (`mode: 'td_only'`, v3 of the
 *    anchor, live from week 8) is included: his volume and yards are still
 *    the model's, these players were inside the fitted population in weeks
 *    6-7 before TD-only anchors existed, and in week 8 they read the same
 *    way (4.86 projected vs 2.73 scored, n=33; MAE 3.50 -> 2.92 at 0.70).
 *    Without them the rule is nearly inert: on the 2026-11-01 Classic 82 of
 *    246 skill players were TD-only anchored and 1 was unanchored. Full-set
 *    anchors are NOT touched (bias -0.03 to -0.27 in weeks 7-8). Showdown is
 *    NOT touched: the same fit made its held-out week worse (4.79 -> 5.23,
 *    n=74).
 * 2. `backup_qb_zero` (Classic): every quarterback who is not his team's
 *    expected starter is zeroed and blocked. 66 served backups averaged 10
 *    points at $4,100 and scored 0 in 88% of their games (bias -9.05). The
 *    expected starter is, in order: the quarterback the user confirmed, the
 *    one and only quarterback the depth chart lists QB1 (unblocked), else the
 *    team's top-projected active quarterback (the top-projected one scored
 *    14.2 a game on the same slates). A team with one active quarterback is
 *    left alone.
 * 3. `showdown_dst_scale` (Showdown): a DST is multiplied by 0.65, including
 *    its special-teams candidate block when it carries one. Showdown DSTs were
 *    over-projected in 5 of 7 weeks (6.32 projected vs 4.15 scored, n=68,
 *    bias -2.17) with or without the special-teams flag. Classic DSTs are
 *    unbiased (+0.45) and untouched.
 *
 * The fourth fix of the same decision, leaving position-prior rows out of
 * the optimizer pool unless locked, is a pool rule and lives in
 * `nfl-optimizer.ts` (`POSITION_PRIOR` eligibility).
 *
 * Composition with `nfl-range-calibration-v1` (range-calibration.ts, merged
 * the same day, fitted on the same captures over ALL QB/RB/WR/TE and applied
 * after ownership): its per-position line a + b x mean passes near identity
 * at the unanchored players' averages (WR 7.6 -> 7.6), so rule 1 carries the
 * unanchored slice and the line compresses the anchored stars rule 1 exempts;
 * its not-play step skips a quarterback rule 2 has already zeroed (`isOut`),
 * and it does not touch DST. A row rule 1 scaled to 5 is nudged back to about
 * 4 by the line (net 0.80 instead of 0.70), which is accepted and recorded.
 *
 * Stated constants, fitted on one season's weeks: change one and bump the
 * version, so a graded week names the rule it ran under.
 */
import { scaleDistribution } from './absence-commit';
import { BOOM_THRESHOLDS } from './replacement-upside';

export const PROJECTION_CORRECTION_VERSION = 'nfl-projection-correction-v1';
/** Rule 1 factor on an unanchored Classic RB/WR/TE. */
export const UNANCHORED_SKILL_SHRINK = 0.70;
/** Rule 1 applies from this projection up (the fitted population). */
export const UNANCHORED_SKILL_MIN_PROJECTION = 3;
export const UNANCHORED_SKILL_POSITIONS: readonly string[] = ['RB', 'WR', 'TE'];
/** Rule 3 factor on a Showdown DST. */
export const SHOWDOWN_DST_SCALE = 0.65;
/** The depth-chart role `resolveAvailability` gives a listed QB1. */
export const EXPECTED_STARTER_ROLE = 'Expected starter · QB1';

export type CorrectionRule = 'unanchored_skill_shrink' | 'backup_qb_zero' | 'showdown_dst_scale';
export type CorrectionFormat = 'classic' | 'showdown';

export interface CorrectionRange { mean: number; p10: number | null; median: number | null; p90: number | null; boom: number | null }

export interface ProjectionCorrection {
  version: string;
  rule: CorrectionRule;
  /** after.mean / before.mean (0 for a zeroed quarterback). */
  factor: number;
  before: CorrectionRange;
  after: CorrectionRange;
  note: string;
  /** Rule 2 only: the quarterback this one lost the job to, and how he was identified. */
  starter?: { name: string; how: 'confirmed' | 'depth_chart' | 'top_projected' };
  /** Rule 1 only: what the player had before the shrink, nothing or a TD-only anchor. */
  anchorMode?: 'none' | 'td_only';
}

export interface CorrectionPlayer {
  dkPlayerId: number;
  name: string;
  team: string;
  position: string;
  isOut: boolean;
  projectionStatus: string;
  historyGames?: number | null;
  ourProj: number | null;
  floorFpts: number | null;
  medianFpts?: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
  /** Only `mode` is read: a `td_only` anchor does not exempt a player from rule 1. */
  marketAnchor?: { mode?: string | null } | null;
  absenceCommit?: unknown | null;
  availability?: { role?: string; blockedReason?: string | null; confirmedStarter?: boolean } | null;
  specialTeams?: { mean: number; p10: number; p50: number; p90: number; boom: number } | null;
}

export type Corrected<P> = P & { projectionCorrection?: ProjectionCorrection | null };

export interface ProjectionCorrectionReport {
  version: string;
  format: CorrectionFormat;
  /** Rule 1: players scaled, and how many RB/WR/TE were considered. */
  shrunk: number;
  skillConsidered: number;
  /** Rule 2: quarterbacks zeroed, with the starter each lost to. */
  qbZeroed: { name: string; team: string; before: number; starter: string; how: 'confirmed' | 'depth_chart' | 'top_projected' }[];
  /** Rule 2: the expected starter named per team (only teams with 2+ active quarterbacks). */
  starters: { team: string; name: string; how: 'confirmed' | 'depth_chart' | 'top_projected' }[];
  /** Rule 3: DSTs scaled. */
  dstScaled: number;
}

const finite = (v: number | null | undefined): v is number => typeof v === 'number' && Number.isFinite(v);
const active = (p: CorrectionPlayer) => !p.isOut && p.projectionStatus !== 'out' && finite(p.ourProj) && p.ourProj > 0;

function range(p: CorrectionPlayer): CorrectionRange {
  return { mean: p.ourProj ?? 0, p10: finite(p.floorFpts) ? p.floorFpts : null, median: finite(p.medianFpts) ? p.medianFpts : null,
    p90: finite(p.ceilingFpts) ? p.ceilingFpts : null, boom: finite(p.boomRate) ? p.boomRate : null };
}

/**
 * Scale a whole range. When the three quantiles are present the boom rate is
 * re-read off the scaled curve at the position's boom line (the absence
 * commit's own method); otherwise only what exists is scaled and the boom
 * rate is dropped rather than left at the unscaled value.
 */
function scaled(before: CorrectionRange, factor: number, position: string): CorrectionRange {
  if (finite(before.p10) && finite(before.median) && finite(before.p90)) {
    const d = scaleDistribution({ mean: before.mean, p10: before.p10, median: before.median, p90: before.p90, boom: null }, factor,
      BOOM_THRESHOLDS[position] ?? null);
    return { mean: d.mean, p10: d.p10, median: d.median, p90: d.p90, boom: d.boom };
  }
  return { mean: before.mean * factor, p10: finite(before.p10) ? before.p10 * factor : null,
    median: finite(before.median) ? before.median * factor : null, p90: finite(before.p90) ? before.p90 * factor : null, boom: null };
}

function apply<P extends CorrectionPlayer>(p: P, correction: ProjectionCorrection): Corrected<P> {
  return { ...p, ourProj: correction.after.mean, floorFpts: correction.after.p10, medianFpts: correction.after.median,
    ceilingFpts: correction.after.p90, boomRate: correction.after.boom, projectionCorrection: correction };
}

/** Rule 2: the expected starter among a team's active quarterbacks, or null when there is nothing to decide. */
export function expectedStarter<P extends CorrectionPlayer>(qbs: readonly P[], confirmedDkPlayerId?: number | null)
  : { starter: P; how: 'confirmed' | 'depth_chart' | 'top_projected' } | null {
  const live = qbs.filter(active);
  if (live.length < 2) return null;
  const confirmed = live.find((q) => q.dkPlayerId === confirmedDkPlayerId || q.availability?.confirmedStarter === true);
  if (confirmed) return { starter: confirmed, how: 'confirmed' };
  const listed = live.filter((q) => q.availability?.role === EXPECTED_STARTER_ROLE && !q.availability?.blockedReason);
  if (listed.length === 1) return { starter: listed[0], how: 'depth_chart' };
  const top = [...live].sort((a, b) => (b.ourProj ?? 0) - (a.ourProj ?? 0))[0];
  return { starter: top, how: 'top_projected' };
}

/**
 * Apply the three rules to one slate's players. Pure: rows are copied, never
 * mutated, and a player at most one rule applies to (the rules are disjoint
 * by position). `confirmedStartingQbs` is team -> DK player id, as the page
 * sends it.
 */
export function correctProjections<P extends CorrectionPlayer>(players: readonly P[],
  options: { format: CorrectionFormat; confirmedStartingQbs?: Record<string, number> | null }): { players: Corrected<P>[]; report: ProjectionCorrectionReport } {
  const { format } = options;
  const report: ProjectionCorrectionReport = { version: PROJECTION_CORRECTION_VERSION, format, shrunk: 0, skillConsidered: 0, qbZeroed: [], starters: [], dstScaled: 0 };
  const corrections = new Map<number, ProjectionCorrection>();
  const blocked = new Map<number, string>();

  if (format === 'classic') {
    const byTeam = new Map<string, P[]>();
    for (const p of players) {
      if (p.position !== 'QB') continue;
      const key = (p.team ?? '').toUpperCase();
      byTeam.set(key, [...(byTeam.get(key) ?? []), p]);
    }
    for (const [team, qbs] of [...byTeam.entries()].sort(([a], [b]) => a.localeCompare(b))) {
      const confirmedId = options.confirmedStartingQbs
        ? Object.entries(options.confirmedStartingQbs).find(([t]) => t.toUpperCase() === team)?.[1] ?? null : null;
      const found = expectedStarter(qbs, confirmedId);
      if (!found) continue;
      report.starters.push({ team, name: found.starter.name, how: found.how });
      const howText = found.how === 'confirmed' ? 'confirmed by you' : found.how === 'depth_chart' ? 'listed QB1 on the depth chart' : 'the top projected quarterback';
      for (const q of qbs) {
        if (q === found.starter || !active(q)) continue;
        const before = range(q);
        const note = `Not ${team}'s expected starter (${found.starter.name}, ${howText}); starter workload not supported. Backup quarterbacks on 2026 slates scored 0 in 88% of their games against a 10-point projection (nfl-projection-correction-v1). Confirm him as the starter to use him.`;
        corrections.set(q.dkPlayerId, { version: PROJECTION_CORRECTION_VERSION, rule: 'backup_qb_zero', factor: 0, before,
          after: { mean: 0, p10: 0, median: 0, p90: 0, boom: 0 }, note, starter: { name: found.starter.name, how: found.how } });
        blocked.set(q.dkPlayerId, note);
        report.qbZeroed.push({ name: q.name, team, before: before.mean, starter: found.starter.name, how: found.how });
      }
    }
  }

  const out = players.map((p): Corrected<P> => {
    const qb = corrections.get(p.dkPlayerId);
    if (qb) {
      const reason = blocked.get(p.dkPlayerId)!;
      return { ...apply(p, qb), isOut: true, availability: p.availability ? { ...p.availability, blockedReason: reason } : p.availability };
    }
    if (format === 'classic' && UNANCHORED_SKILL_POSITIONS.includes(p.position) && active(p)) {
      report.skillConsidered += 1;
      const fullAnchor = Boolean(p.marketAnchor) && p.marketAnchor?.mode !== 'td_only';
      if (fullAnchor || p.absenceCommit || (p.historyGames ?? 0) <= 0 || (p.ourProj ?? 0) < UNANCHORED_SKILL_MIN_PROJECTION) return p;
      const before = range(p);
      const after = scaled(before, UNANCHORED_SKILL_SHRINK, p.position);
      report.shrunk += 1;
      return apply(p, { version: PROJECTION_CORRECTION_VERSION, rule: 'unanchored_skill_shrink', factor: UNANCHORED_SKILL_SHRINK, before, after,
        anchorMode: p.marketAnchor ? 'td_only' : 'none',
        note: `${p.marketAnchor ? 'Only a touchdown line' : 'No prop line'}: ${p.position}s without a full set of lines were over-projected by a quarter on 2026 weeks 6-8, so the ${p.marketAnchor ? 'TD-anchored' : "model's"} ${before.mean.toFixed(1)} is scaled by ${UNANCHORED_SKILL_SHRINK} to ${after.mean.toFixed(1)}.` });
    }
    if (format === 'showdown' && p.position === 'DST' && active(p)) {
      const before = range(p);
      const after = scaled(before, SHOWDOWN_DST_SCALE, 'DST');
      report.dstScaled += 1;
      const corrected = apply(p, { version: PROJECTION_CORRECTION_VERSION, rule: 'showdown_dst_scale', factor: SHOWDOWN_DST_SCALE, before, after,
        note: `Showdown DSTs were over-projected by 2.2 points on 2026 weeks 2-8 (6.3 projected, 4.2 scored), so the model's ${before.mean.toFixed(1)} is scaled by ${SHOWDOWN_DST_SCALE} to ${after.mean.toFixed(1)}.` });
      if (p.specialTeams) {
        const st = scaleDistribution({ mean: p.specialTeams.mean, p10: p.specialTeams.p10, median: p.specialTeams.p50, p90: p.specialTeams.p90, boom: null },
          SHOWDOWN_DST_SCALE, BOOM_THRESHOLDS.DST ?? null);
        corrected.specialTeams = { ...p.specialTeams, mean: st.mean, p10: st.p10, p50: st.median, p90: st.p90, boom: st.boom ?? p.specialTeams.boom * SHOWDOWN_DST_SCALE };
      }
      return corrected;
    }
    return p;
  });
  return { players: out, report };
}
