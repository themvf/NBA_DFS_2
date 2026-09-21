/**
 * Phase 8 (spec §15): point-in-time contest backtesting.
 *
 * Backtests run on IMMUTABLE point-in-time slate packages: every input carries
 * the as-of time it was actually known. A leakage check fails any package whose
 * source timestamp is after contest lock (P8-AC2). Evaluation is walk-forward by
 * season/week with DISJOINT train/holdout windows (P8-AC4).
 *
 * Metrics are split by data availability and NEVER fabricated:
 *   - construction-quality metrics (legality, calibration, candidate recall)
 *     are always computable from projections + actual results;
 *   - field-relative and ROI metrics require actual contest field + payout data
 *     and are OMITTED when that data is absent (P8-AC3). A release without
 *     contest data may improve construction quality but MUST NOT claim ROI.
 *
 * This module provides the framework and honest gating. It does not ship
 * licensed contest data, which is not present in this repository.
 */

export interface PointInTimeSource {
  id: string;
  /** When this input was actually known. */
  asOf: string;
  digest: string;
}

export interface PointInTimeSlatePackage {
  season: number;
  week: number;
  slateType: "showdown" | "classic";
  contestId: string | null;
  /** Contest lock time. Everything used for generation must be known before this. */
  lockAt: string;
  /** Pre-lock inputs (salaries, projections, roles, odds, ownership estimates). */
  inputs: PointInTimeSource[];
  /** Actual player fantasy results (post-lock facts, used ONLY for scoring). */
  actualPlayerScores: Record<number, number> | null;
  /** Actual contest field, entry fee, payout table, tie rules — often unlicensed/absent. */
  contest: {
    fieldSize: number;
    maxEntries: number;
    entryFee: number;
    payoutTable: Array<{ rankFrom: number; rankTo: number; payout: number }>;
    tieRule: "split" | "duplicate" | null;
    /** Actual winning/field lineups when reconstructable. */
    fieldLineups: Array<{ playerIds: number[]; score: number }> | null;
  } | null;
  codeVersion: string;
  modelVersion: string;
}

export type BacktestCapability = "construction_only" | "field_relative";

export interface LeakageFinding {
  sourceId: string;
  asOf: string;
  detail: string;
}

/** P8-AC2: any input known after lock is leakage and fails the package. */
export function detectLeakage(pkg: PointInTimeSlatePackage): LeakageFinding[] {
  const lock = Date.parse(pkg.lockAt);
  const findings: LeakageFinding[] = [];
  if (!Number.isFinite(lock)) return [{ sourceId: "__lock__", asOf: pkg.lockAt, detail: "Invalid or missing contest lock time." }];
  for (const src of pkg.inputs) {
    const t = Date.parse(src.asOf);
    if (!Number.isFinite(t)) findings.push({ sourceId: src.id, asOf: src.asOf, detail: "Input has no valid as-of time." });
    else if (t > lock) findings.push({ sourceId: src.id, asOf: src.asOf, detail: `Input known ${new Date(t).toISOString()} is AFTER contest lock ${pkg.lockAt}.` });
  }
  return findings;
}

/** Whether a package can support field-relative/ROI metrics, or construction only. */
export function backtestCapability(pkg: PointInTimeSlatePackage): BacktestCapability {
  const hasContest = Boolean(pkg.contest && pkg.contest.payoutTable.length && pkg.contest.fieldLineups && pkg.actualPlayerScores);
  return hasContest ? "field_relative" : "construction_only";
}

export interface ConstructionMetrics {
  /** Fraction of generated lineups that are legal. */
  legalityRate: number;
  /** Whether any generated lineup would have reached the actual top score (recall proxy). */
  bestLineupScore: number | null;
  actualWinningScore: number | null;
  /** Best generated score as a fraction of the actual winning score. */
  ceilingCoverage: number | null;
}

export interface FieldRelativeMetrics {
  capability: "field_relative";
  entryFee: number;
  fieldSize: number;
  /** Realized finish rate of the best lineup (top fraction). Requires field lineups. */
  bestLineupPercentile: number | null;
  /** Net payout across the portfolio minus entry fees. Requires payout table. */
  netPayout: number | null;
  roi: number | null;
}

export interface BacktestRow {
  season: number;
  week: number;
  contestId: string | null;
  capability: BacktestCapability;
  codeVersion: string;
  modelVersion: string;
  leakage: LeakageFinding[];
  construction: ConstructionMetrics;
  field: FieldRelativeMetrics | null;
}

/**
 * Evaluate one slate package against a generated portfolio. Returns a row that
 * traces to the package's inputs and versions (P8-AC1). Refuses to score if
 * leakage is present.
 */
export function evaluateBacktestRow(
  pkg: PointInTimeSlatePackage,
  portfolio: Array<{ playerIds: number[]; legal: boolean }>,
): BacktestRow {
  const leakage = detectLeakage(pkg);
  const capability = backtestCapability(pkg);

  const legalCount = portfolio.filter((l) => l.legal).length;
  const legalityRate = portfolio.length ? legalCount / portfolio.length : 0;

  const scoreLineup = (playerIds: number[]) => pkg.actualPlayerScores
    ? playerIds.reduce((s, id) => s + (pkg.actualPlayerScores![id] ?? 0), 0)
    : null;
  const generatedScores = portfolio.map((l) => scoreLineup(l.playerIds)).filter((s): s is number => s !== null);
  const bestLineupScore = generatedScores.length ? Math.max(...generatedScores) : null;
  const actualWinningScore = pkg.contest?.fieldLineups?.length ? Math.max(...pkg.contest.fieldLineups.map((f) => f.score)) : null;
  const ceilingCoverage = bestLineupScore !== null && actualWinningScore ? bestLineupScore / actualWinningScore : null;

  const construction: ConstructionMetrics = { legalityRate, bestLineupScore, actualWinningScore, ceilingCoverage };

  let field: FieldRelativeMetrics | null = null;
  // Field-relative metrics are produced ONLY when the contest data is present
  // AND there is no leakage. Otherwise they are omitted, not guessed (P8-AC3).
  if (capability === "field_relative" && !leakage.length && pkg.contest && bestLineupScore !== null) {
    const field_ = pkg.contest.fieldLineups!;
    const beaten = field_.filter((f) => f.score < bestLineupScore).length;
    const bestLineupPercentile = field_.length ? 1 - beaten / field_.length : null;
    // Net payout across the portfolio: rank each generated lineup against the field.
    let gross = 0;
    for (const s of generatedScores) {
      const rank = field_.filter((f) => f.score > s).length + 1;
      const bracket = pkg.contest.payoutTable.find((p) => rank >= p.rankFrom && rank <= p.rankTo);
      gross += bracket?.payout ?? 0;
    }
    const cost = portfolio.length * pkg.contest.entryFee;
    field = { capability: "field_relative", entryFee: pkg.contest.entryFee, fieldSize: pkg.contest.fieldSize,
      bestLineupPercentile, netPayout: gross - cost, roi: cost > 0 ? (gross - cost) / cost : null };
  }

  return { season: pkg.season, week: pkg.week, contestId: pkg.contestId, capability, codeVersion: pkg.codeVersion, modelVersion: pkg.modelVersion, leakage, construction, field };
}

// --- Walk-forward evaluation --------------------------------------------

export interface WalkForwardWindow {
  trainSeasons: Array<{ season: number; week: number }>;
  holdout: { season: number; week: number };
}

/** P8-AC4: model selection and holdout evaluation must use DISJOINT periods. */
export function assertDisjointWindow(window: WalkForwardWindow): void {
  const key = (w: { season: number; week: number }) => `${w.season}-${w.week}`;
  const train = new Set(window.trainSeasons.map(key));
  if (train.has(key(window.holdout))) {
    throw new Error(`Leakage: holdout ${key(window.holdout)} appears in the training window.`);
  }
}

export interface PromotionDecision {
  approved: boolean;
  reasons: string[];
}

export interface BacktestSummary {
  rows: BacktestRow[];
  /** Rows with any leakage — excluded from metric aggregation. */
  leakedRows: number;
  /** Split so field-relative slates are never mixed with projection-only ones (P8-AC3). */
  fieldRelativeRows: number;
  constructionOnlyRows: number;
  meanLegalityRate: number;
  meanCeilingCoverage: number | null;
  meanRoi: number | null;
}

export function summarizeBacktest(rows: BacktestRow[]): BacktestSummary {
  const clean = rows.filter((r) => r.leakage.length === 0);
  const fieldRows = clean.filter((r) => r.field);
  const coverage = clean.map((r) => r.construction.ceilingCoverage).filter((c): c is number => c !== null);
  const rois = fieldRows.map((r) => r.field!.roi).filter((r): r is number => r !== null);
  return {
    rows,
    leakedRows: rows.length - clean.length,
    fieldRelativeRows: fieldRows.length,
    constructionOnlyRows: clean.length - fieldRows.length,
    meanLegalityRate: clean.length ? clean.reduce((s, r) => s + r.construction.legalityRate, 0) / clean.length : 0,
    meanCeilingCoverage: coverage.length ? coverage.reduce((a, b) => a + b, 0) / coverage.length : null,
    meanRoi: rois.length ? rois.reduce((a, b) => a + b, 0) / rois.length : null,
  };
}

/**
 * P8-AC5: a promotion decision must cite the experiment, sample and uncertainty,
 * and MUST NOT claim ROI improvement without field-relative data.
 */
export function promotionDecision(summary: BacktestSummary, options: { minSlates: number; requireFieldForRoiClaim: boolean }): PromotionDecision {
  const reasons: string[] = [];
  const usable = summary.rows.length - summary.leakedRows;
  if (summary.leakedRows > 0) reasons.push(`${summary.leakedRows} slate(s) had leakage and were excluded.`);
  if (usable < options.minSlates) { reasons.push(`Only ${usable} clean slate(s); need ${options.minSlates}.`); return { approved: false, reasons }; }
  if (options.requireFieldForRoiClaim && summary.fieldRelativeRows === 0) {
    reasons.push("No field-relative contest data: an ROI improvement cannot be claimed. Construction-quality gains may still be reported.");
  }
  reasons.push(`Mean legality ${(summary.meanLegalityRate * 100).toFixed(1)}% over ${usable} slate(s).`);
  if (summary.meanCeilingCoverage !== null) reasons.push(`Mean ceiling coverage ${(summary.meanCeilingCoverage * 100).toFixed(1)}%.`);
  if (summary.meanRoi !== null) reasons.push(`Mean ROI ${(summary.meanRoi * 100).toFixed(1)}% across ${summary.fieldRelativeRows} field-relative slate(s).`);
  const approved = summary.meanLegalityRate >= 0.99;
  return { approved, reasons };
}
