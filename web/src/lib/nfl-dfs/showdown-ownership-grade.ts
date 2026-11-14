/**
 * Showdown ownership after-slate grade (`nfl-showdown-ownership-grade-v1`):
 * reading the rows ingest/nfl_showdown_ownership_grade.py stores. The grading
 * itself is model/nfl_showdown_ownership_grade.py; nothing is recomputed here.
 *
 * A row holds every ownership estimate that was stored before one Showdown's
 * kickoff, each with its miss against the real field: the mean gap per player
 * in percentage points, captain and flex apart, all models on the same players.
 *
 * Pure. Two models are only ever compared on the slates where both were
 * stored, because an average over different games says nothing about which
 * model is closer.
 */

export const NFL_SHOWDOWN_OWNERSHIP_GRADE_VERSION = "nfl-showdown-ownership-grade-v1";

export type GradeFamily = "lineup_model" | "fitted_model" | "stat_api" | "rule_of_thumb" | "linestar" | "other";
export type GradeRole = "shown" | "ours" | "shadow";

export type SlotGrade = {
  n: number; miss: number | null; bias: number | null; chalk_n: number; chalk_miss: number | null;
  worst: { name: string; estimate: number; real: number }[];
};
export type GradedModel = { role: GradeRole; version: string; family: GradeFamily; captain: SlotGrade | null; flex: SlotGrade | null };
export type OwnershipGradeRow = {
  slateKey: string; game: string; firstKickoff: string; season: number | null; week: number | null;
  contestId: string; contestName: string | null; entryCount: number | null; labelsComplete: boolean;
  captureSource: string; capturedAt: string; models: GradedModel[];
};
export type WaitingSlate = { game: string; kickoff: string; reason: string };

export const FAMILY_LABEL: Record<GradeFamily, string> = {
  lineup_model: "Lineup model", fitted_model: "Per-player model", stat_api: "stat-api",
  rule_of_thumb: "Rule of thumb", linestar: "LineStar", other: "Other",
};
const FAMILY_ORDER: GradeFamily[] = ["lineup_model", "stat_api", "fitted_model", "rule_of_thumb", "linestar", "other"];
const ROLE_ORDER: GradeRole[] = ["shown", "ours", "shadow"];

/** One graded model per family for a slate; the number the build read wins over a copy stored beside it. */
export function modelsByFamily(row: OwnershipGradeRow): Map<GradeFamily, GradedModel> {
  const out = new Map<GradeFamily, GradedModel>();
  for (const role of ROLE_ORDER) for (const model of row.models) {
    if (model.role === role && !out.has(model.family) && model.flex?.miss != null) out.set(model.family, model);
  }
  return out;
}

export type HeadToHead = {
  a: GradeFamily; b: GradeFamily; slates: number;
  aFlex: number; bFlex: number; aCaptain: number | null; bCaptain: number | null;
  /** Slates on which `a` had the smaller flex miss; a tie counts for neither. */
  aCloser: number; bCloser: number;
};

const mean = (values: number[]) => values.reduce((total, v) => total + v, 0) / values.length;

/**
 * Every pair of models that was stored together on at least one slate, on
 * those slates only. Each slate counts once (its own average over players).
 */
export function headToHead(rows: readonly OwnershipGradeRow[]): HeadToHead[] {
  const perSlate = rows.map(modelsByFamily);
  const out: HeadToHead[] = [];
  for (let i = 0; i < FAMILY_ORDER.length; i += 1) for (let j = i + 1; j < FAMILY_ORDER.length; j += 1) {
    const a = FAMILY_ORDER[i], b = FAMILY_ORDER[j];
    const shared = perSlate.filter((m) => m.has(a) && m.has(b)).map((m) => [m.get(a)!, m.get(b)!] as const);
    if (!shared.length) continue;
    const captain = shared.filter(([x, y]) => x.captain?.miss != null && y.captain?.miss != null);
    out.push({
      a, b, slates: shared.length,
      aFlex: mean(shared.map(([x]) => x.flex!.miss!)), bFlex: mean(shared.map(([, y]) => y.flex!.miss!)),
      aCaptain: captain.length ? mean(captain.map(([x]) => x.captain!.miss!)) : null,
      bCaptain: captain.length ? mean(captain.map(([, y]) => y.captain!.miss!)) : null,
      aCloser: shared.filter(([x, y]) => x.flex!.miss! < y.flex!.miss!).length,
      bCloser: shared.filter(([x, y]) => y.flex!.miss! < x.flex!.miss!).length,
    });
  }
  return out.sort((x, y) => y.slates - x.slates);
}

/** How many graded slates each model was stored on, most first. */
export function slatesByFamily(rows: readonly OwnershipGradeRow[]): { family: GradeFamily; slates: number }[] {
  const counts = new Map<GradeFamily, number>();
  for (const row of rows) for (const family of modelsByFamily(row).keys()) counts.set(family, (counts.get(family) ?? 0) + 1);
  return FAMILY_ORDER.filter((family) => counts.has(family)).map((family) => ({ family, slates: counts.get(family)! }));
}

/** The models of one slate in display order, the number the build read first. */
export function orderedModels(row: OwnershipGradeRow): GradedModel[] {
  return [...modelsByFamily(row).values()].sort((x, y) => ROLE_ORDER.indexOf(x.role) - ROLE_ORDER.indexOf(y.role));
}
