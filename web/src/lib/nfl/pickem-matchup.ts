/** Numerical residual only. Decision authority is resolved by the server registry reader. */
export type ResidualFeature = { definitionId: string; value: number | null; snapshotId: string;
  availableAt: string; sourceManifest?: unknown };
export type ResidualModel = { artifactId: string; definitionId: string; version: string; consumerId: "nfl-pickem";
  useCase: string; cohort: string; coefficients: Record<string, number>; intercept: number;
  trainedThrough: string; trainingManifest: unknown };
export type ResidualInput = { gameId: string; decisionCutoff: string; kickoff: string;
  baseline: { homeConditional: number; tie: number | null; marketCapturedAt: string; source: string };
  model: ResidualModel | null; features: ResidualFeature[] };
export type MatchupForecast = { forecastId?: string; input: ResidualInput; status: "unavailable" | "fallback" | "shadow" | "qualified";
  baseline: { home: number; away: number; tie: number } | null;
  candidate: { home: number; away: number; tie: number; homeConditional: number } | null;
  residual: number | null; contributions: Array<{ definitionId: string; value: number; coefficient: number; deltaLogit: number }>;
  qualificationIds: string[]; reasons: string[] };
const stamp = (s: string) => Date.parse(s);
export function matchupResidual(input: ResidualInput): MatchupForecast {
  const out: MatchupForecast = { input, status: "unavailable", baseline: null, candidate: null,
    residual: null, contributions: [], qualificationIds: [], reasons: [] };
  const { homeConditional: p, tie: t, marketCapturedAt } = input.baseline;
  if (![p, t].every(n => n != null && Number.isFinite(n) && n >= 0 && n <= 1)) {
    out.reasons.push("A valid market probability and frozen tie component are required"); return out;
  }
  out.baseline = { home: (1 - t!) * p, away: (1 - t!) * (1 - p), tie: t! };
  const cutoff = stamp(input.decisionCutoff), kickoff = stamp(input.kickoff), quote = stamp(marketCapturedAt);
  if (![cutoff, kickoff, quote].every(Number.isFinite) || quote > cutoff || cutoff >= kickoff) {
    out.reasons.push("Market/cutoff/kickoff chronology is invalid"); return out;
  }
  const maxAge = kickoff - cutoff <= 86400000 ? 7200000 : 86400000;
  if (cutoff - quote > maxAge) { out.reasons.push("Market quote was stale at the forecast cutoff"); return out; }
  const model = input.model;
  if (!model) { out.reasons.push("No fitted and registered matchup model artifact is available; market baseline retained"); return out; }
  if (!model.artifactId || !model.definitionId || model.consumerId !== "nfl-pickem" || !model.trainingManifest ||
      !Number.isFinite(model.intercept) || !Number.isFinite(stamp(model.trainedThrough)) || stamp(model.trainedThrough) >= cutoff ||
      !Object.keys(model.coefficients).length) { out.reasons.push("Incomplete or ineligible model artifact"); return out; }
  let residual = model.intercept;
  for (const [definitionId, coefficient] of Object.entries(model.coefficients)) {
    const rows = input.features.filter(f => f.definitionId === definitionId);
    const f = rows[0];
    if (rows.length !== 1 || !f.snapshotId || !Number.isFinite(coefficient) || f.value == null || !Number.isFinite(f.value) ||
        !Number.isFinite(stamp(f.availableAt)) || stamp(f.availableAt) > cutoff) {
      out.reasons.push(`Feature ${definitionId} is missing, ambiguous, or unavailable at cutoff; exact market fallback`);
      out.status = "fallback"; out.residual = 0; out.candidate = { ...out.baseline, homeConditional: p }; return out;
    }
    const deltaLogit = f.value * coefficient; residual += deltaLogit;
    out.contributions.push({ definitionId, value: f.value, coefficient, deltaLogit });
  }
  if (!Number.isFinite(residual)) { out.reasons.push("Non-finite fitted residual"); return out; }
  const bounded = Math.min(1 - 1e-6, Math.max(1e-6, p));
  const candidate = residual === 0 ? p : 1 / (1 + Math.exp(-(Math.log(bounded / (1 - bounded)) + residual)));
  out.residual = residual; out.candidate = { home: (1 - t!) * candidate, away: (1 - t!) * (1 - candidate), tie: t!, homeConditional: candidate };
  out.status = "shadow"; out.reasons.push("Under evaluation; active forecast is unchanged until this consumer/configuration is qualified");
  return out;
}

/** Three-class metrics with shared clipping/normalization; observed ties are scored. */
export function threeWayLoss(prob: {home: number; away: number; tie: number}, outcome: "home" | "away" | "tie") {
  const keys = ["home", "away", "tie"] as const;
  if (keys.some(k => !Number.isFinite(prob[k]) || prob[k] < 0 || prob[k] > 1) || Math.abs(keys.reduce((s,k)=>s+prob[k],0)-1)>1e-8)
    throw new Error("Invalid three-class probability");
  const clipped = keys.map(k => Math.max(1e-6, prob[k])), total = clipped.reduce((a,b)=>a+b,0);
  const normalized = clipped.map(x=>x/total);
  return { logLoss: -Math.log(normalized[keys.indexOf(outcome)]),
    brier: normalized.reduce((s, p, i) => s + (p - Number(keys[i] === outcome)) ** 2, 0) };
}
