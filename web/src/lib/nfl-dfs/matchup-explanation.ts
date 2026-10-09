/** Display adapter for immutable research rows. Fallbacks retain saved production values. */
type Summary = { mean?: number; p10?: number; p50?: number; p90?: number; boom?: number; stat_means?: Record<string, number> };
type Step = { component?: string; before?: number; after?: number; factor?: number; points_delta?: number; features?: Record<string, number> };
export type FrozenMatchupProjection = {
  baseline?: { model_proj_fpts?: number; ceiling_fpts?: number; floor_fpts?: number; median_fpts?: number };
  shadow?: { status?: string; reason?: string; baseline?: Summary; candidate?: Summary; delta?: number; ledger?: Step[]; matchup_manifest_hash?: string };
  sources?: unknown[];
};

export type MatchupProjectionComparison = {
  status: string; reason: string; frozenAt: string; baselineRunId: string;
  baseline: number | null; candidate: number | null; delta: number;
  baselineP90: number | null; candidateP90: number | null;
  candidateP10: number | null; candidateMedian: number | null; candidateBoom: number | null;
  component: string | null; sourceCount: number; manifestHash: string;
  efficiencyBefore: number | null; efficiencyAfter: number | null;
  opportunity: number | null; opportunityLabel: string;
  evidence: { label: string; value: number; unit: string }[];
};

const labels: Record<string, [string, string]> = {
  own_pressure: ['Offense pressure faced', '%'], opp_pressure: ['Defense pressure created', '%'],
  own_ybc: ['Offense yards before contact', 'yards/carry'], opp_ybc: ['Defense yards before contact allowed', 'yards/carry'],
  own_yac: ['Offense yards after contact', 'yards/carry'], opp_yac: ['Defense yards after contact allowed', 'yards/carry'],
};
const reasons: Record<string, string> = {
  prospective_gate_not_yet_scorable: 'Awaiting enough forward results to measure whether this improves accuracy',
  ineligible_or_out: 'No matchup adjustment for an unavailable player',
  no_registered_effect_for_position: 'This position has no approved matchup adjustment in the current experiment',
  no_baseline_draws: 'The saved scoring distribution could not be reproduced',
  no_fitted_research_model: 'No fitted matchup model is available',
  insufficient_matching_pressure_or_rb_contact_history: 'Insufficient matching offense and defense history',
  saved_baseline_not_reproduced_or_availability_adjusted: 'The saved availability-adjusted distribution could not be reproduced; the saved projection is retained',
  no_positive_baseline_efficiency: 'Insufficient baseline opportunities for an efficiency adjustment',
};
const finite = (v: unknown) => typeof v === 'number' && Number.isFinite(v) ? v : null;

export function matchupExplanation(projection: FrozenMatchupProjection, frozenAt: string, baselineRunId: string): MatchupProjectionComparison | null {
  const shadow = projection.shadow;
  if (!shadow) return null;
  const applied = shadow.status === 'under_evaluation';
  const step = applied ? shadow.ledger?.[0] : undefined;
  const passing = step?.component === 'passing_yards';
  // An unreproduced raw simulation is not the saved production forecast.
  const baseline = finite(projection.baseline?.model_proj_fpts) ?? finite(shadow.baseline?.mean);
  const baselineP90 = finite(projection.baseline?.ceiling_fpts) ?? (applied ? finite(shadow.baseline?.p90) : null);
  return {
    status: shadow.status ?? 'unavailable', reason: reasons[shadow.reason ?? ''] ?? 'Matchup evidence is unavailable', frozenAt, baselineRunId,
    baseline, candidate: applied ? finite(shadow.candidate?.mean) : baseline, delta: applied ? finite(shadow.delta) ?? 0 : 0,
    baselineP90, candidateP90: applied ? finite(shadow.candidate?.p90) : baselineP90,
    candidateP10: applied ? finite(shadow.candidate?.p10) : finite(projection.baseline?.floor_fpts),
    candidateMedian: applied ? finite(shadow.candidate?.p50) : finite(projection.baseline?.median_fpts),
    candidateBoom: applied ? finite(shadow.candidate?.boom) : null,
    component: step?.component ?? null, sourceCount: projection.sources?.length ?? 0, manifestHash: shadow.matchup_manifest_hash ?? '',
    efficiencyBefore: finite(step?.before), efficiencyAfter: finite(step?.after),
    opportunity: applied ? finite(shadow.baseline?.stat_means?.[passing ? 'attempts' : 'carries']) : null,
    opportunityLabel: passing ? 'pass attempts' : 'carries',
    evidence: Object.entries(step?.features ?? {}).flatMap(([key, value]) => labels[key] && finite(value) !== null
      ? [{ label: labels[key][0], value, unit: labels[key][1] }] : []),
  };
}
