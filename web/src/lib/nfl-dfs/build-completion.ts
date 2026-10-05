/** A full roster count is insufficient when requested portfolio targets were missed. */
export function buildCompletion(input: {
  requestedLineups: number; generatedLineups: number;
  exposureReport?: readonly { name: string; binding: string | null }[] | null;
  salaryBandReport?: readonly { withinPlan: boolean }[] | null;
  archetypePlan?: readonly { label: string; requested: number; realized: number }[] | null;
}): { status: 'complete' | 'partial' | 'failed'; issues: string[] } {
  const issues = (input.exposureReport ?? []).filter(r => /missed/.test(r.binding ?? ''))
    .map(r => `${r.name}: ${r.binding}`);
  if (input.generatedLineups !== input.requestedLineups)
    issues.unshift(`${input.generatedLineups} of ${input.requestedLineups} requested lineups generated.`);
  if (input.salaryBandReport?.some(r => !r.withinPlan)) issues.push('Salary distribution targets were not met.');
  for (const plan of input.archetypePlan ?? [])
    if (plan.realized !== plan.requested) issues.push(`${plan.label}: ${plan.realized} of ${plan.requested} requested lineups.`);
  return { status: !input.generatedLineups ? 'failed' : issues.length ? 'partial' : 'complete', issues };
}
