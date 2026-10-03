import type { NflAirMatchupEvidence } from './air-matchup-evidence';

export type AirShadowScenario = { attemptsMultiplier: number; neutral: {mean:number;p50:number;p90:number;over100:number}; matchup: {mean:number;p50:number;p90:number;over100:number} };

/** Paired opportunity sensitivity only. It does not simulate catches, fantasy points, or a contest field. */
export function simulateAirMatchupOpportunity(evidence:NflAirMatchupEvidence, seed:number, draws=10000): Record<string,AirShadowScenario> {
  if (evidence.state !== 'ready' || evidence.player.targetShare == null || evidence.player.airYardsPerTarget == null ||
      evidence.projectedTeamPassAttempts == null || evidence.matchupFactor == null) throw new Error('Ready air matchup evidence required.');
  if (!Number.isInteger(draws) || draws < 100) throw new Error('At least 100 draws required.');
  let state=seed>>>0;
  const uniform=()=>{state=(Math.imul(state,1664525)+1013904223)>>>0;return (state+.5)/4294967296;};
  const normal=()=>Math.sqrt(-2*Math.log(uniform()))*Math.cos(2*Math.PI*uniform());
  const summaries:Record<string,AirShadowScenario>={};
  for (const [name,multiplier] of [['leading',.9],['neutral',1],['trailing',1.1]] as const) {
    const base:number[]=[],matchup:number[]=[];
    for (let i=0;i<draws;i++) {
      // Shared attempt, target, and target-depth draws make the two arms paired.
      // Dispersion and +/-10% scripts are explicit research assumptions, not fitted estimates.
      const attempts=Math.max(0,Math.round(evidence.projectedTeamPassAttempts*multiplier*(1+.15*normal())));
      let targets=0;
      for(let j=0;j<attempts;j++) if(uniform()<evidence.player.targetShare) targets++;
      const depth=evidence.player.airYardsPerTarget*Math.exp(.3*normal()-.045);
      const neutral=targets*depth;
      base.push(neutral); matchup.push(neutral*evidence.matchupFactor);
    }
    const describe=(values:number[])=>{values.sort((a,b)=>a-b);return {
      mean:values.reduce((a,b)=>a+b,0)/draws,p50:values[Math.floor(.5*(draws-1))],
      p90:values[Math.floor(.9*(draws-1))],over100:values.filter(v=>v>=100).length/draws,
    };};
    summaries[name]={attemptsMultiplier:multiplier,neutral:describe(base),matchup:describe(matchup)};
  }
  return summaries;
}
