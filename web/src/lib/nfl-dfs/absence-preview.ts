import type { Availability } from './availability';
import type { PlayerContext } from './player-context';
import { workloadScenario } from './workload-scenario';

type Player = { dkPlayerId: number; name: string; position: string; team: string; gameKey: string | null; salary: number; isOut: boolean; availability?: Availability };
const nameKey = (s: string) => s.normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]/g,'');
const teamKey = (s: string) => ({WAS:'WSH',LA:'LAR',JAC:'JAX',AZ:'ARI'}[s] ?? s);

/** Evidence-gated historical sensitivity, never an automatic injury uplift. */
export function previewAbsence(data: PlayerContext, receiver: Player, teammate: Player, now: number) {
  if (receiver.dkPlayerId === teammate.dkPlayerId || receiver.position !== 'WR' || !['QB','WR','TE'].includes(teammate.position)) throw new Error('Choose a WR and a different QB, WR or TE teammate.');
  if (teamKey(receiver.team) !== teamKey(teammate.team) || !receiver.gameKey || receiver.gameKey !== teammate.gameKey) throw new Error('Players must share the same team and slate game.');
  for (const p of [receiver,teammate]) {
    const a=p.availability, evaluated=Date.parse(a?.evaluatedAt??''), kickoff=Date.parse(a?.kickoff??'');
    if (!a?.fresh || !Number.isFinite(evaluated) || evaluated>now || now-evaluated>60000 || !Number.isFinite(kickoff) || kickoff<=now) throw new Error('Refresh matching pregame roster and availability evidence.');
  }
  if (receiver.isOut || receiver.availability?.blockedReason) throw new Error('The selected receiver is excluded.');
  if (!teammate.availability?.officialConfirmed || teammate.availability.status !== 'INACTIVE') throw new Error('This scenario requires a verified official inactive report; questionable or undated reports cannot trigger it.');
  if (receiver.availability!.kickoff !== teammate.availability!.kickoff) throw new Error('Kickoff evidence conflicts.');
  const identities=[receiver,teammate].map(p=>data.players.filter(h=>nameKey(h.name)===nameKey(p.name)&&h.position===p.position));
  if (identities.some(matches=>matches.length!==1)) throw new Error('No unique historical name and position match; no fuzzy identity or rookie projection is inferred.');
  if (data.season >= new Date(now).getUTCFullYear()) throw new Error('This preview requires a completed prior season.');
  const teamGames=Object.entries(data.games).filter(([,g])=>teamKey(g.team)===teamKey(receiver.team));
  if (teamGames.some(([,g])=>!Number.isFinite(Date.parse(g.date)) || Date.parse(g.date)>=now)) throw new Error('Historical dates are unresolved or contain future games.');
  const historicalTeam=teamGames[0]?.[1].team ?? receiver.team;
  const estimate=workloadScenario(data,identities[0][0].id,identities[1][0].id,historicalTeam,19,'absent',receiver.salary);
  return {version:'verified-absence-preview-v1',optimizerEnabled:false as const,evaluatedAt:new Date(now).toISOString(),historicalSeason:data.season,
    receiver:{...receiver,historicalId:identities[0][0].id},teammate:{...teammate,historicalId:identities[1][0].id},sources:data.sources,estimate,
    limits:['Historical same-team association, not a causal injury effect or calibrated next-game forecast.','Current QB, coaching, opponent and efficiency changes are not modeled. Multiple absences are not additive.','Scored games only; missing/DNP outcomes limit the sample. No automatic redistribution or optimizer projection changes.','Classic/FLEX salary thresholds; individual probabilities do not imply lineup win probability.']};
}
