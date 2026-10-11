import type { NflGeneratedLineup } from "./nfl-optimizer";
import { summarizeRunRisks } from "@/lib/nfl-dfs/run-risk-summary";
import { buildCompletion } from '@/lib/nfl-dfs/build-completion';
import { kickerRoleBlockedReason } from '@/lib/nfl-dfs/availability';
import { ruledOutPlayer } from '@/lib/nfl-dfs/confirmed-starter';

export default function RunRiskSummary({ lineups, uncalibratedLeverage, requestedLineups, exposureReport, currentPlayers = [], withheld = [] }: {
  lineups: NflGeneratedLineup[]; uncalibratedLeverage: boolean; requestedLineups?: number;
  exposureReport?: { name: string; binding: string | null }[];
  currentPlayers?: { dkPlayerId: number; name: string; position?: string; depthRole?: string | null;
    isOut?: boolean; dkStatus?: string | null; projectionStatus?: string | null; team?: string;
    availability?: { status: string; role?: string; chartRole?: string; blockedReason?: string | null };
    absenceCommit?: { before: { mean: number }; after: { mean: number } } | null }[];
  withheld?: { team: string; pool: string; donors: { name: string }[] }[];
}) {
  const summary = summarizeRunRisks(lineups);
  const completion = buildCompletion({ requestedLineups: requestedLineups ?? lineups.length, generatedLineups: lineups.length, exposureReport });
  const uncertain = currentPlayers.filter(p => /^(QUESTIONABLE|DOUBTFUL|Q|D)$/i.test(p.availability?.status ?? ''))
    .map(p => ({ ...p, count: lineups.filter(l => l.playerIds.includes(p.dkPlayerId)).length })).filter(p => p.count);
  const invalidKickers = currentPlayers.filter(p => kickerRoleBlockedReason({ ...p, position: p.position ?? '' })
    && lineups.some(l => l.playerIds.includes(p.dkPlayerId)));
  const newlyOut = currentPlayers.filter(p => (p.isOut || ruledOutPlayer({...p,availability:p.availability?{...p.availability,blockedReason:p.availability.blockedReason??null}:null})) && lineups.some(l => l.playerIds.includes(p.dkPlayerId)))
    .map(p => ({name:p.name,count:lineups.filter(l => l.playerIds.includes(p.dkPlayerId)).length}));
  // Starter-aware projections (nfl-absence-commit-v1) on the current pool, by team.
  const committed = currentPlayers.filter(p => p.absenceCommit);
  const committedOn = (team: string, pool: string) => committed.filter(p => p.team === team
    && (pool === 'rush' ? p.position === 'RB' : p.position === 'WR' || p.position === 'TE'));
  // A saved lineup built before a commit carries the old number for that player.
  const builtOnOld = committed.filter(p => lineups.some(l => l.slots.some(s => s.player.dkPlayerId === p.dkPlayerId
    && Math.abs((s.player.ourProj ?? 0) - p.absenceCommit!.after.mean) > 0.05)));
  if (summary.sourceFamilies.length < 2 && !uncalibratedLeverage && !builtOnOld.length && !summary.dstOpponentLineups.length && !completion.issues.length && !uncertain.length && !withheld.length && !invalidKickers.length && !newlyOut.length) return null;
  return <section role="status" className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-950">
    <h2 className="font-bold">Review before export</h2>
    <ul className="mt-2 list-disc space-y-1 pl-5">
      {newlyOut.map(p => <li key={p.name}><b>Rebuild required:</b> {p.name} is unavailable and appears in {p.count} of {lineups.length} saved lineups. Export is blocked until these entries are rebuilt.</li>)}
      {invalidKickers.length ? <li><b>Rebuild required:</b> {invalidKickers.map(p => p.name).join(', ')} has no supported kicking role. These saved lineups cannot be exported.</li> : null}
      {completion.issues.length ? <li><b>Completed with unmet targets.</b> {completion.issues.join('; ')}. Adjust the targets or construction rules and generate again.</li> : null}
      {uncertain.map(p => <li key={p.dkPlayerId}><b>{p.name}</b> is {p.availability?.status.toLowerCase()} and appears in {p.count} of {lineups.length} lineups ({Math.round(p.count / lineups.length * 100)}%). Confirm final availability before export; active status does not establish a full workload.</li>)}
      {builtOnOld.length ? <li><b>Rebuild recommended:</b> these lineups were built before starter-aware projections changed {builtOnOld.map(p => `${p.name} (${p.absenceCommit!.before.mean.toFixed(1)} → ${p.absenceCommit!.after.mean.toFixed(1)})`).join(', ')}. Generate again to build from the current numbers.</li> : null}
      {withheld.map(p => committedOn(p.team, p.pool).length ? <li key={`${p.team}-${p.pool}`}><b>{p.team} {p.pool === 'rush' ? 'backfield' : 'receiving'} role change:</b> {p.donors.map(d => d.name).join(', ')} unavailable. Starter-aware projections applied: {committedOn(p.team, p.pool).map(c => `${c.name} ${c.absenceCommit!.before.mean.toFixed(1)} → ${c.absenceCommit!.after.mean.toFixed(1)}`).join(', ')}.</li> : <li key={`${p.team}-${p.pool}`}><b>{p.team} {p.pool === 'rush' ? 'backfield' : 'receiving'} role change:</b> {p.donors.map(d => d.name).join(', ')} unavailable. Teammates retain baseline projections because automatic {p.pool === 'rush' ? 'carry' : 'target'} redistribution has not improved fantasy-point accuracy. Their low exposure does not establish that their roles are unchanged.</li>)}
      {summary.sourceFamilies.length > 1 ? <li>These saved lineups contain players scored from different projection sources ({summary.sourceFamilies.join(", ")}). Their objective scores may not be comparable. Regenerate from one source before exporting.</li> : null}
      {uncalibratedLeverage ? <li>Uncalibrated ownership leverage influenced this run. Review exposures before exporting.</li> : null}
      {summary.dstOpponentLineups.length ? <li>{summary.dstOpponentLineups.length} of {lineups.length} lineups pair a DST with an opposing offensive player. Review whether each pairing fits your strategy. Lineups: {summary.dstOpponentLineups.join(", ")}.</li> : null}
    </ul>
  </section>;
}
