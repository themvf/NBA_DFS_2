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
  withheld?: { team: string; pool: string; donors: { name: string; historicalUnits?: number }[] }[];
}) {
  const summary = summarizeRunRisks(lineups);
  const completion = buildCompletion({ requestedLineups: requestedLineups ?? lineups.length, generatedLineups: lineups.length, exposureReport });
  const uncertain = currentPlayers.filter(p => /^(QUESTIONABLE|DOUBTFUL|Q|D)$/i.test(p.availability?.status ?? ''))
    .map(p => ({ ...p, count: lineups.filter(l => l.playerIds.includes(p.dkPlayerId)).length })).filter(p => p.count)
    .sort((a, b) => b.count - a.count);
  const statusWord = (status?: string) => /^(DOUBTFUL|D)$/i.test(status ?? '') ? 'doubtful' : 'questionable';
  const invalidKickers = currentPlayers.filter(p => kickerRoleBlockedReason({ ...p, position: p.position ?? '' })
    && lineups.some(l => l.playerIds.includes(p.dkPlayerId)));
  const newlyOut = currentPlayers.filter(p => (p.isOut || ruledOutPlayer({...p,availability:p.availability?{...p.availability,blockedReason:p.availability.blockedReason??null}:null})) && lineups.some(l => l.playerIds.includes(p.dkPlayerId)))
    .map(p => ({name:p.name,count:lineups.filter(l => l.playerIds.includes(p.dkPlayerId)).length}));
  // Starter-aware projections (nfl-absence-commit-v1) on the current pool, by team.
  const committed = currentPlayers.filter(p => p.absenceCommit);
  const moved = (p: typeof committed[number]) => Math.abs(p.absenceCommit!.after.mean - p.absenceCommit!.before.mean) >= 0.05;
  const changeText = (p: typeof committed[number]) => `${p.name} ${p.absenceCommit!.before.mean.toFixed(1)} → ${p.absenceCommit!.after.mean.toFixed(1)}`;
  // One entry per team: every absent player named once across pools; teams whose
  // teammates' projections moved get their own line, the rest share one summary.
  const byTeam = new Map<string, { names: string[]; units: Map<string, number | undefined> }>();
  for (const w of withheld) {
    const entry: { names: string[]; units: Map<string, number | undefined> } = byTeam.get(w.team) ?? { names: [], units: new Map() };
    for (const d of w.donors) {
      if (!entry.names.includes(d.name)) entry.names.push(d.name);
      const prior = entry.units.get(d.name);
      entry.units.set(d.name, d.historicalUnits === undefined ? prior : Math.max(prior ?? 0, d.historicalUnits));
    }
    byTeam.set(w.team, entry);
  }
  const teamsWithChanges = [...byTeam.keys()].filter(team => committed.some(p => p.team === team && moved(p)));
  const unchangedTeams = [...byTeam.keys()].filter(team => !teamsWithChanges.includes(team));
  // An absence that carried a role (>= ROLE_UNITS carries or receptions a game, or unknown volume) is named;
  // the rest are counted, so a practice-squad call-up does not get its own sentence.
  const ROLE_UNITS = 2;
  const namedAbsences: string[] = []; let minorAbsences = 0;
  for (const team of unchangedTeams) {
    const entry = byTeam.get(team)!;
    const named = entry.names.filter(name => { const u = entry.units.get(name); return u === undefined || u >= ROLE_UNITS; });
    minorAbsences += entry.names.length - named.length;
    if (named.length) namedAbsences.push(`${team} ${named.join(', ')}`);
  }
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
      {uncertain.length ? <li><b>Confirm before export:</b> {uncertain.map(p => `${p.name} is ${statusWord(p.availability?.status)} (${p.count} of ${lineups.length}, ${Math.round(p.count / lineups.length * 100)}%)`).join('; ')}. Active status does not establish a full workload.</li> : null}
      {builtOnOld.length ? <li><b>Rebuild recommended:</b> these lineups were built before starter-aware projections changed {builtOnOld.map(p => `${p.name} (${p.absenceCommit!.before.mean.toFixed(1)} → ${p.absenceCommit!.after.mean.toFixed(1)})`).join(', ')}. Generate again to build from the current numbers.</li> : null}
      {teamsWithChanges.map(team => <li key={team}><b>{team}:</b> {byTeam.get(team)!.names.join(', ')} unavailable. Starter-aware projections applied: {committed.filter(p => p.team === team && moved(p)).map(changeText).join(', ')}.</li>)}
      {unchangedTeams.length ? <li><b>Baseline projections kept</b> for teammates of unavailable players on {unchangedTeams.length} {unchangedTeams.length === 1 ? 'team' : 'teams'}{namedAbsences.length ? ` (${namedAbsences.join('; ')}${minorAbsences ? `; ${minorAbsences} other${minorAbsences === 1 ? '' : 's'} averaged under ${ROLE_UNITS} carries or receptions a game` : ''})` : ` (${minorAbsences} player${minorAbsences === 1 ? '' : 's'}, each under ${ROLE_UNITS} carries or receptions a game)`}. Automatic redistribution has not improved fantasy-point accuracy, and low exposure does not establish that their roles are unchanged.</li> : null}
      {summary.sourceFamilies.length > 1 ? <li>These saved lineups contain players scored from different projection sources ({summary.sourceFamilies.join(", ")}). Their objective scores may not be comparable. Regenerate from one source before exporting.</li> : null}
      {uncalibratedLeverage ? <li>Uncalibrated ownership leverage influenced this run. Review exposures before exporting.</li> : null}
      {summary.dstOpponentLineups.length ? <li>{summary.dstOpponentLineups.length} of {lineups.length} lineups pair a DST with an opposing offensive player. Review whether each pairing fits your strategy. Lineups: {summary.dstOpponentLineups.join(", ")}.</li> : null}
    </ul>
  </section>;
}
