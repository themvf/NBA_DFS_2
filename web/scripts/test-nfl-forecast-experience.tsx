import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import { specialTeamsStatus } from '../src/lib/nfl-dfs/special-teams-status';
import { readSpecialTeamsProjection, SPECIAL_TEAMS_VERSION } from '../src/lib/nfl-dfs/special-teams-projection';
import { buildSlateCheck, type SlateCheckInput } from '../src/lib/nfl-dfs/slate-check';
import SlateCheckCard from '../src/app/dfs/nfl/slate-check-card';
import SpecialTeamsStatusCard from '../src/app/dfs/nfl/special-teams-status-card';
import WorkspaceStepper from '../src/app/dfs/nfl/workspace-stepper';

const kickoff = '2026-10-04T17:00:00Z';
const before = Date.parse(kickoff) - 1000;
const forecast = readSpecialTeamsProjection({ special_teams_candidate: {
  version: SPECIAL_TEAMS_VERSION, status: 'candidate', position: 'DST',
  mean: 8, p10: -2, p50: 7, p90: 17, boom: .3, feature_snapshot: { authority: 'candidate_only' },
} }, 'DST').projection;
const player = { name: 'Bills', position: 'DST', isOut: false, specialTeams: forecast, specialTeamsReason: null };
const missing = { ...player, ...{ specialTeams: null, specialTeamsReason: readSpecialTeamsProjection({}, 'DST').reason } };
const status = (players: Parameters<typeof specialTeamsStatus>[0]['players'] = [player], now = before, refreshAvailable = false, firstKickoff: string | null = kickoff) =>
  specialTeamsStatus({ players, now, refreshAvailable, firstKickoff })!;

assert.match(status().text, /1 of 1 defenses have opponent forecasts/);
assert.match(status().text, /apply automatically/);
assert.equal(status().action, undefined);
const old = status([missing]);
assert.match(old.text, /0 of 1 defenses/);
assert.match(old.text, /1 player uses historical forecasts/);
assert.doesNotMatch(old.text, /apply automatically/);
assert.equal(old.action, 'update_data');
assert.equal(status([missing], before, true).action, 'refresh_projections');
const partial = status([player, { ...missing, name: 'Dolphins' }]);
assert.match(partial.text, /1 of 2 defenses/);
assert.equal(partial.missing.length, 1);
const unavailable = status([{ ...missing, specialTeamsReason: 'Opponent implied total is missing.' }]);
assert.equal(unavailable.action, undefined, 'rebuilding unchanged missing inputs is not promised as a repair');
assert.match(unavailable.text, /inputs are missing or could not be verified/);
const invalid = status([{ ...missing, specialTeamsReason: readSpecialTeamsProjection({ special_teams_candidate: {} }, 'DST').reason }]);
assert.equal(invalid.action, undefined);
assert.equal(status([{ ...player, isOut: true }]), null, 'OUT special teams do not inflate fallback counts');
assert.equal(status([missing], before, false, null).action, undefined);
assert.doesNotMatch(status([missing], before, false, null).text, /Update data/);
const showdown = status([missing, { ...missing, name: 'Kicker', position: 'K' }]);
assert.match(showdown.text, /0 of 1 defenses.*0 of 1 kickers.*2 players use historical/);
const archive = status([missing], Date.parse(kickoff), true);
assert.equal(archive.action, undefined);
assert.match(archive.text, /Saved forecasts are preserved/);
assert.doesNotMatch(archive.text, /Update data|Refresh projections/);
const immutable = JSON.stringify(missing);
status([missing], Date.parse(kickoff), true);
assert.equal(JSON.stringify(missing), immutable, 'describing an archive never changes its forecast');

const base: SlateCheckInput = {
  label: 'Classic', now: before, firstKickoff: kickoff, deployedBuild: true, incompleteWarning: null,
  refreshAvailable: true, projectionAsOf: new Date(before).toISOString(), rosterStaleWarning: null,
  rosterCapturedAt: null, qbs: [], opponentAdjustments: null, ownership: null, availability: null,
  liveDk: null, upside: null, unmatched: [], specialTeams: old,
  experimentalSources: [{ label: 'Workload (experimental)', usable: false, reason: 'study 7ff4d404' }],
  pipeline: { error: null, failing: [{ label: 'Research job', affectsBuild: false,
    failedAt: new Date(before).toISOString(), url: 'https://example.com/run', streak: 7, streakCapped: false }] },
};
const pregame = buildSlateCheck(base);
assert.equal(pregame.items.find(i => i.id === 'source:Workload (experimental)')?.category, 'research');
assert.equal(pregame.items.find(i => i.id === 'pipeline:Research job')?.category, 'research');
assert.equal(pregame.items.find(i => i.id === 'special-teams')?.action, 'update_data');
const closed = buildSlateCheck({ ...base, now: Date.parse(kickoff), specialTeams: archive });
assert.equal(closed.archived, true);
assert.equal(closed.needs, 0);
assert.ok(closed.items.every(i => !i.action));
assert.doesNotMatch(closed.items.find(i => i.id === 'projections')!.text, /current|Refresh/);
const noop = () => {};
const closedHtml = renderToStaticMarkup(<SlateCheckCard check={closed} pending={false} onAction={noop} />);
assert.doesNotMatch(closedHtml, /<button|bg-emerald-50/);
assert.match(closedHtml, /Saved slate checks/);
assert.match(closedHtml, /<details[^>]*><summary[^>]*>Research diagnostics/);
assert.doesNotMatch(closedHtml, /<details[^>]*\bopen\b/);
assert.match(closedHtml, /study 7ff4d404/, 'provenance is retained inside the closed disclosure');
const fallbackHtml = renderToStaticMarkup(<SpecialTeamsStatusCard status={old} pending={true} onAction={noop} />);
assert.match(fallbackHtml, /aria-live="polite"/);
assert.match(fallbackHtml, /<button[^>]*disabled/);
assert.match(fallbackHtml, /min-h-11/);
const archivedForecastHtml = renderToStaticMarkup(<SpecialTeamsStatusCard status={archive} pending={false} onAction={noop} />);
assert.doesNotMatch(archivedForecastHtml, /<button|Update data|Refresh projections/);
const stepper = renderToStaticMarkup(<WorkspaceStepper locked stage="results" onChange={noop}
  notes={{ slate: 'Saved', build: 'Settings', review: 'Saved', results: 'Closed' }}
  done={{ slate: true, build: true, review: false, results: false }} />);
assert.match(stepper, /Saved settings/);
assert.match(stepper, /Saved lineups/);
assert.doesNotMatch(stepper, /Review and export/);
console.log('NFL forecast experience: complete, partial, missing, invalid, unavailable, Showdown, archive and disclosure states passed.');
