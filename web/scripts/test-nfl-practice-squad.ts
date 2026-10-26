import assert from 'node:assert/strict';
import { practiceSquadBlock, presentPinnedGameAvailability, resolveGameAvailability, type RosterEvidence } from '../src/lib/nfl-dfs/availability';
import { ruledOutPlayer } from '../src/lib/nfl-dfs/confirmed-starter';

const dev = { rosterWeek: 7, team: 'CHI', status: 'DEV', description: 'P07', observedAt: '2026-10-25T12:00:00Z' };
const base = { rosterStatus: dev, slateWeek: 7, team: 'CHI', position: 'RB', sleeperDepth: null, sleeperTeam: 'CHI', officialActive: false };

// Practice squad with no depth listing: out, with the reason shown.
const out = practiceSquadBlock(base);
assert.match(out.blockedReason!, /^Practice squad, not on the depth chart/);
assert.match(out.blockedReason!, /week 7 roster.*Sleeper depth: none/);
// A depth listing below the role-giving rank does not confirm him.
assert.match(practiceSquadBlock({ ...base, sleeperDepth: 3 }).blockedReason!, /Sleeper depth: RB3/);
// Confirmation by a role-giving depth rank: kept, and the disagreement is shown.
for (const [position, depth] of [['RB', 2], ['WR', 3], ['TE', 2], ['QB', 1]] as const) {
  const kept = practiceSquadBlock({ ...base, position, sleeperDepth: depth });
  assert.equal(kept.blockedReason, null, `${position}${depth} confirms`);
  assert.match(kept.note!, /Sources differ.*Kept in the pool/);
}
// Depth on another team confirms nothing.
assert.ok(practiceSquadBlock({ ...base, sleeperDepth: 1, sleeperTeam: 'GB' }).blockedReason);
// Confirmation by the official inactive list.
assert.equal(practiceSquadBlock({ ...base, officialActive: true }).blockedReason, null);
// Active / elevated (ACT) players and players with no captured status are untouched.
for (const status of [{ ...dev, status: 'ACT', description: 'P07' }, { ...dev, status: 'ACT', description: 'A01' }, null, undefined])
  assert.deepEqual(practiceSquadBlock({ ...base, rosterStatus: status }), { blockedReason: null, note: null });
// Only the slate's own week counts: last week's DEV is not evidence (27 of 416 such players played in 2026 weeks 2-7).
assert.deepEqual(practiceSquadBlock({ ...base, slateWeek: 6 }), { blockedReason: null, note: null });
assert.deepEqual(practiceSquadBlock({ ...base, slateWeek: 8 }), { blockedReason: null, note: null });
assert.deepEqual(practiceSquadBlock({ ...base, slateWeek: null }), { blockedReason: null, note: null });
// A practice-squad row under another club: he has signed elsewhere, so the rule is not applied (and says why).
const moved = practiceSquadBlock({ ...base, rosterStatus: { ...dev, team: 'MIA' } });
assert.equal(moved.blockedReason, null); assert.match(moved.note!, /Sources differ.*MIA.*not applied/);
// Team aliases are one franchise.
assert.ok(practiceSquadBlock({ ...base, team: 'LAR', rosterStatus: { ...dev, team: 'LA' }, sleeperTeam: 'LAR' }).blockedReason);

// Through the resolver: a fresh Sleeper capture listing him Active with no depth (Salvon Ahmed, every 2026 week).
const now = Date.parse('2026-10-25T14:00:00Z');
const evidence: RosterEvidence = { team: 'CHI', position: 'RB', fetchedAt: '2026-10-25T13:07:00Z', rosterStatus: dev,
  sleeper: { team: 'CHI', position: 'RB', status: 'Active', injury_status: null, depth_chart_order: null } };
const resolved = resolveGameAvailability(evidence, 'CHI', 'RB', now, 7, '2026-10-25T17:00:00Z');
assert.match(resolved.blockedReason!, /^Practice squad/);
assert.match(resolved.roleBlockedReason!, /^Practice squad/);
// A role block, not an injury: excluded from the pool, but not a "ruled out" starter whose work moves to teammates.
assert.equal(ruledOutPlayer({ availability: resolved }), false);
// A pinned health decision (EXPECTED_ACTIVE from Sleeper) keeps the block, as it keeps a listed QB2.
const active = { version: 'player-game-availability-v1', state: 'EXPECTED_ACTIVE', projection_status: null, source: 'sleeper',
  observation_id: 1, source_snapshot_id: 2, available_at: '2026-10-25T13:07:00Z', as_of_at: '2026-10-25T13:30:00Z',
  kickoff: '2026-10-25T17:00:00Z', reason: 'Qualified.', qualifying_observation_ids: [1], display_only_observation_ids: [] };
assert.match(presentPinnedGameAvailability(active, resolved.role, resolved.roleBlockedReason, resolved.practiceSquadNote).blockedReason!, /^Practice squad/);
// Confirmed by a fresh RB2 listing: in the pool, with the disagreement in the warnings of both presenters.
const listed = resolveGameAvailability({ ...evidence, sleeper: { ...(evidence.sleeper as object), depth_chart_order: 2 } }, 'CHI', 'RB', now, 7, '2026-10-25T17:00:00Z');
assert.equal(listed.blockedReason, null);
assert.ok(listed.warnings!.some((w) => /Sources differ/.test(w)));
const pinnedListed = presentPinnedGameAvailability(active, listed.role, listed.roleBlockedReason, listed.practiceSquadNote);
assert.equal(pinnedListed.blockedReason, null);
assert.ok(pinnedListed.warnings!.some((w) => /Sources differ/.test(w)));
// A STALE depth listing may block but never clear: it does not confirm a practice-squad player.
const stale = resolveGameAvailability({ ...evidence, fetchedAt: '2026-10-20T13:00:00Z', sleeper: { ...(evidence.sleeper as object), depth_chart_order: 1 } }, 'CHI', 'RB', now, 7, '2026-10-25T17:00:00Z');
assert.match(stale.blockedReason!, /^Practice squad/);
// The read failing is reported, never read as "not on a practice squad".
const failed = resolveGameAvailability({ ...evidence, rosterStatus: null, rosterStatusReadFailed: true }, 'CHI', 'RB', now, 7, '2026-10-25T17:00:00Z');
assert.equal(failed.blockedReason, null);
assert.ok(failed.warnings!.some((w) => /Practice-squad status could not be loaded/.test(w)));
console.log('NFL practice-squad rule: exclusion, depth and official confirmation, stale, moved, week and read-failure checks passed');
