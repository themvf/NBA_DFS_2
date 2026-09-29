/**
 * B4: experimental sources report whether they can forecast, using the same rule
 * generation applies; decision time is kept apart from request time; captures
 * from after a slate's cutoff never replace its candidates.
 */
import assert from 'node:assert/strict';
import { PgDialect } from 'drizzle-orm/pg-core';
import { availabilityCurrent, evaluationCurrent, liveClock, type Availability, type DecisionClock } from '../src/lib/nfl-dfs/availability';
import { calibratedRelease, type CalibratedRelease, type CalibratedProjection } from '../src/lib/nfl-dfs/calibrated-projection';
import { computeSourceAvailability, sourceBlockedReason, type SourcePlayer } from '../src/lib/nfl-dfs/source-availability';
import { calibratedSnapshotsQuery, volumeShareRunQuery } from '../src/lib/nfl-dfs/source-queries';
import type { WorkloadProjection } from '../src/lib/nfl-dfs/workload-projection';

const decisionAt = '2026-09-28T20:52:56.860Z', decision = Date.parse(decisionAt);
const kickoff = '2026-09-29T00:15:00.000Z';
const base: Availability = { role: 'Listed WR1', status: 'ACTIVE', source: 'test', capturedAt: decisionAt, blockedReason: null, fresh: true, evaluatedAt: decisionAt, kickoff, pinned: true };
const at = (now: number, onNewestRun = true): DecisionClock => ({ now, decisionAt, onNewestRun });

// --- The 60-second rule applies only to live-resolved evidence ------------------
assert.equal(availabilityCurrent(base, at(decision + 3 * 3600000)).ok, true, 'pinned decision on the newest run: current hours later');
assert.match(availabilityCurrent(base, at(decision + 60, false)).reason, /newer projection run/);
assert.equal(availabilityCurrent(base, liveClock(decision + 61000)).ok, false, 'no decision time: 60s live rule');
assert.equal(availabilityCurrent(base, liveClock(decision + 59000)).ok, true);
assert.match(availabilityCurrent(base, at(Date.parse(kickoff))).reason, /started/);
assert.match(availabilityCurrent(base, at(decision - 1000)).reason, /future/, 'a decision later than the request is refused');
assert.equal(availabilityCurrent({ ...base, fresh: false }, at(decision + 1000)).ok, false);
assert.equal(availabilityCurrent({ ...base, blockedReason: 'Listed QB2' }, at(decision + 1000)).reason, 'Listed QB2');
assert.equal(evaluationCurrent({ ...base, blockedReason: 'Unavailable: INACTIVE' }, at(decision + 1000)).ok, true, 'timing check ignores the block');
assert.equal(availabilityCurrent({ ...base, evaluatedAt: '2026-09-28T20:00:00.000Z' }, at(decision + 1000)).ok, false, 'evaluated at some other time: live rule, stale');
// Python isoformat microseconds (as stored on pinned decisions) equal the run cutoff after parsing.
assert.equal(availabilityCurrent({ ...base, evaluatedAt: '2026-09-28T20:52:56.860464+00:00' }, at(decision + 5000)).ok, true);

// --- Source availability mirrors generation --------------------------------------
const gameInfo = 'PHI@CHI 09/28/2026 08:15PM ET';
const wr = (n: number, extra: Partial<SourcePlayer> = {}): SourcePlayer => ({ position: 'WR', team: 'CHI', gameInfo, isOut: false, availability: base, ...extra, workload: extra.workload === undefined ? null : extra.workload, workloadReason: extra.workloadReason ?? `reason ${n}` });
const forecast: WorkloadProjection = { mean: 12, p10: 4, p50: 11, p90: 22, targets: 7, baselineTargets: 6, historyGames: 8, snapshotId: 'a', recipeDigest: 'b', rosterDigest: 'c', capturedAt: '2026-09-28T19:00:00.000Z', kickoff, identity: 'x', season: 2026, week: 3, injuryAdjusted: false };
const dst = (calibrated: CalibratedProjection | null): SourcePlayer => ({ position: 'DST', team: 'CHI', gameInfo, isOut: false, availability: { ...base, role: 'Role unresolved', fresh: false }, calibrated, calibrationReason: calibrated ? undefined : 'No frozen candidate for this player and week.' });
const calibrated = { mean: 8, p10: 2, p50: 7, p90: 15, boom: .1, baselineMean: 7, baselineP10: 2, baselineP90: 14, snapshotId: '1', capturedAt: '2026-09-28T19:00:00.000Z', kickoff, recipeDigest: 'r', studyDigest: 's', releaseVersion: 'v' } satisfies CalibratedProjection;
const context = { release: calibratedRelease as CalibratedRelease, volumeShareReason: null, calibratedReason: null, slateReason: null };

const players = [wr(1, { workload: forecast }), wr(2), dst(calibrated)];
const live = computeSourceAvailability(players, at(decision + 3600000), context);
assert.equal(live.workload.positions.WR.usable, true); assert.equal(live.workload.positions.WR.count, 1);
assert.match(live.workload.positions.QB.reason, /No qualifying QB recipe in the current shadow study \(7ff4d404\)/);
assert.equal(live.workload.usable, true);
assert.equal(live.calibrated.positions.DST.count, 1); assert.equal(live.calibrated.usable, true);
assert.match(live.calibrated.positions.QB.reason, /Release gate did not qualify QB/);
assert.equal(sourceBlockedReason(live, 'workload', { QB: true, RB: false, WR: true, TE: false }), null);
assert.match(sourceBlockedReason(live, 'workload', { QB: true, RB: false, WR: false, TE: false })!, /QB: No qualifying QB recipe/);
assert.equal(sourceBlockedReason(live, 'workload', undefined), null, 'legacy saved settings are WR-only');
assert.equal(sourceBlockedReason(live, 'our'), null);

// A newer run, a started game, a missing WR run: each is a stated reason, and generation refuses.
const superseded = computeSourceAvailability(players, at(decision + 3600000, false), context);
assert.equal(superseded.workload.positions.WR.usable, false); assert.match(superseded.workload.positions.WR.reason, /newer projection run/);
assert.match(sourceBlockedReason(superseded, 'workload', { QB: false, RB: false, WR: true, TE: false })!, /WR: A newer projection run/);
const started = computeSourceAvailability(players, at(Date.parse(kickoff) + 1), context);
assert.equal(started.workload.usable, false); assert.equal(started.calibrated.usable, false);
assert.match(started.calibrated.reason, /started/);
const noRun = computeSourceAvailability(players, at(decision + 60), { ...context, volumeShareReason: 'No WR volume-share run exists for 2026 week 3.' });
assert.equal(noRun.workload.positions.WR.reason, 'No WR volume-share run exists for 2026 week 3.');
assert.equal(noRun.workload.usable, false);
assert.match(noRun.workload.reason, /WR: No WR volume-share run/);
const expired = computeSourceAvailability([wr(1, { workload: { ...forecast, capturedAt: '2026-09-25T00:00:00.000Z' } })], at(decision + 60), context);
assert.match(expired.workload.positions.WR.reason, /expired/);
const noSnapshots = computeSourceAvailability(players, at(decision + 60), { ...context, calibratedReason: 'Shadow study 7ff4d404 froze no forecasts.' });
assert.equal(noSnapshots.calibrated.positions.DST.reason, 'Shadow study 7ff4d404 froze no forecasts.');
assert.match(sourceBlockedReason(noSnapshots, 'calibrated')!, /Calibrated projections are unavailable/);

// --- Captures are bounded by the slate's projection cutoff ------------------------
const dialect = new PgDialect(), asOf = new Date(decisionAt);
const snapshots = dialect.sqlToQuery(calibratedSnapshotsQuery(calibratedRelease.studyId, 2026, 3, asOf));
assert.match(snapshots.sql, /captured_at <= \$4/); assert.deepEqual(snapshots.params, [calibratedRelease.studyId, 2026, 3, asOf.toISOString()]);
assert.match(snapshots.sql, /ORDER BY player_id,captured_at DESC,id DESC/, 'newest capture at or before the cutoff');
const runs = dialect.sqlToQuery(volumeShareRunQuery(2026, 3, asOf));
assert.match(runs.sql, /as_of_at <= \$3/); assert.deepEqual(runs.params, [2026, 3, asOf.toISOString()]);

console.log('Source availability: decision-time vs 60s live rule, generation-matched per-position verdicts, stated reasons, and as-of-bounded captures passed.');
