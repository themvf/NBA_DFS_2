/**
 * nfl-absence-commit-v1: the starter-aware projection the optimizer builds
 * from when a starter is out. Pins the two rules, their order, the committee
 * case and the version.
 */
import assert from 'node:assert/strict';
import { ABSENCE_COMMIT_VERSION, computeAbsenceCommits, projectedVolume, type CommitPlayer } from '../src/lib/nfl-dfs/absence-commit';
import type { TeamUsageWindow } from '../src/lib/nfl-dfs/replacement-upside';

const dist = (mean: number) => ({ mean, p10: mean * 0.3, median: mean * 0.9, p90: mean * 1.9, boom: 0.02 });
const u = (carries: number, targets = 0, receptions = Math.round(targets * 0.7)) => ({ carries, targets, receptions });
const player = (key: number, name: string, position: string, over: Partial<CommitPlayer> = {}): CommitPlayer =>
  ({ key, name, position, team: 'NYJ', out: false, dist: dist(6), statMeans: { carries: 6.8, receptions: 1.2 }, upside: null, ...over });
// Most recent game first: week 4, 3, 2, 1 of 2026, then 2025 week 18.
const games = [{ season: 2026, week: 4 }, { season: 2026, week: 3 }, { season: 2026, week: 2 }, { season: 2026, week: 1 }, { season: 2025, week: 18 }];

// 1. Ongoing absence (the Allen case): the starter missed week 4 and is out; the backup started week 4.
{
  const hall = player(1, 'Breece Hall', 'RB', { out: true, dist: dist(15) });
  const allen = player(2, 'Braelon Allen', 'RB', { dist: dist(5.4) });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: {
    1: [null, u(13, 3), u(16, 5), u(22, 2), u(18, 3)],
    2: [u(14, 2), u(4, 3), u(5, 1), u(10, 1), u(6, 1)],
  } };
  const report = computeAbsenceCommits([hall, allen], [window], 2026);
  assert.equal(report.commits.length, 1);
  const c = report.commits[0];
  assert.equal(c.version, ABSENCE_COMMIT_VERSION);
  assert.equal(c.rule, 'own_role_games');
  assert.deepEqual(c.games?.map((g) => g.week), [4], 'only games this season without the starter count');
  const projected = projectedVolume('RB', allen.statMeans)!; // 6.8 carries + 1.2 catches
  const without = 14 + 1; // week 4: 14 carries, 2 targets -> 1 catch
  const expectedScale = (0.5 * without + 0.5 * projected) / projected;
  assert.equal(c.scale, Math.round(expectedScale * 100) / 100);
  assert.ok(Math.abs(c.after.mean - 5.4 * expectedScale) < 0.02, 'whole range scaled by the volume ratio');
  assert.equal(c.before.mean, 5.4, 'the model\'s own number is kept');
  assert.ok(c.after.p90 > c.before.p90 && c.after.boom! >= 0);
}

// 2. Committee: the backup did NOT get the work without the starter, so he is projected lower.
{
  const starter = player(1, 'Starter', 'RB', { out: true, dist: dist(15) });
  const backup = player(2, 'Committee Back', 'RB', { dist: dist(6), statMeans: { carries: 8, receptions: 1 } });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [null, null, u(18, 3), u(20, 2), null], 2: [u(3, 0), u(4, 1), u(9, 1), u(8, 1), null] } };
  const c = computeAbsenceCommits([starter, backup], [window], 2026).commits[0];
  assert.equal(c.games?.length, 2);
  assert.ok(c.scale! < 1 && c.after.mean < c.before.mean, 'a committee back is projected down, not up');
}

// 3. First game: the starter played last week, so the job mixture becomes the projection.
{
  const starter = player(1, 'Starter', 'RB', { out: true, dist: dist(15) });
  const ifJob = { mean: 9.9, p10: 2, median: 8, p90: 22, boom: 0.08 };
  const backup = player(2, 'Backup', 'RB', { dist: dist(5), upside: { version: 'v', key: 2, role: 'lead', room: 'RB', pi: 0.5,
    from: { key: 1, name: 'Starter', position: 'RB', volume: 18, unit: 'carries' }, baseline: dist(5), ifStarterRole: ifJob, note: '' } });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [u(18, 2), u(17, 3), u(20, 2), u(19, 1), null], 2: [u(4, 1), u(5, 0), u(3, 1), u(6, 1), null] } };
  const c = computeAbsenceCommits([starter, backup], [window], 2026).commits[0];
  assert.equal(c.rule, 'first_game');
  assert.deepEqual(c.after, ifJob);
}

// 4. Own games beat a borrowed range: a player with games without the starter takes rule 1 even when flagged for rule 2.
{
  const starter = player(1, 'Starter', 'RB', { out: true, dist: dist(15) });
  const backup = player(2, 'Backup', 'RB', { dist: dist(5), upside: { version: 'v', key: 2, role: 'lead', room: 'RB', pi: 0.5,
    from: { key: 1, name: 'Starter', position: 'RB', volume: 18, unit: 'carries' }, baseline: dist(5), ifStarterRole: dist(10), note: '' } });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [null, u(17, 3), u(20, 2), null, null], 2: [u(15, 2), u(5, 0), u(3, 1), u(16, 2), null] } };
  assert.equal(computeAbsenceCommits([starter, backup], [window], 2026).commits[0].rule, 'own_role_games');
}

// 5. Last season's games never count, and an active starter changes nothing.
{
  const starter = player(1, 'Starter', 'RB', { out: false, dist: dist(15) });
  const backup = player(2, 'Backup', 'RB', { dist: dist(5) });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [u(18), u(17), u(20), u(19), null], 2: [u(4), u(5), u(3), u(6), u(20)] } };
  assert.equal(computeAbsenceCommits([starter, backup], [window], 2026).commits.length, 0);
}

// 6a. A starter out the whole window gives no contrast, so nothing changes (the Kittle case).
{
  const starter = player(1, 'Long Gone', 'TE', { out: true, dist: dist(9), statMeans: { receptions: 4 } });
  const te = player(2, 'Star TE', 'TE', { dist: dist(16), statMeans: { receptions: 6 } });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [null, null, null, null, u(0, 6)], 2: [u(0, 6), u(0, 7), u(0, 4), u(0, 5), null] } };
  assert.equal(computeAbsenceCommits([starter, te], [window], 2026).commits.length, 0);
}

// 6b. First game never lowers: a backup projected above the out starter keeps his number.
{
  const starter = player(1, 'Starter', 'RB', { out: true, dist: dist(8) });
  const backup = player(2, 'Better Back', 'RB', { dist: dist(14), upside: { version: 'v', key: 2, role: 'lead', room: 'RB', pi: 0.5,
    from: { key: 1, name: 'Starter', position: 'RB', volume: 12, unit: 'carries' }, baseline: dist(14), ifStarterRole: dist(11), note: '' } });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [u(12), u(11), u(13), u(12), null], 2: [u(15), u(14), u(16), u(15), null] } };
  assert.equal(computeAbsenceCommits([starter, backup], [window], 2026).commits.length, 0);
}

// 6. Rooms stay separate: a WR absence does not move a running back.
{
  const wr = player(1, 'WR One', 'WR', { out: true, dist: dist(14), statMeans: { receptions: 5 } });
  const rb = player(2, 'RB Two', 'RB', { dist: dist(8) });
  const window: TeamUsageWindow = { team: 'NYJ', games, usage: { 1: [null, u(0, 9), u(0, 8), u(0, 10), null], 2: [u(20, 4), u(12, 2), u(11, 2), u(10, 2), null] } };
  assert.equal(computeAbsenceCommits([wr, rb], [window], 2026).commits.length, 0);
}

console.log('Absence commit: ongoing-absence scaling, committee down-scaling, first-game mixture, rule order, season and room bounds passed.');
