import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  UPSIDE_GRADE_SPEC, UPSIDE_GRADE_VERSION, captureFromRow, clusterBootstrap, fitWidening, gradeReplacementUpside, inWindow,
  pinball, resolveActual, selectPlayerGames, upsideVerdict,
  type ControlRow, type GradeCapture, type GradeCapturePlayer, type GradeGame, type GradeResult,
} from '../src/lib/nfl-dfs/replacement-upside-grade';
import type { ReplacementUpside, UpsideDistribution } from '../src/lib/nfl-dfs/replacement-upside';

const close = (a: number | null, b: number, tol = 1e-9, msg?: string) => assert.ok(a != null && Math.abs(a - b) <= tol, msg ?? `${a} != ${b}`);

// The registration is frozen: these literals must match docs/nfl-replacement-upside-grading.md.
{
  assert.equal(UPSIDE_GRADE_VERSION, 'nfl-replacement-upside-grade-v2');
  assert.deepEqual(JSON.parse(JSON.stringify(UPSIDE_GRADE_SPEC)), {
    featureVersion: 'nfl-replacement-upside-v2',
    windows: [{ season: 2026, firstWeek: 4 }, { season: 2027, firstWeek: 1 }],
    tau: 0.9, floors: { events: 60, flagged: 130, weeks: 8 },
    bootstrap: { draws: 10000, seed: 20260928, level: 0.95 },
    widening: { min: 1, max: 2.5, step: 0.01 }, controlMinMean: 1, boomClip: 0.001,
  });
  const doc = fs.readFileSync('../docs/nfl-replacement-upside-grading.md', 'utf8');
  for (const line of ['| Absence events (clusters) | 60 |', '| Flagged player-games | 130 |', '| Distinct weeks with a flagged player | 8 |',
    'nfl-replacement-upside-grade-v2', '20260928', '10,000']) {
    assert.ok(doc.includes(line), `registration doc must state: ${line}`);
  }
  assert.ok(!inWindow(2026, 3) && inWindow(2026, 4) && inWindow(2027, 1), 'window: 2026 week 4 onward, then 2027');
}

// Pinball loss at the 90th percentile.
close(pinball(10, 22), 10.8);
close(pinball(10, 5), 0.5);
close(pinball(10, 10), 0);

// Fixtures.
const dist = (mean: number, p90: number, boom: number | null): UpsideDistribution => ({ mean, p10: mean * 0.3, median: mean * 0.9, p90, boom });
const upside = (role: 'lead' | 'other', pi: number, base: UpsideDistribution, mix: UpsideDistribution, key = 1): ReplacementUpside => ({
  version: 'nfl-replacement-upside-v2', key, role, room: 'RB', pi,
  from: { key: 99, name: 'Starter RB', position: 'RB', volume: 15, unit: 'carries' },
  baseline: base, ifStarterRole: mix, note: 'fixture',
});
const player = (over: Partial<GradeCapturePlayer> & { dkPlayerId: number }): GradeCapturePlayer => ({
  playerId: over.dkPlayerId, name: `P${over.dkPlayerId}`, team: 'AAA', position: 'RB', isOut: false,
  projection: 5, ceiling: 10, boom: 0.02, upside: null, unchanged: null, ...over,
});
const FEATURE = { version: 'nfl-replacement-upside-v2', flagged: 2, skipped: [] };
const game = (id: number, week: number, kickoff: string, completed = true): GradeGame => ({ id, season: 2026, week, kickoff, completed });
const cap = (over: Partial<GradeCapture> & { digest: string; game: GradeGame; players: GradeCapturePlayer[] }): GradeCapture => ({
  uploadId: 'u1', uploadCreatedAt: '2026-09-30T00:00:00.000Z', observedAt: new Date(Date.parse(over.game.kickoff) - 60_000).toISOString(),
  capturedAt: new Date(Date.parse(over.game.kickoff) - 60_000).toISOString(), origin: 'live_pool', codeRevision: 'abc',
  feature: FEATURE, ...over,
  game: { id: over.game.id, season: over.game.season, week: over.game.week, kickoff: over.game.kickoff },
});
const result = (playerId: number, gameId: number, actual: number, over: Partial<GradeResult> = {}): GradeResult => ({
  id: String(playerId * 1000 + gameId), playerId, gameId, team: 'AAA', position: 'RB', actual, status: 'exact',
  computedAt: '2026-10-06T12:00:00.000Z', ...over,
});

// captureFromRow reads the frozen context and each player's evidence.
{
  const u = upside('lead', 0.5, dist(6, 12, 0.02), dist(11, 24, 0.08));
  const c = captureFromRow({ digest: 'd', uploadId: 'u', uploadCreatedAt: '2026-09-30T00:00:00Z', observedAt: '2026-10-04T16:59:00Z',
    capturedAt: '2026-10-04T16:59:00Z', payload: {
      origin: 'live_pool', codeRevision: 'rev', game: { id: 7, season: 2026, week: 4, kickoff: '2026-10-04T17:00:00Z' },
      context: { replacementUpside: FEATURE },
      players: [{ dkPlayerId: 1, playerId: 11, name: 'A', team: 'AAA', position: 'RB', isOut: false, projection: 6, ceiling: 12, boom: 0.02,
        evidence: { replacementUpside: u } },
      { dkPlayerId: 2, playerId: 12, name: 'B', team: 'AAA', position: 'WR', isOut: false, projection: 9, ceiling: 20, boom: 0.05,
        evidence: { replacementUpsideUnchanged: { from: 'Star', reason: 'r' } } }],
    } });
  assert.equal(c.feature?.version, 'nfl-replacement-upside-v2');
  assert.deepEqual(c.players[0].upside, u);
  assert.equal(c.players[1].unchanged?.from, 'Star');
  assert.equal(c.codeRevision, 'rev');
  const old = captureFromRow({ digest: 'd', uploadId: 'u', uploadCreatedAt: '2026-09-30T00:00:00Z', observedAt: '2026-10-04T16:59:00Z',
    capturedAt: '2026-10-04T16:59:00Z', payload: { origin: 'live_pool', game: { id: 7, season: 2026, week: 4, kickoff: '2026-10-04T17:00:00Z' },
      context: {}, players: [] } });
  assert.equal(old.feature, null, 'a capture from before the context field carries no feature');
}

// Selection: last pregame live capture per player-game across uploads; outcome-blind ties.
{
  const g4 = game(1, 4, '2026-10-04T17:00:00.000Z');
  const early = cap({ digest: 'a', game: g4, players: [player({ dkPlayerId: 1, projection: 1 })], observedAt: '2026-10-04T15:00:00.000Z' });
  const oldUpload = cap({ digest: 'b', game: g4, players: [player({ dkPlayerId: 1, projection: 2 })] });
  const newUpload = cap({ digest: 'c', game: g4, uploadId: 'u2', uploadCreatedAt: '2026-10-01T00:00:00.000Z', players: [player({ dkPlayerId: 1, projection: 3 })] });
  const late = cap({ digest: 'd', game: g4, players: [player({ dkPlayerId: 1, projection: 4 })], observedAt: '2026-10-04T17:01:00.000Z', capturedAt: '2026-10-04T17:01:00.000Z' });
  const archived = cap({ digest: 'e', game: g4, players: [player({ dkPlayerId: 1, projection: 5 })], origin: 'saved_optimizer' });
  const week3 = cap({ digest: 'f', game: game(2, 3, '2026-09-27T17:00:00.000Z'), players: [player({ dkPlayerId: 1 })] });
  const { selected, excluded } = selectPlayerGames([early, oldUpload, late, newUpload, archived, week3]);
  assert.equal(selected.length, 1);
  assert.equal(selected[0].player.projection, 3, 'newest upload wins a same-minute tie; post-kickoff and archived captures never count');
  assert.deepEqual(excluded, { notLiveCaptures: 1, outOfWindowCaptures: 1, notPregameCaptures: 1, unlinkedPlayers: 0 });
}

// DraftKings convention for the outcome.
{
  const g4 = game(1, 4, '2026-10-04T17:00:00.000Z');
  const c = cap({ digest: 'a', game: g4, players: [player({ dkPlayerId: 1 })] });
  const p = c.players[0];
  const exact = (rows: GradeResult[]) => {
    const m = new Map<number, Map<number, GradeResult>>();
    for (const r of rows) { const x = m.get(r.gameId) ?? new Map(); x.set(r.playerId, r); m.set(r.gameId, x); }
    return m;
  };
  const games = new Map([[1, g4]]);
  assert.deepEqual(resolveActual(c, p, games, exact([result(50, 1, 12)])), { status: 'scored', actual: 0 }, 'no row in a results-bearing game = 0');
  assert.deepEqual(resolveActual(c, p, games, exact([result(1, 1, 7.5)])), { status: 'scored', actual: 7.5 });
  assert.equal(resolveActual(c, p, games, exact([])).status, 'awaiting_source');
  assert.equal(resolveActual(c, p, new Map([[1, { ...g4, completed: false }]]), exact([result(50, 1, 12)])).status, 'pending_result');
  assert.equal(resolveActual(c, p, new Map([[1, { ...g4, kickoff: '2026-10-05T00:15:00.000Z' }]]), exact([result(50, 1, 12)])).status, 'schedule_changed');
  assert.equal(resolveActual(c, p, games, exact([result(1, 1, 7.5, { team: 'BBB' })])).status, 'awaiting_source', 'no player-feed row for his own team yet');
  assert.equal(resolveActual(c, p, games, exact([result(1, 1, 7.5, { team: 'BBB' }), result(50, 1, 12)])).status, 'result_identity_conflict');
  // Only DST rows (the team feed can land before the player feed): not a zero, still waiting.
  assert.equal(resolveActual(c, p, games, exact([result(90, 1, 9, { position: 'DST' })])).status, 'awaiting_source', 'a DST row alone cannot vouch for a player zero');
  // The other team's player feed alone does not vouch for this player's team.
  assert.equal(resolveActual(c, p, games, exact([result(60, 1, 14, { team: 'BBB', position: 'WR' })])).status, 'awaiting_source');
  // Once his team has a player-feed row, a missing row of his own is DraftKings' 0.
  assert.deepEqual(resolveActual(c, p, games, exact([result(90, 1, 9, { position: 'DST' }), result(51, 1, 3, { position: 'QB' })])), { status: 'scored', actual: 0 });
}

// Widening: flat loss ties to the smaller k; an under-covered control set pushes k up.
{
  const rows = (pattern: number[]): ControlRow[] => pattern.map((actual, i) => ({ season: 2026, week: 4, gameId: i, playerId: i, position: 'WR', actual, baseP90: 10 }));
  assert.equal(fitWidening(rows([...Array(9).fill(5), 11])), 1);
  assert.equal(fitWidening(rows([...Array(8).fill(5), 14, 14])), 1.4);
}

// Bootstrap: deterministic, and constant values give a zero-width interval.
{
  const rows = Array.from({ length: 20 }, (_, i) => ({ cluster: `e${i % 7}`, values: [1.5, i % 2 ? 1 : -1, null] }));
  const a = clusterBootstrap(rows, 3, 500, 1), b = clusterBootstrap(rows, 3, 500, 1);
  assert.deepEqual(a, b);
  assert.deepEqual([a[0].mean, a[0].lo, a[0].hi], [1.5, 1.5, 1.5]);
  assert.equal(a[2].n, 0); assert.equal(a[2].mean, null);
}

// Decision table.
{
  const iv = (mean: number, lo: number, hi: number) => ({ n: 100, mean, lo, hi });
  assert.equal(upsideVerdict(iv(-1, -2, -0.1), iv(-0.5, -1, -0.1), iv(-0.1, -0.3, 0.1)), 'PROMOTE');
  assert.equal(upsideVerdict(iv(-1, -2, -0.1), iv(-0.5, -1, -0.1), iv(0.1, -0.1, 0.3)), 'PROMOTE_CEILING_ONLY');
  assert.equal(upsideVerdict(iv(-1, -2, -0.1), iv(-0.5, -1, -0.1), { n: 0, mean: null, lo: null, hi: null }), 'PROMOTE_CEILING_ONLY');
  assert.equal(upsideVerdict(iv(-1, -2, -0.1), iv(-0.1, -0.5, 0.2), iv(-1, -2, -0.5)), 'NOT_PROMOTED_GENERIC');
  assert.equal(upsideVerdict(iv(-0.2, -0.6, 0.2), iv(-0.1, -0.5, 0.2), iv(-1, -2, -0.5)), 'NOT_PROMOTED');
  assert.equal(upsideVerdict(iv(1, 0.4, 1.6), iv(1, 0.4, 1.6), iv(1, 0.4, 1.6)), 'RETIRE');
}

/**
 * A whole season: each event is one team whose starting back sits (lead and
 * other flagged), and an opponent WR room at full strength (two controls).
 */
function season(events: number, weeks: number, o: {
  lead: [UpsideDistribution, UpsideDistribution]; other: [UpsideDistribution, UpsideDistribution];
  leadActual: (e: number) => number; otherActual: (e: number) => number; controlP90: number; controlActual: (c: number) => number;
}) {
  const captures: GradeCapture[] = [], games: GradeGame[] = [], results: GradeResult[] = [];
  let c = 0;
  for (let e = 0; e < events; e += 1) {
    const week = 4 + (e % weeks), id = 1000 + e;
    const kickoff = new Date(Date.parse('2026-10-01T17:00:00.000Z') + (week - 4) * 7 * 864e5 + e * 60_000).toISOString();
    const g = game(id, week, kickoff);
    games.push(g);
    const pid = (k: number) => e * 10 + k;
    const team = `T${e}`, opp = `O${e}`;
    captures.push(cap({ digest: `cap${e}`, game: g, players: [
      player({ dkPlayerId: pid(1), team, isOut: true, projection: 16, ceiling: 27 }),
      player({ dkPlayerId: pid(2), team, projection: o.lead[0].mean, ceiling: o.lead[0].p90, upside: upside('lead', 0.5, o.lead[0], o.lead[1], pid(2)) }),
      player({ dkPlayerId: pid(3), team, projection: o.other[0].mean, ceiling: o.other[0].p90, upside: upside('other', 0.2, o.other[0], o.other[1], pid(3)) }),
      player({ dkPlayerId: pid(4), team: opp, position: 'WR', projection: 15, ceiling: 30 }),
      player({ dkPlayerId: pid(5), team: opp, position: 'WR', projection: 6, ceiling: o.controlP90 }),
      player({ dkPlayerId: pid(6), team: opp, position: 'WR', projection: 5, ceiling: o.controlP90 }),
      player({ dkPlayerId: pid(7), team: opp, position: 'WR', projection: 0.4, ceiling: 3 }),
    ] }));
    results.push(result(pid(2), id, o.leadActual(e), { team }), result(pid(3), id, o.otherActual(e), { team }),
      result(pid(4), id, 20, { team: opp, position: 'WR' }),
      result(pid(5), id, o.controlActual(c++), { team: opp, position: 'WR' }), result(pid(6), id, o.controlActual(c++), { team: opp, position: 'WR' }));
  }
  return { captures, games, results, now: '2026-12-31T00:00:00.000Z' };
}

// Blinded until every floor is met: counts only, no metric, no row, no widening.
{
  const input = season(10, 3, { lead: [dist(6, 10, 0.02), dist(11, 20, 0.15)], other: [dist(4, 8, 0.01), dist(5, 12, 0.05)],
    leadActual: () => 26, otherActual: () => 3, controlP90: 10, controlActual: () => 5 });
  const r = gradeReplacementUpside(input);
  assert.equal(r.revealed, false);
  assert.equal(r.floorsMet, false);
  for (const key of ['metrics', 'rows', 'widening', 'verdict', 'descriptive']) assert.ok(!(key in r), `blinded report must not carry ${key}`);
  assert.equal(r.accrual.flaggedScored, 20);
  assert.equal(r.accrual.events, 10);
  assert.equal(r.accrual.controlsScored, 20, 'the full-strength room gives two controls; its top WR and the 0.4-point WR are not controls');
  assert.equal(r.health.gamesWithFeatureCapture, 10);
  assert.ok(!JSON.stringify(r).includes('"actual"'), 'no outcome value leaks into a blinded report');
}

// Floors met, the job mechanism is real: PROMOTE.
{
  const r = gradeReplacementUpside(season(66, 8, {
    lead: [dist(6, 10, 0.02), dist(11, 20, 0.15)], other: [dist(4, 8, 0.01), dist(5, 12, 0.05)],
    leadActual: (e) => (e % 5 < 2 ? 26 : 4), otherActual: (e) => (e % 10 === 0 ? 26 : 3),
    controlP90: 10, controlActual: (c) => (c % 11 === 0 ? 11 : 5),
  }));
  assert.equal(r.revealed, true);
  if (!r.revealed) throw new Error('unreachable');
  assert.equal(r.floors.events.have, 66); assert.equal(r.floors.flagged.have, 132); assert.equal(r.floors.weeks.have, 8);
  assert.equal(r.widening.k, 1);
  assert.ok(r.metrics.passed.g1 && r.metrics.passed.g2 && r.metrics.passed.g3, JSON.stringify(r.metrics));
  assert.equal(r.verdict, 'PROMOTE');
  assert.deepEqual(gradeReplacementUpside(season(66, 8, {
    lead: [dist(6, 10, 0.02), dist(11, 20, 0.15)], other: [dist(4, 8, 0.01), dist(5, 12, 0.05)],
    leadActual: (e) => (e % 5 < 2 ? 26 : 4), otherActual: (e) => (e % 10 === 0 ? 26 : 3),
    controlP90: 10, controlActual: (c) => (c % 11 === 0 ? 11 : 5),
  })), r, 'deterministic');
}

// Backups score little: the mixture ceiling is confirmed worse, RETIRE.
{
  const r = gradeReplacementUpside(season(66, 8, {
    lead: [dist(6, 10, 0.02), dist(11, 20, 0.15)], other: [dist(4, 8, 0.01), dist(5, 12, 0.05)],
    leadActual: (e) => 1 + (e % 3), otherActual: (e) => 1 + (e % 2), controlP90: 10, controlActual: (c) => (c % 11 === 0 ? 11 : 5),
  }));
  assert.ok(r.revealed && r.verdict === 'RETIRE');
}

// Every ceiling runs low by the same factor: the mixture beats the baseline but not a generic widening.
{
  const r = gradeReplacementUpside(season(66, 8, {
    lead: [dist(6, 10, 0.02), dist(8, 13.5, 0.05)], other: [dist(4, 8, 0.01), dist(5, 10.8, 0.03)],
    leadActual: (e) => (e % 10 < 3 ? 14 : 5), otherActual: (e) => (e % 10 < 3 ? 14 : 5),
    controlP90: 10, controlActual: (c) => (c % 10 < 3 ? 14 : 5),
  }));
  if (!r.revealed) throw new Error('floors should be met');
  assert.equal(r.widening.k, 1.4);
  assert.ok(r.metrics.passed.g1 && !r.metrics.passed.g2, JSON.stringify(r.metrics));
  assert.equal(r.verdict, 'NOT_PROMOTED_GENERIC');
}

// A newer capture without the feature wins selection and removes the player-game; an older feature capture is not reused.
{
  const input = season(2, 2, { lead: [dist(6, 10, 0.02), dist(11, 20, 0.15)], other: [dist(4, 8, 0.01), dist(5, 12, 0.05)],
    leadActual: () => 26, otherActual: () => 3, controlP90: 10, controlActual: () => 5 });
  const stale = { ...input.captures[0], digest: 'nofeature', uploadId: 'u2', uploadCreatedAt: '2026-10-02T00:00:00.000Z', feature: null };
  const r = gradeReplacementUpside({ ...input, captures: [...input.captures, stale] });
  assert.equal(r.accrual.events, 1, 'the game whose last capture lacks the feature drops out');
  assert.ok(r.health.playerGamesWithoutFeature >= 7);
}

console.log('Verified replacement-upside grade: frozen spec, capture selection, DK outcome rule, blinding, widening, bootstrap and the decision table passed');
