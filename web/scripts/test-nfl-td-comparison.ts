import assert from "node:assert/strict";
import { actualCount, OUTCOME, scores, state, validateForecast, validateResult, type Forecast, type Game, type Result } from "../src/lib/nfl/td-comparison";
const game: Game = { gameId: "2026_09_A_B", season: 2026, week: 9, kickoff: "2026-11-08T18:00:00Z", home: "B", away: "A", completed: false };
const evidence = [{ source: "PBP capture", capturedAt: "2026-11-06T12:00:00Z", sha256: "a".repeat(64) }];
const forecast: Forecast = { schemaVersion: 1, outcome: OUTCOME, game, generatedAt: "2026-11-08T17:30:00Z", inputCutoff: "2026-11-08T17:00:00Z", versions: { direct: "a1", opportunity: "b1", baseline: "c1" }, implementationSha256: "b".repeat(64), evidence, availabilityNotes: ["Test fixture only"], players: [
  { identity: "00-0000001", name: "One", team: "A", position: "RB", availability: "Available", probabilities: { direct: .8, opportunity: .5, baseline: .2 } },
  { identity: "00-0000002", name: "Two", team: "B", position: "TE", availability: "Questionable", probabilities: { direct: .2, opportunity: .5, baseline: .8 } },
] };
const result: Result = { schemaVersion: 1, outcome: OUTCOME, gameId: game.gameId, verifiedAt: "2026-11-09T10:00:00Z", complete: true, includesOvertime: true,
  evidence: [{ ...evidence[0], capturedAt: "2026-11-09T09:00:00Z" }, { ...evidence[0], source: "Official box", capturedAt: "2026-11-09T09:00:00Z" }],
  pbp: [{ identity: "00-0000001", team: "A", rushingTds: 1, receivingTds: 1 }, { identity: "00-0000002", team: "B", rushingTds: 0, receivingTds: 0 }], box: [], teamTotals: { A: 2, B: 0 } };
result.box = structuredClone(result.pbp);
let passed = 0;
function test(name: string, fn: () => void) { fn(); passed++; console.log(`PASS ${name}`); }
const freeze = "2026-11-08T17:40:00Z", imported = "2026-11-09T11:00:00Z", final = { ...game, completed: true };
test("canonical pregame forecast accepted", () => validateForecast(forecast, game, freeze));
test("late and backdated forecast rejected", () => { assert.throws(() => validateForecast(forecast, game, "2026-11-08T17:40:01Z")); assert.throws(() => validateForecast(forecast, game, "2026-11-08T17:00:00Z")); });
test("canonical identity mismatch rejected", () => assert.throws(() => validateForecast(forecast, { ...game, home: "C" }, freeze)));
test("first-TD/regulation-only contract rejected", () => assert.throws(() => validateForecast({ ...forecast, outcome: "first_td" } as unknown as Forecast, game, freeze)));
test("future evidence rejected", () => assert.throws(() => validateForecast({ ...forecast, evidence: [{ ...evidence[0], capturedAt: "2026-11-08T17:01:00Z" }] }, game, freeze)));
test("identities and probabilities validated", () => {
  assert.throws(() => validateForecast({ ...forecast, players: [forecast.players[0], forecast.players[0]] }, game, freeze));
  for (const value of [NaN, Infinity, -1, 1.01, null]) { const f = structuredClone(forecast); f.players[0].probabilities.direct = value as number; assert.throws(() => validateForecast(f, game, freeze)); }
});
test("reconciled full-game results accepted", () => validateResult(result, forecast, final, imported));
test("incomplete game and overtime exclusion rejected", () => { assert.throws(() => validateResult(result, forecast, game, imported)); assert.throws(() => validateResult({ ...result, includesOvertime: false } as unknown as Result, forecast, final, imported)); });
test("missing actual never becomes zero", () => { const r = { ...result, pbp: [result.pbp[0]], box: [result.box[0]] }; assert.throws(() => validateResult(r, forecast, final, imported)); assert.equal(actualCount(r, "00-0000002"), null); });
test("TD credit disagreement rejected", () => { const r = structuredClone(result); r.box[0].rushingTds = 0; assert.throws(() => validateResult(r, forecast, final, imported)); });
test("team total disagreement rejected", () => assert.throws(() => validateResult({ ...result, teamTotals: { A: 1, B: 0 } }, forecast, final, imported)));
test("extra scorers retained without expanding forecast population", () => { const r = structuredClone(result); const extra = { identity: "00-0000003", team: "B", rushingTds: 0, receivingTds: 1 }; r.pbp.push(extra); r.box.push(extra); r.teamTotals.B = 1; validateResult(r, forecast, final, imported); assert.equal(scores([{ game: final, forecast, result: r, frozenAt: freeze }])[0].count, 2); });
test("paired scores use binary outcomes and include zeros", () => { const s = scores([{ game: final, forecast, result, frozenAt: freeze }]); assert.ok(Math.abs(s[0].brier! - .04) < 1e-12); assert.ok(Math.abs(s[0].logLoss! + Math.log(.8)) < 1e-12); assert.equal(s[1].brier, .25); assert.equal(s[0].count, 2); });
test("unknown outcomes have no scores", () => assert.equal(scores([{ game, forecast, result: null, frozenAt: freeze }])[0].brier, null));
test("partial unvalidated result cannot silently grade a missing player as zero", () => assert.equal(scores([{ game: final, forecast, result: { ...result, pbp: [result.pbp[0]] }, frozenAt: freeze }])[0].count, 0));
test("malformed rows are excluded and availability states explicit", () => { const row = { game, forecast: null, result: null, frozenAt: null }; assert.equal(state(row, 0), "Forecast pending"); assert.equal(state(row, Date.parse(game.kickoff)), "No frozen forecast"); assert.equal(scores([{ ...row, forecast, result, error: "bad" }])[0].count, 0); });
test("0/1 probabilities have finite log loss and inclusive final bin", () => { const f = structuredClone(forecast); f.players[0].probabilities.direct = 0; f.players[1].probabilities.direct = 1; const s = scores([{ game: final, forecast: f, result, frozenAt: freeze }])[0]; assert.ok(Number.isFinite(s.logLoss)); assert.equal(s.bins[4].count, 1); });
console.log(`${passed} TD comparison checks passed`);
