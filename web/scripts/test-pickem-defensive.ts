import assert from "node:assert/strict";
import { comparePickemDefensiveForecasts, selectPickemDefensiveForecasts } from "../src/lib/nfl/pickem-defensive";
import { matchupResidual, type ResidualInput } from "../src/lib/nfl/pickem-matchup";
import { evOptimalEntry } from "../src/lib/nfl/pickem-strategy";
import type { PickemSlate } from "../src/db/queries";
import type { PickemEvidence } from "../src/lib/nfl/pickem-evidence";

const quoteAt = "2026-10-04T16:00:00Z", cutoff = "2026-10-04T16:05:00Z";
const kickoff = "2026-10-04T17:00:00Z", asOf = "2026-10-04T16:10:00Z";
const input: ResidualInput = { gameId: "fixture", decisionCutoff: cutoff, kickoff,
  baseline: { homeConditional: .49, tie: .01, marketCapturedAt: quoteAt, source: "market" },
  model: { artifactId: "fixture", definitionId: "combined", version: "v1", consumerId: "nfl-pickem",
    useCase: "game-win", cohort: "regular-season", coefficients: { pressure: .12 }, intercept: 0,
    trainedThrough: "2026-01-01T00:00:00Z", trainingManifest: { fixture: true } },
  features: [{ definitionId: "pressure", value: 1, snapshotId: "capture", availableAt: quoteAt }] };
const matchup = matchupResidual(input);
assert.equal(matchup.status, "shadow");
assert.ok(matchup.candidate!.homeConditional > .5);
const slate = { season: 2026, weeks: [4], games: [{ gameId: 1, week: 4, pHome: .49, pTie: .01,
  provenance: "market_ml_novig", kickoff, completed: false, homeAbbrev: "HOME", awayAbbrev: "AWAY" }] } as PickemSlate;
const evidence = { loadedAt: asOf, warnings: [], games: { 1: {
  opening: null, latest: { capturedAt: quoteAt, pHome: .49, homeSpread: null, source: "market" },
  matchup, news: [], performance: [] } } } as PickemEvidence;
const approved = selectPickemDefensiveForecasts(slate, evidence, "approved", asOf);
assert.equal(approved.slate.games[0].pHome, .49);
assert.deepEqual(approved.appliedGameIds, []);
const experimental = selectPickemDefensiveForecasts(slate, evidence, "experimental", asOf);
assert.deepEqual(experimental.appliedGameIds, [1]);
assert.equal(experimental.slate.games[0].pHome, matchup.candidate!.homeConditional);
assert.equal(experimental.slate.games[0].provenance, "experimental_defensive_matchup");
const comparison = comparePickemDefensiveForecasts(slate, evidence, asOf);
assert.deepEqual(comparison.comparisons[1], { gameId: 1, baselineHome: .49,
  baselinePlusDefenseHome: matchup.candidate!.homeConditional, applied: true });
const zeroTieSlate = { ...slate, games: [{ ...slate.games[0], pTie: 0 }] };
const zeroTieComparison = comparePickemDefensiveForecasts(zeroTieSlate, evidence, asOf);
assert.deepEqual(zeroTieComparison.appliedGameIds, [1]);
assert.equal(zeroTieComparison.slate.games[0].pTie, 0);
assert.equal(zeroTieComparison.slate.games[0].pHome, matchup.candidate!.homeConditional);
const missing = comparePickemDefensiveForecasts(slate, { ...evidence, games: { 1: { ...evidence.games[1], matchup: null } } }, asOf);
assert.deepEqual(missing.comparisons[1], { gameId: 1, baselineHome: .49, baselinePlusDefenseHome: .49, applied: false, reason: "Opponent forecast has not been generated" });
assert.equal(evOptimalEntry(approved.slate.games.map(g => ({ ...g, fieldHomePct: null })), "straight").pickHome[0], false);
assert.equal(evOptimalEntry(experimental.slate.games.map(g => ({ ...g, fieldHomePct: null })), "straight").pickHome[0], true);
assert.deepEqual(selectPickemDefensiveForecasts(slate, evidence, "experimental", kickoff).appliedGameIds, []);
assert.deepEqual(selectPickemDefensiveForecasts(slate, evidence, "experimental", "2026-10-04T18:10:00Z").appliedGameIds, []);
assert.deepEqual(selectPickemDefensiveForecasts(slate, { ...evidence, games: { 1: { ...evidence.games[1],
  latest: { ...evidence.games[1].latest!, pHome: .48 } } } }, "experimental", asOf).appliedGameIds, []);
assert.deepEqual(selectPickemDefensiveForecasts(slate, { ...evidence, games: { 1: { ...evidence.games[1],
  latest: { ...evidence.games[1].latest!, capturedAt: "2026-10-04T16:09:00Z" } } } }, "experimental", asOf).appliedGameIds, [1]);
console.log("Pick'em defensive comparison: baseline preserved, adjusted card flip, lock and exact-market fallback passed.");

const movedQuote = "2026-10-04T16:09:00Z";
const movedSlate = { ...slate, games: [{ ...slate.games[0], pHome: .55 }] };
const movedEvidence = { ...evidence, games: { 1: { ...evidence.games[1], latest: {
  ...evidence.games[1].latest!, capturedAt: movedQuote, pHome: .55 } } } };
const moved = selectPickemDefensiveForecasts(movedSlate, movedEvidence, "experimental", asOf);
assert.deepEqual(moved.appliedGameIds, [1]);
assert.equal(moved.forecasts[1].input.baseline.marketCapturedAt, movedQuote);
assert.equal(moved.forecasts[1].input.baseline.homeConditional, .55);
assert.equal(moved.forecasts[1].residual, matchup.residual);
assert.equal(moved.forecasts[1].input.features, input.features);
assert.ok(moved.slate.games[0].pHome > .55);
const reopened = matchupResidual(JSON.parse(JSON.stringify(moved.forecasts[1].input)));
assert.equal(reopened.candidate!.homeConditional, moved.slate.games[0].pHome);
const staleInput = { ...input, baseline: { ...input.baseline, marketCapturedAt: "2026-10-04T12:00:00Z" } };
const staleMatchup = matchupResidual(staleInput);
assert.equal(staleMatchup.status, "unavailable");
const recovered = selectPickemDefensiveForecasts(slate, { ...evidence, games: { 1: {
  ...evidence.games[1], matchup: staleMatchup } } }, "experimental", asOf);
assert.deepEqual(recovered.appliedGameIds, [1]);
const staleNow = selectPickemDefensiveForecasts(slate, { ...evidence, games: { 1: {
  ...evidence.games[1], latest: { ...evidence.games[1].latest!, capturedAt: "2026-10-04T12:00:00Z" } } } }, "experimental", asOf);
assert.deepEqual(staleNow.appliedGameIds, []);
assert.equal(staleNow.reasons[1], "Odds need a fresh capture");
const frozenAfterKick = comparePickemDefensiveForecasts(slate, evidence, "2026-10-04T18:00:00Z");
assert.deepEqual(frozenAfterKick.appliedGameIds, []);
assert.equal(frozenAfterKick.comparisons[1].historical, true);
assert.equal(frozenAfterKick.comparisons[1].baselineHome, .49);
assert.equal(frozenAfterKick.comparisons[1].baselinePlusDefenseHome, matchup.candidate!.homeConditional);
assert.equal(frozenAfterKick.comparisons[1].reason, "Saved pregame forecast");
console.log("Fresh-quote recomputation, stale recovery, saved-input replay and historical pair display passed.");
