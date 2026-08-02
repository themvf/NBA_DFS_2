import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { queryRows } from "../src/db/query-result";
import { buildSnakeSlots, nextControlledPick, picksUntilControlled } from "../src/lib/fantasy-football/draft-engine";
import { recommendPlayers } from "../src/lib/fantasy-football/recommendations";
import { fantasyBadgeClass } from "../src/lib/fantasy-football/badge-style";
import { filterFantasyRankings } from "../src/lib/fantasy-football/ranking-filters";
import type { FantasyRankingRow } from "../src/db/queries-fantasy-football";
import { buildProjectionExplanation } from "../src/lib/fantasy-football/projection-explanation";
import { buildBestBallDraftBoard, canAddBestBallPlayer, getBestBallRosterStatus, parseBestBallDraftState } from "../src/lib/fantasy-football/best-ball";
import {
  bestBallAdvisorDraftSignature,
  buildBestBallAdvisorProviderSnapshot,
  buildBestBallAdvisorSnapshot,
  enrichBestBallAdvisorResult,
  getValidatedBestBallAdvisorOutput,
  validateBestBallAdvisorOutput,
} from "../src/lib/fantasy-football/ai-draft-advisor";

assert.deepEqual(queryRows<{ id: number }>({ rows: [{ id: 3 }] }), [{ id: 3 }]);
assert.deepEqual(queryRows<{ id: number }>([{ id: 2 }]), [{ id: 2 }]);
assert.deepEqual(queryRows<{ id: number }>({}), []);

const slots = buildSnakeSlots(4, 3);
assert.deepEqual(slots.map((slot) => slot.teamSlot), [1, 2, 3, 4, 4, 3, 2, 1, 1, 2, 3, 4]);
assert.equal(nextControlledPick(3, 2, 4, 3), 7);
assert.equal(picksUntilControlled(3, 2, 4, 3), 4);

const recommendations = recommendPlayers([
  { playerId: 1, position: "RB", ourRank: 12, ecr: 15, adp: 25, projectedPoints: 250, tier: 2, confidence: 0.8 },
  { playerId: 2, position: "QB", ourRank: 8, ecr: 8, adp: 8, projectedPoints: 330, tier: 1, confidence: 0.9 },
], ["QB"], 10);
assert.equal(recommendations[0].playerId, 1);
assert.equal(recommendations[0].adpDelta, 13);
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_TARGETS", class: "fact" }), /blue/);
assert.match(fantasyBadgeClass({ code: "NFL_TOP_10_RUSH_TDS", class: "fact" }), /orange/);
assert.match(fantasyBadgeClass({ code: "TEAM_TARGET_LEADER", class: "fact" }), /cyan/);
assert.match(fantasyBadgeClass({ code: "INJURY", class: "risk" }), /red/);
assert.match(fantasyBadgeClass({ code: "TOP_3_POSITION_POINTS", class: "fact" }), /yellow/);
const filterRows = [
  { playerId: 1, name: "A.J. Brown", position: "WR", team: "NE" },
  { playerId: 2, name: "Bijan Robinson", position: "RB", team: "ATL" },
] as FantasyRankingRow[];
assert.deepEqual(filterFantasyRankings(filterRows, { name: "brown", position: "WR", team: "NE" }).map((row) => row.playerId), [1]);
assert.equal(filterFantasyRankings(filterRows, { name: "", position: "RB", team: "" }).length, 1);
const explanation = buildProjectionExplanation({
  method: "history_regression",
  season_inputs: [{ season: 2025, ppg: 20.61, weight: 0.55 }],
  weighted_history_ppg: 17.79,
  position_prior_ppg: 14,
  regression_prior_games: 4,
  regressed_ppg: 17.06,
  expected_games_before_injury: 15.9,
  role_factor: 1,
  base_points_before_injury: 271.25,
  injury_factor: 1,
  final_points: 271.25,
  not_modeled: ["current teammates", "future schedule"],
});
assert.equal(explanation.method, "Weighted history + regression");
assert.ok(explanation.lines.includes("2025: 20.6 PPG × 55%"));
assert.ok(explanation.lines.includes("17.06 PPG × 15.90 games × 1.00 role = 271.3"));
assert.deepEqual(explanation.notModeled, ["current teammates", "future schedule"]);
const baselineExplanation = buildProjectionExplanation({
  method: "history_regression",
  weighted_history_ppg: 20.2,
  position_prior_ppg: 8,
  regression_prior_games: 4,
  regression_sample_games: 51,
  regressed_ppg: 19.31,
  baseline_games: 17,
  expected_games_after_injury: 16,
  role_factor: 1,
  base_points_before_injury: 328.27,
  final_points: 328.27,
  injury_factor: 1,
  availability_adjustment_applied_to_baseline: false,
});
assert.ok(baselineExplanation.lines.includes("Regression: 51 historical games + 4 position-prior games at 8.0 PPG → 19.3 PPG"));
assert.ok(baselineExplanation.lines.includes("19.31 PPG × 17.00 games × 1.00 role = 328.3"));
assert.ok(baselineExplanation.lines.includes("Availability estimate: 16.0 active games (modeled separately; not deducted from this baseline)"));
const validBestBallRoster = [
  ...Array.from({ length: 3 }, (_, index) => ({ playerId: index + 1, position: "QB", team: index ? "BUF" : "NE" })),
  ...Array.from({ length: 6 }, (_, index) => ({ playerId: index + 10, position: "RB", team: "ATL" })),
  ...Array.from({ length: 8 }, (_, index) => ({ playerId: index + 20, position: "WR", team: "LAR" })),
  ...Array.from({ length: 3 }, (_, index) => ({ playerId: index + 30, position: "TE", team: "KC" })),
];
assert.equal(getBestBallRosterStatus(validBestBallRoster).valid, true);
assert.equal(getBestBallRosterStatus(validBestBallRoster.slice(0, 19)).valid, false);
assert.equal(canAddBestBallPlayer(validBestBallRoster, { playerId: 99, position: "WR", team: "SEA" }), false);
assert.equal(canAddBestBallPlayer([], { playerId: 100, position: "K", team: "DAL" }), false);
const bestBallSlots = buildSnakeSlots(12, 20);
assert.equal(bestBallSlots.length, 240);
assert.deepEqual(bestBallSlots.slice(0, 14).map((slot) => slot.teamSlot), [1,2,3,4,5,6,7,8,9,10,11,12,12,11]);
assert.deepEqual(bestBallSlots.filter((slot) => slot.teamSlot === 1).slice(0, 4).map((slot) => slot.overallPick), [1,24,25,48]);
const draftBoard = buildBestBallDraftBoard(Array.from({ length: 14 }, (_, index) => 1001 + index));
assert.equal(draftBoard.length, 20);
assert.equal(draftBoard[0].length, 12);
assert.deepEqual(draftBoard[0][0], { round: 1, pickInRound: 1, teamSlot: 1, overallPick: 1, playerId: 1001 });
assert.deepEqual(draftBoard[1][11], { round: 2, pickInRound: 1, teamSlot: 12, overallPick: 13, playerId: 1013 });
assert.deepEqual(draftBoard[1][10], { round: 2, pickInRound: 2, teamSlot: 11, overallPick: 14, playerId: 1014 });
assert.equal(draftBoard[1][9].playerId, null);
assert.deepEqual(parseBestBallDraftState('{"userSlot":7,"playerIds":[4,5,5,6]}'), { userSlot: 7, playerIds: [4,5,6] });
assert.deepEqual(parseBestBallDraftState('bad-json'), { userSlot: 1, playerIds: [] });

function advisorRow(overrides: Partial<FantasyRankingRow> & Pick<FantasyRankingRow, "playerId" | "name" | "position">): FantasyRankingRow {
  return {
    team: null, rookie: false, byeWeek: null, injuryStatus: null, ecr: null, positionRank: null,
    ourRank: null, tier: null, adp: null, projectedPoints: null, fantasyProsProjectedPoints: null,
    fantasyProsProjectionFetchedAt: null, fantasyProsProjectionUpdatedAt: null, ourProjectedPoints: null,
    games2025: null, fantasyPoints2025: null, projectionDetails: null, expectedGames: null, confidence: null,
    indicators: [], ...overrides,
  };
}
const advisorRows = [
  advisorRow({ playerId: 1, name: "Team One Pick", position: "WR", team: "BUF", byeWeek: 7, ourRank: 1, positionRank: 1, adp: 1, ourProjectedPoints: 300, fantasyProsProjectedPoints: 298, games2025: 17, fantasyPoints2025: 290, confidence: 0.9 }),
  advisorRow({ playerId: 2, name: "My First Pick", position: "QB", team: "BAL", byeWeek: 8, ourRank: 2, positionRank: 1, adp: 2, ourProjectedPoints: 350, fantasyProsProjectedPoints: 340, games2025: 17, fantasyPoints2025: 345, confidence: 0.9 }),
  advisorRow({ playerId: 3, name: "Available Runner", position: "RB", team: "ATL", byeWeek: 5, ourRank: 3, positionRank: 1, adp: 4, ourProjectedPoints: 280, fantasyProsProjectedPoints: 275, games2025: 17, fantasyPoints2025: 270, confidence: 0.8 }),
  advisorRow({ playerId: 4, name: "Available Receiver", position: "WR", team: "LAR", byeWeek: 9, ourRank: 4, positionRank: 2, adp: 5, ourProjectedPoints: 270, fantasyProsProjectedPoints: 268, games2025: 16, fantasyPoints2025: 260, confidence: 0.8 }),
  advisorRow({ playerId: 5, name: "Available Tight End", position: "TE", team: "KC", byeWeek: 10, ourRank: 5, positionRank: 1, adp: 6, ourProjectedPoints: 240, fantasyProsProjectedPoints: 235, games2025: 17, fantasyPoints2025: 230, confidence: 0.75 }),
];
const advisorSnapshot = buildBestBallAdvisorSnapshot(advisorRows, { rankingSetId: 42, userSlot: 2, playerIds: [1, 2] });
assert.equal(advisorSnapshot.projectionModel, "ff-independent-v1.4");
assert.equal(advisorSnapshot.draft.currentOverallPick, 3);
assert.equal(advisorSnapshot.draft.targetOverallPick, 23);
assert.equal(advisorSnapshot.draft.picksUntilUser, 20);
assert.deepEqual(advisorSnapshot.userRoster.map((player) => player.playerId), [2]);
assert.deepEqual(advisorSnapshot.candidates.map((player) => player.playerId), [3, 4, 5]);
assert.deepEqual(advisorSnapshot.candidates.map((player) => player.candidateKey), ["C01", "C02", "C03"]);
const providerSnapshotJson = JSON.stringify(buildBestBallAdvisorProviderSnapshot(advisorSnapshot));
assert.doesNotMatch(providerSnapshotJson, /"playerIds?"/);
assert.match(providerSnapshotJson, /"candidateKey":"C01"/);
assert.deepEqual(advisorSnapshot.userByeWeeks.QB, [8]);
const advisorOutput = validateBestBallAdvisorOutput({
  recommendedCandidateKey: " c01 ",
  confidence: 0.82,
  whyNow: "Best combination of projection and availability.",
  rosterFit: "Adds the first running back without duplicating the quarterback bye.",
  evidence: ["V1.4 projects 280 points.", "ADP is 4."],
  risks: "Role uncertainty remains.",
  alternatives: [{ candidateKey: "C02", reason: "Receiver value." }, { candidateKey: "C03", reason: "Tight-end value." }],
  strategyUntilNextTurn: "Watch the running-back tier.",
  whatWouldChange: "A confirmed role change.",
}, advisorSnapshot);
assert.equal(advisorOutput.recommendedPlayerId, 3);
assert.equal(advisorOutput.confidence, 82);
assert.deepEqual(advisorOutput.risks, ["Role uncertainty remains."]);
assert.equal(enrichBestBallAdvisorResult(advisorOutput, advisorSnapshot).recommendation.name, "Available Runner");
assert.equal(validateBestBallAdvisorOutput({
  recommendation: {
    candidate_key: "Available Runner",
    confidence: 0.82,
    why_now: advisorOutput.whyNow,
    roster_fit: advisorOutput.rosterFit,
    evidence: advisorOutput.evidence,
    risk: advisorOutput.risks[0],
    alternatives: [{ candidate_key: "candidate C2", reason: "Receiver value." }, { player_name: "Available Tight End", reason: "Tight-end value." }],
    strategy_until_next_turn: advisorOutput.strategyUntilNextTurn,
    what_would_change: advisorOutput.whatWouldChange,
  },
}, advisorSnapshot).recommendedPlayerId, 3);
const looseAdvisorOutput = validateBestBallAdvisorOutput({
  selection: { primary: "Available Runner" },
  explanation: "Best available player for this pick.",
  strategy: "Reassess after the next pick.",
  caveats: "Role may change.",
}, advisorSnapshot);
assert.equal(looseAdvisorOutput.recommendedPlayerId, 3);
assert.equal(looseAdvisorOutput.confidenceProvided, false);
assert.equal(looseAdvisorOutput.alternatives.length, 2);
assert.throws(() => validateBestBallAdvisorOutput({ recommendedCandidateKey: "C99" }, advisorSnapshot), /no longer legal or available/);
async function testAdvisorCorrection() {
  let advisorAttempts = 0;
  const correctedAdvisor = await getValidatedBestBallAdvisorOutput(advisorSnapshot, async (correction) => {
    advisorAttempts += 1;
    if (!correction) return { recommendedCandidateKey: "C99" };
    assert.match(correction.validationError, /no longer legal or available/);
    return {
      recommendedCandidateKey: "C01", confidence: 80, whyNow: "Corrected legal choice.",
      rosterFit: "Adds a running back.", evidence: ["V1.4 points", "ADP"], risks: ["Role risk"],
      alternatives: [{ candidateKey: "C02", reason: "Receiver." }, { candidateKey: "C03", reason: "Tight end." }],
      strategyUntilNextTurn: "Watch tiers.", whatWouldChange: "New role data.",
    };
  });
  assert.equal(advisorAttempts, 2);
  assert.equal(correctedAdvisor.retried, true);
  assert.equal(correctedAdvisor.output.recommendedPlayerId, 3);
  await assert.rejects(
    () => getValidatedBestBallAdvisorOutput(advisorSnapshot, async () => ({ recommendedCandidateKey: "C99" })),
    /rechecked the live board.*no longer legal or available/,
  );
}
assert.notEqual(
  bestBallAdvisorDraftSignature({ rankingSetId: 42, userSlot: 2, playerIds: [1, 2] }),
  bestBallAdvisorDraftSignature({ rankingSetId: 42, userSlot: 2, playerIds: [1, 2, 3] }),
);
const advisorActionSource = readFileSync(new URL("../src/app/fantasy-football/best-ball/advisor-actions.ts", import.meta.url), "utf8");
assert.doesNotMatch(advisorActionSource, /NEXT_PUBLIC_(OPENAI|DEEPSEEK)/);
void testAdvisorCorrection().then(() => console.log("fantasy-football tests passed")).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
