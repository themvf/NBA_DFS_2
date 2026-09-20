import assert from "node:assert/strict";
import { EMPTY_EVIDENCE, evidenceGrades, marketReview, noVigHome, safeSourceUrl,
  recentForm, scenarioDecision, timestamp, type FrozenEvidence, type GameEvidence, type Performance } from "../src/lib/nfl/pickem-evidence";

const now = "2026-09-20T15:00:00Z", kickoff = "2026-09-20T17:00:00Z";
const evidence: GameEvidence = { ...EMPTY_EVIDENCE,
  opening: { capturedAt: "2026-09-18T15:00:00Z", pHome: 0.53, homeSpread: -1, source: "test" },
  latest: { capturedAt: "2026-09-20T14:00:00Z", pHome: 0.45, homeSpread: 2.5, source: "test" },
  news: [{ id: "1", team: "ATL", category: "quarterback", headline: "QB unavailable", detail: "",
    source: "Official report", url: "https://example.com/report", status: "confirmed",
    publishedAt: "2026-09-20T14:30:00Z", observedAt: "2026-09-20T14:40:00Z" }],
};
assert.equal(noVigHome(-150, 130)?.toFixed(4), "0.5798");
assert.equal(noVigHome(null, 130), null);
assert.equal(noVigHome(0, 0), null);
assert.equal(noVigHome(NaN, 130), null);
assert.equal(timestamp("2026-09-20 15:00:00+00"), Date.parse(now));
assert.equal(safeSourceUrl("javascript:alert(1)"), null);
assert.equal(safeSourceUrl("https://user:secret@example.com"), null);
assert.equal(safeSourceUrl("https://example.com/report"), "https://example.com/report");
const review = marketReview(evidence, 0.532, kickoff, now);
assert.equal(review.favoriteChanged, true);
assert.equal(review.probabilityConflict, true);
assert.equal(review.newsAfterQuote, 1);
assert.equal(review.stale, false);
assert.equal(marketReview(evidence, 0.45, kickoff, now).probabilityConflict, false);
assert.equal(marketReview(EMPTY_EVIDENCE, 0.53, kickoff, now).stale, true);
assert.equal(marketReview(evidence, 0.53, kickoff, "2026-09-20T16:01:00Z").stale, true);
assert.equal(marketReview(evidence, 0.53, kickoff, "2026-09-20T18:00:00Z").stale, false);
assert.equal(marketReview(evidence, 0.53, "2026-09-27T17:00:00Z", now).maxAgeHours, 24);
assert.equal(marketReview({ ...evidence, latest: { ...evidence.latest!, capturedAt: "2026-09-21T00:00:00Z" } }, 0.53, kickoff, now).stale, true);
assert.equal(marketReview({ ...evidence, opening: { ...evidence.opening!, pHome: 0.5 } }, 0.5, kickoff, now).favoriteChanged, false);
assert.equal(marketReview({ ...evidence, latest: { ...evidence.latest!, pHome: null } }, 0.53, kickoff, now).probabilityConflict, true);
const s = scenarioDecision(0.4, true, 3);
assert.ok(Math.abs(s.switchCost + 0.6) < 1e-9);
assert.ok(Math.abs(s.expectedPoints - 1.2) < 1e-9);
assert.equal(s.preferredHome, false);
assert.equal(scenarioDecision(0.4, false, 1).p, 0.6);
const frozen: FrozenEvidence = { ...evidence, version: 1, recordedAt: now, probabilityComputedAt: now,
  narrative: "crowded", favoriteHome: true, scenario: null, marketBaselinePickHome: false, marketBaselineConfidence: 3 };
const game = { gameId: 1, pHome: 0.6, homeWon: false, recommendedPickHome: true, recommendedConfidence: 3,
  fieldHomeShare: 0.7, fieldSource: "observed", evidence: frozen };
const grade = evidenceGrades([game, { ...game, gameId: 2, evidence: null }, { ...game, gameId: 3, homeWon: null, fieldSource: "modeled" }]);
assert.equal(grade.n, 1);
assert.equal(grade.points, 0);
assert.equal(grade.marketPoints, 3);
assert.equal(grade.brier, 0.36);
assert.equal(grade.marketBrier, 0.45 ** 2);
assert.equal(grade.narrative.crowded.n, 1);
assert.ok(Math.abs(grade.narrative.crowded.sum - 10) < 1e-9);
assert.equal(evidenceGrades([{ ...game, evidence: { ...frozen, marketBaselineConfidence: null } }]).n, 0);
assert.equal(evidenceGrades([]).brier, null);
assert.equal(evidenceGrades([{ ...game, fieldSource: "modeled" }]).narrative.crowded.n, 0);
const performance = [1, 2, 3, 4, 5].flatMap(week => ["ATL", "CAR"].map(team => ({ gameId: week, week, team,
  plays: 50, epaPerPlay: week / 10, successRate: 0.5, rushYards: 100, turnovers: 1,
  fieldGoalsMade: 1, defensiveReturnTdsAllowed: 0, kickReturnTdsAllowed: 0 } satisfies Performance)));
assert.equal(recentForm(performance, "ATL", 1).games, 0);
assert.equal(recentForm(performance, "ATL", 2).offenseEpa, 0.1);
assert.equal(recentForm(performance, "ATL", 5).games, 3);
assert.ok(Math.abs(recentForm(performance, "ATL", 5).offenseEpa! - 0.3) < 1e-9, "Current/future week excluded, only last three included");
assert.equal(recentForm(performance.filter(p => p.team === "ATL"), "ATL", 5).defenseEpa, null);
console.log("Pick'em evidence assertions passed (freshness, favorite changes, source links, scenarios, paired grading).");
