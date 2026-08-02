import { getFantasyRankings, getLatestRankingSet } from "../src/db/queries-fantasy-football";
import {
  buildBestBallAdvisorSnapshot,
  validateBestBallAdvisorOutput,
  type BestBallAdvisorProvider,
} from "../src/lib/fantasy-football/ai-draft-advisor";
import {
  callBestBallAdvisorProvider,
  DEEPSEEK_BEST_BALL_MODEL,
  OPENAI_BEST_BALL_MODEL,
} from "../src/lib/fantasy-football/ai-draft-advisor-providers";
import { BEST_BALL_POSITIONS } from "../src/lib/fantasy-football/best-ball";

async function main() {
  const providers: BestBallAdvisorProvider[] = ["openai", "deepseek"];
  const rankingSet = await getLatestRankingSet("PPR");
  if (!rankingSet) throw new Error("No PPR ranking set is available.");
  const rankings = (await getFantasyRankings(rankingSet.id))
    .filter((player) => BEST_BALL_POSITIONS.includes(player.position as "QB" | "RB" | "WR" | "TE"))
    .slice(0, 260);
  if (rankings.length < 26) throw new Error("Not enough Best Ball players are available for verification.");

  // Representative live-draft state: slot 1 is on the clock for its second pick (overall 24).
  const snapshot = buildBestBallAdvisorSnapshot(rankings, {
    rankingSetId: Number(rankingSet.id),
    userSlot: 1,
    playerIds: rankings.slice(0, 23).map((player) => player.playerId),
  });

  let failures = 0;
  for (const provider of providers) {
    const requiredKey = provider === "openai" ? "OPENAI_API_KEY" : "DEEPSEEK_API_KEY";
    if (!process.env[requiredKey]) {
      failures += 1;
      console.log(JSON.stringify({ provider, status: "BLOCKED", error: `${requiredKey} is not configured.` }));
      continue;
    }
    const startedAt = Date.now();
    try {
      const raw = await callBestBallAdvisorProvider(provider, snapshot);
      const result = validateBestBallAdvisorOutput(raw, snapshot);
      const recommendation = snapshot.candidates.find((candidate) => candidate.playerId === result.recommendedPlayerId);
      console.log(JSON.stringify({
        provider,
        model: provider === "openai" ? OPENAI_BEST_BALL_MODEL : DEEPSEEK_BEST_BALL_MODEL,
        targetOverallPick: snapshot.draft.targetOverallPick,
        recommendation: recommendation?.name,
        confidence: result.confidence,
        alternatives: result.alternatives.length,
        elapsedMs: Date.now() - startedAt,
        status: "PASS",
      }));
    } catch (error) {
      failures += 1;
      console.log(JSON.stringify({ provider, status: "FAIL", error: error instanceof Error ? error.message : "Unknown provider error" }));
    }
  }
  if (failures) throw new Error(`${failures} provider verification(s) did not pass.`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
