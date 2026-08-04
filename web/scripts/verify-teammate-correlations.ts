import { getFantasyRankings, getLatestRankingSet, getTeammateCorrelations } from "../src/db/queries-fantasy-football";
import { buildBestBallAdvisorSnapshot } from "../src/lib/fantasy-football/ai-draft-advisor";
import { BEST_BALL_POSITIONS } from "../src/lib/fantasy-football/best-ball";

async function main() {
  const rankingSet = await getLatestRankingSet("PPR");
  if (!rankingSet) throw new Error("No PPR ranking set is available.");
  const rankings = (await getFantasyRankings(rankingSet.id))
    .filter((player) => BEST_BALL_POSITIONS.includes(player.position as "QB" | "RB" | "WR" | "TE"))
    .slice(0, 260);
  if (rankings.length < 26) throw new Error("Not enough Best Ball players are available for verification.");

  const correlations = await getTeammateCorrelations(rankings.map((player) => player.playerId));
  console.log(`getTeammateCorrelations returned ${correlations.length} rows for the top-260 pool.`);
  if (correlations.length === 0) throw new Error("No correlation rows returned -- expected thousands.");

  // Build a roster that should actually contain a real BUF pass-catcher, so we
  // can see correlationsWithRoster populate against a real drafted roster
  // instead of an empty one. userSlot=1 owns pick index 0 (overall 1) and, in a
  // 12-team snake, pick index 23 (overall 24) -- put Josh Allen at index 0 so
  // the user's roster is guaranteed to contain him.
  const bills = rankings.filter((player) => player.team === "BUF");
  const allen = rankings.find((player) => player.name.includes("Allen") && player.team === "BUF");
  console.log(`Found ${bills.length} BUF players in the pool; Allen present: ${Boolean(allen)}`);
  if (!allen) throw new Error("Josh Allen not found in the ranking pool -- cannot build a deterministic test roster.");

  // Fill the other 23 picks from the bottom of the pool (not top picks) so the
  // top-40 remaining candidates -- including Allen's real BUF teammates --
  // stay available for the advisor to consider, instead of being drafted away.
  const filler = rankings
    .filter((player) => player.playerId !== allen.playerId && player.team !== "BUF")
    .slice(-23)
    .map((player) => player.playerId);
  const draftedIds = [allen.playerId, ...filler];
  const snapshot = buildBestBallAdvisorSnapshot(
    rankings,
    { rankingSetId: Number(rankingSet.id), userSlot: 1, playerIds: draftedIds },
    correlations,
  );

  console.log("rosterTeamConcentration:", JSON.stringify(snapshot.rosterTeamConcentration));

  const withCorrelations = snapshot.candidates.filter((candidate) => candidate.correlationsWithRoster.length > 0);
  console.log(`${withCorrelations.length} of ${snapshot.candidates.length} candidates have >=1 correlationsWithRoster entry.`);
  if (withCorrelations.length === 0) throw new Error("No candidate surfaced a correlationsWithRoster entry -- wiring is broken.");

  const sample = withCorrelations[0];
  console.log(`Sample candidate: ${sample.name} -- correlationsWithRoster:`, JSON.stringify(sample.correlationsWithRoster, null, 2));

  const instructionsText = snapshot.instructions.join(" ");
  if (!instructionsText.toLowerCase().includes("correlation")) {
    throw new Error("Snapshot instructions do not mention correlation guidance.");
  }
  console.log("Instructions include correlation guidance: OK");
  console.log("VERIFICATION PASSED");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
