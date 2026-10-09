export const dynamic = "force-dynamic";

import { getNflArchetypeGames, getNflArchetypeParticipants, getNflArchetypePlays } from "@/db/queries";
import { getNflArchetypeGameById } from "@/db/nfl-team-identity";
import { getCurrentEligibleNflContext, getCurrentNflAvailabilityCoverage, getNflContextResearchSummary, type NflAvailabilityCoverage, type NflResolvedContext } from "@/db/nfl-context";
import PbpArchetypeClient from "./pbp-archetype-client";

export const metadata = { title: "NFL PBP Archetypes" };

export default async function NflPbpArchetypePage({
  searchParams,
}: {
  searchParams: Promise<{ game?: string }>;
}) {
  const { game } = await searchParams;
  const recentGames = await getNflArchetypeGames();
  // Direct links to older seasons must resolve even when the selector is limited to recent games.
  const requestedGame = game && !recentGames.some(row => row.gameId === game)
    ? await getNflArchetypeGameById(game) : null;
  const games = requestedGame ? [requestedGame, ...recentGames] : recentGames;
  const selected = game && games.some(row => row.gameId === game) ? game : games[0]?.gameId ?? null;
  const [plays, participants, research] = await Promise.all([
    selected ? getNflArchetypePlays(selected) : Promise.resolve([]),
    selected ? getNflArchetypeParticipants(selected) : Promise.resolve([]),
    getNflContextResearchSummary(),
  ]);
  const selectedGame = games.find(row => row.gameId === selected) ?? null;
  const contexts: NflResolvedContext[] = selectedGame
    ? (await Promise.all(
        [selectedGame.awayTeam, selectedGame.homeTeam].map(async subjectId => {
          try {
            return await getCurrentEligibleNflContext({
              definitionId: "neutral_offensive_snap_interval_seconds@v1",
              subjectId,
              targetId: selectedGame.gameId,
              consumerId: "nfl_pbp_explorer",
              useCase: "game_explanation",
              cohort: "all_teams",
              usage: "descriptive",
              requestedAsOf: new Date(),
            });
          } catch {
            // Context stays optional until a compatible snapshot and active
            // policy exist. The underlying PBP evidence remains valid.
            return null;
          }
        }),
      )).filter((value): value is NflResolvedContext => value !== null)
    : [];
  let availability: NflAvailabilityCoverage | null = null;
  if (selectedGame) {
    try {
      availability = await getCurrentNflAvailabilityCoverage(selectedGame.gameId, new Date());
    } catch {
      // Availability remains optional until a Phase 2 context publication
      // exists for this game under the active descriptive policy.
    }
  }
  return <PbpArchetypeClient games={games} gameId={selected} plays={plays} participants={participants} contexts={contexts} availability={availability} research={research} />;
}
