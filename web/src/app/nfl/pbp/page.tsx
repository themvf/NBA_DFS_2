export const dynamic = "force-dynamic";

import { getNflArchetypeGames, getNflArchetypeParticipants, getNflArchetypePlays } from "@/db/queries";
import PbpArchetypeClient from "./pbp-archetype-client";

export const metadata = { title: "NFL PBP Archetypes" };

export default async function NflPbpArchetypePage({
  searchParams,
}: {
  searchParams: Promise<{ game?: string }>;
}) {
  const { game } = await searchParams;
  const games = await getNflArchetypeGames();
  // Default to the most recently labelled game rather than an empty table.
  const selected = game && games.some(row => row.gameId === game) ? game : games[0]?.gameId ?? null;
  const [plays, participants] = selected
    ? await Promise.all([getNflArchetypePlays(selected), getNflArchetypeParticipants(selected)])
    : [[], []];
  return <PbpArchetypeClient games={games} gameId={selected} plays={plays} participants={participants} />;
}
