import data from "@/data/nfl-player-context-2025.json";
import type { PlayerContext } from "@/lib/nfl-dfs/player-context";
import PlayerHistory from "./player-history";
import Link from "next/link";

export const metadata = { title: "NFL DFS · Player Context" };

export default async function Page({ searchParams }: { searchParams: Promise<{ player?: string; name?: string }> }) {
  const { player, name } = await searchParams;
  const source = data as unknown as PlayerContext;
  const matches = source.players.filter(p => player ? p.id === player : name ? p.name.toLowerCase() === name.toLowerCase() : p.name === "George Pickens");
  if (matches.length !== 1) return <main className="mx-auto max-w-3xl space-y-4 p-8"><h1 className="text-2xl font-bold">Player history unavailable</h1><p>No unique 2025 QB/WR/TE history matched this player. Missing history is not a zero-score season.</p><Link className="underline" href="/dfs/nfl/history">Browse historical players</Link><p><Link href="/dfs/nfl">← NFL DFS</Link></p></main>;
  const chosen = matches[0];
  const rows = source.rows.filter(r => r.playerId === chosen.id);
  const games = Object.fromEntries(rows.map(r => [r.gameKey, source.games[r.gameKey]]));
  return <PlayerHistory key={chosen.id} data={{ ...source, games, rows }} selectedId={chosen.id} />;
}
