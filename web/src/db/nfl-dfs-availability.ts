import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { RosterEvidence } from "@/lib/nfl-dfs/availability";

export async function getNflRosterEvidence(season: number): Promise<Map<number, RosterEvidence>> {
  const result = await db.execute(sql`SELECT id, team_abbrev, position, fetched_at,
    metadata->'sleeper' AS sleeper FROM ff_players WHERE season=${season}`);
  return new Map(result.rows.map(r => [Number(r.id), { team: String(r.team_abbrev ?? ""), position: String(r.position), fetchedAt: new Date(r.fetched_at as string).toISOString(), sleeper: r.sleeper }]));
}
