import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { LivePool } from "@/lib/nfl-dfs/live-dk-status";

/**
 * The most recent DraftKings pool observation that could be this slate.
 *
 * Selection is by team set and format, which is an exact key: a 13-game Sunday
 * main slate and a 15-game Sunday-Monday slate cover different teams, and a
 * showdown covers two. The one genuine collision is a single game offered in
 * several contest types at once (Captain Mode, Snake Showdown, Single Stat --
 * all with the same two teams), and that is settled downstream by comparing
 * salaries, because only the Captain Mode pool prices players the way the
 * salary file does. Both filters have to hold; neither is sufficient alone.
 *
 * `nfl_dfs_dk_pool_polls` carries the heartbeat separately. A snapshot is
 * written only when the pool CHANGES, so the newest snapshot answers "when did
 * DraftKings last change its mind", not "when did we last look" -- and a status
 * feed that conflates those two is worse than no status feed.
 */
export async function getLiveDkPool(
  format: string,
  teams: readonly string[],
): Promise<{ pool: LivePool | null; lastPolledAt: Date | null; lastPollOk: boolean | null }> {
  if (!teams.length) return { pool: null, lastPolledAt: null, lastPollOk: null };
  const key = [...teams].sort();

  const found = await db.execute(sql`
    SELECT snapshot_id, draft_group_id, format, teams, captured_at
      FROM nfl_dfs_dk_pool_snapshots
     WHERE format = ${format}
       AND (SELECT COALESCE(jsonb_agg(value ORDER BY value), '[]'::jsonb)
              FROM jsonb_array_elements_text(teams)) = ${JSON.stringify(key)}::jsonb
     ORDER BY captured_at DESC
     LIMIT 1`);
  const row = found.rows[0] as
    | { snapshot_id: string; draft_group_id: string | number; format: string; teams: unknown; captured_at: string | Date }
    | undefined;
  if (!row) return { pool: null, lastPolledAt: null, lastPollOk: null };

  const draftGroupId = Number(row.draft_group_id);
  const players = await db.execute(sql`
    SELECT normalized_name, name, team, salary, status, is_disabled
      FROM nfl_dfs_dk_pool_player_status
     WHERE snapshot_id = ${row.snapshot_id}::uuid`);

  const poll = await db.execute(sql`
    SELECT polled_at, ok FROM nfl_dfs_dk_pool_polls
     WHERE draft_group_id = ${draftGroupId}
     ORDER BY polled_at DESC LIMIT 1`);
  const latestPoll = poll.rows[0] as { polled_at: string | Date; ok: boolean } | undefined;

  return {
    pool: {
      draftGroupId,
      format: row.format,
      teams: (Array.isArray(row.teams) ? row.teams : JSON.parse(String(row.teams))) as string[],
      capturedAt: new Date(row.captured_at),
      players: players.rows.map((p) => ({
        normalizedName: String(p.normalized_name),
        name: String(p.name),
        team: p.team === null ? null : String(p.team),
        salary: p.salary === null ? null : Number(p.salary),
        status: p.status === null ? null : String(p.status),
        isDisabled: Boolean(p.is_disabled),
      })),
    },
    lastPolledAt: latestPoll ? new Date(latestPoll.polled_at) : null,
    lastPollOk: latestPoll ? Boolean(latestPoll.ok) : null,
  };
}
