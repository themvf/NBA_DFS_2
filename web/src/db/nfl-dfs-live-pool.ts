import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { LivePool } from "@/lib/nfl-dfs/live-dk-status";

/**
 * What DraftKings most recently said about a pool that could be this slate.
 *
 * Selection is by team set and format, which is an exact key: a 13-game Sunday
 * main slate and a 15-game Sunday-Monday slate cover different teams, and a
 * showdown covers two. The one genuine collision is a single game offered in
 * several contest types at once (Captain Mode, Snake Showdown, Single Stat --
 * all with the same two teams), and that is settled downstream by comparing
 * salaries, because only the Captain Mode pool prices players the way the
 * salary file does. Both filters have to hold; neither is sufficient alone.
 *
 * The answer is read through the LATEST SUCCESSFUL POLL, not the newest
 * snapshot. Snapshots are deduplicated by content, so:
 *   - a poll that finds nothing changed writes no snapshot, and the newest
 *     snapshot's time says when DraftKings last CHANGED, not when we last
 *     LOOKED. Judging freshness on it meant a slate uploaded after the last
 *     change could never be overlaid, however many polls followed (found on
 *     the 2026-09-24 Thursday slate);
 *   - a status that flips A -> B -> A points the latest poll back at the old A
 *     snapshot, while "newest snapshot" would still say B.
 * Every poll records the snapshot it saw, changed or not, so the latest one is
 * both the content and its as-of time.
 */
export async function getLiveDkPool(
  format: string,
  teams: readonly string[],
): Promise<{ pool: LivePool | null; lastPolledAt: Date | null; lastPollOk: boolean | null }> {
  if (!teams.length) return { pool: null, lastPolledAt: null, lastPollOk: null };
  const key = [...teams].sort();

  const found = await db.execute(sql`
    SELECT s.snapshot_id, s.draft_group_id, s.format, s.teams,
           s.captured_at AS changed_at, p.polled_at AS observed_at
      FROM nfl_dfs_dk_pool_polls p
      JOIN nfl_dfs_dk_pool_snapshots s ON s.snapshot_id = p.snapshot_id
     WHERE p.ok
       AND s.format = ${format}
       AND (SELECT COALESCE(jsonb_agg(value ORDER BY value), '[]'::jsonb)
              FROM jsonb_array_elements_text(s.teams)) = ${JSON.stringify(key)}::jsonb
     ORDER BY p.polled_at DESC
     LIMIT 1`);
  const row = found.rows[0] as
    | { snapshot_id: string; draft_group_id: string | number; format: string; teams: unknown;
        changed_at: string | Date; observed_at: string | Date }
    | undefined;
  if (!row) return { pool: null, lastPolledAt: null, lastPollOk: null };

  const draftGroupId = Number(row.draft_group_id);
  const players = await db.execute(sql`
    SELECT normalized_name, name, team, salary, status, is_disabled
      FROM nfl_dfs_dk_pool_player_status
     WHERE snapshot_id = ${row.snapshot_id}::uuid`);

  // The very latest look, successful or not: a failing poller must show up as
  // a stale "last checked" rather than quietly keep an old answer looking live.
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
      capturedAt: new Date(row.observed_at),
      changedAt: new Date(row.changed_at),
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
