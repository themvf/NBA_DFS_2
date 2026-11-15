"use server";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { GameLine } from "@/lib/nfl-dfs/slate-read";

/**
 * The latest pregame total and spread for the games around a slate's first
 * kickoff. Read-only; a failure is returned as a message, never thrown, so the
 * slate read shows "no lines" instead of breaking the page.
 */
export async function readNflSlateGameLines(firstKickoff: string | null): Promise<{ lines: GameLine[]; error: string | null }> {
  const at = Date.parse(firstKickoff ?? "");
  if (!Number.isFinite(at)) return { lines: [], error: "This slate has no kickoff time, so its lines can't be looked up." };
  try {
    const from = new Date(at - 12 * 3600_000).toISOString();
    const to = new Date(at + 4 * 86400_000).toISOString();
    const result = await db.execute(sql`SELECT h.abbreviation AS home, a.abbreviation AS away, o.vegas_total AS total, o.home_spread AS spread, o.captured_at AS captured
      FROM nfl_season_games g
      JOIN nfl_teams h ON h.team_id = g.home_team_id
      JOIN nfl_teams a ON a.team_id = g.away_team_id
      LEFT JOIN LATERAL (
        SELECT vegas_total, home_spread, captured_at FROM game_odds_history
        WHERE sport = 'nfl' AND matchup_id = g.matchup_id AND captured_at <= g.kickoff AND vegas_total IS NOT NULL
        ORDER BY captured_at DESC LIMIT 1) o ON TRUE
      WHERE g.game_type = 'REG' AND g.kickoff >= ${from}::timestamptz AND g.kickoff <= ${to}::timestamptz`);
    const num = (v: unknown) => (v == null || !Number.isFinite(Number(v)) ? null : Number(v));
    const lines = (result.rows as Record<string, unknown>[]).map((r) => ({
      home: String(r.home), away: String(r.away), total: num(r.total), homeSpread: num(r.spread),
      capturedAt: r.captured ? new Date(String(r.captured)).toISOString() : null,
    }));
    return { lines, error: null };
  } catch (reason) {
    return { lines: [], error: reason instanceof Error ? `Game lines could not be read: ${reason.message}` : "Game lines could not be read." };
  }
}
