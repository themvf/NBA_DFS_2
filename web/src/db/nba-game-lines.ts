import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { isMissingRelation } from "@/lib/database-errors";
import type { GameLine } from "@/lib/nba-game-leaders";

/**
 * Each game's spread and total on an Eastern date (ingest/nba_game_lines.py): the newest row
 * captured before tip, keyed by Odds API event id. Rows are written only when a line moves, so
 * the newest row is the line in force. Empty when nothing has been captured or the table is new.
 */
export async function getNbaGameLines(gameDate: string): Promise<Record<string, GameLine>> {
  try {
    const result = await db.execute(sql`
      SELECT DISTINCT ON (event_id) event_id, home_team, away_team, home_spread, total, book_count, captured_at
      FROM nba_game_lines
      WHERE game_date = ${gameDate}::date AND captured_at < commence_time
      ORDER BY event_id, captured_at DESC`);
    const out: Record<string, GameLine> = {};
    for (const r of result.rows as { event_id: string; home_team: string; away_team: string; home_spread: number | null;
      total: number | null; book_count: number; captured_at: string | Date }[]) {
      out[r.event_id] = {
        homeTeam: r.home_team, awayTeam: r.away_team,
        homeSpread: r.home_spread == null ? null : Number(r.home_spread), total: r.total == null ? null : Number(r.total),
        books: Number(r.book_count), capturedAt: new Date(r.captured_at).toISOString(),
      };
    }
    return out;
  } catch (e) {
    if (isMissingRelation(e)) return {};
    throw e;
  }
}
