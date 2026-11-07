import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

export type FirstTdPlayer = { name: string; team: string; prob: number; fair: number; td: number; touches: number };

export type FirstTdGame = {
  cfbdGameId: number;
  kickoff: string;
  away: string;
  home: string;
  awayImplied: number;
  homeImplied: number;
  pHomeFirst: number;
  otherOffense: number;
  nonOffense: number;
  players: FirstTdPlayer[];
  savedAt: string;
  version: string;
  /** The first touchdown play once the game's play-by-play is stored; null before then. */
  actual: { playType: string; text: string } | null;
};

const ET = "America/New_York";

/** Eastern dates that have saved forecasts, newest first. */
export async function getFirstTdDates(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT to_char(kickoff AT TIME ZONE ${ET}, 'YYYY-MM-DD') AS d
    FROM cfb_first_td_predictions ORDER BY d DESC LIMIT 30`);
  return (result.rows as { d: string }[]).map((r) => r.d);
}

/**
 * Each game on an Eastern date with its latest forecast saved before kickoff.
 * The publisher never writes a started game, so this is the frozen pregame number.
 */
export async function getFirstTdGames(date: string): Promise<FirstTdGame[]> {
  const result = await db.execute(sql`
    WITH latest AS (
      SELECT DISTINCT ON (p.cfbd_game_id) p.*, r.created_at, r.version
      FROM cfb_first_td_predictions p JOIN cfb_first_td_runs r ON r.id = p.run_id
      WHERE (p.kickoff AT TIME ZONE ${ET})::date = ${date}::date AND r.created_at < p.kickoff
      ORDER BY p.cfbd_game_id, r.created_at DESC)
    SELECT l.*, td.play_type, td.play_text
    FROM latest l
    LEFT JOIN cfb_matchups m ON m.cfbd_game_id = l.cfbd_game_id
    -- Plays are read through the indexed game_id; cfb_plays.cfbd_game_id has no index, and filtering on it
    -- scanned every play once per game (104 s for one Saturday's 51 games).
    LEFT JOIN LATERAL (
      SELECT play_type, play_text FROM cfb_plays c
      WHERE c.game_id = m.id
        AND (c.play_type ILIKE '%touchdown%'
             OR (c.scoring AND c.play_text ILIKE '%touchdown%' AND c.play_type IN ('Rush', 'Pass Reception', 'Pass Completion')))
      ORDER BY c.period, c.clock_seconds_remaining DESC, c.drive_number, c.play_number LIMIT 1) td ON TRUE
    ORDER BY l.kickoff, l.home`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    cfbdGameId: Number(r.cfbd_game_id),
    kickoff: new Date(r.kickoff as string).toISOString(),
    away: r.away as string,
    home: r.home as string,
    awayImplied: Number(r.away_implied),
    homeImplied: Number(r.home_implied),
    pHomeFirst: Number(r.p_home_first),
    otherOffense: Number(r.other_offense),
    nonOffense: Number(r.non_offense),
    players: r.players as FirstTdPlayer[],
    savedAt: new Date(r.created_at as string).toISOString(),
    version: r.version as string,
    actual: r.play_type ? { playType: r.play_type as string, text: (r.play_text as string) ?? "" } : null,
  }));
}
