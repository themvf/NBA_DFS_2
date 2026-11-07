import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { availabilityOf, teamChipsOf, type FirstTdGame, type FirstTdGrades, type FirstTdPlayer } from "@/db/cfb-first-td";

/** Reader for model/nfl_first_td.py's tables; same shapes as the CFB reader so the page view is shared. */

const ET = "America/New_York";
const LINES_VERSION = "nfl-first-td-lines-v1";

export async function getNflFirstTdDates(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT to_char(kickoff AT TIME ZONE ${ET}, 'YYYY-MM-DD') AS d
    FROM nfl_first_td_predictions ORDER BY d DESC LIMIT 30`);
  return (result.rows as { d: string }[]).map((r) => r.d);
}

/** Each game on an Eastern date with its latest forecast saved before kickoff (the filtered version first). */
export async function getNflFirstTdGames(date: string): Promise<FirstTdGame[]> {
  const result = await db.execute(sql`
    WITH latest AS (
      SELECT DISTINCT ON (p.game_id) p.game_id, p.kickoff, p.away, p.home, p.away_implied, p.home_implied,
             p.p_home_first, p.other_offense, p.non_offense, p.players, p.availability, p.context,
             r.created_at, r.version
      FROM nfl_first_td_predictions p JOIN nfl_first_td_runs r ON r.id = p.run_id
      WHERE (p.kickoff AT TIME ZONE ${ET})::date = ${date}::date AND r.created_at < p.kickoff
      ORDER BY p.game_id, (r.version = ${LINES_VERSION}) DESC, r.created_at DESC)
    SELECT l.*, td.play_type, td.description
    FROM latest l
    LEFT JOIN LATERAL (
      SELECT a.play_type, a.description FROM nfl_pbp_archetypes a
      WHERE a.game_id = l.game_id AND a.two_point_result IS NULL
        AND EXISTS (SELECT 1 FROM nfl_pbp_play_participants t WHERE t.game_id = a.game_id
                    AND t.play_id = a.play_id AND t.role = 'touchdown')
      ORDER BY a.quarter, a.game_seconds_remaining DESC, a.play_id LIMIT 1) td ON TRUE
    ORDER BY l.kickoff, l.home`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    gameKey: r.game_id as string,
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
    actual: r.play_type ? { playType: r.play_type === "run" ? "Rushing TD" : r.play_type === "pass" ? "Receiving TD" : String(r.play_type), text: (r.description as string) ?? "" } : null,
    availability: availabilityOf(r.availability),
    teamChips: teamChipsOf(r.context),
  }));
}

export async function getNflFirstTdGrades(): Promise<FirstTdGrades | null> {
  const result = await db.execute(sql`
    SELECT count(*)::int AS games,
           count(*) FILTER (WHERE abs(a.prob - b.prob) > 1e-12)::int AS changed,
           avg(-ln(greatest(a.prob, 1e-9))) AS base_ll, avg(-ln(greatest(b.prob, 1e-9))) AS lines_ll,
           avg(a.top_pick_hit::int) AS base_top, avg(b.top_pick_hit::int) AS lines_top
    FROM nfl_first_td_grades a
    JOIN nfl_first_td_grades b ON b.game_id = a.game_id AND b.version = ${LINES_VERSION}
    WHERE a.version = 'nfl-first-td-v1'`);
  const r = result.rows[0] as Record<string, unknown> | undefined;
  if (!r || !Number(r.games)) return null;
  return {
    games: Number(r.games), changed: Number(r.changed),
    baseLogLoss: Number(r.base_ll), linesLogLoss: Number(r.lines_ll),
    baseTopPick: Number(r.base_top), linesTopPick: Number(r.lines_top),
  };
}
