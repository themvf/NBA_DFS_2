import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { isMissingRelation } from "@/lib/database-errors";
import { ensureNbaContestRunTables } from "@/db/ensure-schema";
import type { BoxGame, RunGame, RunPayload } from "@/lib/nba-contest-runs";

const iso = (v: unknown) => new Date(v as string | Date).toISOString();
const parse = <T,>(v: unknown): T => (typeof v === "string" ? JSON.parse(v) : v) as T;

/** The chosen games as the stored lines describe them: teams and tip time are never taken from the browser. */
export async function getContestRunGames(gameDate: string, eventIds: string[]): Promise<RunGame[]> {
  if (eventIds.length === 0) return [];
  const r = await db.execute(sql`
    SELECT DISTINCT ON (event_id) event_id, commence_time, home_team_name, away_team_name
    FROM prop_odds_history
    WHERE sport = 'nba' AND market = 'player_points' AND game_date = ${gameDate}::date
      AND event_id IN (${sql.join(eventIds.map((id) => sql`${id}`), sql`, `)})
    ORDER BY event_id, captured_at DESC`);
  return (r.rows as Record<string, unknown>[]).map((x) => ({
    eventId: String(x.event_id), commenceTime: iso(x.commence_time),
    homeTeam: String(x.home_team_name ?? ""), awayTeam: String(x.away_team_name ?? ""),
  }));
}

export type SavedRun = { id: number; createdAt: string; firstTip: string; late: boolean; payload: RunPayload };

/**
 * Stores a run once per distinct content. The database clock stamps it, and a run that
 * already exists keeps its first row and time, so a repeat press writes nothing.
 */
export async function saveContestRun(payload: RunPayload, sha256: string, firstTip: string): Promise<{ id: number; createdAt: string; late: boolean; repeat: boolean }> {
  await ensureNbaContestRunTables();
  const inserted = await db.execute(sql`
    INSERT INTO nba_contest_runs (run_sha256, version, game_date, first_tip, lines_captured_at, field_size, entry_count, payload)
    VALUES (${sha256}, ${payload.version}, ${payload.gameDate}::date, ${firstTip}::timestamptz,
      ${payload.linesCapturedAt}::timestamptz, ${payload.fieldSize}, ${payload.entries.length}, ${JSON.stringify(payload)}::jsonb)
    ON CONFLICT (run_sha256) DO NOTHING
    RETURNING id, created_at, (created_at >= first_tip) AS late`);
  let row = inserted.rows[0] as Record<string, unknown> | undefined;
  const repeat = !row;
  if (!row) {
    const existing = await db.execute(sql`SELECT id, created_at, (created_at >= first_tip) AS late FROM nba_contest_runs WHERE run_sha256 = ${sha256}`);
    row = existing.rows[0] as Record<string, unknown> | undefined;
  }
  if (!row) throw new Error("The run was neither stored nor found.");
  return { id: Number(row.id), createdAt: iso(row.created_at), late: row.late === true, repeat };
}

/** Saved runs for a date, newest first. A database that has never stored one reads as none. */
export async function getContestRuns(gameDate: string, limit = 30): Promise<SavedRun[]> {
  try {
    const r = await db.execute(sql`
      SELECT id, created_at, first_tip, (created_at >= first_tip) AS late, payload
      FROM nba_contest_runs WHERE game_date = ${gameDate}::date
      ORDER BY created_at DESC, id DESC LIMIT ${limit}`);
    return (r.rows as Record<string, unknown>[]).map((x) => ({
      id: Number(x.id), createdAt: iso(x.created_at), firstTip: iso(x.first_tip), late: x.late === true, payload: parse<RunPayload>(x.payload),
    }));
  } catch (error) {
    if (isMissingRelation(error)) return [];
    throw error;
  }
}

/** Final box scores for a date, the same rows the morning grades read (completed games only). */
export async function getContestRunFinals(gameDate: string): Promise<BoxGame[]> {
  const r = await db.execute(sql`
    SELECT g.espn_event_id, g.home_team, g.away_team, p.player_key, p.played, p.points
    FROM nba_box_score_games g
    JOIN nba_box_score_players p ON p.espn_event_id = g.espn_event_id
    WHERE g.game_date = ${gameDate}::date AND g.completed`);
  const games = new Map<string, BoxGame>();
  for (const x of r.rows as Record<string, unknown>[]) {
    const id = String(x.espn_event_id);
    let g = games.get(id);
    if (!g) games.set(id, (g = { homeTeam: String(x.home_team), awayTeam: String(x.away_team), players: [] }));
    g.players.push({ playerKey: String(x.player_key), played: x.played === true, points: x.points == null ? null : Number(x.points) });
  }
  return [...games.values()];
}
