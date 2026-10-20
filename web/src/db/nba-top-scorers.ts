import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { PropRow, SeasonStat, TeamName } from "@/lib/nba-top-scorers";

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : String(v));
const num = (v: unknown) => (v == null ? null : Number(v));
const day = (v: unknown) => (v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10));

/**
 * Every player row of the LATEST capture of each game on an Eastern date.
 * Taking the latest capture per game (not per player) matters: a player whose
 * line the books pulled since an earlier capture drops off, instead of a stale
 * line surviving from hours before.
 */
export async function getNbaPointsProps(gameDate: string): Promise<PropRow[]> {
  const result = await db.execute(sql`
    WITH latest AS (
      SELECT event_id, max(captured_at) AS captured_at
      FROM prop_odds_history
      WHERE sport = 'nba' AND market = 'player_points' AND game_date = ${gameDate}::date
      GROUP BY event_id
    )
    SELECT p.event_id, p.commence_time, p.home_team_name, p.away_team_name, p.player, p.books, p.captured_at
    FROM prop_odds_history p
    JOIN latest l ON l.event_id = p.event_id AND l.captured_at = p.captured_at
    WHERE p.sport = 'nba' AND p.market = 'player_points'`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    eventId: String(r.event_id),
    commenceTime: iso(r.commence_time),
    homeTeam: String(r.home_team_name ?? ""),
    awayTeam: String(r.away_team_name ?? ""),
    player: String(r.player),
    books: (typeof r.books === "string" ? JSON.parse(r.books) : r.books) as PropRow["books"],
    capturedAt: iso(r.captured_at),
  }));
}

/** Players quoted in an earlier capture of the date but missing from their game's latest one. */
export async function getNbaPulledLines(gameDate: string): Promise<{ player: string; eventId: string; lastSeenAt: string }[]> {
  const result = await db.execute(sql`
    WITH dated AS (
      SELECT event_id, player, captured_at
      FROM prop_odds_history
      WHERE sport = 'nba' AND market = 'player_points' AND game_date = ${gameDate}::date
    ), latest AS (
      SELECT event_id, max(captured_at) AS captured_at FROM dated GROUP BY event_id
    )
    SELECT d.event_id, d.player, max(d.captured_at) AS last_seen_at
    FROM dated d JOIN latest l ON l.event_id = d.event_id
    GROUP BY d.event_id, d.player, l.captured_at
    HAVING max(d.captured_at) < l.captured_at
    ORDER BY d.player`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    player: String(r.player),
    eventId: String(r.event_id),
    lastSeenAt: iso(r.last_seen_at),
  }));
}

/** Recent Eastern dates that have stored points lines, newest first. */
export async function getNbaPointsPropDates(limit = 14): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT game_date FROM prop_odds_history
    WHERE sport = 'nba' AND market = 'player_points' AND game_date IS NOT NULL
    ORDER BY game_date DESC LIMIT ${limit}`);
  return (result.rows as Record<string, unknown>[]).map((r) => day(r.game_date));
}

/** Per-game averages for one season, with the player's team name for matching to a game. */
export async function getNbaSeasonStats(season: string): Promise<SeasonStat[]> {
  const result = await db.execute(sql`
    SELECT nps.name, nps.position, nps.ppg, nps.games, t.name AS team_name, t.abbreviation
    FROM nba_player_stats nps
    LEFT JOIN teams t ON t.team_id = nps.team_id
    WHERE nps.season = ${season}`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    name: String(r.name),
    position: r.position == null ? null : String(r.position),
    ppg: num(r.ppg),
    games: num(r.games),
    teamName: r.team_name == null ? null : String(r.team_name),
    teamAbbrev: r.abbreviation == null ? null : String(r.abbreviation),
  }));
}

/** Every NBA team's name and abbreviation, for labelling opponents. */
export async function getNbaTeams(): Promise<TeamName[]> {
  const result = await db.execute(sql`SELECT name, abbreviation FROM teams`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    name: String(r.name),
    abbreviation: String(r.abbreviation),
  }));
}
