import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { readNflQbMatchupContexts } from "@/db/nfl-qb-matchup-context";
import type { QbMatchupContext } from "@/lib/nfl-dfs/qb-matchup-context";
import {
  STAT_KEYS, type RollingGame, type RollingPlayer, type RollingProjectionBoard, type StatMeans,
} from "@/lib/nfl-dfs/rolling-projection-view";

const aliases: Record<string, string> = { LA: "LAR", WAS: "WSH", JAC: "JAX", AZ: "ARI" };
const canonical = (value: unknown) => aliases[String(value)] ?? String(value);
const dateText = (value: unknown) => value instanceof Date ? value.toISOString() : String(value);
const optionalNumber = (value: unknown) => value == null ? null : Number(value);
const records = (result: { rows: unknown[] }) => result.rows as Record<string, unknown>[];
const statColumns = sql.raw(STAT_KEYS.map(key => `(stat_means->>'${key}')::float8 AS "stat_${key}"`).join(", "));
const statMeans = (row: Record<string, unknown>) =>
  Object.fromEntries(STAT_KEYS.map(key => [key, optionalNumber(row[`stat_${key}`])])) as StatMeans;

/** Projection rows come from the independent scheduled model run, never a salary upload. */
export async function getNflRollingProjectionBoard(requestedWeek?: string): Promise<RollingProjectionBoard> {
  const now = new Date();
  const [runResult, futureResult] = await Promise.all([
    db.execute(sql`
      SELECT DISTINCT ON (season, week) run_id AS "runId", season, week,
        model_version AS "modelVersion", as_of_at AS "asOf", player_count AS "playerCount"
      FROM nfl_dfs_projection_runs
      WHERE week IS NOT NULL AND as_of_at <= ${now}
      ORDER BY season DESC, week DESC, as_of_at DESC, created_at DESC
      LIMIT 24`),
    db.execute(sql`
      SELECT DISTINCT season, week FROM nfl_season_games
      WHERE game_type = 'REG' AND kickoff > ${now}
      ORDER BY season, week`),
  ]);
  const options = records(runResult).map(row => ({ runId: String(row.runId), season: Number(row.season),
    week: Number(row.week), modelVersion: String(row.modelVersion), asOf: dateText(row.asOf),
    playerCount: Number(row.playerCount) }));
  const future = new Set(records(futureResult).map(row => `${row.season}-${row.week}`));
  const run = options.find(row => `${row.season}-${row.week}` === requestedWeek)
    ?? options.find(row => future.has(`${row.season}-${row.week}`)) ?? options[0] ?? null;
  if (!run) return { options, run: null, players: [], games: [], matchupAsOf: null, omittedPlayers: 0, contextError: null };

  const [fixtureResult, projectionResult] = await Promise.all([
    db.execute(sql`
      SELECT g.nflverse_game_id AS "gameId", g.kickoff,
        home.abbreviation AS home, away.abbreviation AS away
      FROM nfl_season_games g
      JOIN nfl_teams home ON home.team_id = g.home_team_id
      JOIN nfl_teams away ON away.team_id = g.away_team_id
      WHERE g.season = ${run.season} AND g.week = ${run.week}
        AND g.game_type = 'REG' AND g.kickoff > ${now}`),
    db.execute(sql`
      SELECT id, player_name AS name, team, opponent, position, projection_status AS status,
        model_proj_fpts AS projected, floor_fpts AS floor, median_fpts AS median,
        ceiling_fpts AS ceiling, boom_rate AS "boomRate",
        history_games AS "historyGames", salary, ${statColumns}
      FROM nfl_dfs_player_projections
      WHERE run_id = ${run.runId} ORDER BY position, player_name`),
  ]);
  const fixtures = records(fixtureResult).map(row => ({ gameId: String(row.gameId),
    home: canonical(row.home), away: canonical(row.away), kickoff: dateText(row.kickoff) }));
  const fixtureByTeam = new Map<string, { opponent: string; kickoff: string }>();
  for (const { home, away, kickoff } of fixtures) {
    fixtureByTeam.set(home, { opponent: away, kickoff });
    fixtureByTeam.set(away, { opponent: home, kickoff });
  }
  let contexts = new Map<string, QbMatchupContext>();
  let contextError: string | null = null;
  if (fixtureByTeam.size) {
    try { contexts = await readNflQbMatchupContexts(run.season, run.week, now); }
    catch { contextError = "Play-by-play or market context is temporarily unavailable."; }
  }
  let omittedPlayers = 0;
  const players = records(projectionResult).flatMap((row): RollingPlayer[] => {
    const team = canonical(row.team), opponent = canonical(row.opponent);
    const fixture = fixtureByTeam.get(team);
    if (!fixture || fixture.opponent !== opponent) { omittedPlayers++; return []; }
    return [{ id: Number(row.id), name: String(row.name), team, opponent,
      position: String(row.position), status: String(row.status),
      projected: optionalNumber(row.projected), floor: optionalNumber(row.floor),
      median: optionalNumber(row.median), ceiling: optionalNumber(row.ceiling),
      boomRate: optionalNumber(row.boomRate),
      historyGames: Number(row.historyGames), salary: optionalNumber(row.salary),
      kickoff: fixture.kickoff, stats: statMeans(row), teamContext: contexts.get(team) ?? null }];
  });
  const games = fixtures.map(({ gameId, home, away, kickoff }): RollingGame => {
    const homeContext = contexts.get(home), awayContext = contexts.get(away);
    return { gameId, kickoff, home, away,
      homeImplied: homeContext?.impliedPoints ?? null, awayImplied: awayContext?.impliedPoints ?? null,
      homeSpread: homeContext?.teamSpread ?? null, total: homeContext?.gameTotal ?? awayContext?.gameTotal ?? null };
  }).sort((a, b) => a.kickoff.localeCompare(b.kickoff) || a.away.localeCompare(b.away));
  return { options, run, players, games, matchupAsOf: now.toISOString(), omittedPlayers, contextError };
}
