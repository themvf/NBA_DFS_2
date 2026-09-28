import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { WINDOW_GAMES, type TeamUsageWindow } from "@/lib/nfl-dfs/replacement-upside";

/**
 * Each team's last WINDOW_GAMES completed regular-season games before the
 * slate's week (crossing into last season when needed) and every slate
 * player's targets and carries in them, from the nflverse weekly feed.
 *
 * A game with no stat row for a player counts as not active, the same
 * activity proxy the pre-2019 blind studies validated. A row recorded for a
 * different team (a traded player's old club) does not count toward this
 * team's window.
 *
 * `players` maps the caller's key (dkPlayerId) to {team, gsis}; `team` must
 * already be the database abbreviation (nfl_teams / ff_player_week_stats).
 */
export async function readTeamUsageWindows(
  season: number,
  week: number,
  players: ReadonlyMap<number, { team: string; gsis: string }>,
): Promise<TeamUsageWindow[]> {
  const teams = [...new Set([...players.values()].map((p) => p.team))];
  const gsisIds = [...new Set([...players.values()].map((p) => p.gsis))];
  if (!teams.length || !gsisIds.length) return [];
  const games = await db.execute(sql`SELECT t.abbreviation AS team, g.season, g.week
    FROM nfl_season_games g JOIN nfl_teams t ON t.team_id IN (g.home_team_id, g.away_team_id)
    WHERE g.game_type='REG' AND g.completed AND g.season BETWEEN ${season - 1} AND ${season}
      AND (g.season < ${season} OR g.week < ${week})
      AND t.abbreviation IN (${sql.join(teams.map((t) => sql`${t}`), sql`, `)})
    ORDER BY g.season DESC, g.week DESC`);
  const stats = await db.execute(sql`SELECT p.gsis_id AS gsis, w.season, w.week, w.team,
      (w.source_row->>'targets')::float AS targets, (w.source_row->>'carries')::float AS carries
    FROM ff_player_week_stats w JOIN ff_players p ON p.id = w.player_id
    WHERE w.season_type='REG' AND w.source='nflverse' AND w.season BETWEEN ${season - 1} AND ${season}
      AND p.gsis_id IN (${sql.join(gsisIds.map((g) => sql`${g}`), sql`, `)})`);
  const usage = new Map<string, { targets: number; carries: number }>();
  for (const row of stats.rows as { gsis: string; season: number; week: number; team: string | null;
    targets: number | null; carries: number | null }[]) {
    const key = `${row.gsis}|${Number(row.season)}|${Number(row.week)}|${row.team ?? ""}`;
    if (!usage.has(key)) usage.set(key, { targets: Number(row.targets) || 0, carries: Number(row.carries) || 0 });
  }
  const byTeam = new Map<string, { season: number; week: number }[]>();
  for (const row of games.rows as { team: string; season: number; week: number }[]) {
    const list = byTeam.get(row.team) ?? [];
    if (list.length < WINDOW_GAMES) list.push({ season: Number(row.season), week: Number(row.week) });
    byTeam.set(row.team, list);
  }
  return teams.map((team) => {
    const window = byTeam.get(team) ?? [];
    const teamUsage: TeamUsageWindow["usage"] = {};
    for (const [key, player] of players) {
      if (player.team !== team) continue;
      teamUsage[key] = window.map((g) => usage.get(`${player.gsis}|${g.season}|${g.week}|${team}`) ?? null);
    }
    return { team, games: window, usage: teamUsage };
  });
}
