import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { validateForecast, validateResult, type Forecast, type Game, type RecordRow, type Result } from "@/lib/nfl/td-comparison";

export async function getTdComparison(season?: number): Promise<{ seasons: number[]; season: number; rows: RecordRow[] }> {
  const options = await db.execute(sql`SELECT DISTINCT season FROM nfl_season_games WHERE game_type = 'REG' ORDER BY season DESC`);
  const seasons = options.rows.map(r => Number(r.season));
  const selected = season && seasons.includes(season) ? season : seasons[0];
  if (!selected) throw new Error("No canonical seasons available");
  const result = await db.execute(sql`
    SELECT g.nflverse_game_id AS "gameId", g.season, g.week, g.kickoff, g.completed,
      h.abbreviation AS home, a.abbreviation AS away,
      f.payload AS forecast, f.frozen_at AS "frozenAt", r.payload AS result, r.imported_at AS "importedAt"
    FROM nfl_season_games g
    JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
    LEFT JOIN nfl_td_comparison_forecasts f ON f.game_id = g.nflverse_game_id
    LEFT JOIN LATERAL (SELECT payload, imported_at FROM nfl_td_comparison_results
      WHERE game_id = g.nflverse_game_id ORDER BY id DESC LIMIT 1) r ON TRUE
    WHERE g.season = ${selected} AND g.game_type = 'REG' ORDER BY g.kickoff, g.nflverse_game_id`);
  const iso = (v: unknown) => v instanceof Date ? v.toISOString() : String(v);
  const rows = result.rows.map(raw => {
    const game: Game = { gameId: String(raw.gameId), season: Number(raw.season), week: Number(raw.week), kickoff: iso(raw.kickoff), completed: raw.completed === true, home: String(raw.home), away: String(raw.away) };
    const row: RecordRow = { game, forecast: raw.forecast as Forecast | null, result: raw.result as Result | null, frozenAt: raw.frozenAt ? iso(raw.frozenAt) : null };
    try {
      if (row.forecast) validateForecast(row.forecast, game, row.frozenAt!);
      if (row.result) { if (!row.forecast) throw new Error("Results without a forecast"); validateResult(row.result, row.forecast, game, iso(raw.importedAt)); }
    } catch (error) { console.error("TD comparison record rejected", game.gameId, error); row.error = "Stored evidence did not pass validation"; row.forecast = null; row.result = null; }
    return row;
  });
  return { seasons, season: selected, rows };
}
