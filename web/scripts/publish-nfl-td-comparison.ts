/** Run with: node --env-file=.env.local --import tsx scripts/publish-nfl-td-comparison.ts init|forecast|result [file.json] */
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { neon } from "@neondatabase/serverless";
import { validateForecast, validateResult, type Forecast, type Game, type Result } from "../src/lib/nfl/td-comparison";

async function main() {
  const [command, path] = process.argv.slice(2);
  if (!["init", "forecast", "result"].includes(command)) throw new Error("Use init, forecast FILE, or result FILE");
  const sql = neon(process.env.DATABASE_URL!);
  if (command === "init") {
    const ddl = readFileSync(new URL("./nfl-td-comparison-schema.sql", import.meta.url), "utf8");
    for (const statement of ddl.replace(/--[^\n]*/g, "").split(";").filter(s => s.trim())) await sql.query(statement);
    console.log("TD comparison tables ready"); return;
  }
  if (!path) throw new Error("A reviewed JSON input file is required");
  const raw = readFileSync(path, "utf8"), payload = JSON.parse(raw);
  const digest = createHash("sha256").update(raw).digest("hex");
  const gameId = command === "forecast" ? payload.game?.gameId : payload.gameId;
  const games = await sql`SELECT g.nflverse_game_id AS "gameId", g.season, g.week, g.kickoff, g.completed,
    h.abbreviation AS home, a.abbreviation AS away, clock_timestamp() AS "serverNow"
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.nflverse_game_id = ${gameId} AND g.game_type = 'REG'`;
  if (games.length !== 1) throw new Error("Game must uniquely match the canonical regular-season schedule");
  const game = games[0] as Game & { serverNow: string };
  if (command === "forecast") {
    validateForecast(payload as Forecast, game, game.serverNow);
    // The server assigns the freeze time; the insert rechecks the deadline so a
    // slow publisher cannot slip a late forecast through after validation.
    const inserted = await sql`INSERT INTO nfl_td_comparison_forecasts (game_id,digest,payload)
      SELECT ${gameId},${digest},${JSON.stringify(payload)}::jsonb FROM nfl_season_games
      WHERE nflverse_game_id=${gameId} AND kickoff=${game.kickoff}::timestamptz
        AND NOT completed AND clock_timestamp() <= kickoff - interval '20 minutes'
      ON CONFLICT (game_id) DO NOTHING RETURNING game_id`;
    if (!inserted.length) throw new Error("No forecast written: game already frozen, completed, changed, or past deadline");
  } else {
    const forecasts = await sql`SELECT payload,frozen_at FROM nfl_td_comparison_forecasts WHERE game_id=${gameId}`;
    if (forecasts.length !== 1) throw new Error("No frozen forecast to grade");
    validateForecast(forecasts[0].payload as Forecast, game, forecasts[0].frozen_at);
    validateResult(payload as Result, forecasts[0].payload as Forecast, game, game.serverNow);
    const inserted = await sql`INSERT INTO nfl_td_comparison_results (game_id,digest,payload)
      VALUES (${gameId},${digest},${JSON.stringify(payload)}::jsonb)
      ON CONFLICT (game_id,digest) DO NOTHING RETURNING id`;
    if (!inserted.length) { console.log("Identical result evidence already imported"); return; }
  }
  console.log(`${command} published for ${gameId}; sha256 ${digest}`);
}
main().catch(error => { console.error(error instanceof Error ? error.message : "Publication failed"); process.exitCode = 1; });
