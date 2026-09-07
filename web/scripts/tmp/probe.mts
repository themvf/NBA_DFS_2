import { neon } from "@neondatabase/serverless";
const sql = neon(process.env.DATABASE_URL!);

const seasons = await sql`
  SELECT season, COUNT(*) games,
         COUNT(*) FILTER (WHERE home_score IS NOT NULL) scored,
         COUNT(*) FILTER (WHERE completed) completed,
         MIN(week) minw, MAX(week) maxw
  FROM nfl_season_games GROUP BY season ORDER BY season`;
console.log("nfl_season_games:", JSON.stringify(seasons));

const wp = await sql`
  SELECT season, provenance, COUNT(*) n, MIN(week) minw, MAX(week) maxw
  FROM nfl_game_win_probs GROUP BY season, provenance ORDER BY season, provenance`;
console.log("win_probs:", JSON.stringify(wp));

const cols = await sql`
  SELECT column_name, data_type FROM information_schema.columns
  WHERE table_name='nfl_season_games' ORDER BY ordinal_position`;
console.log("game cols:", cols.map(c=>c.column_name).join(","));
