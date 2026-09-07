import { neon } from "@neondatabase/serverless";
const sql = neon(process.env.DATABASE_URL!);
const c = await sql`
  SELECT season,
    COUNT(*) n,
    COUNT(quoted_spread_line) spread,
    COUNT(quoted_home_ml) hml,
    COUNT(market_home_ml) mml,
    COUNT(*) FILTER (WHERE home_score = away_score) ties,
    COUNT(*) FILTER (WHERE div_game) divg,
    COUNT(DISTINCT quote_source) srcs
  FROM nfl_season_games WHERE season IN (2023,2024,2025) GROUP BY season ORDER BY season`;
console.log(JSON.stringify(c, null, 1));
const s = await sql`SELECT DISTINCT quote_source FROM nfl_season_games WHERE season=2025`;
console.log("sources:", JSON.stringify(s));
const sample = await sql`
  SELECT week, quoted_spread_line sp, quoted_home_ml hml, quoted_away_ml aml, home_score hs, away_score as_
  FROM nfl_season_games WHERE season=2025 AND week=1 ORDER BY id LIMIT 5`;
console.log("sample wk1 2025:", JSON.stringify(sample));
