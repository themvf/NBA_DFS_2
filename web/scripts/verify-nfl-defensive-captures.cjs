const { neon } = require('@neondatabase/serverless');
const query = neon(process.env.DATABASE_URL);
Promise.all([query.query(`SELECT r.upload_id::text, r.baseline_run_id::text, r.as_of_at,
  min(p.kickoff) AS first_kickoff, count(*)::int AS rows,
  count(*) FILTER (WHERE p.projection->'shadow'->>'status'='under_evaluation')::int AS changed
  FROM nfl_matchup_forecast_runs r
  JOIN nfl_matchup_player_forecasts p ON p.run_id=r.run_id
  GROUP BY r.run_id ORDER BY r.as_of_at DESC LIMIT 10`),
  query.query(`SELECT u.upload_id::text,u.projection_run_id::text,r.model_version,u.format,u.player_count
    FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_projection_runs r ON r.run_id=u.projection_run_id
    WHERE u.upload_id='d5d97cc7-0574-491b-ae89-efad4b836b37'::uuid`)] )
  .then(([captures,slates]) => console.log(JSON.stringify({captures,slates}, null, 2)))
  .catch(error => { console.error(error.message); process.exitCode = 1; });
