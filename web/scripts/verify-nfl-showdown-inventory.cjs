const { neon } = require('@neondatabase/serverless');
const query = neon(process.env.DATABASE_URL);
query.query(`SELECT u.upload_id::text,u.projection_run_id::text,u.player_count,u.file_name,
  min(p.game_info) AS game_info
  FROM nfl_dfs_slate_uploads u JOIN nfl_dfs_slate_players p ON p.upload_id=u.upload_id
  WHERE u.format='showdown' GROUP BY u.upload_id ORDER BY u.created_at DESC LIMIT 12`)
  .then(rows=>console.log(JSON.stringify(rows,null,2)))
  .catch(error=>{console.error(error.message);process.exitCode=1});
