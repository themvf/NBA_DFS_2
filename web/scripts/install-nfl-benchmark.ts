import {sql} from 'drizzle-orm';
import {db} from '../src/db';
import {BENCHMARK_DDL} from '../src/db/nfl-dfs-benchmark';
async function main(){
  await db.execute(sql.raw(BENCHMARK_DDL));
  const counts=await db.execute(sql`SELECT COUNT(*) players,
    COUNT(*) FILTER(WHERE fantasypros_proj IS NOT NULL) fantasypros,
    COUNT(*) FILTER(WHERE linestar_proj IS NOT NULL) linestar
    FROM nfl_dfs_slate_players WHERE upload_id=(SELECT upload_id FROM nfl_dfs_slate_uploads ORDER BY created_at DESC LIMIT 1)`);
  console.log('Benchmark table ready; latest salary-slate projection coverage:',counts.rows[0]);
}
main().catch(()=>{console.error('Benchmark installation failed. Check database configuration.');process.exitCode=1;});
