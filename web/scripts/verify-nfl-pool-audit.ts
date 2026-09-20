import assert from 'node:assert/strict';
import { sql } from 'drizzle-orm';
import { db } from '../src/db';
import { poolAuditIndex,readPoolReview } from '../src/db/nfl-dfs-pool-audit';
async function main() {
  const index=await poolAuditIndex();
  const ids=process.argv.slice(2);
  const reports=[];
  for(const id of ids) {
    const upload=index.uploads.find(u=>u.id===id);assert.ok(upload,'Upload exists');
    const review=await readPoolReview(id);
    assert.equal(review.rows.length,upload.players,'Every salary player accounted for');
    assert.equal(new Set(review.rows.map(r=>r.player.dkPlayerId)).size,upload.players);
    reports.push({uploadId:id,players:review.rows.length,games:review.captures.length,
      pregamePlayers:review.rows.filter(r=>r.pregame).length,latePlayers:review.rows.filter(r=>!r.pregame).length,
      outPlayers:review.rows.filter(r=>r.player.isOut).length,unmatched:review.rows.filter(r=>!r.player.playerId).length,
      firstObserved:review.captures[0]?.observedAt,jones:review.rows.filter(r=>r.player.name.includes('Aaron Jones')).map(r=>({projection:r.player.projection,observedAt:r.observedAt,pregame:r.pregame,origin:r.origin}))});
  }
  const trigger=await db.execute(sql`SELECT tgenabled FROM pg_trigger WHERE tgname='nfl_pool_capture_immutable'`);
  assert.equal(trigger.rows[0]?.tgenabled,'O');
  console.log(JSON.stringify({reports,appendOnlyTrigger:'enabled',health:index.health},null,2));
}
main().catch(e=>{console.error(e);process.exitCode=1;});
