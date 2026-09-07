/** Read-only verification against the current shared database. */
import assert from 'node:assert/strict';
import {getNflIdentityRoster} from '../src/db/nfl-identity';
import {nflIdentityLocalLinks,resolveNflRosterIdentity,matchNflIdentity} from '../src/lib/nfl-dfs/identity';
import {db} from '../src/db';
import {sql} from 'drizzle-orm';
import {getNflRosterEvidence} from '../src/db/nfl-dfs-availability';

async function main(){
  const roster=await getNflIdentityRoster(2026);
  assert.ok(roster.available);
  const links=nflIdentityLocalLinks(roster.candidates);
  assert.deepEqual(links.get(30),[30,364]);
  assert.deepEqual(links.get(34),[34,560]);
  const projections=await db.execute(sql`SELECT player_name AS name, team, position, player_gsis_id AS "gsisId"
    FROM nfl_dfs_player_projections WHERE run_id=(SELECT run_id FROM nfl_dfs_projection_runs WHERE season=2026 ORDER BY as_of_at DESC,created_at DESC LIMIT 1)`);
  const checks=[];
  for(const name of ['Trevor Lawrence','Puka Nacua']){
    const player=roster.candidates.find(p=>p.name===name && p.registryStatus==='resolved')!;
    const resolved=resolveNflRosterIdentity(player,roster.candidates);
    assert.ok(resolved.gsisId);
    const matched=matchNflIdentity({...player,gsisId:resolved.gsisId},projections.rows as {name:string;team:string;position:string;gsisId:string}[]);
    assert.equal(matched.method,'gsis_id');
    checks.push({name,identity:resolved.method,projection:matched.method});
  }
  const constraint=await db.execute(sql`SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint
    WHERE conrelid='nfl_dfs_slate_players'::regclass AND conname='nfl_dfs_slate_players_identity_method_check'`);
  assert.ok(String(constraint.rows[0].definition).includes('identifier_conflict'));
  const availability=await getNflRosterEvidence(2026,1);
  assert.ok([...availability.values()].every(r=>!r.injuryReadFailed),'Injury observation read failed');
  for(const [original,target] of [[30,364],[34,560]]){
    for(const injury of availability.get(original)?.injuries??[]){
      assert.ok(availability.get(target)?.injuries?.some(r=>r.id===injury.id));
    }
  }
  console.log(JSON.stringify({currentRows:roster.candidates.length,registered:roster.candidates.filter(p=>p.registryStatus==='resolved').length,
    duplicateLocalLinksVerified:true,pukaInjuryRows:availability.get(560)?.injuries?.length??0,checks},null,2));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
