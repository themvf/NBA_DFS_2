import assert from 'node:assert/strict';
import {allocateRoles,type RoleMember} from '../src/lib/nfl-dfs/role-allocation';
import snapshot from '../src/data/nfl-team-context.json';
const now=Date.parse('2026-09-06T12:00:00Z');
const base:RoleMember={id:'a',name:'Returner',position:'WR',evidence_current:true,out:false,new_team:false,rookie:false,captured_at:new Date(now).toISOString(),prior_target_share:.3,prior_carry_share:.1};
const rookie={...base,id:'b',name:'Rookie',rookie:true,prior_target_share:null,prior_carry_share:null};
const transfer={...base,id:'c',name:'Arrival',new_team:true};
const members=[base,rookie,transfer];
const r=allocateRoles(members,{},30,25,now);
assert.equal(r.rows[0].targets,9);assert.equal(r.rows[1].targets,0);assert.equal(r.rows[2].targets,0);assert.equal(r.unassignedTargets,21);
const adjusted=allocateRoles(members,{b:{targets:.2},c:{targets:.1}},30,25,now);
assert.ok(Math.abs(adjusted.rows.reduce((s,r)=>s+r.targets,0)+adjusted.unassignedTargets-30)<1e-8);
assert.throws(()=>allocateRoles(members,{b:{targets:.8}},30,25,now),/exceed/);
assert.throws(()=>allocateRoles(members,{unknown:{targets:.1}},30,25,now),/no longer/);
assert.throws(()=>allocateRoles([{...base,out:true}],{a:{targets:.2}},30,25,now),/unavailable/);
assert.equal(allocateRoles(members,{},30,25,now+73*3600000).targetShare,0);
assert.deepEqual(allocateRoles(members,{b:{targets:.2}},30,25,now),allocateRoles(members,JSON.parse(JSON.stringify({b:{targets:.2}})),30,25,now));
console.log('Role allocation: budgets, newcomers, stale/out evidence and reproducibility passed');
for(const team of snapshot.teams){
 const allocation=allocateRoles(team.players,{},30,25,Date.parse(snapshot.as_of));
 assert.ok(allocation.targetShare<=1+1e-9&&allocation.carryShare<=1+1e-9,team.team);
}
console.log('All 32 current roster allocations conserve targets and carries');

assert.equal(allocateRoles([{...base,position:'QB',role:'Listed QB2'}],{},30,25,now).carryShare,0);
