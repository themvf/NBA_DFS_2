import assert from 'node:assert/strict';
import { evaluateDefensiveActivation, type DefensiveActivation, type DefensiveQualification } from '../src/lib/nfl-dfs/defensive-policy';

const now=new Date('2026-09-27T12:00:00Z');
const context={profile:'pfr-efficiency' as const,baselineModelVersion:'nfl-dfs-historical-v5',
  configurationHash:'config-1',scoringVersion:'nfl-dfs-historical-v5-dk-scoring',implementationHash:'capture-1'};
const policy:DefensiveActivation={id:'activation-1',state:'active',consumer:'nfl-dfs-optimizer',
  ...context,qualificationId:'verdict-1',effectiveAt:'2026-09-27T00:00:00Z',expiresAt:'2026-10-01T00:00:00Z'};
const qualification:DefensiveQualification={id:'verdict-1',verdict:'PASS',...context,expiresAt:'2026-10-01T00:00:00Z'};
assert.equal(evaluateDefensiveActivation(policy,qualification,context,now).active,true);
assert.equal(evaluateDefensiveActivation(null,null,context,now).active,false);
for(const state of ['pending','revoked'] as const)assert.equal(evaluateDefensiveActivation({...policy,state},qualification,context,now).active,false);
for(const verdict of ['FAIL','NO_VERDICT'] as const)assert.equal(evaluateDefensiveActivation(policy,{...qualification,verdict},context,now).active,false);
assert.equal(evaluateDefensiveActivation(policy,{...qualification,expiresAt:'2026-09-26T00:00:00Z'},context,now).active,false);
assert.equal(evaluateDefensiveActivation({...policy,consumer:'nfl-dfs-optimizer',configurationHash:'other'},qualification,context,now).active,false);
assert.equal(evaluateDefensiveActivation(policy,qualification,{...context,scoringVersion:'other'},now).active,false);
assert.equal(evaluateDefensiveActivation(policy,qualification,{...context,baselineModelVersion:'nfl-dfs-historical-v6'},now).active,false);
assert.equal(evaluateDefensiveActivation(policy,qualification,{...context,profile:'allowed-rushing-volume'},now).active,false);
console.log('Defensive approved policy requires exact active PASS identity and configuration; pending, failed, expired, revoked and incompatible states fall back.');
