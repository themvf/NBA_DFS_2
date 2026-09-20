import assert from 'node:assert/strict';
import { resolveSlateWeek, type ScheduledGame } from '../src/lib/nfl-dfs/slate-week';
import { generationSettings, sameGenerationSettings } from '../src/lib/nfl-dfs/generation-settings';

const schedule: ScheduledGame[] = [
  {season:2026,week:2,awayTeam:'MIA',homeTeam:'SF',kickoff:'2026-09-20T20:25:00Z'},
  {season:2026,week:3,awayTeam:'MIA',homeTeam:'SF',kickoff:'2026-09-27T20:25:00Z'},
  {season:2026,week:2,awayTeam:'SEA',homeTeam:'ARI',kickoff:'2026-09-20T20:25:00Z'},
  {season:2025,week:18,awayTeam:'LAR',homeTeam:'JAX',kickoff:'2026-01-05T01:20:00Z'},
];
const game = (gameKey:string, date:string) => ({gameKey,gameInfo:`${gameKey} ${date} 04:25PM ET`});
assert.deepEqual(resolveSlateWeek([game('MIA@SF','09/20/2026'),game('SEA@AZ','09/20/2026')],schedule),{season:2026,week:2});
assert.deepEqual(resolveSlateWeek([game('LA@JAC','01/04/2026')],schedule),{season:2025,week:18});
assert.throws(()=>resolveSlateWeek([game('MIA@SF','09/20/2026'),game('MIA@SF','09/27/2026')],schedule),/multiple/);
assert.throws(()=>resolveSlateWeek([game('MIA@SF','09/21/2026')],schedule),/uniquely/);
assert.throws(()=>resolveSlateWeek([{gameKey:'MIA@SF',gameInfo:null}],schedule),/dated/);
assert.throws(()=>resolveSlateWeek([game('MIA@SF','09/20/2026')],[...schedule,schedule[0]]),/uniquely/);

const settings = {mode:'gpp' as const, projectionSource:'our' as const, allowDkFallback:true,
  nLineups:20,minSalary:49000,maxExposure:.6,minUnique:2,stackPassCatchers:1 as const,bringBack:true,randomness:.08};
const generated = generationSettings(settings,'classic',[1,2],[3],{'4':25});
assert(sameGenerationSettings(generated,generationSettings(settings,'classic',[2,1],[3],{'4':25})));
for (const change of [{nLineups:10},{minSalary:48000},{maxExposure:.5},{minUnique:3},
  {stackPassCatchers:2 as const},{bringBack:false},{randomness:0},{allowDkFallback:false}]) {
  assert(!sameGenerationSettings(generated,generationSettings({...settings,...change},'classic',[1,2],[3],{'4':25})),JSON.stringify(change));
}
assert(!sameGenerationSettings(generated,generationSettings(settings,'classic',[1],[3],{'4':25})));
assert(!sameGenerationSettings(generated,generationSettings(settings,'classic',[1,2],[],{'4':25})));
assert(!sameGenerationSettings(generated,generationSettings(settings,'classic',[1,2],[3],{'4':50})));
assert(!sameGenerationSettings(generated,generationSettings(settings,'showdown',[1,2],[3],{'4':25})));
console.log('Slate week matching and full generation settings contracts passed');
