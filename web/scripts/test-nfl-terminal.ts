import assert from "node:assert/strict";
import { nflMarket, nflQuoteValue } from "../src/lib/nfl-terminal";

const now = Date.parse("2026-09-06T16:00:00Z");
const quote = { spread_home:-3, spread_away:3, spread_home_price:-115, spread_away_price:-105,
  ml_home:-150, ml_away:130, total_line:45.5, over:-120, under:100, last_update:new Date(now).toISOString() };
assert.equal(nflQuoteValue(quote,"spread","away"),3);
assert.equal(nflQuoteValue(quote,"total","under"),45.5);
assert.ok(Math.abs(nflQuoteValue(quote,"moneyline","home")!+nflQuoteValue(quote,"moneyline","away")!-1)<1e-12);
assert.equal(nflQuoteValue({...quote,ml_away:0},"moneyline","home"),null);
const game: Parameters<typeof nflMarket>[0] = { commenceTime:new Date(now+3600000).toISOString(), trail:[
  {capturedAt:new Date(now-600000).toISOString(),books:{draftkings:{...quote,spread_home:-2,spread_away:2}}},
  {capturedAt:new Date(now).toISOString(),books:{draftkings:quote,polymarket:{...quote,spread_home:-20}}},
  {capturedAt:new Date(now+10000).toISOString(),books:{draftkings:{...quote,spread_home:-10}}},
] };
const home = nflMarket(game,"spread","home",now), away = nflMarket(game,"spread","away",now);
assert.equal(home.current,-3); assert.equal(home.move,-1); assert.equal(away.move,1);
assert.equal(home.books.length,1); assert.equal(home.books[0].price,-115); assert.equal(away.books[0].price,-105);
assert.equal(home.books[0].fresh,true);
assert.equal(nflMarket(game,"total","under",now).books[0].price,100);
assert.equal(nflMarket(game,"spread","home",now+600000).books[0].fresh,false);
assert.equal(nflMarket({...game,trail:[{...game.trail[0],books:{draftkings:{...quote,last_update:new Date(now+1000).toISOString()}}} ]},"spread","home",now).books[0].fresh,false);
assert.equal(nflMarket({...game,trail:[game.trail[0],{...game.trail[1],books:{}}]},"spread","home",now).current,null);
assert.equal(nflMarket({...game,commenceTime:new Date(now).toISOString()},"spread","home",now).current,-2);
console.log("NFL terminal checks passed: side-specific prices, no-vig probability, source separation, capture boundaries, missing quotes, freshness.");
