import assert from "node:assert/strict";
import { clusterInterval, nhlQuoteStatus, steamBucket, summarizeNhlSignals, type NhlAuditRow } from "../src/lib/nhl-signal-audit";
import { buildNhlMarket, quoteFresh } from "../src/lib/nhl-market";

const row=(overrides: Partial<NhlAuditRow>={}):NhlAuditRow=>({id:1,matchupId:1,alertType:"steam",side:"home",
  createdAt:"2026-10-12T20:00:00Z",outcome:"won",entryDecimal:2,entryVerified:true,clvPp:1,priceClvPct:3,intervalMinutes:30,
  capturedAt:"2026-10-12T19:57:00Z",updatedAt:"2026-10-12T19:56:00Z",startAt:"2026-10-12T21:00:00Z",...overrides});
assert.equal(nhlQuoteStatus(row().capturedAt,row().updatedAt,row().createdAt,row().startAt),"fresh");
assert.equal(nhlQuoteStatus("2026-10-12T20:01:00Z",row().updatedAt,row().createdAt,row().startAt),"invalid_time");
assert.equal(nhlQuoteStatus(row().capturedAt,"2026-10-12T19:58:00Z",row().createdAt,row().startAt),"invalid_time");
assert.equal(nhlQuoteStatus(null,row().updatedAt,row().createdAt,row().startAt),"unknown");
assert.equal(steamBucket(30),"steam_short");assert.equal(steamBucket(30.01),"steam_medium");
assert.equal(steamBucket(180),"steam_medium");assert.equal(steamBucket(180.01),"steam_long");assert.equal(steamBucket(-1),"steam_unknown");
const sample=[row(),row({id:2,alertType:"walking",side:"away",outcome:"lost",entryDecimal:3}),
  row({id:3,matchupId:2,outcome:"lost",entryDecimal:null,intervalMinutes:180.01}),
  row({id:4,matchupId:3,outcome:null,entryVerified:false,intervalMinutes:null})];
const result=summarizeNhlSignals(sample),all=result.find(r=>r.alertType==="all_moneyline")!;
assert.equal(all.observations,4);assert.equal(all.uniqueGames,3);assert.equal(all.repeatedAlerts,1);assert.equal(all.opposingGames,1);
assert.equal(all.settled,3);assert.equal(all.pricedSettled,2);assert.equal(all.missingPrices,1);assert.equal(all.roiPerBet,0);
assert.equal(all.priceClvN,4);assert.equal(result.find(r=>r.alertType==="steam_short")!.observations,1);
assert.equal(result.find(r=>r.alertType==="steam_long")!.observations,1);
assert.equal(clusterInterval([row(),row({id:2})],"roi"),null); // two alerts, only one game
assert.deepEqual(clusterInterval(sample,"clv"),clusterInterval(sample,"clv"));
assert.equal(summarizeNhlSignals([row({entryVerified:false})])[0].pricedSettled,0);
assert.equal(summarizeNhlSignals([])[0].roiPerBet,null);
const correlated=Array.from({length:120},(_,i)=>row({id:i}));
assert.equal(summarizeNhlSignals(correlated)[0].stage,"collecting");
assert.equal(quoteFresh("2026-10-12T20:01:00Z","2026-10-12T20:00:00Z","2026-10-12T20:00:00Z"),false);
assert.equal(quoteFresh("invalid","2026-10-12T20:00:00Z","2026-10-12T20:00:00Z"),false);
assert.equal(quoteFresh("2026-10-12T19:59:00Z","2026-10-12T20:01:00Z","2026-10-12T20:00:00Z"),false);
const game={homeTeam:"X",awayTeam:"Y",homeAbbrev:"X",awayAbbrev:"Y",commenceTime:row().startAt,latestCapturedAt:row().capturedAt,
  openingBooks:null,currentBooks:{fanduel:{ml_home:100,ml_away:-120,last_update:row().updatedAt!}},closingBooks:null,history:[]};
assert.equal(buildNhlMarket(game,"moneyline","home","price",row().startAt!).books[0].fresh,false);
console.log("NHL signal audit passed: exact denominators, game clustering, timing, interval boundaries, pregame freshness.");
