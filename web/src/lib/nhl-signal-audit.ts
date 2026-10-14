/** NHL paper-ledger statistics. Resampling games does not validate a model. */
import type { MarketSignalScorecardRow } from "@/db/queries";

export const NHL_AUDIT_VERSION = "nhl-moneyline-audit-v2";
export type NhlAuditFields = {
  uniqueGames: number; settledGames: number; repeatedAlerts: number; opposingGames: number;
  pricedSettled: number; missingPrices: number; fresh: number; stale: number; unknownTiming: number;
  freshPricedSettled: number; freshRoi: number | null;
  priceClvN: number; avgPriceClvPct: number | null;
  roiInterval: [number, number] | null; priceClvInterval: [number, number] | null;
};
export type NhlAuditRow = {
  id: number; matchupId: number; alertType: string; side: string; createdAt: string;
  outcome: string | null; entryDecimal: number | null; entryVerified: boolean;
  clvPp: number | null; priceClvPct: number | null; intervalMinutes: number | null;
  capturedAt: string | null; updatedAt: string | null; startAt: string | null;
};

export function nhlQuoteStatus(capturedAt: string | null, updatedAt: string | null, decisionAt: string, startAt: string | null): "fresh" | "stale" | "unknown" | "invalid_time" {
  const times = [capturedAt, updatedAt, decisionAt, startAt].map(v => v == null ? NaN : Date.parse(v));
  if (times.some(v => !Number.isFinite(v))) return "unknown";
  const [c,u,a,s] = times;
  if (c > a || u > c || a >= s || c >= s) return "invalid_time";
  return a-c <= 300_000 && a-u <= 300_000 ? "fresh" : "stale";
}

export function steamBucket(minutes: number | null): string {
  if (minutes == null || !Number.isFinite(minutes) || minutes <= 0) return "steam_unknown";
  return minutes <= 30 ? "steam_short" : minutes <= 180 ? "steam_medium" : "steam_long";
}
const mean = (v: number[]) => v.length ? v.reduce((a,b) => a+b,0)/v.length : null;
const median = (v: number[]) => { if (!v.length) return null; const a=[...v].sort((a,b)=>a-b); return (a[Math.floor((a.length-1)/2)]+a[Math.ceil((a.length-1)/2)])/2; };
function units(r: NhlAuditRow): number | null {
  if (!r.entryVerified || r.entryDecimal == null || !Number.isFinite(r.entryDecimal) || r.entryDecimal <= 1) return null;
  return r.outcome === "won" ? r.entryDecimal-1 : r.outcome === "lost" ? -1 : null;
}

/** Percentile bootstrap, sampling whole games with replacement; seed and count fixed. */
export function clusterInterval(rows: NhlAuditRow[], metric: "roi" | "clv"): [number,number] | null {
  const groups=new Map<number,{sum:number;n:number}>();
  for (const r of rows) {
    const value=metric === "roi" ? units(r) : r.priceClvPct;
    if (value == null || !Number.isFinite(value)) continue;
    const g=groups.get(r.matchupId) ?? {sum:0,n:0}; g.sum+=value; g.n++; groups.set(r.matchupId,g);
  }
  if (groups.size < 2) return null;
  const games=[...groups.entries()].sort(([a],[b])=>a-b).map(([,g])=>g);
  let seed=20261012;
  const random=()=>{ seed=(Math.imul(1664525,seed)+1013904223)>>>0; return seed/4294967296; };
  const samples:number[]=[];
  for(let b=0;b<2000;b++) {
    let sum=0,n=0;
    for(let i=0;i<games.length;i++){const g=games[Math.floor(random()*games.length)];sum+=g.sum;n+=g.n;}
    samples.push(sum/n);
  }
  samples.sort((a,b)=>a-b);
  return [samples[Math.floor(samples.length*.025)],samples[Math.floor(samples.length*.975)]];
}

export function summarizeNhlSignals(rows: NhlAuditRow[]): (MarketSignalScorecardRow & NhlAuditFields)[] {
  const kinds=["all_moneyline","pinnacle_divergence","dk_value","steam","walking","steam_short","steam_medium","steam_long","steam_unknown"];
  const supported=new Set(["pinnacle_divergence","dk_value","steam","walking"]);
  rows=rows.filter(r=>supported.has(r.alertType));
  return kinds.map(kind=>{
    const rs=rows.filter(r=>kind === "all_moneyline" || r.alertType === kind || (r.alertType === "steam" && steamBucket(r.intervalMinutes) === kind));
    const settled=rs.filter(r=>r.outcome === "won" || r.outcome === "lost");
    const priced=settled.filter(r=>units(r)!=null);
    const status=(r:NhlAuditRow)=>nhlQuoteStatus(r.capturedAt,r.updatedAt,r.createdAt,r.startAt);
    const fresh=rs.filter(r=>status(r)==="fresh");
    const freshPriced=priced.filter(r=>status(r)==="fresh");
    const gameSides=new Map<number,Set<string>>();
    for(const r of rs){const sides=gameSides.get(r.matchupId)??new Set<string>();sides.add(r.side);gameSides.set(r.matchupId,sides);}
    const probabilities=rs.flatMap(r=>r.clvPp==null?[]:[r.clvPp]);
    const prices=rs.flatMap(r=>r.priceClvPct==null?[]:[r.priceClvPct]);
    const settledGames=new Set(settled.map(r=>r.matchupId)).size;
    return {alertType:kind,observations:rs.length,pending:rs.filter(r=>r.outcome==null).length,
      settled:settled.length,wins:settled.filter(r=>r.outcome==="won").length,losses:settled.filter(r=>r.outcome==="lost").length,
      voids:rs.filter(r=>r.outcome==="void").length,nClv:probabilities.length,avgClvPp:mean(probabilities),medianClvPp:median(probabilities),
      beatClose:mean(probabilities.map(p=>p>0?1:0)),units:priced.length?priced.reduce((s,r)=>s+units(r)!,0):null,
      roiPerBet:mean(priced.map(r=>units(r)!)),lastTriggeredAt:rs.length?rs.map(r=>r.createdAt).sort().at(-1)!:null,
      stage:settledGames>=100?"validation_ready":settledGames>=50?"initial_review":"collecting",
      uniqueGames:gameSides.size,settledGames,repeatedAlerts:rs.length-gameSides.size,
      opposingGames:[...gameSides.values()].filter(s=>s.size>1).length,pricedSettled:priced.length,
      missingPrices:settled.length-priced.length,fresh:fresh.length,stale:rs.filter(r=>status(r)==="stale").length,
      unknownTiming:rs.filter(r=>["unknown","invalid_time"].includes(status(r))).length,
      freshPricedSettled:freshPriced.length,freshRoi:mean(freshPriced.map(r=>units(r)!)),priceClvN:prices.length,avgPriceClvPct:mean(prices),
      roiInterval:clusterInterval(settled,"roi"),priceClvInterval:clusterInterval(rs,"clv")};
  });
}
