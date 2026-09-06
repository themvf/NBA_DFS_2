export const TRACKING_SPORTS = ["mlb", "nfl", "cfb", "tennis"] as const;
export const TRACKING_SIGNALS = ["steam","walking","reversal","reference_led","price_pressure","pinnacle_divergence","book_disagreement","market_convergence","late_move","favorite_flip","key_cross","spread_steam","spread_walking","total_steam","total_walking","dk_value","pinnacle_favorite_forward","pinnacle_polymarket_delta","mlb_total_price_steam","mlb_total_price_walking","mlb_total_price_reversal","mlb_run_line_points_steam","mlb_run_line_points_walking","mlb_run_line_points_reversal","mlb_total_steam","mlb_total_walking","mlb_total_reversal","mlb_run_line_steam","mlb_run_line_walking","mlb_run_line_reversal","mlb_moneyline_reversal"];
export const TRACKING_RESULTS = ["won","lost","push","draw","void","pending","unavailable"];
export function trackingFilters(params: Record<string,string|string[]|undefined>, now = new Date()) {
  const value=(k:string)=>typeof params[k]==="string"?params[k] as string:"";
  const validDate=(v:string)=>/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString().slice(0,10)===v;
  const today=new Intl.DateTimeFormat("en-CA",{timeZone:"America/New_York",year:"numeric",month:"2-digit",day:"2-digit"}).format(now);
  const end=validDate(value("to"))?value("to"):today;
  const start=validDate(value("from"))?value("from"):new Date(Date.parse(end)-89*86400000).toISOString().slice(0,10);
  return {sport:TRACKING_SPORTS.includes(value("sport") as typeof TRACKING_SPORTS[number])?value("sport"):"all",signal:TRACKING_SIGNALS.includes(value("signal"))?value("signal"):"all",result:TRACKING_RESULTS.includes(value("result"))?value("result"):"all",from:start>end?end:start,to:start>end?start:end,page:Math.min(100000,Math.max(1,Number.parseInt(value("page"),10)||1))};
}
export type TrackingFilters=ReturnType<typeof trackingFilters>;
export type TrackingGroup={sport:string;signal:string;market:string;version:string;total:number;wins:number;losses:number;pushes:number;draws:number;voids:number;pending:number;unavailable:number;units:number|null;priced:number;events:number};
export type TrackingEntry={id:number;sport:string;date:string;matchup:string;signal:string;market:string;version:string;side:string;result:string;reason:string|null;observedAt:string;entryLine:number|null;entryPrice:number|null;units:number|null};
export function trackingTotals(groups:TrackingGroup[]) {return groups.reduce((a,g)=>({total:a.total+g.total,wins:a.wins+g.wins,losses:a.losses+g.losses,pushes:a.pushes+g.pushes,draws:a.draws+g.draws,voids:a.voids+g.voids,pending:a.pending+g.pending,unavailable:a.unavailable+g.unavailable,units:a.units+(g.units??0),priced:a.priced+g.priced}),{total:0,wins:0,losses:0,pushes:0,draws:0,voids:0,pending:0,unavailable:0,units:0,priced:0});}
