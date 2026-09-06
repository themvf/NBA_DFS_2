"use client";
import { useState } from "react";
import s from "./sportsbook-history.module.css";
export type BookHistoryPoint = { at: string; values: Record<string, number | null> };
const names: Record<string,string> = {draftkings:"DraftKings", fanduel:"FanDuel", betmgm:"BetMGM", betrivers:"BetRivers", pinnacle:"Pinnacle", fanatics:"Fanatics", williamhill_us:"Caesars", bovada:"Bovada", betonlineag:"BetOnline"};
const colors = ["#52dbd1","#f6a800","#a78bfa","#60a5fa","#f472b6","#4ade80","#fb923c","#e879f9","#cbd5e1"];
const name = (key:string) => names[key] ?? key.replaceAll("_"," ");
function color(key:string) { const index=Object.keys(names).indexOf(key); return colors[index<0 ? [...key].reduce((a,c)=>a+c.charCodeAt(0),0)%colors.length : index]; }
const time=(at:string)=>new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",month:"short",day:"numeric",hour:"numeric",minute:"2-digit"}).format(new Date(at));
export default function SportsbookHistory({points:input, percentage=false, label, markers=[]}:{points:BookHistoryPoint[]; percentage?:boolean; label:string; markers?:{at:string; label:string}[]}) {
  const [selected,setSelected]=useState("all"); const [hidden,setHidden]=useState<string[]>([]);
  const points=input.filter(p=>Number.isFinite(Date.parse(p.at))).slice().sort((a,b)=>Date.parse(a.at)-Date.parse(b.at));
  const books=[...new Set(points.flatMap(p=>Object.keys(p.values)))].filter(k=>k!=="polymarket" && points.some(p=>Number.isFinite(p.values[k])));
  const matched=books.filter(k=>points.every(p=>Number.isFinite(p.values[k])));
  const consensus=points.map(p=>{const values=matched.map(k=>p.values[k]!).sort((a,b)=>a-b);return values.length ? percentage ? values.reduce((a,b)=>a+b,0)/values.length : values[Math.floor((values.length-1)/2)] : null;});
  const series=selected==="consensus" ? ["consensus"] : selected==="all" ? books.filter(k=>!hidden.includes(k)) : books.filter(k=>k===selected);
  const v=(index:number,key:string)=>key==="consensus" ? consensus[index] : points[index].values[key];
  const values=points.flatMap((_,i)=>series.flatMap(k=>Number.isFinite(v(i,k)) ? [v(i,k)!] : []));
  const summary=selected==="all"||selected==="consensus" ? consensus : points.map(p=>p.values[selected]);
  const observed=summary.filter((n):n is number=>typeof n==="number"&&Number.isFinite(n));
  const first=observed[0],last=observed.at(-1);
  const fmt=(n:number|undefined)=>n==null?"—":`${n.toFixed(1)}${percentage?"%":""}`;
  const low=Math.min(...values),high=Math.max(...values),pad=Math.max((high-low)*.2,percentage?.3:.25);
  const start=points.length?Date.parse(points[0].at):0,end=points.length?Date.parse(points.at(-1)!.at):0;
  const x=(at:string)=>55+(Date.parse(at)-start)/Math.max(end-start,1)*660;
  const y=(value:number)=>205-(value-low+pad)/(high-low+pad*2)*165;
  return <div className={s.panel}>
    <div className={s.controls}><span>{label}</span><select aria-label="Chart bookmaker" value={selected} onChange={e=>setSelected(e.target.value)}><option value="all">Individual sportsbooks</option><option value="consensus">Matched-book consensus</option>{books.map(k=><option key={k} value={k}>{name(k)}</option>)}</select></div>
    <p className={s.caption}>Summary: {selected==="all"||selected==="consensus"?"matched-book consensus":name(selected)}.</p>
    <div className={s.metrics}><div><span>FIRST OBSERVED</span><strong>{fmt(first)}</strong></div><div><span>LATEST</span><strong>{fmt(last)}</strong></div><div><span>CHANGE</span><strong>{observed.length>1&&last!=null?`${last-first>0?"+":""}${(last-first).toFixed(1)} ${percentage?"pp":"pts"}`:"Awaiting captures"}</strong></div><div><span>COMPARABLE BOOKS</span><strong>{selected==="all"||selected==="consensus"?matched.length:observed.length?1:0}</strong></div></div>
    {selected==="all"&&<div className={s.legend} role="group" aria-label="Show or hide sportsbooks">{books.map(k=><button key={k} aria-pressed={!hidden.includes(k)} onClick={()=>setHidden(h=>h.includes(k)?h.filter(b=>b!==k):[...h,k])}><i style={{background:color(k)}}/>{name(k)}</button>)}{!!hidden.length&&<button onClick={()=>setHidden([])}>Show all</button>}</div>}
    {!values.length?<p className={s.empty}>{books.length?"No comparable observations in this selection. Choose a sportsbook to inspect its history.":"No stored sportsbook history for this selection."}</p>:<svg className={s.chart} viewBox="0 0 760 250" role="img" aria-label={`${label} sportsbook history`}>
      <title>{`${label} sportsbook history`}</title>
      {[low-pad,(low+high)/2,high+pad].map(t=><g key={t}><line x1="55" x2="720" y1={y(t)} y2={y(t)} stroke="#283330"/><text x="4" y={y(t)+4}>{fmt(t)}</text></g>)}
      {series.map(k=><g key={k}>{points.slice(1).map((p,i)=>{const a=v(i,k),b=v(i+1,k);return a==null||b==null?null:<line key={p.at} x1={x(points[i].at)} x2={x(p.at)} y1={y(a)} y2={y(b)} stroke={color(k)} strokeWidth="2" strokeDasharray={Date.parse(p.at)-Date.parse(points[i].at)>1800000?"5 5":undefined}/>;})}{points.map((p,i)=>{const n=v(i,k);return n==null?null:<circle key={p.at} cx={x(p.at)} cy={y(n)} r="3" fill={color(k)}><title>{`${name(k)} · ${time(p.at)} ET · ${fmt(n)}`}</title></circle>;})}</g>)}
      {markers.filter(m=>Date.parse(m.at)>=start&&Date.parse(m.at)<=end).map((m,i)=><line key={`${m.at}:${i}`} x1={x(m.at)} x2={x(m.at)} y1="35" y2="205" stroke="#f6a800" strokeDasharray="3 5"><title>{`${m.label} · ${time(m.at)} ET`}</title></line>)}
      <text x="55" y="237">{time(points[0].at)} ET</text><text x="715" y="237" textAnchor="end">{time(points.at(-1)!.at)} ET</text>
    </svg>}
    <p className={s.caption}>Each color is one sportsbook. Tap its name to hide or show it. Missing quotes leave gaps; identical lines overlap. Dashed connections span more than 30 minutes.</p>
  </div>;
}
