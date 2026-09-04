"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Search } from "lucide-react";
import type { TennisMatchRow, MlbLineMovementRow, LineAlertRow } from "@/db/queries";
import s from "./tennis-terminal.module.css";

const ml = (v: number | null) => v == null ? "—" : `${v > 0 ? "+" : ""}${v}`;
const pct = (v: number | null) => v == null ? "—" : `${(v * 100).toFixed(1)}%`;
const time = (v: string | null) => v && Number.isFinite(Date.parse(v))
  ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York" }).format(new Date(v)) + " ET" : "Time unavailable";
const label = (v: string) => v.replaceAll("_", " ").toUpperCase();

function Trail({ row, mini = false }: { row?: MlbLineMovementRow; mini?: boolean }) {
  const points = (row?.trail ?? []).filter(p => Number.isFinite(p.homeProb) && Number.isFinite(Date.parse(p.capturedAt)))
    .slice().sort((a,b) => Date.parse(a.capturedAt)-Date.parse(b.capturedAt));
  if (!points.length) return <div className={s.noChart}>{mini ? "NO LOADED TRAIL" : "No probability trail in the loaded feed for this match."}</div>;
  const width = 640, height = 240, left = 52, right = 620;
  const start = Date.parse(points[0].capturedAt), end = Date.parse(points.at(-1)!.capturedAt);
  const lo = Math.max(0, Math.min(...points.map(p => p.homeProb)) - .015);
  const hi = Math.min(1, Math.max(...points.map(p => p.homeProb)) + .015);
  const x = (p: typeof points[number]) => end === start ? (left+right)/2 : left+(Date.parse(p.capturedAt)-start)/(end-start)*(right-left);
  const y = (p: typeof points[number]) => 20+(hi-p.homeProb)/(hi-lo || 1)*180;
  return <svg viewBox={`0 0 ${width} ${height}`} className={mini ? s.mini : s.chart} role="img"
    aria-label={`${points.length} captured probability observations${mini ? "" : "; dashed connections indicate gaps over 30 minutes"}`}>
    {!mini && [0,.5,1].map(t => <g key={t}><line x1={left} x2={right} y1={20+t*180} y2={20+t*180} stroke="#27302f"/><text x="2" y={24+t*180} fill="#98a6a0" fontSize="11">{pct(hi-t*(hi-lo))}</text></g>)}
    {points.slice(1).map((p,i) => <line key={`${p.capturedAt}-${i}`} x1={x(points[i])} y1={y(points[i])} x2={x(p)} y2={y(p)} stroke="#f6a800" strokeWidth={mini ? 5 : 2} strokeDasharray={Date.parse(p.capturedAt)-Date.parse(points[i].capturedAt)>1800000 ? "7 5" : undefined}/>)}
    {points.map((p,i) => <circle key={i} cx={x(p)} cy={y(p)} r={mini ? 5 : 3} fill="#f6a800"><title>{time(p.capturedAt)} · {pct(p.homeProb)}</title></circle>)}
    {!mini && <><text x={left} y="229" fill="#98a6a0" fontSize="10">{time(points[0].capturedAt)}</text><text x={right} y="229" textAnchor="end" fill="#98a6a0" fontSize="10">{time(points.at(-1)!.capturedAt)}</text></>}
  </svg>;
}

export default function TennisTerminal({ matchups, movement, alerts, queryDate, children }: {
  matchups: TennisMatchRow[]; movement: MlbLineMovementRow[]; alerts: LineAlertRow[];
  queryDate: string | null; children: ReactNode;
}) {
  const [search,setSearch] = useState("");
  const [tour,setTour] = useState("ALL");
  const [selected,setSelected] = useState<number | null>(null);
  const [market,setMarket] = useState("moneyline");
  const [now,setNow] = useState<number | null>(null);
  useEffect(() => { const initial = setTimeout(() => setNow(Date.now()),0); const id = setInterval(() => setNow(Date.now()),60000); return () => { clearTimeout(initial); clearInterval(id); }; },[]);
  const rows = useMemo(() => matchups.filter(m => (tour === "ALL" || m.tour === tour) &&
    `${m.homePlayer} ${m.awayPlayer} ${m.matchDate}`.toLowerCase().includes(search.toLowerCase())),[matchups,tour,search]);
  const match = rows.find(m => m.id === selected) ?? rows.find(m => movement.some(r => r.matchupId === m.id && r.trail.length > 0)) ?? rows[0];
  const history = movement.find(r => r.matchupId === match?.id);
  const tape = alerts.filter(a => a.matchupId === match?.id).sort((a,b) => Date.parse(b.createdAt)-Date.parse(a.createdAt));
  const age = history && now ? (now-Date.parse(history.closeCapturedAt))/60000 : null;
  const status = !history ? "NO HISTORY" : age != null && age > 30 ? "STALE" : "OBSERVED";
  const delta = history ? (history.closeProb-history.openProb)*100 : null;
  return <div className={s.terminal}>
    <header className={s.topbar}><h1>TENNIS LINE TERMINAL</h1><label className={s.search}><Search size={14}/><input aria-label="Search tennis players or date" placeholder="SEARCH PLAYER OR MATCH" value={search} onChange={e=>setSearch(e.target.value)}/></label><span className={s.amber}>RESEARCH · {queryDate ?? "UPCOMING BOARD"}</span></header>
    <div className={s.shell}>
      <aside className={s.watch}><div className={s.heading}>MARKET WATCH <span>{rows.length} MATCHES</span></div>
        <div className={s.tabs} aria-label="Tour filter">{["ALL","ATP","WTA"].map(t=><button key={t} aria-pressed={tour===t} onClick={()=>setTour(t)}>{t}</button>)}</div>
        <div className={s.watchScroll}>{rows.map(m=>{
          const trail=movement.find(r=>r.matchupId===m.id);
          const signals=alerts.filter(a=>a.matchupId===m.id);
          return <button key={m.id} className={s.watchRow} aria-pressed={match?.id===m.id} onClick={()=>setSelected(m.id)}>
            <span className={s.players}>{m.awayPlayer}<br/>{m.homePlayer}<small>{m.tour} · {time(m.commenceTime)}</small></span>
            <span className={s.watchPrice}>{ml(m.awayMl)}<br/>{ml(m.homeMl)}<Trail row={trail} mini/></span>
            {signals.length>0 && <span className={s.badge}>{[...new Set(signals.map(a=>label(a.alertType)))].slice(0,2).join(" · ")}</span>}
          </button>;
        })}{!rows.length && <p className={s.empty}>No matches match this search or tour.</p>}</div>
      </aside>
      <main className={s.instrument}>{match ? <>
        <div className={s.matchHeader}><div><div className={s.eyebrow}>{match.tour} / MATCH MARKET</div><h2>{match.awayPlayer} <span>vs</span> {match.homePlayer}</h2><p>{time(match.commenceTime)} · {match.completionStatus} · Scheduled start</p></div><div className={s.primaryQuote}>{ml(match.homeMl)}<small>{match.homePlayer} · moneyline</small></div></div>
        <div className={s.tabs} aria-label="Selected market">{["moneyline","total","handicap"].map(t=><button key={t} aria-pressed={market===t} onClick={()=>setMarket(t)}>{t.toUpperCase()}</button>)}</div>
        <div className={s.heading}>{market === "moneyline" ? `${match.homePlayer} · SPORTSBOOK PROBABILITY` : `${market.toUpperCase()} · LATEST QUOTES`}<span>{history?.captures ?? 0} CAPTURES</span></div>
        {market === "moneyline" ? <div className={s.chartArea}><Trail row={history}/><div className={s.chartFoot}><span>● Sportsbook trail · {delta == null ? "—" : `${delta>=0?"+":""}${delta.toFixed(1)}pp since first capture`}</span><span>Dashed = gap &gt;30m · independently scaled</span></div></div> : <div className={s.marketQuotes}>
          <div><small>{market === "total" ? "TOTAL GAMES" : `${match.homePlayer} HANDICAP`}</small><strong>{market === "total" ? match.totalGamesLine ?? "—" : match.setHandicap ?? "—"}</strong></div>
          <div><small>{market === "total" ? "OVER / UNDER" : "HOME / AWAY PRICE"}</small><strong>{market === "total" ? `${ml(match.overOdds)} / ${ml(match.underOdds)}` : `${ml(match.handicapHomeOdds)} / ${ml(match.handicapAwayOdds)}`}</strong></div>
          <p>Current quotes only. No reconstructed {market} path is supplied to this board.</p>
        </div>}
        <div className={s.heading}>EXECUTABLE PRICE COMPARISON <span>LATEST AVAILABLE · NOT A BET RECOMMENDATION</span></div>
        <div className={s.tableScroll}><table><thead><tr><th>PLAYER</th><th>CONSENSUS ML</th><th>BEST BOOK</th><th>BEST ML</th><th>DK ML</th></tr></thead><tbody>
          {(["away","home"] as const).map(side=><tr key={side}><td>{side === "home" ? match.homePlayer : match.awayPlayer}</td><td>{ml(side === "home" ? match.homeMl : match.awayMl)}</td><td>{(side === "home" ? match.bestHomeBook : match.bestAwayBook)?.replaceAll("_"," ") ?? "—"}</td><td className={s.amber}>{ml(side === "home" ? match.bestHomeMl : match.bestAwayMl)}</td><td>{ml(side === "home" ? match.dkHomeMl : match.dkAwayMl)}</td></tr>)}
        </tbody></table></div>
        <div className={s.heading}>MODEL / MARKET <span>CALIBRATION ONLY</span></div>
        <div className={s.metrics}><div><small>MARKET · {match.homePlayer}</small><strong>{pct(match.homeWinProb)}</strong></div><div><small>MODEL · {match.homePlayer}</small><strong>{pct(match.ourProbHome)}</strong></div><div><small>OVERALL ELO · HOME / AWAY</small><strong>{match.homeElo ?? "—"} / {match.awayElo ?? "—"}</strong></div></div>
      </> : <p className={s.empty}>Select a match when the feed is available. Missing quotes are never displayed as zero.</p>}</main>
      <aside className={s.pulse}><div className={s.heading}>DATA PULSE <span>{status}</span></div><div className={s.pulseBlock}><strong className={s.amber}>{status}</strong><p>Last trail observation<br/>{history ? time(history.closeCapturedAt) : "Unavailable"}</p><p>{match?.nBooks ?? "—"} books in match feed · {history?.trackedBooks ?? "—"} tracked in trail</p></div>
        <div className={s.heading}>SIGNAL TAPE <span>{tape.length} RECORDED</span></div>
        <div className={s.tape}>{tape.map((a,i)=><div className={s.pulseBlock} key={`${a.createdAt}-${a.alertType}-${i}`}><strong>{label(a.alertType)}</strong><p>{a.side === "home" ? match?.homePlayer : a.side === "away" ? match?.awayPlayer : label(a.side)}</p><small>{time(a.createdAt)}</small><p>{a.outcome ?? "Outcome pending"} · CLV {a.clvPp == null ? "pending" : `${a.clvPp.toFixed(1)}pp`}</p></div>)}{!tape.length && <p className={s.empty}>No recorded signals in the loaded feed for this match.</p>}</div>
        <div className={s.notice}><strong>RESEARCH TERMINAL</strong><p>Tennis moneyline has no confirmed edge. Ratings are capped at 2★. Model disagreement is not a recommendation.</p><p>Capture targets reach five-minute intervals near scheduled start. Delays and missing observations remain possible.</p></div>
      </aside>
    </div>
    <details className={s.research}><summary>RESEARCH DESK · SURFACE ELO / SIGNAL AUDIT / HISTORICAL RESULTS</summary><div className={s.researchBody}>{children}</div></details>
  </div>;
}
