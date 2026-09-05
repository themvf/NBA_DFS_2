"use client";

import { useEffect, useMemo, useState, type CSSProperties, type ReactNode } from "react";
import { Search } from "lucide-react";
import { useRouter } from "next/navigation";
import type { TennisMatchRow, MlbLineMovementRow, LineAlertRow, MarketCaptureHealth, MarketSignalScorecardRow } from "@/db/queries";
import MarketSignalScorecard from "@/components/market-signal-scorecard";
import MovementIntelligence from "@/components/movement-intelligence";
import { buildMovementInsights, tennisIntelligenceEvents } from "@/lib/movement-intelligence";
import s from "./tennis-terminal.module.css";

const ml = (v: number | null) => v == null ? "—" : `${v > 0 ? "+" : ""}${v}`;
const pct = (v: number | null) => v == null ? "—" : `${(v * 100).toFixed(1)}%`;
const time = (v: string | null) => v && Number.isFinite(Date.parse(v))
  ? new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York" }).format(new Date(v)) + " ET" : "Time unavailable";
const label = (v: string) => v.replaceAll("_", " ").toUpperCase();
const BOOK_LABELS: Record<string,string> = { draftkings: "DraftKings", pinnacle: "Pinnacle", fanduel: "FanDuel", betmgm: "BetMGM", betrivers: "BetRivers", fanatics: "Fanatics", betonlineag: "BetOnline" };
const bookLabel = (v: string) => BOOK_LABELS[v] ?? v.replaceAll("_", " ").replace(/\b\w/g, c => c.toUpperCase());
const BOOK_COLORS = ["#52dbd1", "#b980ff", "#5da9ff", "#75d46b", "#ff6f91", "#e7d66b", "#ff8f4c", "#b6c2bd"];
const alertObservedAt = (alert: LineAlertRow) => typeof alert.details?.trigger_capture_at === "string"
  ? alert.details.trigger_capture_at : alert.createdAt;

function alertMagnitude(alert: LineAlertRow): string | null {
  for (const key of ["move_pp", "avg_move_pp", "price_move_pp", "reference_move_pp", "consensus_move_pp", "movement_pp", "first_leg_pp", "divergence_pp"]) {
    const value = alert.details?.[key];
    if (typeof value === "number" && Number.isFinite(value)) return `${value > 0 ? "+" : ""}${value.toFixed(1)}pp`;
  }
  return null;
}

function movementState(row?: MlbLineMovementRow): string {
  if (!row || row.captures < 2) return "INSUFFICIENT HISTORY";
  const delta = (row.closeProb - row.openProb) * 100;
  const values = row.trail.map(point => point.homeProb).filter(Number.isFinite);
  const maxExcursion = values.length ? Math.max(...values.map(value => Math.abs((value - row.openProb) * 100))) : Math.abs(delta);
  if (maxExcursion >= 1.5 && Math.abs(delta) <= 0.5) return "RETURNED TOWARD MEAN";
  if (Math.abs(delta) < 0.05) return "FLAT 0.0pp";
  return `${delta >= 0 ? "HOME" : "AWAY"} ${delta >= 0 ? "+" : ""}${delta.toFixed(1)}pp`;
}

function Trail({ row, mini = false }: { row?: MlbLineMovementRow; mini?: boolean }) {
  const points = useMemo(() => (row?.trail ?? []).filter(p => Number.isFinite(p.homeProb) && Number.isFinite(Date.parse(p.capturedAt)))
    .slice().sort((a,b) => Date.parse(a.capturedAt)-Date.parse(b.capturedAt)),[row?.trail]);
  const availableBooks = useMemo(() => {
    const counts = new Map<string,number>();
    for (const point of points) for (const book of Object.keys(point.bookHomeProbs ?? {})) counts.set(book,(counts.get(book) ?? 0)+1);
    const priority = (book: string) => book === "pinnacle" ? 0 : book === "draftkings" ? 1 : 2;
    return [...counts].sort(([a,ac],[b,bc]) => priority(a)-priority(b) || bc-ac || a.localeCompare(b)).slice(0,8).map(([book])=>book);
  },[points]);
  const [hiddenByMatch,setHiddenByMatch] = useState<Record<number,string[]>>({});
  const matchupId = row?.matchupId ?? -1;
  const hiddenBooks = hiddenByMatch[matchupId] ?? [];
  const visibleBooks = availableBooks.filter(book => !hiddenBooks.includes(book));
  if (!points.length) return <div className={s.noChart}>{mini ? "NO LOADED TRAIL" : "No probability trail in the loaded feed for this match."}</div>;
  const width = 640, height = 240, left = 52, right = 620;
  const start = Date.parse(points[0].capturedAt), end = Date.parse(points.at(-1)!.capturedAt);
  const displayedValues = points.flatMap(p => [p.homeProb,...visibleBooks.flatMap(book => p.bookHomeProbs?.[book] != null ? [p.bookHomeProbs[book]] : [])]);
  const lo = Math.max(0, Math.min(...displayedValues) - .015);
  const hi = Math.min(1, Math.max(...displayedValues) + .015);
  const x = (p: typeof points[number]) => end === start ? (left+right)/2 : left+(Date.parse(p.capturedAt)-start)/(end-start)*(right-left);
  const y = (value: number) => 20+(hi-value)/(hi-lo || 1)*180;
  const graphic = <svg viewBox={`0 0 ${width} ${height}`} className={mini ? s.mini : s.chart} role="img"
    aria-label={`${points.length} captured probability observations${mini ? "" : "; dashed connections indicate gaps over 30 minutes"}`}>
    {!mini && [0,.5,1].map(t => <g key={t}><line x1={left} x2={right} y1={20+t*180} y2={20+t*180} stroke="#27302f"/><text x="2" y={24+t*180} fill="#98a6a0" fontSize="11">{pct(hi-t*(hi-lo))}</text></g>)}
    {!mini && availableBooks.map((book,bi) => visibleBooks.includes(book) && points.slice(1).flatMap((point,i) => {
      const previous=points[i], from=previous.bookHomeProbs?.[book], to=point.bookHomeProbs?.[book];
      if (from == null || to == null) return [];
      return [<line key={`${book}-${point.capturedAt}`} x1={x(previous)} y1={y(from)} x2={x(point)} y2={y(to)} stroke={BOOK_COLORS[bi]} strokeWidth={book === "pinnacle" || book === "draftkings" ? 1.8 : 1.1} opacity={.82} strokeDasharray={Date.parse(point.capturedAt)-Date.parse(previous.capturedAt)>1800000 ? "6 5" : undefined}><title>{`${bookLabel(book)} · ${time(point.capturedAt)} · ${pct(to)}`}</title></line>];
    }))}
    {points.slice(1).map((p,i) => <line key={`${p.capturedAt}-${i}`} x1={x(points[i])} y1={y(points[i].homeProb)} x2={x(p)} y2={y(p.homeProb)} stroke="#f6a800" strokeWidth={mini ? 5 : 3} strokeDasharray={Date.parse(p.capturedAt)-Date.parse(points[i].capturedAt)>1800000 ? "7 5" : undefined}/>)}
    {points.map((p,i) => <circle key={i} cx={x(p)} cy={y(p.homeProb)} r={mini ? 5 : 2.5} fill="#f6a800"><title>{`Consensus · ${time(p.capturedAt)} · ${pct(p.homeProb)}`}</title></circle>)}
    {!mini && <><text x={left} y="229" fill="#98a6a0" fontSize="10">{time(points[0].capturedAt)}</text><text x={right} y="229" textAnchor="end" fill="#98a6a0" fontSize="10">{time(points.at(-1)!.capturedAt)}</text></>}
  </svg>;
  if (mini) return graphic;
  return <div><div className={s.bookLegend}><span className={s.consensusKey}>CONSENSUS</span>{availableBooks.map((book,i)=><button key={book} aria-pressed={visibleBooks.includes(book)} onClick={()=>setHiddenByMatch(current=>{const hidden=current[matchupId] ?? []; return {...current,[matchupId]:hidden.includes(book)?hidden.filter(v=>v!==book):[...hidden,book]};})} style={{"--book-color":BOOK_COLORS[i]} as CSSProperties}>{bookLabel(book)}</button>)}</div>{graphic}<div className={s.bookNote}>Consensus is bold · individual books use no-vig moneyline probability · top 8 by coverage shown</div></div>;
}

export default function TennisTerminal({ matchups, movement, alerts, queryDate, scorecard, captureHealth, children }: {
  matchups: TennisMatchRow[]; movement: MlbLineMovementRow[]; alerts: LineAlertRow[];
  queryDate: string | null; scorecard: MarketSignalScorecardRow[]; captureHealth: MarketCaptureHealth; children: ReactNode;
}) {
  const router = useRouter();
  const [search,setSearch] = useState("");
  const [tour,setTour] = useState("ALL");
  const [selected,setSelected] = useState<number | null>(null);
  const [market,setMarket] = useState("moneyline");
  const [now,setNow] = useState<number | null>(null);
  useEffect(() => { const initial = setTimeout(() => setNow(Date.now()),0); const id = setInterval(() => { setNow(Date.now()); router.refresh(); },60000); return () => { clearTimeout(initial); clearInterval(id); }; },[router]);
  const rows = useMemo(() => matchups.filter(m => (tour === "ALL" || m.tour === tour) &&
    `${m.homePlayer} ${m.awayPlayer} ${m.matchDate}`.toLowerCase().includes(search.toLowerCase())),[matchups,tour,search]);
  const intelligence = useMemo(() => buildMovementInsights(tennisIntelligenceEvents(rows, movement), alerts, now ?? NaN), [rows, movement, alerts, now]);
  const match = rows.find(m => m.id === selected) ?? rows.find(m => movement.some(r => r.matchupId === m.id && r.trail.length > 0)) ?? rows[0];
  const history = movement.find(r => r.matchupId === match?.id);
  const tape = alerts.filter(a => a.matchupId === match?.id).sort((a,b) => Date.parse(alertObservedAt(b))-Date.parse(alertObservedAt(a)));
  const age = history && now ? (now-Date.parse(history.closeCapturedAt))/60000 : null;
  const pastScheduledStart = now != null && match?.commenceTime != null && Date.parse(match.commenceTime) <= now;
  const status = !history ? "NO LOADED TRAIL" : pastScheduledStart ? "SAVED PRE-MATCH" : age != null && age > 30 ? "STALE" : "OBSERVED";
  const delta = history ? (history.closeProb-history.openProb)*100 : null;
  return <div className={s.terminal}>
    <header className={s.topbar}><h1>TENNIS LINE TERMINAL</h1><label className={s.search}><Search size={14}/><input aria-label="Search tennis players or date" placeholder="SEARCH PLAYER OR MATCH" value={search} onChange={e=>setSearch(e.target.value)}/></label><span className={s.amber}>RESEARCH · {queryDate ?? "UPCOMING BOARD"}</span></header>
    <MovementIntelligence items={intelligence} selectedKey={`${match?.id}:${market}`} loading={now == null} onSelect={(item) => { setSelected(item.matchupId); setMarket(item.market); }} />
    <div className={s.shell}>
      <aside className={s.watch}><div className={s.heading}>MARKET WATCH <span>{rows.length} MATCHES</span></div>
        <div className={s.tabs} aria-label="Tour filter">{["ALL","ATP","WTA"].map(t=><button key={t} aria-pressed={tour===t} onClick={()=>setTour(t)}>{t}</button>)}</div>
        <div className={s.watchScroll}>{rows.map(m=>{
          const trail=movement.find(r=>r.matchupId===m.id);
          const signals=alerts.filter(a=>a.matchupId===m.id && a.origin === "prospective");
          const latest=signals.slice().sort((a,b)=>Date.parse(alertObservedAt(b))-Date.parse(alertObservedAt(a)))[0];
          return <button key={m.id} className={s.watchRow} aria-pressed={match?.id===m.id} onClick={()=>setSelected(m.id)}>
            <span className={s.players}>{m.awayPlayer}<br/>{m.homePlayer}<small>{m.tour} · {time(m.commenceTime)}</small></span>
            <span className={s.watchPrice}>{ml(m.awayMl)}<br/>{ml(m.homeMl)}<Trail row={trail} mini/></span>
            <span className={s.badge}>{latest ? `${label(latest.alertType)} · ${latest.side.toUpperCase()}${alertMagnitude(latest) ? ` · ${alertMagnitude(latest)}` : ""}` : movementState(trail)}</span>
          </button>;
        })}{!rows.length && <p className={s.empty}>No matches match this search or tour.</p>}</div>
      </aside>
      <main className={s.instrument}>{match ? <>
        <div className={s.matchHeader}><div><div className={s.eyebrow}>{match.tour} / MATCH MARKET</div><h2>{match.awayPlayer} <span>vs</span> {match.homePlayer}</h2><p>{time(match.commenceTime)} · {match.completionStatus} · Scheduled start</p></div><div className={s.primaryQuote}>{ml(match.homeMl)}<small>{match.homePlayer} · moneyline</small></div></div>
        <div className={s.tabs} aria-label="Selected market">{["moneyline","total","handicap"].map(t=><button key={t} aria-pressed={market===t} onClick={()=>setMarket(t)}>{t.toUpperCase()}</button>)}</div>
        <div className={s.heading}>{market === "moneyline" ? `${match.homePlayer} · SPORTSBOOK PROBABILITY` : `${market.toUpperCase()} · LATEST QUOTES`}<span>{market !== "moneyline" ? "CURRENT QUOTES ONLY" : history ? `${history.captures} PRE-MATCH CAPTURES` : "CAPTURES UNAVAILABLE"}</span></div>
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
      <aside className={s.pulse}><div className={s.heading}>DATA PULSE <span>{status}</span></div><div className={s.pulseBlock}><strong className={s.amber}>{status}</strong><p>Last trail observation<br/>{history ? time(history.closeCapturedAt) : "Unavailable"}</p><p>{match?.nBooks ?? "—"} books in current match feed · {history?.trackedBooks ?? "—"} comparable at open/close</p></div>
        <div className={s.heading}>CAPTURE HEALTH <span>{captureHealth.status.replaceAll("_", " ")}</span></div><div className={s.pulseBlock}><strong className={captureHealth.status === "healthy" ? s.good : s.warn}>{captureHealth.due ? `${captureHealth.dueCaptured}/${captureHealth.due} DUE CAPTURED` : "NO CHECKPOINTS DUE"}</strong><p>{captureHealth.eventsCovered} matches covered · {captureHealth.missed} missed · {captureHealth.failed} failed</p><small>{captureHealth.pending} future checkpoints scheduled</small></div>
        <div className={s.heading}>SIGNAL TAPE <span>{tape.length} RECORDED</span></div>
        <div className={s.tape}>{tape.map((a,i)=><div className={s.pulseBlock} key={`${a.createdAt}-${a.alertType}-${i}`}><strong>{label(a.alertType)}</strong><p>{a.side === "home" ? match?.homePlayer : a.side === "away" ? match?.awayPlayer : label(a.side)}</p><small>{time(alertObservedAt(a))}{a.details?.origin === "retrospective" ? " · RETROSPECTIVE" : ""}</small><p>{a.outcome ?? "Outcome pending"} · CLV {a.clvPp == null ? "pending" : `${a.clvPp.toFixed(1)}pp`}</p></div>)}{!tape.length && <p className={s.empty}>No recorded signals in the loaded feed for this match.</p>}</div>
        <div className={s.notice}><strong>RESEARCH TERMINAL</strong><p>Tennis moneyline has no confirmed edge. Ratings are capped at 2★. Model disagreement is not a recommendation.</p><p>Capture targets reach five-minute intervals near scheduled start. Delays and missing observations remain possible.</p></div>
      </aside>
    </div>
    <MarketSignalScorecard rows={scorecard} sport="TENNIS" />
    <details className={s.research}><summary>RESEARCH DESK · SURFACE ELO / SIGNAL AUDIT / HISTORICAL RESULTS</summary><div className={s.researchBody}>{children}</div></details>
  </div>;
}
