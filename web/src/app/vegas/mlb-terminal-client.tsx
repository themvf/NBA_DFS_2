"use client";

import Link from "next/link";
import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Activity, ArrowLeft, ArrowRight, RefreshCw, Search } from "lucide-react";
import { fmtPrice, marketTrail, number, quote, signalMarket, signalOutcome, summarizeSignals,
  type MlbMarket, type MlbSide, type MlbTerminalBoard, type MlbTerminalSignal } from "@/lib/mlb-terminal";
import styles from "./mlb-terminal.module.css";

const MARKETS: Record<MlbMarket, string> = { moneyline: "Moneyline", run_line: "Run line", total: "Total" };
function time(value: string | null) {
  return value && Number.isFinite(Date.parse(value)) ? new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(new Date(value)) : "—";
}
function signed(n: number, digits = 1) { return `${n > 0 ? "+" : ""}${n.toFixed(digits)}`; }
function title(type: string) { return type.replace(/^mlb_/, "").replaceAll("_", " "); }
function entry(signal: MlbTerminalSignal) {
  const line = number(signal.details.exec_line);
  return `${signal.side.toUpperCase()}${line == null ? "" : ` ${line}`} · ${fmtPrice(signal.details.dk_odds)}`;
}
function TrailChart({ points, market, priceView = false }: { points: ReturnType<typeof marketTrail>["points"]; market: MlbMarket; priceView?: boolean }) {
  if (points.length < 2) return <div className={styles.empty}>At least two comparable captures are needed to chart movement.</div>;
  const min = Math.min(...points.map((p) => p.value)); const max = Math.max(...points.map((p) => p.value));
  const percentage = market === "moneyline" || priceView;
  const pad = Math.max((max - min) * .2, percentage ? .3 : .25);
  const start = Date.parse(points[0].at); const duration = Math.max(Date.parse(points.at(-1)!.at) - start, 1);
  const x = (at: string) => 55 + (Date.parse(at) - start) / duration * 660;
  const y = (value: number) => 205 - (value - min + pad) / (max - min + pad * 2) * 165;
  return <svg className={styles.chart} viewBox="0 0 760 250" role="img" aria-label={`${MARKETS[market]} history; ${points.length} observations`}>
    <title>{`${MARKETS[market]} observed ${priceView ? "price" : "line"} movement`}</title>
    {[min - pad, (min + max) / 2, max + pad].map((v) => <g key={v}><line x1="55" x2="720" y1={y(v)} y2={y(v)} stroke="#283330" /><text x="4" y={y(v) + 4} fill="#93a39c" fontSize="11">{v.toFixed(1)}{percentage ? "%" : ""}</text></g>)}
    <polyline fill="none" stroke="#52dbd1" strokeWidth="2" points={points.map((p) => `${x(p.at)},${y(p.value)}`).join(" ")} />
    {points.map((p) => <circle key={p.at} cx={x(p.at)} cy={y(p.value)} r="3" fill="#52dbd1"><title>{`${time(p.at)}: ${p.value.toFixed(2)}${percentage ? "%" : " runs"}${p.price != null ? ` at ${fmtPrice(p.price)}` : ""}`}</title></circle>)}
    <text x="55" y="237" fill="#93a39c" fontSize="11">{time(points[0].at)} ET</text><text x="715" y="237" textAnchor="end" fill="#93a39c" fontSize="11">{time(points.at(-1)!.at)} ET</text>
  </svg>;
}

export default function MlbTerminalClient({ board }: { board: MlbTerminalBoard }) {
  const router = useRouter(); const [pending, refresh] = useTransition();
  const [query, setQuery] = useState(""); const [selected, setSelected] = useState<number | null>(null);
  const [market, setMarket] = useState<MlbMarket>("moneyline"); const [side, setSide] = useState<MlbSide>("home");
  const [book, setBook] = useState(""); const [filter, setFilter] = useState("all"); const [clock, setClock] = useState(board.asOf);
  const [priceMode, setPriceMode] = useState(false);
  useEffect(() => { const timer = window.setInterval(() => { setClock(new Date().toISOString()); if (document.visibilityState === "visible") refresh(() => router.refresh()); }, 60_000); return () => clearInterval(timer); }, [router]);
  const todaySignals = board.signals.filter((signal) => signal.date === board.date);
  const games = board.games.filter((game) => `${game.away} ${game.home}`.toLowerCase().includes(query.toLowerCase()) && (filter === "all" || (filter === "signals" ? todaySignals.some((s) => s.matchupId === game.id) : game.startsAt != null && Date.parse(game.startsAt) > Date.parse(clock))));
  const game = games.find((g) => g.id === selected) ?? games[0];
  const history = game?.history ?? []; const latest = history.at(-1);
  const priceView = priceMode && market !== "moneyline" && !!book;
  const trail = marketTrail(history, market, side, book, priceView);
  const first = trail.points[0]; const current = trail.points.at(-1);
  const gameSignals = todaySignals.filter((s) => s.matchupId === game?.id && signalMarket(s) === market);
  const summary = summarizeSignals(board.signals);
  const started = !!game?.startsAt && Date.parse(game.startsAt) <= Date.parse(clock);
  const age = latest ? Math.max(0, Math.floor((Date.parse(clock) - Date.parse(latest.capturedAt)) / 60_000)) : null;
  const moveDate = (delta: number) => { const next = new Date(`${board.date}T12:00:00Z`); next.setUTCDate(next.getUTCDate() + delta); router.push(`/vegas?sport=mlb&date=${next.toISOString().slice(0, 10)}`); };
  function chooseMarket(next: MlbMarket) { setMarket(next); setSide(next === "total" ? "over" : "home"); setBook(""); setPriceMode(false); }
  const formatValue = (value: number | undefined) => value == null ? "—" : `${value.toFixed(1)}${market === "moneyline" || priceView ? "%" : " runs"}`;
  return <div className={styles.terminal}>
    <header className={styles.topbar}><strong className={styles.brand}><Activity size={17} /> MLB LINE TERMINAL</strong><label className={styles.search}><Search size={15} /><input aria-label="Search MLB teams" placeholder="Find a team…" value={query} onChange={(e) => setQuery(e.target.value)} /></label><span className={styles.clock}>{time(clock)} ET</span><button onClick={() => refresh(() => router.refresh())} disabled={pending} aria-label="Refresh stored MLB data"><RefreshCw size={15} className={pending ? styles.spin : ""} /></button></header>
    <nav className={styles.nav} aria-label="MLB views"><span>Line movement</span><Link href="/vegas/mlb-props?sport=mlb">Vegas Props</Link><Link href="/vegas/mlb-props-v2?sport=mlb">Vegas Prop 2</Link><Link href={`/vegas/mlb-diagnostics?sport=mlb&date=${board.date}`}>Model diagnostics</Link><div className={styles.date}><button aria-label="Previous day" onClick={() => moveDate(-1)}><ArrowLeft size={14} /></button><input aria-label="MLB game date" type="date" value={board.date} onChange={(e) => { if (e.target.value) router.push(`/vegas?sport=mlb&date=${e.target.value}`); }} /><button aria-label="Next day" onClick={() => moveDate(1)}><ArrowRight size={14} /></button></div></nav>
    <div className={styles.shell}>
      <aside className={styles.watch}><div className={styles.heading}><span>GAME WATCHLIST</span><span>{games.length}</span></div><select aria-label="Filter games" value={filter} onChange={(e) => setFilter(e.target.value)}><option value="all">All games</option><option value="upcoming">Upcoming</option><option value="signals">With signals</option></select>
        {!games.length && <p className={styles.empty}>No games in this view.</p>}
        {games.map((g) => { const t = marketTrail(g.history, "moneyline", "home"); const last = t.points.at(-1); return <button className={styles.game} aria-pressed={game?.id === g.id} key={g.id} onClick={() => { setSelected(g.id); setBook(""); }}><span>{time(g.startsAt)} ET</span><strong>{g.away} <small>@</small> {g.home}</strong><div><span>{g.status}</span><b>{last ? `${last.value.toFixed(1)}% H` : "No quote"}</b></div><small>Game {g.gamePk ?? g.id} · {g.history.length} captures</small></button>; })}
      </aside>
      <main className={styles.instrument}>
        {game ? <><div className={styles.gameHeader}><div><span className={styles.eyebrow}>{game.park ?? "MLB"} · {time(game.startsAt)} ET</span><h1>{game.away} <small>at</small> {game.home}</h1><p>{game.awayStarter ?? "Starter unconfirmed"} / {game.homeStarter ?? "Starter unconfirmed"}</p></div><div className={styles.gameStatus}>{game.status}{game.homeScore != null && game.awayScore != null && <strong>{game.awayScore} – {game.homeScore}</strong>}</div></div>
          <div className={styles.tabs} role="group" aria-label="Market">{(Object.keys(MARKETS) as MlbMarket[]).map((key) => <button key={key} aria-pressed={market === key} onClick={() => chooseMarket(key)}>{MARKETS[key]}</button>)}</div>
          <div className={styles.controls}><div role="group" aria-label="Selection">{(market === "total" ? ["over", "under"] as const : ["away", "home"] as const).map((s) => <button key={s} aria-pressed={side === s} onClick={() => setSide(s)}>{s === "home" ? game.home : s === "away" ? game.away : s}</button>)}</div><select aria-label="Chart bookmaker" value={book} onChange={(e) => setBook(e.target.value)}><option value="">Matched-book consensus</option>{Object.keys(latest?.books ?? {}).filter((key) => key !== "polymarket").sort().map((key) => <option key={key}>{key}</option>)}</select></div>
          <div className={styles.metrics}><div><span>FIRST OBSERVED</span><strong>{formatValue(first?.value)}</strong></div><div><span>LATEST</span><strong>{formatValue(current?.value)}</strong></div><div><span>CHANGE</span><strong>{first && current && trail.points.length >= 2 ? `${signed(current.value - first.value)} ${market === "moneyline" || priceView ? "pp" : "runs"}` : "Awaiting captures"}</strong></div><div><span>COMPARABLE BOOKS</span><strong>{trail.books.length}</strong></div></div>
          {market !== "moneyline" && <div className={styles.controls} role="group" aria-label="Chart measure"><button aria-pressed={!priceView} onClick={() => setPriceMode(false)}>Line history</button><button disabled={!book} aria-pressed={priceView} onClick={() => setPriceMode(true)}>Price at latest line</button></div>}
          <TrailChart points={trail.points} market={market} priceView={priceView} />
          <p className={styles.caption}>Observed pregame {priceView ? "offered-price implied probability at the selected book's latest line" : market === "moneyline" ? "fair probability" : "line"}; {priceView ? "other handicaps/totals are excluded" : "fixed bookmaker set across this chart"}. {book && market !== "moneyline" && first && current ? `Price: ${fmtPrice(first.price)} → ${fmtPrice(current.price)}. ` : ""}Select a book to inspect prices at its exact line.</p>
          <div className={styles.heading}><span>EXACT BOOK QUOTES</span><span>{started ? "LAST PREGAME" : age == null ? "NO CAPTURE" : `${age}m SINCE CAPTURE`}</span></div>
          <div className={styles.tableWrap}><table><thead><tr><th>Book</th><th>Line</th><th>Price</th><th>Fair probability</th><th>Book updated · ET</th><th>Verified close</th></tr></thead><tbody>{Object.entries(latest?.books ?? {}).filter(([key]) => key !== "polymarket").map(([key, value]) => { const q = quote(value, market, side); const close = game.close?.books[key] && quote(game.close.books[key], market, side); return <tr key={key}><td><button className={styles.bookButton} onClick={() => setBook(key)}>{String(value.title ?? key)}</button></td><td>{q?.line ?? "—"}</td><td>{fmtPrice(q?.price)}</td><td>{q?.fair != null ? `${(q.fair * 100).toFixed(1)}%` : "—"}</td><td>{time(q?.updatedAt ?? null)}</td><td>{close ? `${close.line == null ? "" : `${close.line} / `}${fmtPrice(close.price)}` : "Unavailable"}</td></tr>; })}</tbody></table>{!latest && <p className={styles.empty}>No stored pregame quotes for this game.</p>}</div>
        </> : <div className={styles.empty}>Select a date with MLB games to inspect the market.</div>}
      </main>
      <aside className={styles.pulse}><div className={styles.heading}><span>DATA PULSE</span><Activity size={13} /></div><article><label>CAPTURE STATE</label><strong>{started ? "Pregame archive" : age == null ? "No observations" : age > 35 ? "Stale" : "Current"}</strong><p>{latest ? `Last stored ${time(latest.capturedAt)} ET.` : "Awaiting the next accepted sportsbook capture."}</p><p>Movement targets 30 minutes; closing checkpoints target 6h, 90m, 15m and 2m before first pitch.</p></article><article><label>VERIFIED CLOSE</label><strong>{game?.close ? `Quality ${game.closeQuality}` : "Not available"}</strong><p>{game?.close ? `${time(game.close.capturedAt)} ET · ${game.closeBoundary}` : "A latest quote is not labeled a verified closing line."}</p></article>{board.issues.map((issue) => <article key={issue} className={styles.issue}>{issue}</article>)}<div className={styles.heading}><span>SIGNAL TAPE</span><span>{gameSignals.length}</span></div>{!gameSignals.length && <p className={styles.empty}>No recorded {MARKETS[market].toLowerCase()} signals for this game.</p>}{gameSignals.map((signal) => <article key={signal.id}><label>{title(signal.type)}</label><strong>{entry(signal)}</strong><p>{time(String(signal.details.observed_at ?? signal.observedAt))} ET · {String(signal.details.exec_book ?? "No frozen book")}</p><span className={styles.outcome} data-outcome={signalOutcome(signal)}>{signalOutcome(signal)}</span><p>Evidence #{String(signal.details.trigger_history_id ?? signal.id)}</p></article>)}</aside>
    </div>
    <section className={styles.audit}><div className={styles.heading}><span>SIGNAL SCORECARD</span><span>{board.auditFrom} → {board.date}</span></div><p className={styles.caption}>Immutable observations, grouped by signal version. Win rate = W / (W + L). Push = exact line match; void = no action. Pending outcomes stay out of win rate. Units use a one-unit stake at the frozen entry price; unavailable prices are excluded. No validated edge is implied.</p><div className={styles.tableWrap}><table><thead><tr><th>Signal / cohort</th><th>Signals / dates</th><th>W–L–P</th><th>Void</th><th>Pending / other</th><th>Win rate</th><th>Units / priced</th><th>Verified CLV / n</th><th>Beat close</th></tr></thead><tbody>{summary.map((row) => <tr key={`${row.type}:${row.version}:${row.clvUnit}`}><td>{title(row.type)}<small>{row.version}</small></td><td>{row.n} / {row.dates.size}</td><td>{row.wins}–{row.losses}–{row.pushes}</td><td>{row.voids}</td><td>{row.pending} / {row.unavailable}</td><td>{row.wins + row.losses ? `${(row.wins / (row.wins + row.losses) * 100).toFixed(1)}%` : "—"}</td><td>{row.priced ? `${signed(row.units, 2)} / ${row.priced}` : "No price"}</td><td>{row.clv.length ? `${signed(row.clv.reduce((a, b) => a + b, 0) / row.clv.length)} ${row.clvUnit} / ${row.clv.length}` : "Unavailable"}</td><td>{row.clv.length ? `${(row.clv.filter((v) => v > 0).length / row.clv.length * 100).toFixed(0)}%` : "—"}</td></tr>)}</tbody></table>{!summary.length && <p className={styles.empty}>No recorded game-line signals in this 90-day window.</p>}</div><details><summary>Selected-date signal ledger · {todaySignals.length}</summary><div className={styles.tableWrap}><table><thead><tr><th>Observed · ET</th><th>Game</th><th>Signal</th><th>Entry</th><th>Result</th><th>Reason</th></tr></thead><tbody>{todaySignals.map((s) => <tr key={s.id}><td>{time(s.observedAt)}</td><td>{s.matchup}</td><td>{title(s.type)}</td><td>{entry(s)}</td><td>{signalOutcome(s)}</td><td>{String(s.grade.settlement_reason ?? (s.outcome == null ? "Awaiting result" : "Legacy grade"))}</td></tr>)}</tbody></table></div></details></section>
  </div>;
}
