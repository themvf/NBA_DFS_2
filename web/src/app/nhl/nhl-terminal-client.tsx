"use client";

import { Activity, ArrowLeft, ArrowRight, BellRing, BookOpen, Radio, Search, ShieldAlert, TrendingDown, TrendingUp, Zap } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import type { MarketCaptureHealth, NhlTerminalBoard, NhlTerminalRow } from "@/db/queries";
import SportsbookHistory from "@/components/sportsbook-history";
import { comparableTrail } from "@/lib/movement-intelligence";
import { selectedSportsbooks } from "@/lib/sportsbook-policy";
import {
  NHL_MARKET_LABELS, bookFairProbability, buildNhlMarket, marketSnapshot, nhlFreshnessTargetMinutes, pct, sidesFor,
  signed, tapeSeries, type NhlMarketKey, type NhlMetric, type NhlSide,
} from "@/lib/nhl-market";
import styles from "../cfb/cfb-terminal.module.css";
import n from "./nhl-terminal.module.css";

type PaperPosition = { id: string; game: string; market: string; book: string; entry: string; observedAt: string };
type Point = { time: number; value: number };

function fmtEt(value: string | number, compact = false): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", ...(compact ? { hour: "numeric", minute: "2-digit" } : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }) }).format(new Date(value));
}

const plural = (count: number, noun: string) => `${count} ${noun}${count === 1 ? "" : "s"}`;

function restLabel(days: number | null): string {
  if (days == null) return "No prior game on record";
  if (days === 0) return "BACK-TO-BACK";
  return `${days} day${days === 1 ? "" : "s"} rest`;
}

function gameStatus(game: NhlTerminalRow): string {
  if (game.scheduleState !== "OK") return game.scheduleState === "PPD" ? "POSTPONED" : game.scheduleState;
  if (game.completed && game.homeScore != null && game.awayScore != null) {
    const period = game.lastPeriodType && game.lastPeriodType !== "REG" ? ` (${game.lastPeriodType})` : "";
    return `FINAL ${game.awayAbbrev} ${game.awayScore} · ${game.homeAbbrev} ${game.homeScore}${period}`;
  }
  if (game.gameState === "LIVE" || game.gameState === "CRIT") return "IN PROGRESS";
  return game.commenceTime ? fmtEt(game.commenceTime, true) : "TBD";
}

function favoriteLabel(game: NhlTerminalRow): string {
  const home = marketSnapshot(game.currentBooks, "moneyline", "home").probability;
  if (home == null) return "—";
  return home >= 0.5 ? `${game.homeAbbrev} ${pct(home)}` : `${game.awayAbbrev} ${pct(1 - home)}`;
}

function MiniChart({ points, label, percent }: { points: Point[]; label: string; percent: boolean }) {
  const values = points.map((point) => point.value);
  const low = Math.min(...values), high = Math.max(...values);
  const duration = points.length > 1 ? points.at(-1)!.time - points[0].time : 0;
  const coords = points.map((point) => ({
    x: duration ? 3 + (point.time - points[0].time) / duration * 110 : 58,
    y: high === low ? 16 : 29 - (point.value - low) / (high - low) * 26,
  }));
  const change = points.length > 1 ? points.at(-1)!.value - points[0].value : null;
  const changeLabel = change == null ? "—" : percent ? `${signed(change * 100)}pp` : signed(change);
  return <span className={styles.miniChart} title={`${label}: observed lower-median consensus, independently scaled; dashed segments span over 30 minutes.`}>
    <span>{label} <b>{changeLabel}</b></span>
    {!points.length ? <small>No captures</small> : <svg viewBox="0 0 116 32" role="img" aria-label={`${label}: ${plural(points.length, "capture")}, change ${changeLabel}`}>
      {coords.slice(1).map((point, index) => <line key={index} x1={coords[index].x} y1={coords[index].y} x2={point.x} y2={point.y} stroke="currentColor" strokeWidth="1.5" strokeDasharray={points[index + 1].time - points[index].time > 30 * 60_000 ? "3 3" : undefined} />)}
      {coords.map((point, index) => <circle key={index} cx={point.x} cy={point.y} r="1.8" fill="currentColor" />)}
    </svg>}
  </span>;
}

function WatchGame({ game, active, asOf, onChoose }: { game: NhlTerminalRow; active: boolean; asOf: string; onChoose: () => void }) {
  const moneyline = tapeSeries(game.history, game.commenceTime, "moneyline");
  const total = tapeSeries(game.history, game.commenceTime, "total");
  const move = moneyline.length > 1 ? moneyline.at(-1)!.value - moneyline[0].value : null;
  const Trend = move != null && move > 0 ? TrendingUp : move != null && move < 0 ? TrendingDown : Activity;
  const lead = game.commenceTime ? (Date.parse(game.commenceTime) - Date.parse(asOf)) / 60_000 : NaN;
  const target = Number.isFinite(lead) && lead > 0 ? nhlFreshnessTargetMinutes(lead) : null;
  const stale = game.latestCapturedAt != null && target != null && (Date.parse(asOf) - Date.parse(game.latestCapturedAt)) / 60_000 > target;
  const b2b = [game.awayRestDays === 0 ? game.awayAbbrev : null, game.homeRestDays === 0 ? game.homeAbbrev : null].filter(Boolean);
  return <button type="button" className={styles.watchRow} data-active={active} onClick={onChoose} aria-pressed={active}>
    <span className={styles.watchGame}><strong title={`${game.awayTeam} @ ${game.homeTeam}`}>{game.awayAbbrev} @ {game.homeAbbrev}</strong><small>{gameStatus(game)} · {plural(game.captures, "capture")}</small></span>
    <span className={styles.watchLine}>{favoriteLabel(game)}</span>
    <span className={move != null && move > 0 ? styles.positive : move != null && move < 0 ? styles.negative : styles.neutral} title={`${game.homeAbbrev} vig-free win probability, first capture to latest`}><Trend aria-hidden="true" /> {move == null ? "—" : `${Math.abs(move * 100).toFixed(1)}`}</span>
    <span className={styles.miniCharts}><MiniChart points={moneyline} label={`${game.homeAbbrev} ML %`} percent /><MiniChart points={total} label="TOTAL" percent={false} /></span>
    <span className={styles.watchHealth}>{game.latestCapturedAt ? `OBS ${fmtEt(game.latestCapturedAt)}` : "NEVER CAPTURED"}{stale && !game.completed ? " · STALE" : ""}{game.closeQuality ? ` · CLOSE ${game.closeQuality.toUpperCase()}` : ""}{b2b.length ? <span className={n.b2b}> · B2B {b2b.join(", ")}</span> : null}</span>
  </button>;
}

type Mover = { matchupId: number; fixture: string; homeAbbrev: string; first: Point; last: Point; books: number; totalFrom: number | null; totalTo: number | null };

/** Descriptive open-to-latest moves on a stable book cohort. Not a detector. */
function buildMovers(games: NhlTerminalRow[], now: number): Mover[] {
  return games.flatMap((game) => {
    const start = Date.parse(game.commenceTime ?? "");
    if (game.completed || !Number.isFinite(start) || start <= now) return [];
    const captures = game.history.map((capture) => ({
      time: Date.parse(capture.capturedAt),
      books: Object.fromEntries(Object.entries(selectedSportsbooks(capture.books)).flatMap(([key, book]) => {
        const value = bookFairProbability(book, "moneyline", "home");
        return value == null ? [] : [[key, value]];
      })),
    }));
    const trail = comparableTrail(captures, now, start, 14 * 24 * 60 * 60_000);
    if (trail.length < 2) return [];
    const ordered = captures.filter((c) => c.time <= now && c.time < start).sort((a, b) => a.time - b.time);
    const books = Object.keys(ordered[0]?.books ?? {}).filter((key) => key !== "pinnacle" && ordered.every((c) => c.books[key] != null)).length;
    const total = tapeSeries(game.history, game.commenceTime, "total");
    return [{
      matchupId: game.matchupId, fixture: `${game.awayAbbrev} @ ${game.homeAbbrev}`, homeAbbrev: game.homeAbbrev,
      first: trail[0], last: trail.at(-1)!, books,
      totalFrom: total[0]?.value ?? null, totalTo: total.at(-1)?.value ?? null,
    }];
  }).sort((a, b) => Math.abs(b.last.value - b.first.value) - Math.abs(a.last.value - a.first.value));
}

function Movers({ movers, selected, onSelect }: { movers: Mover[]; selected: number | null; onSelect: (id: number) => void }) {
  const [expanded, setExpanded] = useState(false);
  const shown = expanded ? movers : movers.slice(0, 4);
  return <section className={n.movers} aria-label="Largest moves since first capture">
    <div className={n.moversHead}><h2>LARGEST MOVES SINCE FIRST CAPTURE</h2><span>{movers.length} UPCOMING GAME{movers.length === 1 ? "" : "S"} WITH A TRAIL</span>
      {movers.length > 4 ? <button type="button" aria-expanded={expanded} onClick={() => setExpanded((value) => !value)}>{expanded ? "SHOW TOP 4" : `VIEW ALL · ${movers.length}`}</button> : null}</div>
    <p className={n.moversPolicy}>Moneyline on a stable book cohort (books quoted at every capture; Pinnacle excluded) · descriptive, not a detector signal or a pick</p>
    {shown.length ? <div className={n.moverCards}>{shown.map((mover) => {
      const move = (mover.last.value - mover.first.value) * 100;
      return <button key={mover.matchupId} type="button" className={n.moverCard} aria-pressed={mover.matchupId === selected} onClick={() => onSelect(mover.matchupId)}>
        <strong>{mover.fixture}</strong>
        <span>{mover.homeAbbrev} ML {pct(mover.first.value)} → {pct(mover.last.value)} <b>{signed(move)}pp</b></span>
        <span>Total {mover.totalFrom == null ? "—" : mover.totalFrom.toFixed(1)} → {mover.totalTo == null ? "—" : mover.totalTo.toFixed(1)}</span>
        <small>{plural(mover.books, "book")} · since {fmtEt(mover.first.time, true)}</small>
      </button>;
    })}</div> : <p className={n.moversEmpty}>No upcoming game has two comparable captures yet.</p>}
  </section>;
}

export default function NhlTerminalClient({ board, captureHealth }: { board: NhlTerminalBoard; captureHealth: MarketCaptureHealth | null }) {
  const router = useRouter();
  function goToDate(next: string) { if (next) router.push(`/nhl?date=${next}`); }
  function shiftDate(delta: number) {
    const next = new Date(`${board.gameDate}T12:00:00Z`);
    next.setUTCDate(next.getUTCDate() + delta);
    goToDate(next.toISOString().slice(0, 10));
  }
  const [gameId, setGameId] = useState(board.games[0]?.matchupId ?? 0);
  const [marketKey, setMarketKey] = useState<NhlMarketKey>("moneyline");
  const [side, setSide] = useState<NhlSide>("home");
  const [metric, setMetric] = useState<NhlMetric>("price");
  const [query, setQuery] = useState(""); const [selectedBook, setSelectedBook] = useState("");
  const [observedNow, setObservedNow] = useState(Date.parse(board.asOf));
  const [positions, setPositions] = useState<PaperPosition[]>([]); const [lockMessage, setLockMessage] = useState<string | null>(null);
  useEffect(() => { const timer = window.setInterval(() => { setObservedNow(Date.now()); router.refresh(); }, 60_000); return () => window.clearInterval(timer); }, [router]);
  const game = board.games.find((item) => item.matchupId === gameId) ?? board.games[0] ?? null;
  const view = useMemo(() => game ? buildNhlMarket(game, marketKey, side, metric, board.asOf) : null, [game, marketKey, side, metric, board.asOf]);
  const quote = view?.books.find((item) => item.key === selectedBook) ?? view?.books[0] ?? null;
  const filteredGames = useMemo(() => { const normalized = query.trim().toLowerCase(); return board.games.filter((item) => `${item.awayTeam} ${item.homeTeam} ${item.awayAbbrev} ${item.homeAbbrev} ${item.networks ?? ""}`.toLowerCase().includes(normalized)); }, [board.games, query]);
  const movers = useMemo(() => buildMovers(filteredGames, Math.max(observedNow, Date.parse(board.asOf))), [filteredGames, observedNow, board.asOf]);
  function chooseGame(id: number) { setGameId(id); setSelectedBook(""); setLockMessage(null); }
  function chooseMarket(next: NhlMarketKey) { setMarketKey(next); setSide(sidesFor(next)[0]); setMetric("price"); setSelectedBook(""); setLockMessage(null); }
  function chooseSide(next: NhlSide) { setSide(next); setSelectedBook(""); setLockMessage(null); }
  function addPaperPosition() {
    if (!game || !quote || !quote.fresh) return;
    setPositions((current) => [{ id: `${game.matchupId}-${marketKey}-${quote.key}-${Date.now()}`, game: `${game.awayAbbrev} @ ${game.homeAbbrev}`, market: `${NHL_MARKET_LABELS[marketKey]} · ${side.toUpperCase()}`, book: quote.book, entry: `${quote.line} ${quote.price}`, observedAt: quote.updatedAt ?? board.asOf }, ...current]);
    setLockMessage(`Recorded paper position at ${quote.book}; this did not place a wager.`);
  }
  const statusLabel = board.status.toUpperCase();
  const healthy = board.status === "live" || board.status === "scheduled" || board.status === "final";
  return <div className={styles.terminal}>
    <header className={styles.topbar}><div className={styles.brand}>NHL LINE TERMINAL</div><label className={styles.command}><Search aria-hidden="true" /><span className={styles.srOnly}>Search market watch</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="SEARCH TEAM OR GAME" /></label><div className={styles.marketOpen}><Radio aria-hidden="true" /> {board.games.length ? "MARKET BOARD" : "NO BOARD"}</div><div className={styles.shadowMode} title={board.statusDetail}>{statusLabel} · AS OF {fmtEt(board.asOf, true)}</div></header>
    <nav className={styles.nav} aria-label="NHL board date">
      <span>SLATE DATE</span>
      <div className={styles.date}>
        <button type="button" aria-label="Previous day" onClick={() => shiftDate(-1)}><ArrowLeft size={14} aria-hidden="true" /></button>
        <input aria-label="NHL game date" type="date" value={board.gameDate} onChange={(event) => goToDate(event.target.value)} />
        <button type="button" aria-label="Next day" onClick={() => shiftDate(1)}><ArrowRight size={14} aria-hidden="true" /></button>
      </div>
      <span className={styles.navCount}>{board.games.length} SCHEDULED</span>
    </nav>
    <Movers movers={movers} selected={game?.matchupId ?? null} onSelect={(id) => { chooseGame(id); chooseMarket("moneyline"); }} />
    <section className={styles.watchPane} aria-label="NHL market watch"><div className={styles.sectionTitle}><span>MARKET WATCH</span><span>{board.gameDate}</span></div>
      <p className={styles.watchLegend}>Favorite = vig-free moneyline consensus. Arrow = home win probability, first capture to latest (pp). Charts show observed consensus; dashed gaps exceed 30m. B2B = back-to-back.</p>
      <div className={styles.watchList}>
        {filteredGames.map((item) => <WatchGame key={item.matchupId} game={item} active={item.matchupId === game?.matchupId} asOf={board.asOf} onChoose={() => chooseGame(item.matchupId)} />)}
        {!filteredGames.length ? <div className={styles.empty}>{board.games.length ? "No games match this search." : board.statusDetail}</div> : null}
      </div></section>
    <div className={styles.shell}>
      <main className={styles.instrumentPane}>{!game || !view ? <section className={styles.chartSection}><div className={styles.empty}>No NHL games are loaded for this date. No sample quotes are substituted.</div></section> : <>
        <section className={styles.instrumentHeader}>
          <div className={styles.instrumentTop}><div><div className={styles.instrumentTitle}>{game.awayTeam} @ {game.homeTeam}</div><div className={styles.instrumentMeta}>{game.venue ?? "Venue TBD"}{game.neutralSite ? " (neutral site)" : ""} · {game.commenceTime ? fmtEt(game.commenceTime) : "Start TBD"} · {game.networks ?? "Broadcast TBD"} · {game.gameType === 3 ? "Playoffs" : "Regular season"}</div></div><div className={styles.primaryQuote}><strong>{view.currentLabel}</strong><span>OPEN {view.openLabel} · {view.move.toUpperCase()} · CLOSE {view.closeLabel}</span></div></div>
          <div className={styles.marketTabs}>{(Object.keys(NHL_MARKET_LABELS) as NhlMarketKey[]).map((key) => <button key={key} type="button" data-active={marketKey === key} onClick={() => chooseMarket(key)}>{NHL_MARKET_LABELS[key]}</button>)}</div>
          <div className={styles.marketTabs}>{sidesFor(marketKey).map((option) => <button key={option} type="button" data-active={side === option} onClick={() => chooseSide(option)}>{option === "home" ? game.homeTeam : option === "away" ? game.awayTeam : option.toUpperCase()}</button>)}</div>
          {marketKey !== "moneyline" ? <div className={`${styles.marketTabs} ${n.metricTabs}`} aria-label="Chart metric">{(["price", "line"] as NhlMetric[]).map((option) => <button key={option} type="button" data-active={metric === option} onClick={() => setMetric(option)}>{option === "price" ? "FAIR PRICE AT CONSENSUS LINE" : "LINE"}</button>)}</div> : null}
        </section>
        <section className={styles.chartSection}><div className={styles.chartLabelRow}><span>{view.axisLabel}</span><span>{game.latestCapturedAt ? `observed ${fmtEt(game.latestCapturedAt)} · ${plural(game.captures, "capture")}` : "scheduled · never captured"}</span></div><div className={styles.chartWrap}><SportsbookHistory key={`${game.matchupId}:${marketKey}:${side}:${metric}`} label={view.axisLabel} percentage={view.percentage} points={view.history} /></div></section>
        <section className={styles.lowerGrid}>
          <div className={styles.ladderPane}><div className={styles.sectionTitle}><span>EXACT BOOK QUOTES</span><span>OBSERVED QUOTES</span></div>
            <div className={`${styles.bookHeader} ${n.bookGrid}`}><span>BOOK</span><span>UPDATED</span><span>LINE</span><span>PRICE</span><span>FAIR</span></div>
            {view.books.map((item) => <button key={item.key} type="button" className={`${styles.bookRow} ${n.bookGrid} ${item.atConsensus ? "" : n.offLine}`} title={item.atConsensus ? undefined : "Different line from consensus: not the same proposition"} data-selected={quote?.key === item.key} onClick={() => { setSelectedBook(item.key); setLockMessage(null); }}><span>{item.book}{!item.fresh ? <em>STALE</em> : null}</span><span>{item.updatedAt ? fmtEt(item.updatedAt, true) : "—"}</span><span>{item.line}</span><span>{item.price}</span><span>{item.fair}</span></button>)}
            {!view.books.length ? <div className={styles.empty}>This market or side is not quoted by the captured books.</div> : null}
            <div className={styles.paperAction}><button type="button" disabled={!quote?.fresh} onClick={addPaperPosition}><BookOpen aria-hidden="true" /> {quote?.fresh ? `RECORD PAPER ${quote.book.toUpperCase()} ${quote.line} ${quote.price}` : "PAPER ENTRY DISABLED · QUOTE NOT ≤5M FRESH"}</button><div aria-live="polite">{lockMessage ?? "Displayed quotes are observations, not verified execution availability. FAIR = vig removed from that book's own pair."}</div></div>
          </div>
          <div className={styles.catalystPane}><div className={styles.sectionTitle}><span>MARKET QUALITY</span><span>AUDIT</span></div>
            <div className={styles.catalystRow}><span>NOW</span><strong>SUPPORT</strong><p>{marketKey === "moneyline" ? `${view.current.lineBooks} books with a complete moneyline pair` : `${view.current.lineBooks} books at the consensus line · ${view.current.marketBooks} quoting this market`}</p></div>
            <div className={styles.catalystRow}><span>OPEN</span><strong>HISTORY</strong><p>{plural(game.captures, "accepted pregame capture")}{game.openingCapturedAt ? ` since ${fmtEt(game.openingCapturedAt)}` : ""}; post-start rows excluded</p></div>
            <div className={styles.catalystRow}><span>CLOSE</span><strong>{game.closeQuality ? `GRADE ${game.closeQuality}` : "PENDING"}</strong><p>{game.closingCapturedAt ? `${view.closeMove}; ${Math.round((game.closeLeadSeconds ?? 0) / 60)}m before ${game.closeBoundarySource ?? "scheduled start"}` : "Frozen after the scheduled NHL start; no latest-row proxy."}</p></div>
            <div className={styles.catalystRow}><span>MAP</span><strong>IDENTITY</strong><p>NHL game {game.nhlGameId} · Odds event {game.oddsEventId ?? "not mapped yet"}</p></div>
          </div>
        </section>
        <section className={styles.researchPane}><div className={styles.sectionTitle}><span>GAME CONTEXT</span><span>SCHEDULE-DERIVED · NOT A MODEL INPUT</span></div>
          <div className={n.contextGrid}>
            <div><span>{game.awayAbbrev} REST</span><strong className={game.awayRestDays === 0 ? n.b2b : undefined}>{restLabel(game.awayRestDays)}</strong><small>Days off since the previous game on record</small></div>
            <div><span>{game.homeAbbrev} REST</span><strong className={game.homeRestDays === 0 ? n.b2b : undefined}>{restLabel(game.homeRestDays)}</strong><small>Preseason games are not loaded</small></div>
            <div><span>STATUS</span><strong className={game.completed ? n.final : undefined}>{gameStatus(game)}</strong><small>Finals credit a shootout win as one goal, as books settle</small></div>
            <div><span>NOT CAPTURED YET</span><strong>Starting goalies · injuries</strong><small>The biggest hockey line movers; no approved feed yet</small></div>
          </div>
        </section>
        <section className={styles.blotter}><div className={styles.sectionTitle}><span>SESSION PAPER BLOTTER</span><span>{positions.length} OPEN</span></div>{!positions.length ? <div className={styles.blotterEmpty}>A paper position can be recorded only from an observation no more than five minutes old.</div> : <div className={styles.blotterTableWrap}><table><thead><tr><th>Game</th><th>Market</th><th>Book</th><th>Entry</th><th>Observed</th></tr></thead><tbody>{positions.map((position) => <tr key={position.id}><td>{position.game}</td><td>{position.market}</td><td>{position.book}</td><td>{position.entry}</td><td>{fmtEt(position.observedAt)}</td></tr>)}</tbody></table></div>}</section>
        <section className={styles.researchPane}><div className={styles.sectionTitle}><span>MOVEMENT DETECTORS</span><span>NOT ENABLED FOR NHL</span></div><p className={styles.researchDisclosure}>The CFB detectors are tuned to football point moves (spread 1.0, total 1.5, key numbers 3/7/10/14). Hockey moves are price-first: the puck line is fixed at ±1.5 and totals sit at 5.5–6.5. NHL detectors need hockey-specific, pre-registered thresholds before any signal is recorded, so none are shown here. The quote tape above is collected either way.</p></section>
      </>}</main>
      <aside className={styles.pulsePane}><div className={styles.sectionTitle}><span>DATA PULSE</span><span>{statusLabel}</span></div>
        <article className={styles.pulseRow} data-tone={healthy ? "market" : "critical"}><div><span>{fmtEt(board.asOf, true)}</span><strong>{healthy ? <Zap aria-hidden="true" /> : <ShieldAlert aria-hidden="true" />} FEED STATE</strong></div><h3>{statusLabel}</h3><p>{board.statusDetail}</p></article>
        {captureHealth ? <article className={styles.pulseRow} data-tone={captureHealth.status === "partial" ? "critical" : "market"}><div><span>{captureHealth.eventsCovered} EVENTS</span><strong><Activity aria-hidden="true" /> CHECKPOINTS</strong></div><h3>{captureHealth.due ? `${captureHealth.dueCaptured}/${captureHealth.due} due captured` : "No checkpoints due"}</h3><p>{captureHealth.missed} missed · {captureHealth.failed} failed · {captureHealth.pending} scheduled ahead</p></article> : null}
        {game ? <><article className={styles.pulseRow} data-tone="market"><div><span>{game.latestCapturedAt ? fmtEt(game.latestCapturedAt, true) : "—"}</span><strong><Activity aria-hidden="true" /> CAPTURE</strong></div><h3>{plural(game.captures, "observation")}</h3><p>Every chart point comes from the append-only exact-book ledger.</p></article>
          <div className={styles.sectionTitle}><span>CROSS-MARKET</span><span>RELATED</span></div>
          {(Object.keys(NHL_MARKET_LABELS) as NhlMarketKey[]).map((key) => { const related = buildNhlMarket(game, key, sidesFor(key)[0], "price", board.asOf); return <div key={key} className={styles.relatedRow}><span>{NHL_MARKET_LABELS[key]}</span><strong>{related.currentLabel}</strong><small>{related.move}</small></div>; })}</> : null}
        <div className={styles.disclosure}><BellRing aria-hidden="true" /><div><strong>Research terminal</strong><p>Quotes are observations, not recommendations. No predictive edge or real-money execution is represented.</p></div></div>
      </aside>
    </div>
    <footer className={styles.ticker}><span><strong>STATUS</strong> {board.statusDetail}</span><span><strong>BOARD</strong> {board.games.length} GAMES</span><span><strong>QUARANTINE</strong> {board.unmappedEvents} UNMAPPED EVENTS</span><span><strong>CONSENSUS</strong> LOWER MEDIAN · SAME-LINE FAIR PRICE</span><span><strong>PAPER</strong> FIVE-MINUTE FRESHNESS REQUIRED</span></footer>
  </div>;
}
