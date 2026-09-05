"use client";

import { Activity, BellRing, BookOpen, Radio, Search, ShieldAlert, TrendingDown, TrendingUp, Zap } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import type { CfbBookQuote, CfbResearchBoard, CfbResearchContext, CfbResearchRecord, CfbSignalBacktestRow, CfbTeamFeatureContext, CfbTerminalBoard, CfbTerminalRow, LineAlertRow, MarketCaptureHealth, MarketSignalScorecardRow } from "@/db/queries";
import MarketSignalScorecard from "@/components/market-signal-scorecard";
import styles from "./cfb-terminal.module.css";
import { movementKind, movementSeries, movementSignals } from "@/lib/cfb-movement";

type MarketKey = "spread" | "total" | "moneyline";
type SelectionSide = "home" | "away" | "over" | "under";
type HistoryPoint = { capturedAt: string; time: string; values: Record<string, number> };
type BookRow = { key: string; book: string; line: string; price: string; side: SelectionSide; updatedAt: string | null; fresh: boolean };
type MarketView = { label: string; current: string; open: string; close: string; move: string; closeMove: string; axisLabel: string; series: string[]; history: HistoryPoint[]; books: BookRow[]; selectedLineBookCount: number; marketBookCount: number };
type PaperPosition = { id: string; game: string; market: string; book: string; entry: string; observedAt: string };

const MARKET_LABELS: Record<MarketKey, string> = { spread: "SPREAD", total: "TOTAL", moneyline: "MONEYLINE" };
const SERIES_COLORS = ["#f6a800", "#59b6ff", "#c58cff", "#5fd0a5", "#ff718b"];
const BOOK_PRIORITY = ["pinnacle", "draftkings", "fanduel", "betmgm", "bovada"];

const SIGNAL_LABELS: Record<string, string> = {
  spread_steam: "SPREAD STEAM", total_steam: "TOTAL STEAM",
  spread_walking: "SPREAD WALK", total_walking: "TOTAL WALK",
  key_cross: "KEY CROSS", price_pressure: "PRICE PRESSURE",
  reversal: "REVERSAL", reference_led: "REFERENCE LED",
  pinnacle_divergence: "REFERENCE GAP", dk_value: "PRICE VALUE",
  steam: "ML STEAM", walking: "ML WALK",
  book_disagreement: "BOOK DISAGREEMENT", market_convergence: "CONVERGENCE",
  late_move: "LATE MOVE", favorite_flip: "FAVORITE FLIP",
};

function signalMarket(signal: LineAlertRow): MarketKey {
  const market = signal.details?.market;
  if (market === "spread" || market === "total" || market === "moneyline") return market;
  if (signal.alertType.startsWith("spread") || signal.alertType === "key_cross") return "spread";
  if (signal.alertType.startsWith("total")) return "total";
  return "moneyline";
}

function signalObservedAt(signal: LineAlertRow): string {
  const observedAt = signal.details?.trigger_capture_at;
  return typeof observedAt === "string" && observedAt ? observedAt : signal.createdAt;
}

function pct(value: number | null): string { return value == null ? "—" : `${(value * 100).toFixed(1)}%`; }

function signed(value: number, digits = 1): string { return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`; }
function american(value: number | null | undefined): string { return value == null ? "—" : `${value > 0 ? "+" : ""}${Math.round(value)}`; }
function lowerMedian(values: number[]): number | null {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  return ordered[Math.floor((ordered.length - 1) / 2)];
}
function probability(price: number): number { return price > 0 ? 100 / (price + 100) : Math.abs(price) / (Math.abs(price) + 100); }
function fairHome(book: CfbBookQuote): number | null {
  if (book.ml_home == null || book.ml_away == null) return null;
  const home = probability(Number(book.ml_home)); const away = probability(Number(book.ml_away));
  return home + away > 0 ? home / (home + away) : null;
}
function valueFor(book: CfbBookQuote, market: MarketKey): number | null {
  if (market === "spread") return book.spread_home == null ? null : Number(book.spread_home);
  if (market === "total") return book.total_line == null ? null : Number(book.total_line);
  return fairHome(book);
}
function bookTitle(key: string, quote?: CfbBookQuote): string {
  if (quote?.title) return quote.title;
  return key.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
function fmtEt(value: string, compact = false): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", ...(compact ? { hour: "numeric", minute: "2-digit" } : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }) }).format(new Date(value));
}
function quoteFresh(updatedAt: string | null, capturedAt: string | null, asOf: string): boolean {
  if (!updatedAt || !capturedAt) return false;
  const now = new Date(asOf).getTime();
  return now - new Date(updatedAt).getTime() <= 5 * 60_000 && now - new Date(capturedAt).getTime() <= 5 * 60_000;
}
function selectionFor(market: MarketKey): SelectionSide { return market === "total" ? "over" : "home"; }

function buildBookRows(game: CfbTerminalRow, market: MarketKey, side: SelectionSide, asOf: string): BookRow[] {
  return Object.entries(game.currentBooks ?? {}).flatMap(([key, quote]) => {
    let line: string; let price: number | null | undefined;
    if (market === "spread") {
      const point = side === "away" ? quote.spread_away : quote.spread_home;
      price = side === "away" ? quote.spread_away_price : quote.spread_home_price;
      if (point == null || price == null) return [];
      line = `${side === "home" ? game.homeTeam : game.awayTeam} ${signed(Number(point))}`;
    } else if (market === "total") {
      if (quote.total_line == null) return [];
      price = side === "under" ? quote.under : quote.over;
      if (price == null) return [];
      line = `${side === "under" ? "UNDER" : "OVER"} ${Number(quote.total_line).toFixed(1)}`;
    } else {
      price = side === "away" ? quote.ml_away : quote.ml_home;
      if (price == null) return [];
      line = `${side === "home" ? game.homeTeam : game.awayTeam} ML`;
    }
    const updatedAt = quote.last_update ? String(quote.last_update) : null;
    return [{ key, book: bookTitle(key, quote), line, price: american(Number(price)), side, updatedAt, fresh: quoteFresh(updatedAt, game.latestCapturedAt, asOf) }];
  }).sort((a, b) => {
    const ai = BOOK_PRIORITY.indexOf(a.key); const bi = BOOK_PRIORITY.indexOf(b.key);
    return (ai < 0 ? 99 : ai) - (bi < 0 ? 99 : bi) || a.book.localeCompare(b.book);
  });
}

function buildMarket(game: CfbTerminalRow, market: MarketKey, side: SelectionSide, asOf: string): MarketView {
  const currentValues = Object.values(game.currentBooks ?? {}).flatMap((book) => { const value = valueFor(book, market); return value == null ? [] : [value]; });
  const openingValues = Object.values(game.openingBooks ?? {}).flatMap((book) => { const value = valueFor(book, market); return value == null ? [] : [value]; });
  const closingValues = Object.values(game.closingBooks ?? {}).flatMap((book) => { const value = valueFor(book, market); return value == null ? [] : [value]; });
  const current = lowerMedian(currentValues); const opening = lowerMedian(openingValues);
  const closing = lowerMedian(closingValues);
  const bookKeys = Array.from(new Set(game.history.flatMap((point) => Object.keys(point.books))));
  const orderedBooks = [...BOOK_PRIORITY.filter((book) => bookKeys.includes(book)), ...bookKeys.filter((book) => !BOOK_PRIORITY.includes(book)).sort()].slice(0, 3);
  const history = game.history.map((point) => ({ capturedAt: point.capturedAt, time: fmtEt(point.capturedAt, true), values: Object.fromEntries(orderedBooks.flatMap((key) => { const value = valueFor(point.books[key] ?? {}, market); return value == null ? [] : [[key, value]]; })) }));
  const movement = current != null && opening != null ? current - opening : null;
  let currentLabel = "NO MARKET"; let openingLabel = "—"; let closingLabel = game.closeQuality && !game.closingBooks ? "UNAVAILABLE" : "PENDING";
  if (current != null) currentLabel = market === "spread" ? `${game.homeTeam} ${signed(current)}` : market === "total" ? current.toFixed(1) : `${game.homeTeam} ${(current * 100).toFixed(1)}%`;
  if (opening != null) openingLabel = market === "spread" ? signed(opening) : market === "total" ? opening.toFixed(1) : `${(opening * 100).toFixed(1)}%`;
  if (closing != null) closingLabel = market === "spread" ? signed(closing) : market === "total" ? closing.toFixed(1) : `${(closing * 100).toFixed(1)}%`;
  const closingMovement = closing != null && opening != null ? closing - opening : null;
  const selectedLineBookCount = current == null ? 0 : currentValues.filter((value) => value === current).length;
  return {
    label: `Full-game ${market}`, current: currentLabel, open: openingLabel, close: closingLabel,
    move: movement == null ? "Awaiting two captures" : market === "moneyline" ? `${signed(movement * 100)}pp` : `${signed(movement)} pts`,
    closeMove: closingMovement == null ? "CLV close pending" : market === "moneyline" ? `${signed(closingMovement * 100)}pp open→close` : `${signed(closingMovement)} pts open→close`,
    axisLabel: market === "moneyline" ? `Vig-free ${game.homeTeam} probability` : market === "spread" ? `${game.homeTeam} spread` : "Game total",
    series: orderedBooks, history, books: buildBookRows(game, market, side, asOf), selectedLineBookCount, marketBookCount: currentValues.length,
  };
}

function displayTick(value: number, market: MarketKey): string { return market === "moneyline" ? `${(value * 100).toFixed(0)}%` : signed(value); }

function MarketChart({ market, marketKey, signals }: { market: MarketView; marketKey: MarketKey; signals: LineAlertRow[] }) {
  const width = 760; const height = 300; const frame = { left: 62, right: 738, top: 26, bottom: 250 };
  const values = market.history.flatMap((point) => Object.values(point.values));
  if (!values.length) return <div className={styles.empty}>No eligible pregame history for this market yet.</div>;
  const rawMin = Math.min(...values); const rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.25, marketKey === "moneyline" ? 0.008 : 0.4);
  const min = rawMin - padding; const max = rawMax + padding;
  const x = (index: number) => frame.left + (index / Math.max(market.history.length - 1, 1)) * (frame.right - frame.left);
  const y = (value: number) => frame.top + ((max - value) / Math.max(max - min, 0.001)) * (frame.bottom - frame.top);
  const ticks = Array.from({ length: 5 }, (_, index) => max - (index / 4) * (max - min));
  return <svg className={styles.marketChart} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${market.axisLabel} movement`}><title>{market.axisLabel}</title>
    {ticks.map((tick) => <g key={tick}><line x1={frame.left} x2={frame.right} y1={y(tick)} y2={y(tick)} className={styles.chartGrid} /><text x={frame.left - 9} y={y(tick) + 4} textAnchor="end">{displayTick(tick, marketKey)}</text></g>)}
    <line x1={frame.left} x2={frame.right} y1={frame.bottom} y2={frame.bottom} className={styles.chartAxis} /><line x1={frame.left} x2={frame.left} y1={frame.top} y2={frame.bottom} className={styles.chartAxis} />
    {market.history.map((point, index) => <text key={`${point.time}-${index}`} x={x(index)} y={frame.bottom + 24} textAnchor={index === 0 ? "start" : index === market.history.length - 1 ? "end" : "middle"} className={styles.chartTime}>{point.time}</text>)}
    {market.series.map((series, seriesIndex) => market.history.slice(1).map((point, index) => { const before = market.history[index].values[series]; const after = point.values[series]; return before == null || after == null ? null : <line key={`${series}-${index}`} x1={x(index)} y1={y(before)} x2={x(index + 1)} y2={y(after)} stroke={SERIES_COLORS[seriesIndex]} strokeWidth={seriesIndex === 0 ? 2.6 : 1.9} vectorEffect="non-scaling-stroke" />; }))}
    {market.series.map((series, seriesIndex) => market.history.map((point, index) => { const value = point.values[series]; return value == null ? null : <circle key={`${series}-point-${index}`} cx={x(index)} cy={y(value)} r={seriesIndex === 0 ? 4.5 : 3.5} fill={SERIES_COLORS[seriesIndex]} stroke="#07100e" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />; }))}
    {signals.map((signal, signalIndex) => { const observedAt = signalObservedAt(signal); const at = new Date(observedAt).getTime(); const nearest = market.history.reduce((best, point, index) => Math.abs(new Date(point.capturedAt).getTime() - at) < Math.abs(new Date(market.history[best].capturedAt).getTime() - at) ? index : best, 0); const sx = x(nearest); return <g key={`${signal.alertType}-${observedAt}`}><line x1={sx} x2={sx} y1={frame.top} y2={frame.bottom} stroke={signal.alertType === "reversal" ? "#ff6464" : "#f6a800"} strokeDasharray="4 4" opacity="0.85" /><text x={Math.min(sx + 4, frame.right - 90)} y={frame.top + 11 + signalIndex * 11} className={styles.signalChartLabel}>{SIGNAL_LABELS[signal.alertType] ?? signal.alertType.toUpperCase()}</text></g>; })}
    {market.series.map((series, index) => <g key={series} transform={`translate(${frame.left + index * 150}, 291)`}><line x1="0" x2="18" y1="-4" y2="-4" stroke={SERIES_COLORS[index]} strokeWidth={index === 0 ? 2.6 : 1.9} /><text x="24" y="0" className={styles.legendLabel}>{bookTitle(series)}</text></g>)}
  </svg>;
}

function signalMagnitude(signal: LineAlertRow): string | null {
  for (const key of ["move_pp", "avg_move_pp", "price_move_pp", "reference_move_pp", "consensus_move_pp", "first_leg_pp", "move", "movement", "first_leg"]) {
    const value = signal.details?.[key];
    if (typeof value === "number" && Number.isFinite(value)) return `${value > 0 ? "+" : ""}${value.toFixed(1)}${key.includes("pp") ? "pp" : " pts"}`;
  }
  return null;
}

function recordLabel(record: CfbResearchRecord): string {
  return record.rate == null ? "—" : `${(record.rate * 100).toFixed(1)}%`;
}

function intervalLabel(record: CfbResearchRecord): string {
  return record.ciLow == null || record.ciHigh == null
    ? "interval unavailable"
    : `95% CI ${(record.ciLow * 100).toFixed(1)}–${(record.ciHigh * 100).toFixed(1)}%`;
}

function HistoricalRecord({ label, record }: { label: string; record: CfbResearchRecord }) {
  return <div className={styles.historyMetric}>
    <span>{label}</span>
    <strong>{recordLabel(record)}</strong>
    <small>{record.wins}-{record.losses}-{record.pushes} · n={record.n}</small>
    <small>{intervalLabel(record)}</small>
  </div>;
}

function featureNumber(features: Record<string, unknown>, section: string, key: string): number | null {
  const group = features[section];
  if (!group || typeof group !== "object" || Array.isArray(group)) return null;
  const value = Number((group as Record<string, unknown>)[key]);
  return Number.isFinite(value) ? value : null;
}

function TeamFeatureCard({ feature }: { feature: CfbTeamFeatureContext | null }) {
  if (!feature) return <div className={styles.historyEmpty}>No pre-kickoff feature snapshot yet.</div>;
  const margin = featureNumber(feature.features, "blended", "margin");
  const pointsFor = featureNumber(feature.features, "blended", "points_for");
  const roster = feature.features.roster && typeof feature.features.roster === "object" && !Array.isArray(feature.features.roster)
    ? feature.features.roster as Record<string, unknown> : null;
  const continuity = roster && Number.isFinite(Number(roster.roster_continuity_pct)) ? Number(roster.roster_continuity_pct) : null;
  const returningPpa = roster?.returning_production && typeof roster.returning_production === "object"
    ? Number((roster.returning_production as Record<string, unknown>).percentPPA) : NaN;
  return <article className={styles.featureCard}>
    <div><strong>{feature.teamName}</strong><span>{feature.featureVersion}</span></div>
    <p>{feature.gamesPlayed} completed · {(feature.currentWeight * 100).toFixed(0)}% season / {(feature.priorWeight * 100).toFixed(0)}% prior</p>
    <dl>
      <div><dt>Blended margin</dt><dd>{margin == null ? "—" : signed(margin)}</dd></div>
      <div><dt>Blended points</dt><dd>{pointsFor == null ? "—" : pointsFor.toFixed(1)}</dd></div>
      <div><dt>Roster continuity</dt><dd>{continuity == null ? "—" : `${(continuity * 100).toFixed(0)}%`}</dd></div>
      <div><dt>Returning PPA</dt><dd>{Number.isFinite(returningPpa) ? `${(returningPpa * 100).toFixed(0)}%` : "—"}</dd></div>
    </dl>
    <small>Completeness {feature.sourceCompleteness == null ? "—" : `${(feature.sourceCompleteness * 100).toFixed(0)}%`} · snapshot {feature.asOf ? fmtEt(feature.asOf) : "—"}</small>
  </article>;
}

function HistoryPanel({ context, game }: { context: CfbResearchContext | null; game: CfbTerminalRow }) {
  if (!context) return <section className={styles.historyPane}><div className={styles.sectionTitle}><span>HISTORICAL + TEAM CONTEXT</span><span>UNAVAILABLE</span></div><div className={styles.historyEmpty}>Historical tables are ready, but this game has no research context yet. Live collection is unaffected.</div></section>;
  const teamRows = [
    { name: game.homeTeam, record: context.homeTeam },
    { name: game.awayTeam, record: context.awayTeam },
  ];
  return <section className={styles.historyPane}>
    <div className={styles.sectionTitle}><span>HISTORICAL + TEAM CONTEXT</span><span>DESCRIPTIVE · NO VALIDATED EDGE</span></div>
    <div className={styles.historyDisclosure}><ShieldAlert aria-hidden="true" /><p>CFBD historical references are not verified closing lines. Exact and bucket records include overtime, report uncertainty, and never substitute for the live movement ledger.</p></div>
    <div className={styles.historyGrid}>
      <article className={styles.historyCard}>
        <header><strong>EXACT LINE</strong><span>{context.homeSpread == null ? "NO LINE" : `${game.homeTeam} ${signed(context.homeSpread)}`}</span></header>
        <div className={styles.historyMetrics}><HistoricalRecord label="SU" record={context.exact.su} /><HistoricalRecord label="ATS" record={context.exact.ats} /></div>
        <footer>{context.seasons.length ? `${context.seasons[0]}–${context.seasons.at(-1)}` : "No historical sample"} · historical reference</footer>
      </article>
      <article className={styles.historyCard}>
        <header><strong>REGISTERED BUCKET</strong><span>{context.bucketLabel ?? "OUTSIDE BUCKETS"}</span></header>
        <div className={styles.historyMetrics}><HistoricalRecord label="SU" record={context.bucket.su} /><HistoricalRecord label="ATS" record={context.bucket.ats} /></div>
        <footer>Definition cfb-history-v1 · pushes excluded from rates</footer>
      </article>
      <article className={styles.historyCard}>
        <header><strong>TEAM / REGIME</strong><span>PARTIALLY POOLED</span></header>
        <div className={styles.teamRows}>{teamRows.map(({ name, record }) => <div key={name} className={styles.teamContextRow}><div><strong>{name}</strong><span>{record.coach ? `${record.coach} regime` : "recent team fallback"}</span></div><p>Raw {record.rawRate == null ? "—" : `${(record.rawRate * 100).toFixed(1)}%`} · shrunk {record.shrunkRate == null ? "—" : `${(record.shrunkRate * 100).toFixed(1)}%`}</p><small>{record.wins}-{record.losses}-{record.pushes} · n={record.n} · {record.reliability} sample reliability</small></div>)}</div>
      </article>
    </div>
    <div className={styles.sectionTitle}><span>SEASON + ROSTER SNAPSHOTS</span><span>POINT-IN-TIME ONLY</span></div>
    <div className={styles.featureGrid}><TeamFeatureCard feature={context.homeFeature} /><TeamFeatureCard feature={context.awayFeature} /></div>
    <div className={styles.sectionTitle}><span>RECENT COMPARABLES</span><span>{context.bucketLabel ?? "NO REGISTERED BUCKET"}</span></div>
    {context.comparableGames.length ? <div className={styles.comparableGrid}>{context.comparableGames.map((item) => <article key={item.gameId} className={styles.comparableGame}><span>{item.gameDate}</span><strong>{item.awayTeam} {item.awayScore} · {item.homeTeam} {item.homeScore}</strong><small>{item.homeTeam} {signed(item.homeSpread)} · home ATS {item.atsOutcome}</small></article>)}</div> : <div className={styles.historyEmpty}>No completed comparable games are available for this registered bucket.</div>}
    <div className={styles.hypothesisStrip}><strong>HYPOTHESIS REGISTRY</strong>{context.hypotheses.length ? context.hypotheses.map((item) => <span key={`${item.key}-${item.version}`}>{item.key} {item.version} · {item.status.replaceAll("_", " ")} · prospective n={item.prospectiveN}</span>) : <span>No registered hypothesis has been evaluated.</span>}</div>
  </section>;
}

function MovementMiniChart({ game, market }: { game: CfbTerminalRow; market: "spread" | "total" }) {
  const points = movementSeries(game, market);
  const label = market === "spread" ? "HOME SPREAD" : "TOTAL";
  const values = points.map((point) => point.value);
  const low = Math.min(...values); const high = Math.max(...values);
  const duration = points.length > 1 ? points.at(-1)!.time - points[0].time : 0;
  const coords = points.map((point) => ({
    x: duration ? 3 + (point.time - points[0].time) / duration * 110 : 58,
    y: high === low ? 16 : 29 - (point.value - low) / (high - low) * 26,
  }));
  const change = points.length > 1 ? points.at(-1)!.value - points[0].value : null;
  return <span className={styles.miniChart} title={`${label}: observed consensus, independently scaled; dashed lines indicate gaps over 30 minutes. Book coverage can change.`}>
    <span>{label} <b>{change == null ? "—" : signed(change)}</b></span>
    {!points.length ? <small>No captures</small> : <svg viewBox="0 0 116 32" role="img" aria-label={`${label}: ${points.length} captures${change == null ? ", insufficient history" : `, change ${signed(change)} points`}`}>
      {coords.slice(1).map((point, index) => <line key={index} x1={coords[index].x} y1={coords[index].y} x2={point.x} y2={point.y} stroke="currentColor" strokeWidth="1.5" strokeDasharray={points[index + 1].time - points[index].time > 30 * 60_000 ? "3 3" : undefined} />)}
      {coords.map((point, index) => <circle key={index} cx={point.x} cy={point.y} r="1.8" fill="currentColor" />)}
    </svg>}
  </span>;
}

function WatchGame({ item, signals, active, asOf, onChoose }: { item: CfbTerminalRow; signals: LineAlertRow[]; active: boolean; asOf: string; onChoose: () => void }) {
  const itemMarket = buildMarket(item, "spread", "home", asOf);
  const series = movementSeries(item, "spread");
  const move = series.length > 1 ? series.at(-1)!.value - series[0].value : null;
  const Trend = move != null && move > 0 ? TrendingUp : move != null && move < 0 ? TrendingDown : Activity;
  const recorded = movementSignals(signals, item.matchupId);
  const latestByType = recorded.filter((signal, index) => recorded.findIndex((other) => other.alertType === signal.alertType && other.details?.market === signal.details?.market) === index);
  const stale = item.latestCapturedAt && Date.parse(asOf) - Date.parse(item.latestCapturedAt) > 30 * 60_000;
  return <button type="button" className={styles.watchRow} data-active={active} onClick={onChoose} aria-pressed={active}>
    <span className={styles.watchGame}><strong title={`${item.awayTeam} @ ${item.homeTeam}`}>{item.awayTeam} @ {item.homeTeam}</strong><small>{item.commenceTime ? fmtEt(item.commenceTime, true) : "TBD"} · {item.captures} captures</small></span>
    <span className={styles.watchLine}>{itemMarket.current}</span>
    <span className={move != null && move > 0 ? styles.positive : move != null && move < 0 ? styles.negative : styles.neutral}><Trend aria-hidden="true" /> {move == null ? "—" : Math.abs(move).toFixed(1)}</span>
    <span className={styles.miniCharts}><MovementMiniChart game={item} market="spread" /><MovementMiniChart game={item} market="total" /></span>
    <span className={styles.movementBadges}>{latestByType.map((signal) => <span key={`${signal.alertType}-${signal.details?.market}`} data-kind={movementKind(signal.alertType)} title={`${SIGNAL_LABELS[signal.alertType]} · ${signal.side} · ${fmtEt(signalObservedAt(signal))} · ${signal.outcome ?? "result pending"}`}>
      {signalMarket(signal) === "spread" ? "S" : signalMarket(signal) === "total" ? "T" : "ML"} {movementKind(signal.alertType)?.toUpperCase()} · {signal.side.toUpperCase()}{signalMagnitude(signal) ? ` · ${signalMagnitude(signal)}` : ""} · {fmtEt(signalObservedAt(signal), true)}
    </span>)}{!recorded.length ? <small>{item.captures < 2 ? "INSUFFICIENT HISTORY" : "NO RECORDED STEAM / WALK / REVERSAL"}</small> : null}</span>
    <span className={styles.watchHealth}>{item.latestCapturedAt ? `OBS ${fmtEt(item.latestCapturedAt)}` : "NEVER CAPTURED"}{stale && !item.completed ? " · STALE" : ""}{item.closeQuality ? ` · CLOSE ${item.closeQuality.toUpperCase()}` : ""}</span>
  </button>;
}

export default function CfbTerminalClient({ board, signals, backtest, research, scorecard, captureHealth }: { board: CfbTerminalBoard; signals: LineAlertRow[]; backtest: CfbSignalBacktestRow[]; research: CfbResearchBoard; scorecard: MarketSignalScorecardRow[]; captureHealth: MarketCaptureHealth | null }) {
  const router = useRouter();
  const [gameId, setGameId] = useState(board.games[0]?.matchupId ?? 0);
  const [marketKey, setMarketKey] = useState<MarketKey>("spread");
  const [side, setSide] = useState<SelectionSide>("home");
  const [query, setQuery] = useState(""); const [selectedBook, setSelectedBook] = useState("");
  const [movementFilter, setMovementFilter] = useState("all");
  const [positions, setPositions] = useState<PaperPosition[]>([]); const [lockMessage, setLockMessage] = useState<string | null>(null);
  useEffect(() => { const timer = window.setInterval(() => router.refresh(), 60_000); return () => window.clearInterval(timer); }, [router]);
  const game = board.games.find((item) => item.matchupId === gameId) ?? board.games[0] ?? null;
  const market = useMemo(() => game ? buildMarket(game, marketKey, side, board.asOf) : null, [game, marketKey, side, board.asOf]);
  const quote = market?.books.find((item) => item.key === selectedBook) ?? market?.books[0] ?? null;
  const gameSignals = useMemo(() => signals.filter((item) => item.matchupId === game?.matchupId), [signals, game?.matchupId]);
  const marketSignals = useMemo(() => gameSignals.filter((item) => signalMarket(item) === marketKey), [gameSignals, marketKey]);
  const researchContext = game ? research[game.matchupId] ?? null : null;
  const filteredGames = useMemo(() => { const normalized = query.trim().toLowerCase(); return board.games.filter((item) => `${item.awayTeam} ${item.homeTeam} ${item.network ?? ""}`.toLowerCase().includes(normalized) && (movementFilter === "all" || movementSignals(signals, item.matchupId).some((signal) => movementKind(signal.alertType) === movementFilter))); }, [board.games, query, movementFilter, signals]);
  function chooseGame(id: number) { setGameId(id); setSelectedBook(""); setLockMessage(null); }
  function chooseMarket(next: MarketKey) { setMarketKey(next); setSide(selectionFor(next)); setSelectedBook(""); setLockMessage(null); }
  function chooseSide(next: SelectionSide) { setSide(next); setSelectedBook(""); setLockMessage(null); }
  function addPaperPosition() {
    if (!game || !quote || !quote.fresh) return;
    setPositions((current) => [{ id: `${game.matchupId}-${marketKey}-${quote.key}-${Date.now()}`, game: `${game.awayTeam} @ ${game.homeTeam}`, market: `${MARKET_LABELS[marketKey]} · ${quote.side.toUpperCase()}`, book: quote.book, entry: `${quote.line} ${quote.price}`, observedAt: quote.updatedAt ?? board.asOf }, ...current]);
    setLockMessage(`Recorded paper position at ${quote.book}; this did not place a wager.`);
  }
  const statusLabel = board.status.toUpperCase();
  const sideOptions: SelectionSide[] = marketKey === "total" ? ["over", "under"] : ["home", "away"];
  return <div className={styles.terminal}>
    <header className={styles.topbar}><div className={styles.brand}>CFB LINE TERMINAL</div><label className={styles.command}><Search aria-hidden="true" /><span className={styles.srOnly}>Search market watch</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="SEARCH TEAM OR GAME" /></label><div className={styles.marketOpen}><Radio aria-hidden="true" /> {board.games.length ? "MARKET BOARD" : "NO BOARD"}</div><div className={styles.shadowMode} title={board.statusDetail}>{statusLabel} · AS OF {fmtEt(board.asOf, true)}</div></header>
    <div className={styles.shell}>
      <aside className={styles.watchPane} aria-label="CFB market watch"><div className={styles.sectionTitle}><span>MARKET WATCH</span><span>{board.gameDate}</span></div>
        <div className={styles.movementFilters} aria-label="Filter recorded movements">{["all", "steam", "walk", "reversal"].map((kind) => <button key={kind} type="button" aria-pressed={movementFilter === kind} onClick={() => setMovementFilter(kind)}>{kind.toUpperCase()}</button>)}</div>
        <p className={styles.watchLegend}>S = home spread · T = total · ML = moneyline. Badges are recorded signals, not recommendations. Charts show observed consensus; dashed gaps exceed 30m.</p>
        <div className={styles.watchHeader}><span>GAME</span><span>LINE</span><span>MOVE</span></div><div className={styles.watchList}>
        {filteredGames.map((item) => <WatchGame key={item.matchupId} item={item} signals={signals} active={item.matchupId === game?.matchupId} asOf={board.asOf} onChoose={() => chooseGame(item.matchupId)} />)}
        {!filteredGames.length ? <div className={styles.empty}>{board.games.length ? "No games match this search and movement filter." : board.statusDetail}</div> : null}
      </div></aside>
      <main className={styles.instrumentPane}>{!game || !market ? <section className={styles.chartSection}><div className={styles.empty}>Load the canonical CFB schedule to begin. No sample quotes are substituted.</div></section> : <>
        <section className={styles.instrumentHeader}><div className={styles.instrumentTop}><div><div className={styles.instrumentTitle}>{game.awayTeam} @ {game.homeTeam}</div><div className={styles.instrumentMeta}>{game.venue ?? "Venue TBD"} · {game.commenceTime ? fmtEt(game.commenceTime) : "Kickoff TBD"} · {game.network ?? "Network TBD"} · {market.label}</div></div><div className={styles.primaryQuote}><strong>{market.current}</strong><span>OPEN {market.open} · {market.move.toUpperCase()} · CLV CLOSE {market.close}</span></div></div><div className={styles.marketTabs}>{(Object.keys(MARKET_LABELS) as MarketKey[]).map((key) => <button key={key} type="button" data-active={marketKey === key} onClick={() => chooseMarket(key)}>{MARKET_LABELS[key]}</button>)}</div><div className={styles.marketTabs}>{sideOptions.map((option) => <button key={option} type="button" data-active={side === option} onClick={() => chooseSide(option)}>{option === "home" ? game.homeTeam : option === "away" ? game.awayTeam : option.toUpperCase()}</button>)}</div></section>
        <section className={styles.chartSection}><div className={styles.chartLabelRow}><span>{market.axisLabel}</span><span>{game.latestCapturedAt ? `observed ${fmtEt(game.latestCapturedAt)} · ${marketSignals.length} signals` : "scheduled · never captured"}</span></div><div className={styles.chartWrap}><MarketChart market={market} marketKey={marketKey} signals={marketSignals} /></div></section>
        <section className={styles.lowerGrid}><div className={styles.ladderPane}><div className={styles.sectionTitle}><span>BOOK LADDER</span><span>OBSERVED QUOTES</span></div><div className={styles.bookHeader}><span>BOOK</span><span>UPDATED</span><span>LINE</span><span>PRICE</span></div>{market.books.map((item) => <button key={item.key} type="button" className={styles.bookRow} data-selected={quote?.key === item.key} onClick={() => { setSelectedBook(item.key); setLockMessage(null); }}><span>{item.book}{!item.fresh ? <em>STALE</em> : null}</span><span>{item.updatedAt ? fmtEt(item.updatedAt, true) : "—"}</span><span>{item.line}</span><span>{item.price}</span></button>)}{!market.books.length ? <div className={styles.empty}>This market or side is not quoted by the captured books.</div> : null}<div className={styles.paperAction}><button type="button" disabled={!quote?.fresh} onClick={addPaperPosition}><BookOpen aria-hidden="true" /> {quote?.fresh ? `RECORD PAPER ${quote.book.toUpperCase()} ${quote.line} ${quote.price}` : "PAPER ENTRY DISABLED · QUOTE NOT ≤5M FRESH"}</button><div aria-live="polite">{lockMessage ?? "Displayed quotes are observations, not verified execution availability."}</div></div></div>
          <div className={styles.catalystPane}><div className={styles.sectionTitle}><span>MARKET QUALITY</span><span>AUDIT</span></div><div className={styles.catalystRow}><span>NOW</span><strong>SUPPORT</strong><p>{market.selectedLineBookCount} books at selected consensus line · {market.marketBookCount} books in market</p></div><div className={styles.catalystRow}><span>OPEN</span><strong>HISTORY</strong><p>{game.captures} accepted pregame captures; post-kickoff rows excluded</p></div><div className={styles.catalystRow}><span>CLOSE</span><strong>{game.closeQuality ? `GRADE ${game.closeQuality}` : "PENDING"}</strong><p>{game.closingCapturedAt ? `${market.closeMove}; ${Math.round((game.closeLeadSeconds ?? 0) / 60)}m before ${game.closeBoundarySource}` : "Frozen only after the scheduled CFB kickoff boundary; no latest-row proxy."}</p></div><div className={styles.catalystRow}><span>MAP</span><strong>IDENTITY</strong><p>CFBD game {game.cfbdGameId} · Odds event {game.oddsEventId ?? "provider event unavailable"}</p></div></div></section>
        <HistoryPanel context={researchContext} game={game} />
        <section className={styles.blotter}><div className={styles.sectionTitle}><span>SESSION PAPER BLOTTER</span><span>{positions.length} OPEN</span></div>{!positions.length ? <div className={styles.blotterEmpty}>A paper position can be recorded only from an observation no more than five minutes old.</div> : <div className={styles.blotterTableWrap}><table><thead><tr><th>Game</th><th>Market</th><th>Book</th><th>Entry</th><th>Observed</th></tr></thead><tbody>{positions.map((position) => <tr key={position.id}><td>{position.game}</td><td>{position.market}</td><td>{position.book}</td><td>{position.entry}</td><td>{fmtEt(position.observedAt)}</td></tr>)}</tbody></table></div>}</section>
        <section className={styles.researchPane}><div className={styles.sectionTitle}><span>PROSPECTIVE SIGNAL AUDIT</span><span>CFB-LINES-V1 · NO EDGE CLAIM</span></div>{!backtest.length ? <div className={styles.blotterEmpty}>No prospective CFB signals yet. Metrics appear only after immutable detector observations are recorded.</div> : <div className={styles.researchTableWrap}><table><thead><tr><th>Signal</th><th>Version</th><th>Obs</th><th>Dates</th><th>Settled</th><th>W-L-P</th><th>Avg CLV</th><th>Beat close</th><th>Units</th><th>ROI/bet</th></tr></thead><tbody>{backtest.map((row) => <tr key={`${row.alertType}-${row.signalVersion}`}><td>{SIGNAL_LABELS[row.alertType] ?? row.alertType}</td><td>{row.signalVersion}</td><td>{row.observations}</td><td>{row.gameDates}</td><td>{row.settled}</td><td>{row.wins}-{row.losses}-{row.pushes}</td><td>{row.avgLineClv == null ? "—" : signed(row.avgLineClv)}</td><td>{pct(row.beatClose)}</td><td>{row.units == null ? "—" : signed(row.units, 2)}</td><td>{row.roiPerBet == null ? "—" : `${signed(row.roiPerBet * 100, 1)}%`}</td></tr>)}</tbody></table></div>}<p className={styles.researchDisclosure}>Descriptive research only. Small samples, repeated game dates, and mixed execution books can make apparent ROI unstable; model promotion requires prospective CLV and out-of-sample evidence.</p></section>
      </>}</main>
      <aside className={styles.pulsePane}><div className={styles.sectionTitle}><span>DATA PULSE</span><span>{statusLabel}</span></div><article className={styles.pulseRow} data-tone={board.status === "live" ? "market" : "critical"}><div><span>{fmtEt(board.asOf, true)}</span><strong>{board.status === "live" ? <Zap aria-hidden="true" /> : <ShieldAlert aria-hidden="true" />} FEED STATE</strong></div><h3>{statusLabel}</h3><p>{board.statusDetail}</p></article>{captureHealth ? <article className={styles.pulseRow} data-tone={captureHealth.status === "healthy" ? "market" : "critical"}><div><span>{captureHealth.eventsCovered} EVENTS</span><strong><Activity aria-hidden="true" /> CHECKPOINTS</strong></div><h3>{captureHealth.due ? `${captureHealth.dueCaptured}/${captureHealth.due} due captured` : "No checkpoints due"}</h3><p>{captureHealth.missed} missed · {captureHealth.failed} failed · {captureHealth.pending} scheduled ahead</p></article> : null}{game ? <><article className={styles.pulseRow} data-tone="market"><div><span>{game.latestCapturedAt ? fmtEt(game.latestCapturedAt, true) : "—"}</span><strong><Activity aria-hidden="true" /> CAPTURE</strong></div><h3>{game.captures} observations</h3><p>Every chart point comes from the append-only exact-book ledger.</p></article><div className={styles.sectionTitle}><span>CROSS-MARKET</span><span>RELATED</span></div>{(["spread", "total", "moneyline"] as MarketKey[]).map((key) => { const view = buildMarket(game, key, selectionFor(key), board.asOf); return <div key={key} className={styles.relatedRow}><span>{MARKET_LABELS[key]}</span><strong>{view.current}</strong><small>{view.move}</small></div>; })}<div className={styles.sectionTitle}><span>SIGNAL TAPE</span><span>{gameSignals.length} RECORDED</span></div>{gameSignals.length ? gameSignals.slice(0, 8).map((signal) => { const observedAt = signalObservedAt(signal); return <article key={`${signal.alertType}-${observedAt}`} className={styles.signalRow} data-tone={signal.alertType === "reversal" ? "risk" : "market"}><div><strong>{SIGNAL_LABELS[signal.alertType] ?? signal.alertType.toUpperCase()}</strong><span>{fmtEt(observedAt, true)}</span></div><p>{String(signal.details?.market ?? signalMarket(signal)).toUpperCase()} · {signal.side.toUpperCase()} · line {String(signal.details?.trigger_line ?? "—")} · support {String(signal.details?.consensus_support ?? "—")}</p><small>{String(signal.details?.signal_version ?? "unstamped")} · evidence #{String(signal.details?.trigger_history_id ?? "—")}</small></article>; }) : <div className={styles.signalEmpty}>No qualifying prospective signal for this game.</div>}</> : null}<div className={styles.disclosure}><BellRing aria-hidden="true" /><div><strong>Research terminal</strong><p>Signals are versioned observations, not recommendations. No predictive edge or real-money execution is represented.</p></div></div></aside>
    </div>
    <MarketSignalScorecard rows={scorecard} sport="CFB" />
    <footer className={styles.ticker}><span><strong>STATUS</strong> {board.statusDetail}</span><span><strong>BOARD</strong> {board.games.length} GAMES</span><span><strong>QUARANTINE</strong> {board.unmappedEvents} UNMAPPED EVENTS</span><span><strong>CONSENSUS</strong> LOWER MEDIAN · EXACT-LINE PRICE SUPPORT</span><span><strong>PAPER</strong> FIVE-MINUTE FRESHNESS REQUIRED</span></footer>
  </div>;
}
