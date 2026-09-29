/**
 * Hockey market views over the exact-book NHL tape (game_odds_history.books).
 *
 * Football terminals chart the spread and total *line*. Hockey barely moves
 * lines: the puck line is ±1.5 almost everywhere and totals sit at 5.5–6.5, so
 * the information is in the *price*. Price views therefore compare vig-free
 * probabilities only among books quoting the same line (the same proposition);
 * a 5.5 quote and a 6.5 quote are never blended or charted as one series.
 */
import type { CfbBookMap, CfbBookQuote } from "@/db/queries";
import { SPORTSBOOK_KEYS, SPORTSBOOK_NAMES, selectedSportsbooks } from "./sportsbook-policy";

export type NhlMarketKey = "moneyline" | "puckline" | "total";
export type NhlSide = "home" | "away" | "over" | "under";
export type NhlMetric = "price" | "line";

export const NHL_MARKET_LABELS: Record<NhlMarketKey, string> = { moneyline: "MONEYLINE", puckline: "PUCK LINE", total: "TOTAL" };

/** One capture of the matchup's tape. */
export type NhlCapture = { capturedAt: string; books: CfbBookMap };
export type NhlGameTape = {
  homeTeam: string; awayTeam: string; homeAbbrev: string; awayAbbrev: string;
  commenceTime: string | null; latestCapturedAt: string | null;
  openingBooks: CfbBookMap | null; currentBooks: CfbBookMap | null; closingBooks: CfbBookMap | null;
  history: NhlCapture[];
};

export function americanProbability(price: number | null | undefined): number | null {
  const odds = Number(price);
  if (price == null || !Number.isFinite(odds) || Math.abs(odds) < 100) return null;
  return odds > 0 ? 100 / (odds + 100) : -odds / (-odds + 100);
}

/** Proportional vig removal over one book's two sides. */
export function fairShare(side: number | null | undefined, other: number | null | undefined): number | null {
  const a = americanProbability(side), b = americanProbability(other);
  return a == null || b == null || a + b <= 0 ? null : a / (a + b);
}

/** Observed lower-middle value; never an invented midpoint. */
export function lowerMedian(values: number[]): number | null {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  return ordered[Math.floor((ordered.length - 1) / 2)];
}

export function sidesFor(market: NhlMarketKey): NhlSide[] {
  return market === "total" ? ["over", "under"] : ["home", "away"];
}

/** The line this book quotes for the side, from the side's own perspective. */
export function bookLine(book: CfbBookQuote, market: NhlMarketKey, side: NhlSide): number | null {
  const raw = market === "puckline" ? (side === "away" ? book.spread_away : book.spread_home)
    : market === "total" ? book.total_line : null;
  return raw == null || !Number.isFinite(Number(raw)) ? null : Number(raw);
}

/** American price this book offers for the side (moneyline has no line). */
export function bookPrice(book: CfbBookQuote, market: NhlMarketKey, side: NhlSide): number | null {
  const raw = market === "moneyline" ? (side === "away" ? book.ml_away : book.ml_home)
    : market === "puckline" ? (side === "away" ? book.spread_away_price : book.spread_home_price)
    : side === "under" ? book.under : book.over;
  return raw == null || !Number.isFinite(Number(raw)) ? null : Number(raw);
}

/** Vig-free probability of the side at this book's own line. */
export function bookFairProbability(book: CfbBookQuote, market: NhlMarketKey, side: NhlSide): number | null {
  const opposite: NhlSide = side === "home" ? "away" : side === "away" ? "home" : side === "over" ? "under" : "over";
  if (market === "puckline") {
    // A pair is only a pair when both sides quote mirror-image lines.
    const mine = bookLine(book, market, side), theirs = bookLine(book, market, opposite);
    if (mine == null || theirs == null || mine !== -theirs) return null;
  }
  return fairShare(bookPrice(book, market, side), bookPrice(book, market, opposite));
}

export type MarketSnapshot = {
  /** Consensus (lower-median) line; null for the moneyline. */
  line: number | null;
  /** Lower-median fair probability among books at `line`. */
  probability: number | null;
  /** Books quoting the consensus line with a complete pair. */
  lineBooks: number;
  /** Books quoting this market at all. */
  marketBooks: number;
};

export function marketSnapshot(books: CfbBookMap | null | undefined, market: NhlMarketKey, side: NhlSide): MarketSnapshot {
  const quotes = Object.values(selectedSportsbooks(books ?? {}));
  if (market === "moneyline") {
    const probs = quotes.flatMap(book => { const p = bookFairProbability(book, market, side); return p == null ? [] : [p]; });
    return { line: null, probability: lowerMedian(probs), lineBooks: probs.length, marketBooks: probs.length };
  }
  const lines = quotes.flatMap(book => { const l = bookLine(book, market, side); return l == null ? [] : [l]; });
  const line = lowerMedian(lines);
  const atLine = line == null ? [] : quotes.filter(book => bookLine(book, market, side) === line)
    .flatMap(book => { const p = bookFairProbability(book, market, side); return p == null ? [] : [p]; });
  return { line, probability: lowerMedian(atLine), lineBooks: atLine.length, marketBooks: lines.length };
}

export const signed = (value: number, digits = 1) => `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
export const pct = (value: number | null) => value == null ? "—" : `${(value * 100).toFixed(1)}%`;
export const american = (value: number | null | undefined) => value == null ? "—" : `${value > 0 ? "+" : ""}${Math.round(value)}`;

function sideName(game: Pick<NhlGameTape, "homeAbbrev" | "awayAbbrev">, side: NhlSide): string {
  return side === "home" ? game.homeAbbrev : side === "away" ? game.awayAbbrev : side.toUpperCase();
}

/** "BOS 58.3%", "BOS -1.5 · 38.2%", "6.5 · OVER 52.1%". */
export function snapshotLabel(game: Pick<NhlGameTape, "homeAbbrev" | "awayAbbrev">, market: NhlMarketKey, side: NhlSide, snap: MarketSnapshot): string {
  if (market === "moneyline") return snap.probability == null ? "NO MARKET" : `${sideName(game, side)} ${pct(snap.probability)}`;
  if (snap.line == null) return "NO MARKET";
  const prob = snap.probability == null ? "price unpaired" : pct(snap.probability);
  return market === "puckline" ? `${sideName(game, side)} ${signed(snap.line)} · ${prob}` : `${snap.line.toFixed(1)} · ${side.toUpperCase()} ${prob}`;
}

/**
 * Movement from open to now without mixing propositions: when the consensus
 * line itself moved, report the line move rather than a price change across
 * two different bets.
 */
export function describeMove(market: NhlMarketKey, open: MarketSnapshot, current: MarketSnapshot, suffix = ""): string {
  if (open.probability == null && open.line == null) return "Awaiting two captures";
  if (market !== "moneyline" && open.line != null && current.line != null && open.line !== current.line) {
    return `line ${market === "puckline" ? signed(open.line) : open.line.toFixed(1)} → ${market === "puckline" ? signed(current.line) : current.line.toFixed(1)}${suffix}`;
  }
  if (open.probability == null || current.probability == null) return "Awaiting two captures";
  return `${signed((current.probability - open.probability) * 100)}pp${suffix}`;
}

export type NhlBookRow = { key: string; book: string; line: string; price: string; fair: string; updatedAt: string | null; fresh: boolean; atConsensus: boolean };
export type NhlHistoryPoint = { at: string; values: Record<string, number | null> };
export type NhlMarketView = {
  current: MarketSnapshot; open: MarketSnapshot; close: MarketSnapshot | null;
  currentLabel: string; openLabel: string; closeLabel: string; move: string; closeMove: string;
  axisLabel: string; percentage: boolean; history: NhlHistoryPoint[]; books: NhlBookRow[];
};

/** Quote freshness for a paper entry: both the book update and our capture ≤5m old. */
export function quoteFresh(updatedAt: string | null, capturedAt: string | null, asOf: string): boolean {
  if (!updatedAt || !capturedAt) return false;
  const now = Date.parse(asOf);
  return now - Date.parse(updatedAt) <= 5 * 60_000 && now - Date.parse(capturedAt) <= 5 * 60_000;
}

function bookTitle(key: string, quote?: CfbBookQuote): string {
  return SPORTSBOOK_NAMES[key] ?? quote?.title ?? key.replaceAll("_", " ");
}

export function buildNhlMarket(game: NhlGameTape, market: NhlMarketKey, side: NhlSide, metric: NhlMetric, asOf: string): NhlMarketView {
  const current = marketSnapshot(game.currentBooks, market, side);
  const open = marketSnapshot(game.openingBooks, market, side);
  const close = game.closingBooks ? marketSnapshot(game.closingBooks, market, side) : null;
  const byLine = market !== "moneyline" && metric === "line";
  const focusLine = current.line;
  const history = game.history.map(point => ({
    at: point.capturedAt,
    values: Object.fromEntries(Object.entries(selectedSportsbooks(point.books)).map(([key, book]) => {
      if (byLine) return [key, bookLine(book, market, side)];
      // Price series: only books at the current consensus line; others gap.
      if (market !== "moneyline" && bookLine(book, market, side) !== focusLine) return [key, null];
      const p = bookFairProbability(book, market, side);
      return [key, p == null ? null : p * 100];
    })),
  }));
  const books = Object.entries(selectedSportsbooks(game.currentBooks ?? {})).flatMap(([key, quote]) => {
    const price = bookPrice(quote, market, side), line = bookLine(quote, market, side);
    if (price == null || (market !== "moneyline" && line == null)) return [];
    const updatedAt = quote.last_update ? String(quote.last_update) : null;
    const label = market === "moneyline" ? `${sideName(game, side)} ML`
      : market === "puckline" ? `${sideName(game, side)} ${signed(line!)}` : `${side.toUpperCase()} ${line!.toFixed(1)}`;
    return [{ key, book: bookTitle(key, quote), line: label, price: american(price), fair: pct(bookFairProbability(quote, market, side)),
      updatedAt, fresh: quoteFresh(updatedAt, game.latestCapturedAt, asOf), atConsensus: market === "moneyline" || line === current.line }];
  }).sort((a, b) => {
    const ai = SPORTSBOOK_KEYS.indexOf(a.key), bi = SPORTSBOOK_KEYS.indexOf(b.key);
    return (ai < 0 ? 99 : ai) - (bi < 0 ? 99 : bi) || a.book.localeCompare(b.book);
  });
  const who = side === "home" ? game.homeAbbrev : side === "away" ? game.awayAbbrev : side.toUpperCase();
  const axisLabel = market === "moneyline" ? `Vig-free ${who} win probability`
    : byLine ? (market === "puckline" ? `${who} puck line` : "Game total line")
    : focusLine == null ? "No consensus line"
    : market === "puckline" ? `Vig-free ${who} ${signed(focusLine)} probability (books at ${signed(focusLine)})`
    : `Vig-free ${who} ${focusLine.toFixed(1)} probability (books at ${focusLine.toFixed(1)})`;
  return {
    current, open, close,
    currentLabel: snapshotLabel(game, market, side, current),
    openLabel: snapshotLabel(game, market, side, open),
    closeLabel: close ? snapshotLabel(game, market, side, close) : "PENDING",
    // One capture is both open and current: a 0.0 move there is unobserved, not stable.
    move: game.history.length < 2 ? "Awaiting two captures" : describeMove(market, open, current),
    closeMove: close ? describeMove(market, open, close, " open→close") : "Close pending",
    axisLabel, percentage: !byLine, history, books,
  };
}

/** Lower-median value per capture, for watch-list sparklines. */
export function tapeSeries(history: NhlCapture[], start: string | null, market: "moneyline" | "total"): { time: number; value: number }[] {
  const cutoff = start ? Date.parse(start) : Infinity;
  return history.flatMap(point => {
    const time = Date.parse(point.capturedAt);
    if (!Number.isFinite(time) || time >= cutoff) return [];
    const snap = marketSnapshot(point.books, market, market === "total" ? "over" : "home");
    const value = market === "total" ? snap.line : snap.probability;
    return value == null ? [] : [{ time, value }];
  }).sort((a, b) => a.time - b.time);
}

/**
 * How old the latest capture may be, at a given lead, before a nhl-dense-v1
 * checkpoint has been missed. Worst case is every window captured at its last
 * moment: just before T-140m closes, the newest capture can be from T-330m
 * (190 min old). Each bound is that worst case plus scheduler slack. Until
 * the T-24h window closes (T-1200m) no capture is owed, so nothing is stale.
 */
export function nhlFreshnessTargetMinutes(minutesToStart: number): number | null {
  if (minutesToStart <= 45) return 40;     // worst case 25 (60 -> 35)
  if (minutesToStart <= 140) return 60;    // worst case 40 (140 -> 100)
  if (minutesToStart <= 330) return 210;   // worst case 190 (330 -> 140)
  if (minutesToStart <= 1200) return 900;  // worst case 870 (1200 -> 330)
  return null;
}

/** A game is owed a capture once its T-24h window has closed. */
export const NHL_FIRST_CAPTURE_DUE_MINUTES = 1200;
