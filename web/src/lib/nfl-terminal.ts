import type { CfbBookQuote, NflVegasBoardRow } from "@/db/queries";
import type { IntelligenceMarket, IntelligenceSide } from "./movement-intelligence";

export const NFL_BOOK_COLORS = ["#f6a800", "#59b6ff", "#c58cff", "#5fd0a5", "#ff718b"];
export const nflBookName = (key: string) => ({ pinnacle: "Pinnacle", draftkings: "DraftKings", fanduel: "FanDuel", betmgm: "BetMGM" }[key] ?? key.replaceAll("_", " "));
export const nflSigned = (n: number | null, digits = 1): string => n == null || !Number.isFinite(n) ? "—" : `${n > 0 ? "+" : ""}${n.toFixed(digits)}`;
export const nflPrice = (n: number | null | undefined): string => n == null ? "—" : nflSigned(n, 0);
export const nflPercent = (n: number | null): string => n == null ? "—" : `${(n * 100).toFixed(1)}%`;
const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);
function probability(n: number) { return n > 0 ? 100 / (n + 100) : -n / (-n + 100); }
export function nflQuoteValue(q: CfbBookQuote, market: IntelligenceMarket, side: IntelligenceSide): number | null {
  if (market === "spread") { const n = side === "away" ? q.spread_away : q.spread_home; return finite(n) ? n : null; }
  if (market === "total") return finite(q.total_line) ? q.total_line : null;
  if (!finite(q.ml_home) || !finite(q.ml_away) || Math.abs(q.ml_home) < 100 || Math.abs(q.ml_away) < 100) return null;
  const h = probability(q.ml_home), a = probability(q.ml_away);
  return (side === "away" ? a : h) / (h + a);
}
function median(values: number[]) { const ordered = values.slice().sort((a,b) => a-b); return ordered.length ? ordered[Math.floor((ordered.length - 1)/2)] : null; }
type MarketGame = Pick<NflVegasBoardRow, "commenceTime"> & { trail: Pick<NflVegasBoardRow["trail"][number], "capturedAt" | "books">[] };
export function nflMarket(game: MarketGame, market: IntelligenceMarket, side: IntelligenceSide, now: number) {
  const history = game.trail.filter(p => Number.isFinite(Date.parse(p.capturedAt)) && Date.parse(p.capturedAt) <= now && Date.parse(p.capturedAt) < Date.parse(game.commenceTime))
    .slice().sort((a,b) => Date.parse(a.capturedAt)-Date.parse(b.capturedAt));
  const latest = history.at(-1);
  const points = history.map(p => ({ time: Date.parse(p.capturedAt), values: Object.fromEntries(Object.entries(p.books ?? {}).flatMap(([key,q]) => {
    const value = nflQuoteValue(q, market, side); return key === "polymarket" || value == null ? [] : [[key,value]];
  })) as Record<string,number> }));
  const all = [...new Set(points.flatMap(p => Object.keys(p.values)))];
  const priority = ["pinnacle", "draftkings", "fanduel", "betmgm"];
  const series = [...priority.filter(k => all.includes(k)), ...all.filter(k => !priority.includes(k)).sort()].slice(0,5);
  const current = points.length ? median(Object.values(points.at(-1)!.values)) : null;
  const open = points.length ? median(Object.values(points[0].values)) : null;
  const books = Object.entries(latest?.books ?? {}).filter(([key]) => key !== "polymarket").flatMap(([key,q]) => {
    const value = nflQuoteValue(q,market,side);
    const price = market === "moneyline" ? side === "away" ? q.ml_away : q.ml_home : market === "spread" ? side === "away" ? q.spread_away_price : q.spread_home_price : side === "under" ? q.under : q.over;
    if (value == null) return [];
    const stamp = Date.parse(q.last_update ?? "");
    const fresh = Number.isFinite(stamp) && stamp <= now && now-stamp <= 300_000 && latest != null && now-Date.parse(latest.capturedAt) <= 300_000;
    return [{ key, name:q.title || nflBookName(key), value, price:finite(price) ? price : null, updatedAt:q.last_update ?? null, fresh }];
  }).sort((a,b) => {
    const rank = (k:string) => priority.includes(k) ? priority.indexOf(k) : 99;
    return rank(a.key)-rank(b.key) || a.name.localeCompare(b.name);
  });
  return { points, series, books, current, open, move:current != null && open != null ? current-open : null,
    support:current == null ? 0 : books.filter(b => b.value === current).length };
}
