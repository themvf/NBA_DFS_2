import type { CfbBookQuote, CfbTerminalRow, LineAlertRow, MlbLineMovementRow, TennisMatchRow } from "@/db/queries";

export type IntelligenceMarket = "spread" | "total" | "moneyline";
export type IntelligenceSide = "home" | "away" | "over" | "under";
type Capture = { time: number; books: Record<string, number> };
export type IntelligenceEvent = {
  id: number; home: string; away: string; start: number; completed: boolean;
  markets: Partial<Record<IntelligenceMarket, Capture[]>>;
};
export type MovementInsight = {
  key: string; matchupId: number; market: IntelligenceMarket; side: IntelligenceSide;
  fixture: string; selection: string; label: string; types: string[];
  explanation: string; observedAt: number; support: number; supportLabel: string;
  metric: string; trailLabel: string; trail: { time: number; value: number }[];
};

// Presentation eligibility, not a detector threshold or a profitability rule.
export const INTELLIGENCE_WINDOW_MS = 30 * 60_000;
const labels: Record<string, string> = {
  steam: "STEAM", spread_steam: "STEAM", total_steam: "STEAM",
  walking: "MARKET DRIFT", spread_walking: "MARKET DRIFT", total_walking: "MARKET DRIFT",
  reversal: "REVERSAL", key_cross: "KEY CROSS", reference_led: "REFERENCE LED",
  price_pressure: "PRICE PRESSURE", late_move: "LATE MOVE", favorite_flip: "FAVORITE FLIP",
};

function numeric(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
export function insightMarket(signal: LineAlertRow): IntelligenceMarket | null {
  const explicit = signal.details?.market;
  if (explicit != null) return explicit === "spread" || explicit === "total" || explicit === "moneyline" ? explicit : null;
  if (signal.alertType.startsWith("spread") || signal.alertType === "key_cross") return "spread";
  if (signal.alertType.startsWith("total")) return "total";
  return "moneyline";
}
function observed(signal: LineAlertRow): number {
  return Date.parse(typeof signal.details?.trigger_capture_at === "string" ? signal.details.trigger_capture_at : signal.createdAt);
}
function count(value: unknown): number | null {
  if (Array.isArray(value)) return new Set(value.filter(v => typeof v === "string")).size;
  const n = numeric(value);
  return n != null && Number.isInteger(n) && n >= 0 ? n : null;
}
function support(signal: LineAlertRow) {
  for (const [key, noun] of [["books_moved", "books moved"], ["comparable_books", "comparable books"], ["overlap_books", "comparable books"], ["consensus_support", "books at trigger line"]]) {
    const n = count(signal.details?.[key]);
    if (n != null) return { support: n, supportLabel: `${n} ${noun}` };
  }
  return { support: 0, supportLabel: "Book support unavailable" };
}
function magnitude(signal: LineAlertRow): string {
  for (const key of ["reversal_leg_pp", "drift_pp", "avg_move_pp", "move_pp", "price_move_pp", "retail_follow_pp"]) {
    const n = numeric(signal.details?.[key]);
    if (n != null) return `${Math.abs(n).toFixed(1)} pp`;
  }
  for (const key of ["reversal_leg", "interval_delta", "delta", "retail_follow_move"]) {
    const n = numeric(signal.details?.[key]);
    if (n != null) return `${Math.abs(n).toFixed(1)} pts`;
  }
  const keyNumber = numeric(signal.details?.key_number);
  return keyNumber != null ? `Crossed ${keyNumber}` : "Recorded movement";
}
function explanation(signal: LineAlertRow, selection: string): string {
  const type = signal.alertType;
  if (type === "reversal") return `An earlier move reversed toward ${selection}. The original direction has lost support.`;
  if (type.endsWith("steam")) return `Multiple books moved toward ${selection} within the detector's capture interval.`;
  if (type.endsWith("walking")) return `The recorded market drift favors ${selection} versus the first captured line.`;
  if (type === "reference_led") return `The reference book moved first; retail books subsequently followed toward ${selection}.`;
  if (type === "key_cross") return `The spread crossed a key number toward ${selection}. Inspect the path before following the move.`;
  if (type === "price_pressure") return `Comparable prices shifted toward ${selection}${insightMarket(signal) === "moneyline" ? ". Watch for broader follow-through." : " while the consensus line stayed fixed."}`;
  if (type === "late_move") return `The market moved toward ${selection} near scheduled start. Watch whether that direction holds.`;
  return `The market's favorite changed to ${selection} since the opening capture.`;
}

// A stable book cohort prevents a book entering/leaving the feed from creating
// an apparent move. No fallback to mixed-book consensus or Polymarket.
export function comparableTrail(captures: Capture[], now: number, start: number) {
  const ordered = captures.filter(c => Number.isFinite(c.time) && c.time <= now && c.time < start && c.time >= now - 24 * 60 * 60_000)
    .slice().sort((a, b) => a.time - b.time);
  if (ordered.length < 2) return [];
  const keys = Object.keys(ordered[0].books).filter(key => key !== "polymarket" && key !== "pinnacle" && ordered.every(c => numeric(c.books[key]) != null));
  if (keys.length < 2) return [];
  return ordered.map(c => {
    const values = keys.map(key => c.books[key]).sort((a, b) => a - b);
    return { time: c.time, value: values[Math.floor((values.length - 1) / 2)] };
  });
}

export function buildMovementInsights(events: IntelligenceEvent[], signals: LineAlertRow[], now: number): MovementInsight[] {
  if (!Number.isFinite(now)) return [];
  const eventMap = new Map(events.map(event => [event.id, event]));
  const groups = new Map<string, LineAlertRow[]>();
  for (const signal of signals) {
    const event = eventMap.get(signal.matchupId), market = insightMarket(signal), at = observed(signal);
    if (!event || event.completed || !Number.isFinite(event.start) || event.start <= now || !market || !event.markets[market]) continue;
    if (!labels[signal.alertType] || signal.origin !== "prospective" || signal.details?.origin === "retrospective" || signal.outcome != null) continue;
    if (!Number.isFinite(at) || at > now || now - at > INTELLIGENCE_WINDOW_MS) continue;
    if (!(market === "total" ? ["over", "under"] : ["home", "away"]).includes(signal.side)) continue;
    const latest = Math.max(...event.markets[market]!.map(c => c.time).filter(t => Number.isFinite(t) && t <= now && t < event.start));
    if (!Number.isFinite(latest) || now - latest > INTELLIGENCE_WINDOW_MS) continue;
    const key = `${event.id}:${market}`;
    groups.set(key, [...(groups.get(key) ?? []), signal]);
  }
  return [...groups].map(([key, group]) => {
    // Newer observations supersede old direction; a simultaneous reversal gets
    // precedence so the card cannot headline an obsolete one-way move.
    group.sort((a, b) => observed(b) - observed(a) || Number(b.alertType === "reversal") - Number(a.alertType === "reversal") || a.alertType.localeCompare(b.alertType));
    const latest = group[0], event = eventMap.get(latest.matchupId)!, market = insightMarket(latest)!;
    const selection = latest.side === "home" ? event.home : latest.side === "away" ? event.away : latest.side.toUpperCase();
    const conflicting = group.some(a => observed(a) === observed(latest) && a.side !== latest.side);
    const trail = comparableTrail(event.markets[market]!, now, event.start);
    return {
      key, matchupId: event.id, market, side: latest.side as IntelligenceSide, fixture: `${event.away} @ ${event.home}`,
      selection, label: conflicting ? "MIXED DIRECTION" : labels[latest.alertType], types: [...new Set(group.map(a => labels[a.alertType]))],
      explanation: conflicting ? "Simultaneous signals point to opposing sides. Inspect the recorded evidence before assigning direction." : explanation(latest, selection),
      observedAt: observed(latest), ...support(latest), metric: magnitude(latest),
      trailLabel: market === "total" ? "Total points" : `${event.home} ${market === "spread" ? "spread" : "probability"}`,
      trail,
    };
  }).sort((a, b) => b.support - a.support || b.observedAt - a.observedAt || a.key.localeCompare(b.key));
}

function quoteValue(book: CfbBookQuote, market: IntelligenceMarket): number | null {
  if (market !== "moneyline") return numeric(book[market === "spread" ? "spread_home" : "total_line"]);
  const h = numeric(book.ml_home), a = numeric(book.ml_away);
  if (h == null || a == null || Math.abs(h) < 100 || Math.abs(a) < 100) return null;
  const implied = (v: number) => v > 0 ? 100 / (v + 100) : -v / (-v + 100);
  return implied(h) / (implied(h) + implied(a));
}
export function cfbIntelligenceEvents(games: Pick<CfbTerminalRow, "matchupId" | "homeTeam" | "awayTeam" | "commenceTime" | "completed" | "history">[]): IntelligenceEvent[] {
  return games.map(game => ({
    id: game.matchupId, home: game.homeTeam, away: game.awayTeam, start: Date.parse(game.commenceTime ?? ""), completed: game.completed,
    markets: Object.fromEntries((["spread", "total", "moneyline"] as const).map(market => [market, game.history.flatMap(c => {
      const books = Object.fromEntries(Object.entries(c.books).flatMap(([key, book]) => {
        const value = quoteValue(book, market);
        return value == null ? [] : [[key, value]];
      }));
      return Object.keys(books).length ? [{ time: Date.parse(c.capturedAt), books }] : [];
    })])),
  }));
}
export function tennisIntelligenceEvents(matches: Pick<TennisMatchRow, "id" | "homePlayer" | "awayPlayer" | "commenceTime" | "winner" | "completionStatus">[], movement: Pick<MlbLineMovementRow, "matchupId" | "trail">[]): IntelligenceEvent[] {
  return matches.map(match => ({
    id: match.id, home: match.homePlayer, away: match.awayPlayer, start: Date.parse(match.commenceTime ?? ""),
    completed: match.winner != null || !["scheduled", "pending", "not_started", "unknown"].includes(match.completionStatus.toLowerCase()),
    markets: { moneyline: (movement.find(row => row.matchupId === match.id)?.trail ?? []).map(point => ({ time: Date.parse(point.capturedAt), books: point.bookHomeProbs ?? {} })) },
  }));
}
