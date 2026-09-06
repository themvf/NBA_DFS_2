/** MLB-only display contract. No database or paid API dependencies. */
export type MlbMarket = "moneyline" | "run_line" | "total";
export type MlbSide = "home" | "away" | "over" | "under";
export type MlbBook = Record<string, unknown>;
export type MlbCapture = { id: number; capturedAt: string; books: Record<string, MlbBook> };
export type MlbTerminalGame = {
  id: number; gamePk: string | null; date: string; startsAt: string | null;
  home: string; away: string; status: string; homeScore: number | null; awayScore: number | null;
  homeStarter: string | null; awayStarter: string | null; park: string | null;
  history: MlbCapture[]; close: MlbCapture | null; closeQuality: string | null;
  closeBoundary: string | null;
};
export type MlbTerminalSignal = {
  id: number; matchupId: number; date: string; matchup: string; type: string; side: string;
  observedAt: string; outcome: string | null; details: Record<string, unknown>;
  grade: Record<string, unknown>; clvPp: number | null;
};
export type MlbTerminalBoard = {
  date: string; asOf: string; games: MlbTerminalGame[]; signals: MlbTerminalSignal[];
  issues: string[]; auditFrom: string;
};
export const MLB_GAME_SIGNALS = ["pinnacle_divergence", "pinnacle_polymarket_delta", "steam", "walking", "dk_value",
  "mlb_total_price_steam", "mlb_total_price_walking", "mlb_total_price_reversal", "mlb_run_line_points_steam", "mlb_run_line_points_walking", "mlb_run_line_points_reversal",
  "mlb_total_steam", "mlb_total_walking", "mlb_total_reversal", "mlb_run_line_steam", "mlb_run_line_walking", "mlb_run_line_reversal", "mlb_moneyline_reversal"];
export const MLB_TERMINAL_BOOKS = ["draftkings", "fanduel", "betmgm", "betrivers", "pinnacle", "fanatics", "williamhill_us", "bovada", "betonlineag"];
export function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
export function american(price: unknown): number | null {
  const n = number(price); return n != null && Number.isInteger(n) && Math.abs(n) >= 100 ? n : null;
}
export function probability(price: number): number {
  return price > 0 ? 100 / (price + 100) : -price / (100 - price);
}
export function decimal(price: number): number { return price > 0 ? 1 + price / 100 : 1 + 100 / -price; }
export function fmtPrice(value: unknown): string {
  const price = american(value); return price == null ? "—" : `${price > 0 ? "+" : ""}${price}`;
}
export function normalizeMlbDate(date: string | undefined, now = new Date()): string {
  if (date && /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(Date.parse(`${date}T12:00:00Z`)) && new Date(`${date}T12:00:00Z`).toISOString().slice(0, 10) === date) return date;
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}
export type MlbQuote = { line: number | null; price: number; fair: number | null; updatedAt: string | null };
export function quote(book: MlbBook, market: MlbMarket, side: MlbSide): MlbQuote | null {
  if (market === "total" ? !["over", "under"].includes(side) : !["home", "away"].includes(side)) return null;
  const key = market === "moneyline" ? `ml_${side}` : market === "total" ? side : `spread_${side}_price`;
  const price = american(book[key] ?? (market === "run_line" && side === "home" ? book.spread_price : null));
  const line = market === "moneyline" ? null : number(market === "total" ? book[`${side}_line`] ?? book.total_line : book[`spread_${side}`]);
  if (price == null || (market !== "moneyline" && line == null)) return null;
  const opposite = side === "home" ? "away" : side === "away" ? "home" : side === "over" ? "under" : "over";
  const pairedPrice = american(market === "moneyline" ? book[`ml_${opposite}`] : market === "total" ? book[opposite] : book[`spread_${opposite}_price`] ?? (opposite === "home" ? book.spread_price : null));
  const pairedLine = market === "total" ? number(book[`${opposite}_line`] ?? book.total_line) : number(book[`spread_${opposite}`]);
  const paired = pairedPrice != null && (market === "moneyline" || (market === "total" ? pairedLine === line : pairedLine === -(line ?? 0)));
  const timestamp = book[`${market === "run_line" ? "spreads" : market === "moneyline" ? "h2h" : "totals"}_last_update`] ?? book.last_update;
  return { line, price, fair: paired ? probability(price) / (probability(price) + probability(pairedPrice!)) : null, updatedAt: typeof timestamp === "string" ? timestamp : null };
}
export function marketTrail(history: MlbCapture[], market: MlbMarket, side: MlbSide, selectedBook = "", priceView = false) {
  let usable = history.filter((capture) => Object.entries(capture.books).some(([key, book]) => MLB_TERMINAL_BOOKS.includes(key) && (!selectedBook || key === selectedBook) && quote(book, market, side)));
  if (priceView) {
    if (!selectedBook) usable = [];
    else {
      const last = usable.at(-1);
      const currentLine = last && quote(last.books[selectedBook], market, side)?.line;
      usable = usable.filter((capture) => quote(capture.books[selectedBook], market, side)?.line === currentLine);
    }
  }
  if (!usable.length) return { points: [] as { at: string; value: number; price: number | null }[], books: [] as string[] };
  // Fixed intersection across the plotted trail prevents book turnover appearing as a move.
  const books = Object.keys(usable[0].books).filter((key) => MLB_TERMINAL_BOOKS.includes(key) && (!selectedBook || key === selectedBook) && usable.every((capture) => {
    const q = capture.books[key] && quote(capture.books[key], market, side);
    return q && (market !== "moneyline" || q.fair != null);
  }));
  const points = usable.flatMap((capture) => {
    const quotes = books.map((key) => quote(capture.books[key], market, side)!);
    if (!quotes.length) return [];
    const values = quotes.map((q) => priceView ? probability(q.price) * 100 : market === "moneyline" ? q.fair! * 100 : q.line!).sort((a, b) => a - b);
    return [{ at: capture.capturedAt, value: market === "moneyline" ? values.reduce((a, b) => a + b, 0) / values.length : values[Math.floor((values.length - 1) / 2)], price: selectedBook ? quotes[0].price : null }];
  });
  return { points, books };
}
export function signalMarket(signal: MlbTerminalSignal): MlbMarket {
  return signal.details.market === "total" ? "total" : ["spread", "run_line"].includes(String(signal.details.market)) ? "run_line" : "moneyline";
}
export function signalOutcome(signal: MlbTerminalSignal): string {
  if (signal.outcome === "void" && signal.grade.settlement_reason === "push") return "push";
  return signal.outcome ?? "pending";
}
export function summarizeSignals(signals: MlbTerminalSignal[]) {
  const groups = new Map<string, { type: string; version: string; market: MlbMarket; n: number; wins: number; losses: number; pushes: number; voids: number; pending: number; unavailable: number; priced: number; units: number; clv: number[]; clvUnit: string; dates: Set<string> }>();
  for (const signal of signals) {
    const version = `${String(signal.details.signal_version ?? signal.details.program_version ?? "legacy")}${signal.details.capture_policy ? ` / ${signal.details.capture_policy}` : ""}`;
    const market = signalMarket(signal);
    const clvUnit = String(signal.grade.clv_unit ?? (signal.details.metric === "fair_probability" ? "pp" : signal.details.metric === "runs" || market === "total" ? "runs" : "pp"));
    const key = `${signal.type}:${version}:${market}:${clvUnit}`;
    const row = groups.get(key) ?? { type: signal.type, version, market, n: 0, wins: 0, losses: 0, pushes: 0, voids: 0, pending: 0, unavailable: 0, priced: 0, units: 0, clv: [], clvUnit, dates: new Set<string>() };
    row.n++; row.dates.add(signal.date);
    const result = signalOutcome(signal);
    if (result === "won") row.wins++; else if (result === "lost") row.losses++; else if (result === "push") row.pushes++; else if (result === "void") row.voids++; else if (result === "pending") row.pending++; else row.unavailable++;
    const price = number(signal.details.dk_decimal);
    if (price != null && price > 1 && ["won", "lost", "push"].includes(result)) {
      row.priced++; row.units += result === "won" ? price - 1 : result === "lost" ? -1 : 0;
    }
    // Only explicitly verified grades enter the primary closing-line audit.
    const clv = number(signal.grade.verified_clv) ?? (
      signal.type !== "pinnacle_polymarket_delta" && signal.grade.close_cohort === "verified_clv_v1"
        ? signal.clvPp : null
    );
    if (clv != null) row.clv.push(clv);
    groups.set(key, row);
  }
  return [...groups.values()].sort((a, b) => a.type.localeCompare(b.type));
}
