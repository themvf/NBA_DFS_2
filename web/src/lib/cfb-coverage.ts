import { selectedSportsbooks } from "@/lib/sportsbook-policy";

export type MarketName = "spread" | "total" | "moneyline";
export type MarketCoverage = { books: number; freshAtCapture: number; sameBooksAsPrevious: number | null };
type Quote = Record<string, unknown>;
type Books = Record<string, Quote>;

function number(value: unknown): boolean {
  return value !== null && value !== undefined && Number.isFinite(Number(value));
}

function hasMarket(quote: Quote, market: MarketName): boolean {
  if (market === "spread") return ["spread_home", "spread_away", "spread_home_price", "spread_away_price"].every((field) => number(quote[field]));
  if (market === "total") return ["total_line", "over", "under"].every((field) => number(quote[field]));
  return ["ml_home", "ml_away"].every((field) => number(quote[field]));
}

export function marketCoverage(
  current: Books | null,
  previous: Books | null,
  capturedAt: string | null,
): Record<MarketName, MarketCoverage> {
  const books = selectedSportsbooks(current);
  const prior = previous === null ? null : selectedSportsbooks(previous);
  return Object.fromEntries((["spread", "total", "moneyline"] as const).map((market) => {
    const eligible = Object.entries(books).filter(([, quote]) => hasMarket(quote, market));
    const capturedMs = capturedAt ? Date.parse(capturedAt) : NaN;
    const freshAtCapture = eligible.filter(([, quote]) => {
      const updatedMs = typeof quote.last_update === "string" ? Date.parse(quote.last_update) : NaN;
      const age = capturedMs - updatedMs;
      return Number.isFinite(age) && age >= -90_000 && age <= 5 * 60_000;
    }).length;
    const sameBooksAsPrevious = prior === null ? null : eligible.filter(([key]) => prior[key] && hasMarket(prior[key], market)).length;
    return [market, { books: eligible.length, freshAtCapture, sameBooksAsPrevious }];
  })) as Record<MarketName, MarketCoverage>;
}

export function captureAgeMinutes(capturedAt: string | null, asOf: string): number | null {
  if (!capturedAt) return null;
  const minutes = (Date.parse(asOf) - Date.parse(capturedAt)) / 60_000;
  return Number.isFinite(minutes) && minutes >= 0 ? Math.round(minutes) : null;
}

export function coverageIssues(input: {
  mapped: boolean; kickoff: string; asOf: string; capturedAt: string | null;
  markets: Record<MarketName, MarketCoverage>; dueCheckpoint: boolean; missedCheckpoint: boolean;
}): string[] {
  const issues: string[] = [];
  if (!input.mapped) issues.push("No odds-event mapping");
  if (input.dueCheckpoint) issues.push("Checkpoint due now");
  if (input.missedCheckpoint) issues.push("Missed checkpoint");
  const age = captureAgeMinutes(input.capturedAt, input.asOf);
  const leadMinutes = (Date.parse(input.kickoff) - Date.parse(input.asOf)) / 60_000;
  if (age === null) {
    issues.push("No pregame lines");
    return issues;
  }
  if (leadMinutes <= 720 && age > (leadMinutes <= 360 ? 25 : 90)) issues.push("Capture overdue");
  for (const market of ["spread", "total", "moneyline"] as const) {
    if (input.markets[market].books < 3) issues.push(`${market} has fewer than 3 books`);
    else if (input.markets[market].freshAtCapture < 3) issues.push(`${market} has fewer than 3 fresh books`);
  }
  return issues;
}
