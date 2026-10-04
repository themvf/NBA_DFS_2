/**
 * Consensus of one side's moneyline across books.
 *
 * American prices are not a linear scale: -110 and +110 are both "about even"
 * but their arithmetic mean is 0, and any mixed-sign pair near even money
 * averages to a number strictly inside (-100, +100), which is not a price any
 * book can quote. The Python MLB ingest was repaired for exactly this on
 * 2026-07-02 (see CLAUDE.md, "arithmetic-averaging American odds"); the web
 * writers for NBA still averaged the raw numbers. Average in probability
 * space and convert back, the way every other consensus in this repo does.
 */
export function americanToImplied(price: number): number {
  return price >= 0 ? 100 / (price + 100) : -price / (-price + 100);
}

export function impliedToAmerican(prob: number): number {
  if (prob >= 0.5) return Math.round(-100 * prob / (1 - prob));
  return Math.round(100 * (1 - prob) / prob);
}

/** Probability-space mean of several American prices, back as an American price. */
export function consensusAmerican(prices: number[]): number | null {
  const usable = prices.filter((p) => Number.isFinite(p) && p !== 0 && Math.abs(p) >= 100);
  if (usable.length === 0) return null;
  const mean = usable.reduce((sum, p) => sum + americanToImplied(p), 0) / usable.length;
  return impliedToAmerican(mean);
}

/** True when a price sits in the impossible band no sportsbook can quote. */
export function isImpossibleAmerican(price: number): boolean {
  return price > -100 && price < 100;
}

/** Vig-free home probability from a consensus pair; null when either side is missing. */
export function noVigHomeProbability(homeMl: number | null, awayMl: number | null): number | null {
  if (homeMl == null || awayMl == null) return null;
  const home = americanToImplied(homeMl);
  const away = americanToImplied(awayMl);
  return home / (home + away);
}
