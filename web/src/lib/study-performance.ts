import type { Observation } from "./daily-study-matches";

export function studySeries(rows: Observation[], through: string) {
  const groups = new Map<string, { key: string; id: Observation["id"]; market: Observation["market"]; cohort: string; rows: Observation[] }>();
  const seen = new Set<string>();
  for (const row of rows) {
    if (row.game.date > through || !row.observedAt || !(Date.parse(row.observedAt) < Date.parse(row.game.start))) continue;
    const key = JSON.stringify([row.id, row.market, row.cohort]);
    const identity = JSON.stringify([key, row.game.id]);
    if (seen.has(identity)) continue;
    seen.add(identity);
    const group = groups.get(key) ?? { key, id: row.id, market: row.market, cohort: row.cohort, rows: [] };
    group.rows.push(row);
    groups.set(key, group);
  }
  return [...groups.values()];
}

export function studyUnits(row: Observation) {
  const { price, margin } = row;
  if (price == null || !Number.isFinite(price) || Math.abs(price) < 100 || margin === null) return null;
  return margin > 0 ? (price > 0 ? price / 100 : 100 / -price) : margin < 0 ? -1 : 0;
}

export function studyPerformance(rows: Observation[]) {
  let wins = 0, losses = 0, pushes = 0, pending = 0, priced = 0, units = 0;
  let implied = 0, pricedDecisions = 0, pricedWins = 0;
  const days = new Map<string, { n: number; units: number }>();
  for (const row of rows) {
    if (row.margin === null) { pending++; continue; }
    if (row.margin > 0) wins++;
    else if (row.margin < 0) losses++;
    else pushes++;
    const pnl = studyUnits(row);
    if (pnl === null) continue;
    priced++;
    units += pnl;
    if (row.margin !== 0) {
      const price = row.price!;
      implied += price > 0 ? 100 / (100 + price) : -price / (100 - price);
      pricedDecisions++;
      if (row.margin > 0) pricedWins++;
    }
    const day = days.get(row.game.date) ?? { n: 0, units: 0 };
    day.n++;
    day.units += pnl;
    days.set(row.game.date, day);
  }
  const roi = priced ? units / priced : null;
  // Date-clustered normal approximation to the ratio of units to stakes.
  // Show it only where there are enough date clusters for the approximation to be informative.
  const clusters = [...days.values()];
  const squared = roi === null ? 0 : clusters.reduce((sum, day) => sum + (day.units - day.n * roi) ** 2, 0);
  const halfWidth = clusters.length >= 20 && squared > 0
    ? 1.96 * Math.sqrt(squared * clusters.length / (clusters.length - 1)) / priced : null;
  const range = roi !== null && halfWidth !== null ? [roi - halfWidth, roi + halfWidth] as const : null;
  let running = 0, peak = 0, drawdown = 0;
  const trend = [...days.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([date, day]) => {
    running += day.units;
    peak = Math.max(peak, running);
    drawdown = Math.max(drawdown, peak - running);
    return { date, units: Number(running.toFixed(3)) };
  });
  return { wins, losses, pushes, pending, priced, units, roi, range, trend, drawdown, priceDates: days.size,
    averageBreakEven: pricedDecisions ? implied / pricedDecisions : null,
    pricedHitRate: pricedDecisions ? pricedWins / pricedDecisions : null,
    hitRate: wins + losses ? wins / (wins + losses) : null,
    missingPrice: wins + losses + pushes - priced };
}

export function studyEvidence(cohort: string) {
  if (cohort === "Frozen board entries") return "Frozen CFB entries";
  if (cohort === "Over 235+ paper trial") return "NBA paper tracking";
  return "Exploratory archive";
}

