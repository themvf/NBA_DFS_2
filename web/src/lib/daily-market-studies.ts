import { summarize as nbaSummary } from "./nba-market-studies";
import { summarize as nflSummary } from "./nfl/market-studies";
import type { ResultRow } from "../db/cfb-pattern-watch";

type NbaRows = Parameters<typeof nbaSummary>[0];
type NflRows = Parameters<typeof nflSummary>[0];

export const DAILY_STUDIES_PATH = "/sports/market-studies";

export function shiftDate(date: string, days: number) {
  const value = new Date(date + "T12:00:00Z");
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

export function studyDate(value: unknown, today: string): string {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value) || value > today || value < "1900-01-01") return today;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString().slice(0, 10) === value ? value : today;
}

export function nflSeason(date: string) {
  return Number(date.slice(0, 4)) - (Number(date.slice(5, 7)) <= 2 ? 1 : 0);
}

export function nbaDay(rows: NbaRows) {
  const summary = nbaSummary(rows);
  return {
    games: rows.length,
    finals: rows.filter(row => row.margin !== null).length,
    ats: summary.spreadGraded,
    totals: summary.totals.find(row => row.label === "Last quote · over")!,
    paper: summary.paper,
  };
}

export function nflDay(rows: NflRows) {
  const summary = nflSummary(rows);
  return {
    ...summary.coverage,
    weeks: [...new Set(rows.map(row => row.game.week))],
    ats: summary.spread[0],
    over: summary.totals.find(row => row.label === "Last archive, all · over")!,
    moneylineFavorite: summary.moneyline[0],
  };
}

export function cfbDay(rows: ResultRow[]) {
  const groups = new Map<string, { wins: number; losses: number; pushes: number; units: number }>();
  for (const row of rows) {
    const result = groups.get(row.Pattern) ?? { wins: 0, losses: 0, pushes: 0, units: 0 };
    if (row.Result === "won") result.wins++;
    else if (row.Result === "lost") result.losses++;
    else if (row.Result === "push") result.pushes++;
    else continue;
    result.units += row.Units;
    groups.set(row.Pattern, result);
  }
  return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
}
