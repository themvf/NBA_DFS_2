import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import { isMissingRelation } from "@/lib/database-errors";

// Written by research/nba_wta_trios.py (docs/nba-wta-trios.md). Read-only here.
export const CONTEST_VERSION = "nba-wta-trios-v1";
export const CONTEST_GRADE_VERSION = "nba-wta-grade-v1";

export type ContestEntry = {
  rank: number;
  players: string[];
  names: string[];
  lineSum: number;
  simMean: number;
  simP90: number;
  marginalShare: number;
  portfolioShare: number;
  standaloneShare: number;
};

export type ContestPoolPlayer = {
  key: string;
  player: string;
  eventId: string;
  team: string | null;
  homeTeam: string;
  awayTeam: string;
  line: number;
  pOver: number | null;
  books: number;
  ladderBooks: number;
  floor: number | null;
  ceiling: number | null;
  p30: number | null;
  p40: number | null;
  dnpProb: number;
  fieldOwnership: number;
  simMean: number;
  simP90: number;
};

export type ContestPayload = {
  version: string;
  gameDate: string;
  sims: number;
  assumptions: { fieldSize: number; chalkShare: number; popularityTau: number; teammateRho: number; opponentRho: number };
  simulatedWinningScore: { p25: number; median: number; p75: number };
  pool: ContestPoolPlayer[];
  entries: ContestEntry[];
  baseline: ContestEntry[];
  inputs: { capturedAt: string; firstTip: string; games: number; quotedPlayers: number; duplicatesDropped: string[] };
};

export type ContestPortfolio = { id: number; createdAt: string; payload: ContestPayload };

type CellSide = { best: number; percentile: number; inContention: boolean; gapToBest: number; realizedShare: number };
export type ContestGrade = {
  gameDate: string;
  portfolioId: number;
  bestPoolTrio: number;
  contentionAt: number;
  ours: number[];
  baseline: number[];
  byN: Record<string, { ours: CellSide; baseline: CellSide }>;
};

const parse = <T,>(v: unknown): T => (typeof v === "string" ? JSON.parse(v) : v) as T;

/** The latest freeze for the date (the one the grade reads), or null. */
export async function getContestPortfolio(gameDate: string): Promise<ContestPortfolio | null> {
  try {
    const r = await db.execute(sql`
      SELECT id, created_at, payload FROM nba_wta_portfolios
      WHERE version = ${CONTEST_VERSION} AND game_date = ${gameDate}::date
      ORDER BY created_at DESC, id DESC LIMIT 1`);
    const row = r.rows[0] as { id: number | string; created_at: string | Date; payload: unknown } | undefined;
    if (!row) return null;
    return { id: Number(row.id), createdAt: new Date(row.created_at).toISOString(), payload: parse<ContestPayload>(row.payload) };
  } catch (e) {
    if (isMissingRelation(e)) return null;
    throw e;
  }
}

/** Dates with a freeze, newest first. */
export async function getContestDates(): Promise<string[]> {
  try {
    const r = await db.execute(sql`
      SELECT DISTINCT game_date::text AS d FROM nba_wta_portfolios WHERE version = ${CONTEST_VERSION} ORDER BY d DESC LIMIT 60`);
    return (r.rows as { d: string }[]).map((x) => x.d);
  } catch (e) {
    if (isMissingRelation(e)) return [];
    throw e;
  }
}

/** Every graded night, oldest first (the grade of the latest freeze per date). */
export async function getContestGrades(): Promise<ContestGrade[]> {
  try {
    const r = await db.execute(sql`
      SELECT DISTINCT ON (g.game_date) g.game_date::text AS game_date, g.portfolio_id,
        g.payload->'bestPoolTrio' AS best_pool_trio, g.payload->'contentionAt' AS contention_at,
        g.payload->'ours' AS ours, g.payload->'baseline' AS baseline, g.payload->'byN' AS by_n
      FROM nba_wta_grades g
      JOIN nba_wta_portfolios p ON p.id = g.portfolio_id
      WHERE g.version = ${CONTEST_VERSION} AND g.grade_version = ${CONTEST_GRADE_VERSION}
      ORDER BY g.game_date, p.created_at DESC, g.graded_at DESC`);
    return (r.rows as Record<string, unknown>[]).map((x) => ({
      gameDate: String(x.game_date),
      portfolioId: Number(x.portfolio_id),
      bestPoolTrio: Number(parse(x.best_pool_trio)),
      contentionAt: Number(parse(x.contention_at)),
      ours: parse<number[]>(x.ours),
      baseline: parse<number[]>(x.baseline),
      byN: parse<ContestGrade["byN"]>(x.by_n),
    }));
  } catch (e) {
    if (isMissingRelation(e)) return [];
    throw e;
  }
}
