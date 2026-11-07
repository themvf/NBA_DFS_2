import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

/** Reader for model/first_td_stats.py: the newest stored season stats for one sport. */

export type FirstTdCalibrationBin = { from: number; to: number | null; forecasts: number; stated: number | null; happened: number | null };
export type FirstTdRecord = {
  games: number; log_loss: number; top_pick_hit: number | null; top_pick_stated: number | null;
  calibration: FirstTdCalibrationBin[];
};
export type FirstTdBaseRate = {
  season: string; games: number; rush: number | null; rec: number | null; non_offense: number | null;
  qb_rush: number | null; qb_known: number; favourite_first: number | null; favourite_games: number; home_first: number | null;
};
export type FirstTdStats = {
  computedAt: string;
  gradedVersion: string;
  record: FirstTdRecord | null;
  baseRates: FirstTdBaseRate[];
  leaders: { season: number; players: { name: string; team: string; firsts: number }[];
    tied?: { firsts: number; players: number } | null; teams: { team: string; firsts: number; games: number }[] };
};

export async function getFirstTdStats(sport: "nfl" | "cfb"): Promise<FirstTdStats | null> {
  try {
    const result = await db.execute(sql`
      SELECT computed_at, stats FROM first_td_stats WHERE sport = ${sport} ORDER BY computed_at DESC LIMIT 1`);
    const row = result.rows[0] as { computed_at: string; stats: Record<string, unknown> } | undefined;
    if (!row) return null;
    const s = row.stats;
    return {
      computedAt: new Date(row.computed_at).toISOString(),
      gradedVersion: s.graded_version as string,
      record: (s.record as FirstTdRecord | null) ?? null,
      baseRates: s.base_rates as FirstTdBaseRate[],
      leaders: s.leaders as FirstTdStats["leaders"],
    };
  } catch {
    return null; // table not created yet: the page shows a note instead
  }
}
