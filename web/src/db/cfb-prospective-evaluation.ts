import { sql } from "drizzle-orm";
import { db } from ".";

export type CfbEvaluatedMarket = {
  n: number; game_dates: number;
  model_error: number | null; market_error: number | null;
  model_minus_market: number | null; model_minus_market_ci95: number[] | null;
  anchor_n: number; anchor_error?: number; anchor_market_error?: number;
  anchor_minus_market?: number; anchor_minus_market_ci95?: number[] | null;
  verified_close_n?: number; directional_close_move?: number | null;
  model_logloss?: number; market_logloss?: number; anchor_logloss?: number;
  model_calibration_bias?: number; market_calibration_bias?: number;
  anchor_calibration_bias?: number;
};

export type CfbEvaluationCoverage = {
  frozen: number; awaiting_final: number; final: number;
  verified_close: number; final_without_verified_close: number;
  eligible: Record<string, number>; excluded: Record<string, number>;
  reasons: Record<string, number>;
};

export type CfbEvaluationGame = {
  id: number; date: string; kickoff: string; game: string;
  status: "upcoming" | "awaiting_final" | "final";
  verified_close: boolean; captured: boolean; eligible_markets: string[];
};

export type CfbProspectiveEvaluation = {
  version: string; season: number; generated_at: string;
  coverage: Record<string, CfbEvaluationCoverage>;
  market_comparison: Record<string, Record<string, CfbEvaluatedMarket>>;
  strict_same_capture: Record<string, {
    n: number; game_dates: number; errors: Record<string, number | null>;
    market_error: number | null; v3_minus_v2: number | null;
    v3_minus_v2_ci95: number[] | null;
  }>;
  games: CfbEvaluationGame[];
  interpretation: string;
};

export async function getCfbProspectiveEvaluation(): Promise<CfbProspectiveEvaluation | null> {
  const rows = await db.execute(sql`
    SELECT report_json AS report
    FROM cfb_prospective_evaluation_runs
    WHERE version='cfb-prospective-evaluation-v1'
    ORDER BY id DESC LIMIT 1
  `);
  if (!rows.rows.length) return null;
  return rows.rows[0].report as CfbProspectiveEvaluation;
}
