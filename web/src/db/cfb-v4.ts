import { sql } from "drizzle-orm";
import { db } from ".";

export type CfbV4Evidence = {
  home: string; away: string; generated_at: string; status: string;
  raw: { home_points: number; away_points: number; expected_drives: number[]; rates: number[]; other_points: number[] };
  forecast?: { home_points: number; away_points: number; margin: number; total: number; home_win_probability: number | null };
  market: { book: string; captured_at: string; history_id: number } | null;
  context: { complete: boolean; reasons: string[]; numeric_adjustment_status: string };
  reasons: string[]; primary_eligible: boolean;
  missing_history_games?: { home: number[]; away: number[] };
  input_coverage: { home_games: number; away_games: number; home_ppa_plays: number; away_ppa_plays: number };
  propositions?: Record<string, { p_win: number | null; p_push: number | null; p_loss: number | null; price: number; line?: number; ev: number | null; status: string }>;
};

function missingSchema(error: unknown): boolean {
  const value = error as { code?: string; cause?: { code?: string } };
  return value.code === "42P01" || value.cause?.code === "42P01";
}

export async function getCfbV4Snapshot(gameId: number): Promise<{ frozenAt: string; evidence: CfbV4Evidence } | null> {
  try {
    const result = await db.execute(sql`SELECT s.frozen_at::text AS "frozenAt", s.evidence_json AS evidence
      FROM cfb_v4_snapshots s JOIN cfb_matchups m ON m.id=s.game_id
      JOIN cfb_v4_runs r ON r.id=s.run_id
      WHERE s.game_id=${gameId} AND s.kickoff=m.commence_time AND s.frozen_at<m.commence_time
        AND r.protocol_version='cfb-drive-market-v4.1'
      ORDER BY s.frozen_at DESC,s.id DESC LIMIT 1`);
    return result.rows[0] as { frozenAt: string; evidence: CfbV4Evidence } | undefined ?? null;
  } catch (error) { if (missingSchema(error)) return null; throw error; }
}

export async function getCfbV4Study(): Promise<{ registeredAt: string; digest: string; gitSha: string; report: Record<string, unknown> | null; evaluation: Record<string, unknown> | null } | null> {
  try {
    const result = await db.execute(sql`SELECT p.registered_at::text AS "registeredAt",p.digest,p.git_sha AS "gitSha",
      r.report_json AS report,e.report_json AS evaluation FROM cfb_v4_protocols p
      LEFT JOIN LATERAL(SELECT report_json FROM cfb_v4_runs WHERE protocol_version=p.version ORDER BY id DESC LIMIT 1) r ON TRUE
      LEFT JOIN LATERAL(SELECT report_json FROM cfb_prospective_evaluation_runs WHERE version=p.version ORDER BY id DESC LIMIT 1) e ON TRUE
      WHERE p.version='cfb-drive-market-v4.1'`);
    return result.rows[0] as { registeredAt: string; digest: string; gitSha: string; report: Record<string, unknown> | null; evaluation: Record<string, unknown> | null } | undefined ?? null;
  } catch (error) { if (missingSchema(error)) return null; throw error; }
}
