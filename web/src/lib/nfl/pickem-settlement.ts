import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ensurePickemTables } from "@/db/ensure-schema";

/**
 * Settle every pick'em recommendation whose games have final scores.
 *
 * Pure arithmetic over nfl_season_games. Ties use the immutable card's chosen
 * tie points. Result revisions are retained separately from the frozen forecast.
 *
 * Idempotent. Re-running re-derives every graded field from current scores, so
 * a corrected score simply produces a corrected settlement. Shared by the page's
 * "Settle completed weeks" button and the scheduled /api/cron/pickem-settle,
 * which exists because a card graded only on a button press stayed "pending"
 * for two weeks (the week 8 card, 2026-11-14).
 */
export async function settlePickemLedger(season: number): Promise<{ regraded: number; settled: number }> {
  await ensurePickemTables();
  // Stamp each frozen game with its real result.
  await db.execute(sql`
    UPDATE pickem_recommendation_games g
    SET result_revisions = CASE WHEN s.completed AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL
        AND (jsonb_array_length(g.result_revisions)=0 OR
          (g.result_revisions->-1->>'homeScore')::int IS DISTINCT FROM s.home_score OR
          (g.result_revisions->-1->>'awayScore')::int IS DISTINCT FROM s.away_score)
        THEN g.result_revisions || jsonb_build_array(jsonb_build_object('homeScore',s.home_score,'awayScore',s.away_score,'recordedAt',NOW()))
        ELSE g.result_revisions END,
        result_tie = CASE WHEN s.completed AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL THEN s.home_score=s.away_score ELSE NULL END,
        home_won = CASE
          WHEN NOT s.completed THEN NULL
          WHEN s.home_score IS NULL OR s.away_score IS NULL THEN NULL
          WHEN s.home_score = s.away_score THEN NULL
          ELSE s.home_score > s.away_score
        END,
        baseline_correct = CASE
          WHEN NOT s.completed THEN NULL
          WHEN s.home_score IS NULL OR s.away_score IS NULL OR s.home_score = s.away_score THEN NULL
          ELSE g.baseline_pick_home = (s.home_score > s.away_score)
        END,
        recommended_correct = CASE
          WHEN NOT s.completed THEN NULL
          WHEN s.home_score IS NULL OR s.away_score IS NULL OR s.home_score = s.away_score THEN NULL
          ELSE g.recommended_pick_home = (s.home_score > s.away_score)
        END
    FROM nfl_season_games s, pickem_recommendations r
    WHERE g.game_id = s.id
      AND g.recommendation_id = r.id
      AND r.season = ${season}
      AND r.status <> 'void'
  `);

  // Roll the per-game results up to the recommendation.
  const updated = await db.execute(sql`
    UPDATE pickem_recommendations r
    -- Every graded field stays NULL until something is actually graded.
    -- COALESCE-ing to 0 here would render an unplayed week as a real 0-0
    -- result with a +0 delta, which is the "absent must not look like zero"
    -- rule this whole page is built on.
    SET games_graded = agg.graded,
        baseline_actual_points = CASE WHEN agg.graded > 0 THEN agg.baseline_points END,
        recommended_actual_points = CASE WHEN agg.graded > 0 THEN agg.recommended_points END,
        baseline_correct = CASE WHEN agg.graded > 0 THEN agg.baseline_hits END,
        recommended_correct = CASE WHEN agg.graded > 0 THEN agg.recommended_hits END,
        max_possible_points = CASE WHEN agg.graded > 0 THEN agg.max_points END,
        brier = agg.brier,
        coinflip_brier = agg.coinflip_brier,
        status = CASE WHEN agg.graded > 0 AND agg.graded = r.games_total THEN 'settled' ELSE r.status END,
        -- First settlement time; a re-run (or a corrected score) keeps it.
        settled_at = CASE
          WHEN agg.graded > 0 AND agg.graded = r.games_total THEN COALESCE(r.settled_at, NOW()) ELSE r.settled_at
        END
    FROM (
      SELECT
        g.recommendation_id AS rec_id,
        COUNT(*) FILTER (WHERE g.home_won IS NOT NULL OR g.result_tie) AS graded,
        COALESCE(SUM(CASE WHEN g.result_tie THEN g.baseline_confidence * COALESCE((g.evidence_json->>'tiePoints')::float,0)
          WHEN g.baseline_correct THEN g.baseline_confidence ELSE 0 END), 0) AS baseline_points,
        COALESCE(SUM(CASE WHEN g.result_tie THEN g.recommended_confidence * COALESCE((g.evidence_json->>'tiePoints')::float,0)
          WHEN g.recommended_correct THEN g.recommended_confidence ELSE 0 END), 0) AS recommended_points,
        COUNT(*) FILTER (WHERE g.baseline_correct) AS baseline_hits,
        COUNT(*) FILTER (WHERE g.recommended_correct) AS recommended_hits,
        COALESCE(SUM(CASE WHEN g.result_tie THEN g.recommended_confidence * COALESCE((g.evidence_json->>'tiePoints')::float,0)
          WHEN g.home_won IS NOT NULL THEN g.recommended_confidence ELSE 0 END), 0) AS max_points,
        AVG(POWER(g.p_home - CASE WHEN g.home_won THEN 1 ELSE 0 END, 2))
          FILTER (WHERE g.home_won IS NOT NULL) AS brier,
        AVG(POWER(0.5 - CASE WHEN g.home_won THEN 1 ELSE 0 END, 2))
          FILTER (WHERE g.home_won IS NOT NULL) AS coinflip_brier
      FROM pickem_recommendation_games g
      GROUP BY g.recommendation_id
    ) agg
    WHERE agg.rec_id = r.id
      AND r.season = ${season}
      AND r.status <> 'void'
    RETURNING r.id, r.status
  `);

  const settled = updated.rows.filter(
    (row) => String((row as Record<string, unknown>).status) === "settled",
  ).length;
  return { regraded: updated.rows.length, settled };
}

/** Seasons with a card that is not void, newest first. */
export async function pickemLedgerSeasons(): Promise<number[]> {
  await ensurePickemTables();
  const rows = await db.execute(sql`SELECT DISTINCT season FROM pickem_recommendations WHERE status <> 'void' ORDER BY season DESC`);
  return rows.rows.map((r) => Number((r as Record<string, unknown>).season));
}
