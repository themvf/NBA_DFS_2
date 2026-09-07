"use server";

/**
 * Server actions for the pick'em recommendation ledger.
 *
 * Three rules this file enforces, all of them refusals:
 *
 *  1. A card cannot be frozen after the week's first kickoff. A recommendation
 *     recorded once a result is known is hindsight wearing a timestamp, and a
 *     ledger that accepts one cannot be trusted for any of the others.
 *  2. A recommendation is never rewritten. Changing your mind inserts a new
 *     row and supersedes the old one, which stays readable forever.
 *  3. Settlement never invents a pool outcome. Points and calibration settle
 *     automatically from real scores; whether the entry actually WON is the
 *     one thing we cannot observe, so it is entered by hand or left null --
 *     never defaulted to a loss.
 */

import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { ensurePickemTables } from "@/db/ensure-schema";
import { sql } from "drizzle-orm";

export type ActionResult = { ok: true; message: string } | { ok: false; error: string };

export type FrozenGame = {
  gameId: number;
  homeTeamId: number;
  awayTeamId: number;
  pHome: number;
  provenance: string;
  kickoff: string | null;
  baselinePickHome: boolean;
  baselineConfidence: number;
  recommendedPickHome: boolean;
  recommendedConfidence: number;
  fieldHomeShare: number;
  fieldSource: "observed" | "modeled";
};

export async function createPickemPool(input: {
  name: string;
  season: number;
  format: "confidence" | "straight";
  poolEntries: number;
  notes: string | null;
}): Promise<ActionResult> {
  const name = input.name.trim();
  if (!name) return { ok: false, error: "Name the pool so you can tell it from the others." };
  if (!Number.isFinite(input.poolEntries) || input.poolEntries < 1) {
    return { ok: false, error: "Pool entries must be at least 1." };
  }

  await ensurePickemTables();
  try {
    await db.execute(sql`
      INSERT INTO pickem_pools (name, season, format, pool_entries, notes)
      VALUES (${name}, ${input.season}, ${input.format}, ${Math.round(input.poolEntries)},
              ${input.notes?.trim() || null})
    `);
    revalidatePath("/nfl/pickem");
    return { ok: true, message: `Created ${name}.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not create the pool." };
  }
}

export async function deletePickemPool(poolId: number): Promise<ActionResult> {
  await ensurePickemTables();
  try {
    await db.execute(sql`DELETE FROM pickem_pools WHERE id = ${poolId}`);
    revalidatePath("/nfl/pickem");
    return { ok: true, message: "Pool deleted, along with its ledger rows." };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not delete the pool." };
  }
}

/**
 * Freeze this week's card into the append-only ledger.
 *
 * Both entries go in. The baseline is the provably-optimal max-points card and
 * is the counterfactual the whole ledger exists to measure against; freezing it
 * now rather than rebuilding it later is what makes the comparison paired
 * rather than reconstructed.
 */
export async function freezePickemRecommendation(input: {
  poolId: number | null;
  season: number;
  week: number;
  format: "confidence" | "straight";
  objective: "ev" | "win";
  poolEntries: number;
  sims: number;
  modelVersion: string;
  baselineExpectedPoints: number;
  recommendedExpectedPoints: number;
  baselinePrizeShare: number;
  recommendedPrizeShare: number;
  fieldModel: Record<string, unknown>;
  deviations: unknown[];
  games: FrozenGame[];
}): Promise<ActionResult> {
  if (input.games.length === 0) {
    return { ok: false, error: "There is nothing to freeze — this week has no games with a win probability." };
  }

  await ensurePickemTables();
  try {
    // The lock is the week's FIRST kickoff: a pick'em card is submitted whole,
    // so the moment any game on it starts, the card is no longer a forecast.
    // Verified against the schedule rather than trusted from the client.
    const kickoffRow = await db.execute(sql`
      SELECT MIN(kickoff) AS "locksAt", COUNT(*) FILTER (WHERE kickoff <= NOW()) AS started
      FROM nfl_season_games
      WHERE season = ${input.season} AND week = ${input.week}
    `);
    const kick = kickoffRow.rows[0] as Record<string, unknown> | undefined;
    const locksAt = kick?.locksAt != null ? String(kick.locksAt) : null;
    if (Number(kick?.started ?? 0) > 0) {
      return {
        ok: false,
        error:
          `Week ${input.week} has already started. A card frozen after kickoff is hindsight, ` +
          `not a recommendation — the ledger will not record one.`,
      };
    }

    const inserted = await db.execute(sql`
      INSERT INTO pickem_recommendations
        (pool_id, season, week, format, objective, pool_entries, sims, model_version,
         baseline_expected_points, recommended_expected_points,
         baseline_prize_share, recommended_prize_share,
         field_model_json, deviations_json, locks_at, games_total)
      VALUES (${input.poolId}, ${input.season}, ${input.week}, ${input.format}, ${input.objective},
              ${Math.round(input.poolEntries)}, ${Math.round(input.sims)}, ${input.modelVersion},
              ${input.baselineExpectedPoints}, ${input.recommendedExpectedPoints},
              ${input.baselinePrizeShare}, ${input.recommendedPrizeShare},
              ${JSON.stringify(input.fieldModel)}::jsonb,
              ${JSON.stringify(input.deviations)}::jsonb,
              ${locksAt}::timestamptz, ${input.games.length})
      RETURNING id
    `);
    const recId = Number((inserted.rows[0] as Record<string, unknown>).id);

    for (const g of input.games) {
      await db.execute(sql`
        INSERT INTO pickem_recommendation_games
          (recommendation_id, game_id, home_team_id, away_team_id, p_home, provenance, kickoff,
           baseline_pick_home, baseline_confidence, recommended_pick_home, recommended_confidence,
           field_home_share, field_source)
        VALUES (${recId}, ${g.gameId}, ${g.homeTeamId}, ${g.awayTeamId}, ${g.pHome},
                ${g.provenance}, ${g.kickoff}::timestamptz,
                ${g.baselinePickHome}, ${g.baselineConfidence},
                ${g.recommendedPickHome}, ${g.recommendedConfidence},
                ${g.fieldHomeShare}, ${g.fieldSource})
        ON CONFLICT (recommendation_id, game_id) DO NOTHING
      `);
    }

    // Supersede -- never overwrite. The old row stays readable.
    await db.execute(sql`
      UPDATE pickem_recommendations
      SET superseded_by = ${recId}
      WHERE season = ${input.season} AND week = ${input.week}
        AND id <> ${recId} AND superseded_by IS NULL
        AND pool_id IS NOT DISTINCT FROM ${input.poolId}
    `);

    revalidatePath("/nfl/pickem");
    return {
      ok: true,
      message: `Week ${input.week} frozen: ${input.games.length} games, both entries recorded.`,
    };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not freeze the card." };
  }
}

/**
 * Settle every pending recommendation whose games have final scores.
 *
 * Pure arithmetic over nfl_season_games. Ties are left ungraded rather than
 * scored as misses: pools normally void them, and scoring one as a loss would
 * penalise both entries and corrupt the paired delta, which is the number that
 * has to be exact.
 *
 * Idempotent. Re-running re-derives every graded field from current scores, so
 * a corrected score simply produces a corrected settlement.
 */
export async function settlePickemRecommendations(season: number): Promise<ActionResult> {
  await ensurePickemTables();
  try {
    // Stamp each frozen game with its real result.
    await db.execute(sql`
      UPDATE pickem_recommendation_games g
      SET home_won = CASE
            WHEN s.home_score IS NULL OR s.away_score IS NULL THEN NULL
            WHEN s.home_score = s.away_score THEN NULL
            ELSE s.home_score > s.away_score
          END,
          baseline_correct = CASE
            WHEN s.home_score IS NULL OR s.away_score IS NULL OR s.home_score = s.away_score THEN NULL
            ELSE g.baseline_pick_home = (s.home_score > s.away_score)
          END,
          recommended_correct = CASE
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
          settled_at = CASE
            WHEN agg.graded > 0 AND agg.graded = r.games_total THEN NOW() ELSE r.settled_at
          END
      FROM (
        SELECT
          g.recommendation_id AS rec_id,
          COUNT(*) FILTER (WHERE g.home_won IS NOT NULL) AS graded,
          COALESCE(SUM(g.baseline_confidence) FILTER (WHERE g.baseline_correct), 0) AS baseline_points,
          COALESCE(SUM(g.recommended_confidence) FILTER (WHERE g.recommended_correct), 0) AS recommended_points,
          COUNT(*) FILTER (WHERE g.baseline_correct) AS baseline_hits,
          COUNT(*) FILTER (WHERE g.recommended_correct) AS recommended_hits,
          COALESCE(SUM(g.recommended_confidence) FILTER (WHERE g.home_won IS NOT NULL), 0) AS max_points,
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
    revalidatePath("/nfl/pickem");
    return {
      ok: true,
      message:
        `Settled ${settled} complete week${settled === 1 ? "" : "s"} ` +
        `(${updated.rows.length} row${updated.rows.length === 1 ? "" : "s"} re-graded). ` +
        `Pool finishes are not inferred — record those yourself.`,
    };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not settle." };
  }
}

/**
 * Record how the entry actually finished.
 *
 * The only genuinely decisive measurement on this page, and the only one that
 * cannot be automated -- we can see the scores, but not your pool's field.
 */
export async function recordPickemFinish(input: {
  recommendationId: number;
  finishRank: number | null;
  winningScore: number | null;
  wonPool: boolean | null;
}): Promise<ActionResult> {
  await ensurePickemTables();
  try {
    const current = await db.execute(sql`
      SELECT locks_at FROM pickem_recommendations WHERE id = ${input.recommendationId}
    `);
    const row = current.rows[0] as Record<string, unknown> | undefined;
    if (!row) return { ok: false, error: "No such recommendation." };
    if (row.locks_at != null && new Date(String(row.locks_at)) > new Date()) {
      return { ok: false, error: "That week has not kicked off yet — there is no finish to record." };
    }

    await db.execute(sql`
      UPDATE pickem_recommendations
      SET finish_rank = ${input.finishRank},
          pool_winning_score = ${input.winningScore},
          won_pool = ${input.wonPool}
      WHERE id = ${input.recommendationId}
    `);
    revalidatePath("/nfl/pickem");
    return { ok: true, message: "Finish recorded." };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not record the finish." };
  }
}

export async function voidPickemRecommendation(recommendationId: number): Promise<ActionResult> {
  await ensurePickemTables();
  try {
    await db.execute(sql`
      UPDATE pickem_recommendations SET status = 'void' WHERE id = ${recommendationId}
    `);
    revalidatePath("/nfl/pickem");
    return {
      ok: true,
      message: "Marked void. The row stays in the ledger and is excluded from every summary.",
    };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not void the row." };
  }
}
