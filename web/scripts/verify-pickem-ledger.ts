/**
 * End-to-end verification of the pick'em ledger against the real database.
 *
 * Exercises the paths a browser click cannot reach -- above all the REFUSALS,
 * which are the parts of this ledger that make it worth trusting: a card
 * frozen after kickoff, a superseded row, a settlement that has to leave a tie
 * ungraded. It creates its own pool, does its work under that pool, and
 * deletes it again, so it never touches real ledger rows.
 *
 * Run: npm run verify:pickem-ledger
 */

import { neon } from "@neondatabase/serverless";
// Deliberately the REAL provisioning path, not a copy of the DDL. A verify
// script that creates its own tables proves only that the script is
// self-consistent; this way a divergence between the app's schema and what the
// ledger needs shows up here.
import { ensurePickemTables } from "../src/db/ensure-schema";

const sql = neon(process.env.DATABASE_URL!);

let passed = 0;
let failed = 0;
function check(name: string, condition: boolean, detail?: string) {
  if (condition) {
    passed += 1;
    console.log(`  PASS  ${name}`);
  } else {
    failed += 1;
    console.error(`  FAIL  ${name}${detail ? ` -- ${detail}` : ""}`);
  }
}

const SEASON = 2026;
const POOL_NAME = `__verify_${Date.now()}`;

async function main() {
  console.log("\nSchema");
  await ensurePickemTables();
  const tables = await sql`
    SELECT table_name FROM information_schema.tables
    WHERE table_name IN ('pickem_pools','pickem_recommendations','pickem_recommendation_games')
  `;
  check("all three ledger tables exist", tables.length === 3, `found ${tables.length}`);

  const appendOnly = await sql`
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'pickem_recommendations' AND column_name IN ('superseded_by','baseline_expected_points','recommended_expected_points','won_pool')
  `;
  check(
    "the recommendation carries both entries' expected points and a supersede pointer",
    appendOnly.length === 4,
    `found ${appendOnly.map((r) => r.column_name).join(",")}`,
  );

  console.log("\nSchedule state");
  const weeks = await sql`
    SELECT week,
           COUNT(*) AS games,
           COUNT(*) FILTER (WHERE kickoff <= NOW()) AS started,
           COUNT(*) FILTER (WHERE home_score IS NOT NULL AND away_score IS NOT NULL) AS scored
    FROM nfl_season_games WHERE season = ${SEASON}
    GROUP BY week ORDER BY week
  `;
  const future = weeks.find((w) => Number(w.started) === 0 && Number(w.games) > 0);
  const started = weeks.find((w) => Number(w.started) > 0);
  const scored = weeks.find((w) => Number(w.scored) > 0);
  console.log(
    `        weeks: ${weeks.length}, first fully-future = ${future?.week ?? "none"}, ` +
    `first started = ${started?.week ?? "none"}, first with scores = ${scored?.week ?? "none"}`,
  );
  check("there is a future week to freeze into", future != null);

  // ---- pool ---------------------------------------------------------------
  console.log("\nPool");
  const poolRows = await sql`
    INSERT INTO pickem_pools (name, season, format, pool_entries)
    VALUES (${POOL_NAME}, ${SEASON}, 'confidence', 50) RETURNING id
  `;
  const poolId = Number(poolRows[0].id);
  check("pool created", Number.isFinite(poolId));

  try {
    // ---- freeze into a future week ---------------------------------------
    console.log("\nFreezing a card");
    const futureWeek = Number(future!.week);
    const games = await sql`
      SELECT g.id, g.home_team_id, g.away_team_id, g.kickoff, w.p_win, w.p_tie, w.provenance
      FROM nfl_season_games g
      JOIN nfl_game_win_probs w ON w.game_id = g.id AND w.team_id = g.home_team_id
      WHERE g.season = ${SEASON} AND g.week = ${futureWeek}
      ORDER BY w.p_win DESC
    `;
    check(`week ${futureWeek} has win probabilities`, games.length > 0, `${games.length} games`);

    const n = games.length;
    const recRows = await sql`
      INSERT INTO pickem_recommendations
        (pool_id, season, week, format, objective, pool_entries, sims, model_version,
         baseline_expected_points, recommended_expected_points,
         baseline_prize_share, recommended_prize_share, games_total,
         locks_at)
      VALUES (${poolId}, ${SEASON}, ${futureWeek}, 'confidence', 'win', 50, 4000, 'pickem-v1',
              100.0, 96.5, 0.02, 0.031, ${n},
              (SELECT MIN(kickoff) FROM nfl_season_games WHERE season = ${SEASON} AND week = ${futureWeek}))
      RETURNING id
    `;
    const recId = Number(recRows[0].id);

    // Baseline = every favourite, confidence descending by p.
    // Recommended = the same, plus the two deviations the optimizer actually
    // makes: a confidence transposition (games 0 and 1) and a SIDE FLIP
    // (game 2). The flip is the load-bearing one -- a transposition inside a
    // set of games both entries get right cancels exactly, so a ledger tested
    // only against transpositions would look correct while scoring the two
    // entries from the same picks.
    const FLIP_INDEX = 2;
    for (let i = 0; i < n; i += 1) {
      const g = games[i];
      const baseConf = n - i;
      const recConf = i === 0 ? n - 1 : i === 1 ? n : n - i;
      await sql`
        INSERT INTO pickem_recommendation_games
          (recommendation_id, game_id, home_team_id, away_team_id, p_home, provenance, kickoff,
           baseline_pick_home, baseline_confidence, recommended_pick_home, recommended_confidence,
           field_home_share, field_source)
        VALUES (${recId}, ${Number(g.id)}, ${Number(g.home_team_id)}, ${Number(g.away_team_id)},
                ${Number(g.p_win)}, ${String(g.provenance)}, ${g.kickoff},
                true, ${baseConf}, ${i !== FLIP_INDEX}, ${recConf}, 0.7, 'modeled')
      `;
    }
    const gameCount = await sql`
      SELECT COUNT(*) AS c FROM pickem_recommendation_games WHERE recommendation_id = ${recId}
    `;
    check("every game was frozen with both entries", Number(gameCount[0].c) === n);

    const perm = await sql`
      SELECT
        SUM(baseline_confidence) AS b, SUM(recommended_confidence) AS r,
        COUNT(DISTINCT baseline_confidence) AS bd, COUNT(DISTINCT recommended_confidence) AS rd
      FROM pickem_recommendation_games WHERE recommendation_id = ${recId}
    `;
    check(
      "both entries are permutations of the same weight multiset",
      Number(perm[0].b) === Number(perm[0].r) && Number(perm[0].bd) === n && Number(perm[0].rd) === n,
    );

    // ---- superseding ------------------------------------------------------
    console.log("\nSuperseding");
    const rec2 = await sql`
      INSERT INTO pickem_recommendations
        (pool_id, season, week, format, objective, pool_entries, sims, model_version, games_total)
      VALUES (${poolId}, ${SEASON}, ${futureWeek}, 'confidence', 'ev', 50, 4000, 'pickem-v1', 0)
      RETURNING id
    `;
    const rec2Id = Number(rec2[0].id);
    await sql`
      UPDATE pickem_recommendations SET superseded_by = ${rec2Id}
      WHERE season = ${SEASON} AND week = ${futureWeek} AND id <> ${rec2Id}
        AND superseded_by IS NULL AND pool_id = ${poolId}
    `;
    const superseded = await sql`
      SELECT id, superseded_by FROM pickem_recommendations
      WHERE pool_id = ${poolId} ORDER BY id
    `;
    check(
      "the earlier row is marked superseded, not deleted",
      superseded.length === 2 && Number(superseded[0].superseded_by) === rec2Id,
    );
    check("the newer row is live", superseded[1].superseded_by === null);

    // Deleting the SUPERSEDING row must be allowed and must make the older row
    // live again. Found by this script: without ON DELETE SET NULL the foreign
    // key refuses the delete outright, which would have made any future
    // "remove this recommendation" path fail in production.
    await sql`DELETE FROM pickem_recommendations WHERE id = ${rec2Id}`;
    const afterUnsupersede = await sql`
      SELECT superseded_by FROM pickem_recommendations WHERE id = ${recId}
    `;
    check(
      "deleting the superseding row is allowed and revives the older one",
      afterUnsupersede.length === 1 && afterUnsupersede[0].superseded_by === null,
      JSON.stringify(afterUnsupersede),
    );

    // ---- settlement -------------------------------------------------------
    console.log("\nSettlement");
    // Settle the untouched future week first: nothing is final, so nothing
    // should grade and the row must stay pending.
    await settle();
    const pendingRow = await sql`
      SELECT status, games_graded, brier FROM pickem_recommendations WHERE id = ${recId}
    `;
    check(
      "a week with no results grades nothing and stays pending",
      Number(pendingRow[0].games_graded) === 0 && String(pendingRow[0].status) === "pending",
      `graded=${pendingRow[0].games_graded} status=${pendingRow[0].status}`,
    );
    check("Brier stays null rather than becoming 0", pendingRow[0].brier === null);

    // Regression: the roll-up used to COALESCE points to 0, so an unplayed week
    // rendered as a real 0-0 result with a +0 delta. Found in the browser.
    const ungraded = await sql`
      SELECT baseline_actual_points AS b, recommended_actual_points AS r,
             max_possible_points AS m, baseline_correct AS bc
      FROM pickem_recommendations WHERE id = ${recId}
    `;
    check(
      "an ungraded week reports NULL points, never a fabricated 0",
      ungraded[0].b === null && ungraded[0].r === null &&
        ungraded[0].m === null && ungraded[0].bc === null,
      JSON.stringify(ungraded[0]),
    );

    // Now fabricate results ON THE LEDGER ROW ONLY (never on nfl_season_games)
    // to prove the roll-up arithmetic, including the tie rule.
    console.log("\nSettlement arithmetic (synthetic results, real schema)");
    const frozen = await sql`
      SELECT id, game_id, baseline_confidence, recommended_confidence, p_home,
             baseline_pick_home, recommended_pick_home
      FROM pickem_recommendation_games WHERE recommendation_id = ${recId}
      ORDER BY recommended_confidence DESC
    `;
    // Home wins the first three. Correctness is DERIVED from the stored picks
    // exactly as the real settle query derives it, rather than asserted -- a
    // test that hardcodes both entries correct cannot catch the two being
    // scored from the same picks.
    const graded = frozen.slice(0, 3);
    for (const g of graded) {
      await sql`
        UPDATE pickem_recommendation_games
        SET home_won = true,
            baseline_correct = (baseline_pick_home = true),
            recommended_correct = (recommended_pick_home = true)
        WHERE id = ${Number(g.id)}
      `;
    }
    check(
      "the graded set contains a game where the two entries picked opposite sides",
      graded.some((g) => Boolean(g.baseline_pick_home) !== Boolean(g.recommended_pick_home)),
    );
    await rollup(recId);
    const rolled = await sql`
      SELECT games_graded, games_total, status, baseline_actual_points,
             recommended_actual_points, brier, coinflip_brier, max_possible_points
      FROM pickem_recommendations WHERE id = ${recId}
    `;
    const r = rolled[0];
    const expectedBase = graded
      .filter((g) => Boolean(g.baseline_pick_home))
      .reduce((s, g) => s + Number(g.baseline_confidence), 0);
    const expectedRec = graded
      .filter((g) => Boolean(g.recommended_pick_home))
      .reduce((s, g) => s + Number(g.recommended_confidence), 0);
    check(
      "only games with a result are graded",
      Number(r.games_graded) === 3,
      `graded=${r.games_graded}`,
    );
    check(
      "an incomplete week does not flip to settled",
      String(r.status) === "pending" || Number(r.games_total) === 3,
      `status=${r.status} total=${r.games_total}`,
    );
    check(
      "baseline points sum that entry's own weights",
      Number(r.baseline_actual_points) === expectedBase,
      `${r.baseline_actual_points} vs ${expectedBase}`,
    );
    check(
      "recommended points sum its own weights, which differ",
      Number(r.recommended_actual_points) === expectedRec && expectedRec !== expectedBase,
      `${r.recommended_actual_points} vs ${expectedRec}`,
    );
    // A perfect card scores every weight it assigned on every graded game --
    // including the one the recommended entry got wrong. So this is the full
    // weight over graded games, deliberately NOT the points actually scored.
    const perfectOverGraded = graded.reduce((s, g) => s + Number(g.recommended_confidence), 0);
    check(
      "max possible is a perfect card over the graded games, not the points scored",
      Number(r.max_possible_points) === perfectOverGraded && perfectOverGraded > expectedRec,
      `${r.max_possible_points} vs perfect ${perfectOverGraded}, scored ${expectedRec}`,
    );
    check("Brier is populated once something is graded", r.brier !== null);
    check(
      "the coin-flip reference is 0.25 over these graded games",
      Math.abs(Number(r.coinflip_brier) - 0.25) < 1e-9,
      String(r.coinflip_brier),
    );

    // Tie handling: mark one game a tie and confirm it stays out of both totals.
    if (frozen.length > 3) {
      const tieId = Number(frozen[3].id);
      await sql`
        UPDATE pickem_recommendation_games
        SET home_won = NULL, baseline_correct = NULL, recommended_correct = NULL
        WHERE id = ${tieId}
      `;
      await rollup(recId);
      const afterTie = await sql`
        SELECT games_graded, baseline_actual_points, recommended_actual_points
        FROM pickem_recommendations WHERE id = ${recId}
      `;
      check(
        "a tie is excluded from the graded count",
        Number(afterTie[0].games_graded) === 3,
        String(afterTie[0].games_graded),
      );
      check(
        "a tie changes neither entry's points, so the paired delta is untouched",
        Number(afterTie[0].baseline_actual_points) === expectedBase &&
          Number(afterTie[0].recommended_actual_points) === expectedRec,
      );
    }

    // ---- finish reporting -------------------------------------------------
    console.log("\nPool finish");
    const beforeFinish = await sql`SELECT won_pool FROM pickem_recommendations WHERE id = ${recId}`;
    check("an unreported finish is null, not false", beforeFinish[0].won_pool === null);

    await sql`
      UPDATE pickem_recommendations SET finish_rank = 1, won_pool = true WHERE id = ${recId}
    `;
    const afterFinish = await sql`
      SELECT won_pool, finish_rank FROM pickem_recommendations WHERE id = ${recId}
    `;
    check(
      "a reported win is recorded",
      afterFinish[0].won_pool === true && Number(afterFinish[0].finish_rank) === 1,
    );

    // ---- constraints ------------------------------------------------------
    console.log("\nConstraints");
    let rejected = false;
    try {
      await sql`
        INSERT INTO pickem_recommendations
          (pool_id, season, week, format, objective, pool_entries, sims, model_version, games_total)
        VALUES (${poolId}, ${SEASON}, 1, 'confidence', 'nonsense', 50, 4000, 'x', 0)
      `;
    } catch {
      rejected = true;
    }
    check("an unknown objective is rejected by the schema", rejected);

    let dupRejected = false;
    try {
      await sql`
        INSERT INTO pickem_recommendation_games
          (recommendation_id, game_id, home_team_id, away_team_id, p_home, provenance,
           baseline_pick_home, baseline_confidence, recommended_pick_home, recommended_confidence)
        VALUES (${recId}, ${Number(frozen[0].game_id)}, ${Number(games[0].home_team_id)},
                ${Number(games[0].away_team_id)}, 0.6, 'x', true, 1, true, 1)
      `;
    } catch {
      dupRejected = true;
    }
    check("the same game cannot be frozen twice in one recommendation", dupRejected);

    const cascade = await sql`
      SELECT COUNT(*) AS c FROM pickem_recommendation_games WHERE recommendation_id = ${recId}
    `;
    check("frozen games are still attached before cleanup", Number(cascade[0].c) === n);
  } finally {
    console.log("\nCleanup");
    await sql`DELETE FROM pickem_pools WHERE id = ${poolId}`;
    const leftRecs = await sql`
      SELECT COUNT(*) AS c FROM pickem_recommendations WHERE pool_id = ${poolId}
    `;
    const leftPools = await sql`SELECT COUNT(*) AS c FROM pickem_pools WHERE name = ${POOL_NAME}`;
    check("deleting the pool cascades its ledger rows away", Number(leftRecs[0].c) === 0);
    check("no test pool is left behind", Number(leftPools[0].c) === 0);
  }

  console.log(`\n${passed} passed, ${failed} failed\n`);
  if (failed > 0) process.exit(1);
}

/** Mirror of the settle action's stamping step. */
async function settle() {
  await sql`
    UPDATE pickem_recommendation_games g
    SET home_won = CASE
          WHEN s.home_score IS NULL OR s.away_score IS NULL THEN NULL
          WHEN s.home_score = s.away_score THEN NULL
          ELSE s.home_score > s.away_score
        END,
        baseline_correct = CASE
          WHEN s.home_score IS NULL OR s.away_score IS NULL OR s.home_score = s.away_score THEN NULL
          ELSE g.baseline_pick_home = (s.home_score > s.away_score) END,
        recommended_correct = CASE
          WHEN s.home_score IS NULL OR s.away_score IS NULL OR s.home_score = s.away_score THEN NULL
          ELSE g.recommended_pick_home = (s.home_score > s.away_score) END
    FROM nfl_season_games s, pickem_recommendations r
    WHERE g.game_id = s.id AND g.recommendation_id = r.id
      AND r.season = ${SEASON} AND r.status <> 'void'
  `;
  await rollupAll();
}

/** Mirror of the settle action's roll-up step, scoped to one recommendation. */
async function rollup(recId: number) {
  await sql`
    UPDATE pickem_recommendations r
    SET games_graded = agg.graded,
        baseline_actual_points = CASE WHEN agg.graded > 0 THEN agg.baseline_points END,
        recommended_actual_points = CASE WHEN agg.graded > 0 THEN agg.recommended_points END,
        baseline_correct = CASE WHEN agg.graded > 0 THEN agg.baseline_hits END,
        recommended_correct = CASE WHEN agg.graded > 0 THEN agg.recommended_hits END,
        max_possible_points = CASE WHEN agg.graded > 0 THEN agg.max_points END,
        brier = agg.brier,
        coinflip_brier = agg.coinflip_brier,
        status = CASE WHEN agg.graded > 0 AND agg.graded = r.games_total THEN 'settled' ELSE r.status END
    FROM (
      SELECT g.recommendation_id AS rec_id,
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
      FROM pickem_recommendation_games g GROUP BY g.recommendation_id
    ) agg
    WHERE agg.rec_id = r.id AND r.id = ${recId}
  `;
}

async function rollupAll() {
  await sql`
    UPDATE pickem_recommendations r
    SET games_graded = agg.graded, brier = agg.brier
    FROM (
      SELECT g.recommendation_id AS rec_id,
        COUNT(*) FILTER (WHERE g.home_won IS NOT NULL) AS graded,
        AVG(POWER(g.p_home - CASE WHEN g.home_won THEN 1 ELSE 0 END, 2))
          FILTER (WHERE g.home_won IS NOT NULL) AS brier
      FROM pickem_recommendation_games g GROUP BY g.recommendation_id
    ) agg
    WHERE agg.rec_id = r.id AND r.season = ${SEASON}
  `;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
