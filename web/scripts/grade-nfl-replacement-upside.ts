/**
 * Weekly grade of `nfl-replacement-upside-v1` (pre-registered in
 * docs/nfl-replacement-upside-grading.md).
 *
 *   cd web && npm run grade:nfl-replacement-upside [-- --out report.json]
 *
 * Reads the append-only pool captures (last pregame look per upload and game,
 * digest-verified), the schedule and DraftKings results. Read-only against the
 * database.
 *
 * Until every floor is met the output is BLINDED (counts and health only). The
 * first run past the floors is the one look: it writes
 * artifacts/nfl_replacement_upside_grade_v1_verdict.json, which must be
 * committed at once and is never overwritten. Later runs print the frozen
 * verdict and label everything else post-verdict monitoring.
 */
import { config } from 'dotenv';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

config({ path: '.env.local', quiet: true });

const VERDICT_PATH = path.resolve(process.cwd(), '..', 'artifacts', 'nfl_replacement_upside_grade_v1_verdict.json');

(async () => {
  const outFlag = process.argv.indexOf('--out');
  const outPath = outFlag >= 0 ? process.argv[outFlag + 1] : null;
  const { db } = await import('../src/db');
  const { sql } = await import('drizzle-orm');
  const { auditGames } = await import('../src/db/nfl-dfs-pool-audit');
  const { canonicalAuditJson } = await import('../src/lib/nfl-dfs/audit-json');
  const g = await import('../src/lib/nfl-dfs/replacement-upside-grade');
  const rowsOf = (r: unknown) => (r as { rows: Record<string, unknown>[] }).rows;
  const iso = (v: unknown) => new Date(v as string).toISOString();

  const captures: ReturnType<typeof g.captureFromRow>[] = [];
  const games: Awaited<ReturnType<typeof auditGames>> = [];
  const results: import('../src/lib/nfl-dfs/replacement-upside-grade').GradeResult[] = [];
  for (const window of g.UPSIDE_GRADE_SPEC.windows) {
    const weeks = rowsOf(await db.execute(sql`SELECT DISTINCT week FROM nfl_season_games
      WHERE season=${window.season} AND game_type='REG' AND week>=${window.firstWeek} AND kickoff<=clock_timestamp() ORDER BY week`));
    for (const { week } of weeks) {
      const season = window.season, wk = Number(week);
      games.push(...await auditGames(season, wk));
      // Last live pregame capture per (upload, game); the grader then picks per player across uploads.
      const found = rowsOf(await db.execute(sql`SELECT DISTINCT ON (c.upload_id, c.game_id)
          c.digest, c.upload_id, u.created_at AS upload_created_at, c.observed_at, c.captured_at, c.payload
        FROM nfl_dfs_pool_captures c
        JOIN nfl_dfs_slate_uploads u ON u.upload_id = c.upload_id
        JOIN nfl_season_games sg ON sg.id = c.game_id
        WHERE sg.season=${season} AND sg.week=${wk} AND sg.game_type='REG'
          AND c.observed_at < c.kickoff AND c.payload->>'origin' = 'live_pool'
        ORDER BY c.upload_id, c.game_id, c.observed_at DESC, c.digest`));
      for (const r of found) {
        const digest = String(r.digest);
        if (digest.split(':')[0] !== createHash('sha256').update(canonicalAuditJson(r.payload)).digest('hex')) {
          throw new Error(`Pool capture digest mismatch: ${digest}. Grade refused.`);
        }
        captures.push(g.captureFromRow({ digest, uploadId: String(r.upload_id), uploadCreatedAt: iso(r.upload_created_at),
          observedAt: iso(r.observed_at), capturedAt: iso(r.captured_at), payload: r.payload }));
      }
      for (const r of rowsOf(await db.execute(sql`SELECT id, player_id, game_id, team, position, actual_dk_fpts, scoring_status, computed_at
          FROM nfl_dfs_player_week_results WHERE season=${season} AND week=${wk} AND computed_at<=clock_timestamp()`))) {
        results.push({ id: String(r.id), playerId: Number(r.player_id), gameId: Number(r.game_id), team: String(r.team),
          position: String(r.position), actual: r.actual_dk_fpts == null ? null : Number(r.actual_dk_fpts),
          status: String(r.scoring_status), computedAt: iso(r.computed_at) });
      }
    }
  }

  const report = g.gradeReplacementUpside({ captures, games, results, now: new Date().toISOString() });
  const f = report.floors;
  console.log(`${report.version} | floors: events ${f.events.have}/${f.events.required}, flagged ${f.flagged.have}/${f.flagged.required}, weeks ${f.weeks.have}/${f.weeks.required}`);
  console.log(`health: ${report.health.gamesWithFeatureCapture}/${report.health.completedGamesInWindow} completed games have a pregame capture with the feature; `
    + `${report.health.playerGamesWithoutFeature} player-games without it; ${report.health.skippedStarters.length} skipped starters`);
  console.log(`accrual by role: ${JSON.stringify(report.accrual.byRole)} | statuses: ${JSON.stringify(report.accrual.flaggedStatuses)}`);

  if (!report.revealed) {
    console.log('BLINDED: no outcome metric until every floor is met.');
  } else if (!existsSync(VERDICT_PATH)) {
    const frozen = { version: report.version, featureVersion: report.featureVersion, frozenAt: report.evaluatedAt,
      verdict: report.verdict, meaning: report.meaning, metrics: report.metrics, widening: report.widening,
      floors: report.floors, accrual: report.accrual, health: report.health, spec: report.spec,
      flaggedRows: report.rows.flagged.map((r) => ({ event: r.event, playerId: r.playerId, actual: r.actual, captureDigest: r.captureDigest })) };
    writeFileSync(VERDICT_PATH, `${JSON.stringify(frozen, null, 1)}\n`);
    console.log(`FIRST LOOK. Verdict ${report.verdict}: ${report.meaning}`);
    console.log(`Frozen to ${VERDICT_PATH}. Commit it now; it is never overwritten.`);
    console.log(JSON.stringify(report.metrics, null, 1));
  } else {
    const frozen = JSON.parse(readFileSync(VERDICT_PATH, 'utf8'));
    console.log(`Frozen verdict (${frozen.frozenAt}): ${frozen.verdict}. Everything below is post-verdict monitoring and cannot change it.`);
    console.log(JSON.stringify({ monitoring: report.metrics, exceedance: report.descriptive.exceedance }, null, 1));
  }
  if (outPath) {
    writeFileSync(outPath, `${JSON.stringify(report, null, 1)}\n`);
    console.log(`Report written to ${outPath}${report.revealed ? '' : ' (blinded)'}.`);
  }
  process.exit(0);
})().catch((error) => { console.error(error); process.exit(1); });
