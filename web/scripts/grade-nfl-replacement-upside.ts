/**
 * Weekly grade of `nfl-replacement-upside-v1` (pre-registered in
 * docs/nfl-replacement-upside-grading.md).
 *
 *   cd web && npm run grade:nfl-replacement-upside [-- --out report.json]
 *
 * Runs automatically as the second job of refresh_nfl_dfs_postweek.yml, after
 * that week's DraftKings results are ingested. Reads the append-only pool
 * captures (last pregame look per upload and game, digest-verified), the
 * schedule and DraftKings results.
 *
 * Every run is recorded in nfl_replacement_upside_grade_runs. Until every
 * floor is met the record is BLINDED (counts and health only). The first run
 * past the floors is the one look: it writes the verdict to
 * nfl_replacement_upside_grade_verdicts in the same statement, and the table's
 * primary key stops any later run from writing another. Later runs are
 * post-verdict monitoring and cannot change it.
 */
import { config } from 'dotenv';
import { createHash } from 'node:crypto';
import { appendFileSync, writeFileSync } from 'node:fs';

config({ path: '.env.local', quiet: true });

(async () => {
  const outFlag = process.argv.indexOf('--out');
  const outPath = outFlag >= 0 ? process.argv[outFlag + 1] : null;
  const { db } = await import('../src/db');
  const { sql } = await import('drizzle-orm');
  const { auditGames } = await import('../src/db/nfl-dfs-pool-audit');
  const { readFrozenVerdict, recordGradeRun } = await import('../src/db/nfl-replacement-upside-grade');
  const { canonicalAuditJson } = await import('../src/lib/nfl-dfs/audit-json');
  const g = await import('../src/lib/nfl-dfs/replacement-upside-grade');
  const rowsOf = (r: unknown) => (r as { rows: Record<string, unknown>[] }).rows;
  const iso = (v: unknown) => new Date(v as string).toISOString();

  const captures: ReturnType<typeof g.captureFromRow>[] = [];
  const games: Awaited<ReturnType<typeof auditGames>> = [];
  const results: import('../src/lib/nfl-dfs/replacement-upside-grade').GradeResult[] = [];
  const captureTable = rowsOf(await db.execute(sql`SELECT to_regclass('nfl_dfs_pool_captures') AS name`))[0]?.name;
  for (const window of captureTable ? g.UPSIDE_GRADE_SPEC.windows : []) {
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
  const codeRevision = process.env.GITHUB_SHA ?? process.env.VERCEL_GIT_COMMIT_SHA ?? null;
  const f = report.floors;
  const lines = [
    `${report.version} | floors: events ${f.events.have}/${f.events.required}, flagged ${f.flagged.have}/${f.flagged.required}, weeks ${f.weeks.have}/${f.weeks.required}`,
    `health: ${report.health.gamesWithFeatureCapture}/${report.health.completedGamesInWindow} completed games have a pregame capture with the feature; `
      + `${report.health.playerGamesWithoutFeature} player-games without it; ${report.health.skippedStarters.length} skipped starters`,
    `accrual by role: ${JSON.stringify(report.accrual.byRole)} | statuses: ${JSON.stringify(report.accrual.flaggedStatuses)}`,
  ];

  const already = await readFrozenVerdict(report.version);
  if (!report.revealed) {
    await recordGradeRun({ gradeVersion: report.version, revealed: false, floorsMet: false, codeRevision, report });
    lines.push('BLINDED: no outcome metric until every floor is met.');
  } else {
    const verdict = { verdict: report.verdict, payload: { meaning: report.meaning, metrics: report.metrics, widening: report.widening,
      floors: report.floors, accrual: report.accrual, health: report.health, spec: report.spec,
      flaggedRows: report.rows.flagged.map((r) => ({ event: r.event, playerId: r.playerId, actual: r.actual, captureDigest: r.captureDigest })) } };
    const { runId, froze } = await recordGradeRun({ gradeVersion: report.version, revealed: true, floorsMet: true, codeRevision, report,
      verdict: already ? undefined : verdict });
    if (froze) {
      lines.push(`FIRST LOOK (run ${runId}). Verdict ${report.verdict}: ${report.meaning}`, JSON.stringify(report.metrics));
    } else {
      const frozen = already ?? await readFrozenVerdict(report.version);
      lines.push(`Frozen verdict (${frozen?.frozenAt}): ${frozen?.verdict}. This run is post-verdict monitoring and cannot change it.`,
        JSON.stringify({ monitoring: report.metrics, exceedance: report.descriptive.exceedance }));
    }
  }
  for (const line of lines) console.log(line);
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `### Replacement upside grade\n\n${lines.map((l) => `- ${l}`).join('\n')}\n`);
  }
  if (outPath) {
    writeFileSync(outPath, `${JSON.stringify(report, null, 1)}\n`);
    console.log(`Report written to ${outPath}${report.revealed ? '' : ' (blinded)'}.`);
  }
  process.exit(0);
})().catch((error) => { console.error(error); process.exit(1); });
