import 'server-only';
import { sql } from 'drizzle-orm';
import { db } from '@/db';

/**
 * Storage for the weekly `nfl-replacement-upside-grade-v2` run (v1 superseded 2026-09-29)
 * (docs/nfl-replacement-upside-grading.md).
 *
 * - `nfl_replacement_upside_grade_runs`: one row per weekly run. Blinded runs
 *   hold counts and health only; nothing about outcomes.
 * - `nfl_replacement_upside_grade_verdicts`: at most ONE row per grade
 *   version, written by the first run in which every floor is met. The primary
 *   key is the one-look rule: a later run cannot write a second verdict.
 *
 * Both tables are append-only (a trigger rejects UPDATE and DELETE), so the
 * frozen verdict cannot be edited after the fact, including by hand.
 */
export const UPSIDE_GRADE_DDL = [
  `CREATE TABLE IF NOT EXISTS nfl_replacement_upside_grade_runs (
    id BIGSERIAL PRIMARY KEY, grade_version TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revealed BOOLEAN NOT NULL, floors_met BOOLEAN NOT NULL,
    code_revision TEXT, report JSONB NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS idx_nfl_upside_grade_runs ON nfl_replacement_upside_grade_runs(grade_version,id)`,
  `CREATE TABLE IF NOT EXISTS nfl_replacement_upside_grade_verdicts (
    grade_version TEXT PRIMARY KEY, frozen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    verdict TEXT NOT NULL, run_id BIGINT NOT NULL REFERENCES nfl_replacement_upside_grade_runs(id),
    payload JSONB NOT NULL)`,
  `CREATE OR REPLACE FUNCTION reject_nfl_upside_grade_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'Replacement-upside grade records are append-only'; END $$`,
  `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='nfl_upside_grade_runs_immutable') THEN
    CREATE TRIGGER nfl_upside_grade_runs_immutable BEFORE UPDATE OR DELETE ON nfl_replacement_upside_grade_runs
    FOR EACH ROW EXECUTE FUNCTION reject_nfl_upside_grade_mutation(); END IF; END $$`,
  `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='nfl_upside_grade_verdicts_immutable') THEN
    CREATE TRIGGER nfl_upside_grade_verdicts_immutable BEFORE UPDATE OR DELETE ON nfl_replacement_upside_grade_verdicts
    FOR EACH ROW EXECUTE FUNCTION reject_nfl_upside_grade_mutation(); END IF; END $$`,
];

export async function installUpsideGrade() { for (const ddl of UPSIDE_GRADE_DDL) await db.execute(sql.raw(ddl)); }

async function installed() {
  const r = await db.execute(sql`SELECT to_regclass('nfl_replacement_upside_grade_runs') AS runs,
    to_regclass('nfl_replacement_upside_grade_verdicts') AS verdicts`);
  return Boolean(r.rows[0]?.runs && r.rows[0]?.verdicts);
}

export interface FrozenVerdict { gradeVersion: string; frozenAt: string; verdict: string; runId: number; payload: Record<string, unknown> }
export interface GradeRunRow { id: number; evaluatedAt: string; revealed: boolean; floorsMet: boolean; codeRevision: string | null; report: Record<string, unknown> }

const toVerdict = (r: Record<string, unknown>): FrozenVerdict => ({ gradeVersion: String(r.grade_version),
  frozenAt: new Date(r.frozen_at as string).toISOString(), verdict: String(r.verdict), runId: Number(r.run_id),
  payload: r.payload as Record<string, unknown> });
const toRun = (r: Record<string, unknown>): GradeRunRow => ({ id: Number(r.id), evaluatedAt: new Date(r.evaluated_at as string).toISOString(),
  revealed: Boolean(r.revealed), floorsMet: Boolean(r.floors_met), codeRevision: (r.code_revision as string | null) ?? null,
  report: r.report as Record<string, unknown> });

export async function readFrozenVerdict(gradeVersion: string): Promise<FrozenVerdict | null> {
  if (!await installed()) return null;
  const r = await db.execute(sql`SELECT * FROM nfl_replacement_upside_grade_verdicts WHERE grade_version=${gradeVersion}`);
  return r.rows[0] ? toVerdict(r.rows[0]) : null;
}

/**
 * Record one run. When `verdict` is given (every floor met), the run and the
 * verdict are written in ONE statement, and the verdict insert is a no-op if a
 * verdict for this grade version already exists. Returns whether this run is
 * the one that froze it.
 */
export async function recordGradeRun(input: {
  gradeVersion: string; revealed: boolean; floorsMet: boolean; codeRevision: string | null;
  report: unknown; verdict?: { verdict: string; payload: unknown };
}): Promise<{ runId: number; froze: boolean }> {
  await installUpsideGrade();
  const report = JSON.stringify(input.report);
  if (!input.verdict) {
    const r = await db.execute(sql`INSERT INTO nfl_replacement_upside_grade_runs(grade_version,revealed,floors_met,code_revision,report)
      VALUES(${input.gradeVersion},${input.revealed},${input.floorsMet},${input.codeRevision},${report}::jsonb) RETURNING id`);
    return { runId: Number(r.rows[0].id), froze: false };
  }
  const r = await db.execute(sql`WITH run AS (
      INSERT INTO nfl_replacement_upside_grade_runs(grade_version,revealed,floors_met,code_revision,report)
      VALUES(${input.gradeVersion},${input.revealed},${input.floorsMet},${input.codeRevision},${report}::jsonb) RETURNING id),
    frozen AS (INSERT INTO nfl_replacement_upside_grade_verdicts(grade_version,verdict,run_id,payload)
      SELECT ${input.gradeVersion},${input.verdict.verdict},run.id,${JSON.stringify(input.verdict.payload)}::jsonb FROM run
      ON CONFLICT (grade_version) DO NOTHING RETURNING run_id)
    SELECT run.id, (SELECT count(*) FROM frozen)::int AS froze FROM run`);
  return { runId: Number(r.rows[0].id), froze: Number(r.rows[0].froze) === 1 };
}

/** Latest run and the frozen verdict, for the results page. Null before the first run. */
export async function readUpsideGradeStatus(gradeVersion: string): Promise<{ latest: GradeRunRow | null; verdict: FrozenVerdict | null; runs: number } | null> {
  if (!await installed()) return null;
  const [latest, verdict, count] = await Promise.all([
    db.execute(sql`SELECT * FROM nfl_replacement_upside_grade_runs WHERE grade_version=${gradeVersion} ORDER BY id DESC LIMIT 1`),
    db.execute(sql`SELECT * FROM nfl_replacement_upside_grade_verdicts WHERE grade_version=${gradeVersion}`),
    db.execute(sql`SELECT count(*)::int AS n FROM nfl_replacement_upside_grade_runs WHERE grade_version=${gradeVersion}`),
  ]);
  return { latest: latest.rows[0] ? toRun(latest.rows[0]) : null, verdict: verdict.rows[0] ? toVerdict(verdict.rows[0]) : null,
    runs: Number(count.rows[0]?.n ?? 0) };
}
