/**
 * Ghost-lineup orchestration (`nfl-ghost-factorial-v1`,
 * docs/nfl-ghost-factorial-study.md). Runs on Vercel with no owner action:
 *
 *   1. gradable slates   the Sunday main Classic (the draft group whose teams
 *                        are exactly that Sunday's games before 7 PM ET) and
 *                        every primetime Showdown (kickoff 7 PM ET or later):
 *                        the contests stat-api serves in full for grading.
 *   2. freeze            between FREEZE_FROM and FREEZE_UNTIL minutes before
 *                        the slate's first kickoff: a system slate from the
 *                        newest pool snapshot, read through the owner's own
 *                        workspace path, frozen ONCE (one row, one digest).
 *   3. arms              the house (arm 0) and the 32 design arms, each built
 *                        from that frozen input and saved as a research run
 *                        with a lock check before and after the build. Arms
 *                        fan out one per function call; a call that cannot
 *                        be made falls back to building in this process.
 *
 * Every stage writes an event row, including the ones that did nothing for a
 * reason (no snapshot, no main-slate match, lock passed), so a slate that was
 * never enrolled is visible on /health rather than silently absent. Nothing
 * here prints a lineup or a projection: the repository is public and so are
 * its logs, but these routes' logs are Vercel's; still, only counts and ids.
 */
import { createHash, randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ensureNflGhostTables } from "@/db/ensure-schema";
import { optimizeNflLineups, NFL_OPTIMIZER_VERSION, type NflOptimizerSettings } from "@/app/dfs/nfl/nfl-optimizer";
import { ghostFreezeSlate } from "@/app/dfs/nfl/actions";
import { canonicalAuditJson } from "../audit-json";
import { buildCompletion } from "../build-completion";
import { canonicalNflTeam } from "../dk-salary-csv";
import { buildArm } from "./arm";
import { allArms, extraArms, ghostDesign, GHOST_DESIGN_VERSION, GHOST_EXTRA_ARMS_VERSION, type GhostFormat } from "./design";
import { GHOST_HOUSE_VERSION, houseSettings } from "./house";
import { dkGameInfo, SYSTEM_SLATE_VERSION, type PoolRow, type ScheduledGame } from "./system-slate";
import { freezeTiming, FRESH_LINES_DEADLINE_MINUTES, FRESH_LINES_MINUTES, type FreezeTiming } from "./freeze-timing";

export const FREEZE_FROM_MINUTES = 105;
export const FREEZE_UNTIL_MINUTES = 20;
/** No arm starts building inside this many minutes of the first kickoff. */
export const BUILD_STOP_MINUTES = 4;
/** No arm saves inside this many seconds of the first kickoff. */
export const SAVE_STOP_SECONDS = 60;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Row = Record<string, any>;
const rows = (r: unknown) => (r as { rows: Row[] }).rows;
const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

async function event(e: { draftGroupId?: number | null; freezeId?: string | null; arm?: number | null; stage: string; ok: boolean; detail?: string | null }) {
  await db.execute(sql`INSERT INTO nfl_dfs_ghost_events (design_version, draft_group_id, freeze_id, arm, stage, ok, detail)
    VALUES (${GHOST_DESIGN_VERSION}, ${e.draftGroupId ?? null}, ${e.freezeId ?? null}, ${e.arm ?? null}, ${e.stage}, ${e.ok}, ${(e.detail ?? "").slice(0, 1000) || null})`);
}

const etHour = (iso: string) => Number(new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "2-digit", hourCycle: "h23" }).format(new Date(iso)));
const etDay = (iso: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short" }).format(new Date(iso));
const etDate = (iso: string) => new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" }).format(new Date(iso));

export type GradableSlate = { draftGroupId: number; format: GhostFormat; snapshotId: string; firstKickoff: string;
  games: (ScheduledGame & { season: number; week: number })[]; kind: "main" | "primetime" };

/** Draft groups whose first kickoff is within `hours`, kept only when gradable (main Classic, primetime Showdown). */
export async function gradableSlates(now: Date, hours = 6): Promise<{ slates: GradableSlate[]; skipped: { draftGroupId: number; reason: string }[] }> {
  const latest = rows(await db.execute(sql`SELECT DISTINCT ON (draft_group_id) snapshot_id, draft_group_id, format, start_date, teams, captured_at
    FROM nfl_dfs_dk_pool_snapshots WHERE start_date BETWEEN ${now.toISOString()}::timestamptz AND ${now.toISOString()}::timestamptz + make_interval(hours => ${hours})
    ORDER BY draft_group_id, captured_at DESC`));
  if (!latest.length) return { slates: [], skipped: [] };
  const schedule = rows(await db.execute(sql`SELECT g.id, g.season, g.week, g.kickoff, h.abbreviation AS home, a.abbreviation AS away
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.kickoff BETWEEN ${now.toISOString()}::timestamptz - interval '1 day' AND ${now.toISOString()}::timestamptz + make_interval(hours => ${hours + 24})`))
    .map(g => ({ gameId: Number(g.id), season: Number(g.season), week: Number(g.week), kickoff: new Date(g.kickoff).toISOString(),
      home: canonicalNflTeam(String(g.home)), away: canonicalNflTeam(String(g.away)) }));
  const slates: GradableSlate[] = [], skipped: { draftGroupId: number; reason: string }[] = [];
  for (const s of latest) {
    const gid = Number(s.draft_group_id), start = new Date(s.start_date).toISOString();
    const teams = new Set((s.teams as string[]).map(canonicalNflTeam));
    const games = schedule.filter(g => teams.has(g.home) && teams.has(g.away) && Math.abs(Date.parse(g.kickoff) - Date.parse(start)) < 30 * 3600_000);
    const covered = new Set(games.flatMap(g => [g.home, g.away]));
    if (!games.length || [...teams].some(t => !covered.has(t))) { skipped.push({ draftGroupId: gid, reason: "teams do not resolve to scheduled games" }); continue; }
    const first = games.map(g => g.kickoff).sort()[0];
    if (s.format === "showdown") {
      if (games.length !== 1) { skipped.push({ draftGroupId: gid, reason: "showdown pool is not one game" }); continue; }
      if (etHour(first) < 19) { skipped.push({ draftGroupId: gid, reason: "not a primetime showdown (no full-standings flagship to grade)" }); continue; }
      slates.push({ draftGroupId: gid, format: "showdown", snapshotId: String(s.snapshot_id), firstKickoff: first, games, kind: "primetime" });
    } else {
      // The main slate: every game that Sunday from noon ET until 7 PM ET,
      // starting at that window's first kickoff. A morning London game is
      // its own slate, and "Afternoon Only"/"Turbo" groups start later.
      const sunday = etDay(first) === "Sun";
      const mainGames = schedule.filter(g => etDay(g.kickoff) === "Sun" && etDate(g.kickoff) === etDate(first)
        && etHour(g.kickoff) >= 12 && etHour(g.kickoff) < 19);
      const mainStart = mainGames.map(g => g.kickoff).sort()[0];
      if (mainStart !== first) { skipped.push({ draftGroupId: gid, reason: "classic group is not the Sunday main slate" }); continue; }
      const mainTeams = new Set(mainGames.flatMap(g => [g.home, g.away]));
      const exact = sunday && mainTeams.size === teams.size && [...teams].every(t => mainTeams.has(t));
      if (!exact) { skipped.push({ draftGroupId: gid, reason: "classic group is not the Sunday main slate" }); continue; }
      slates.push({ draftGroupId: gid, format: "classic", snapshotId: String(s.snapshot_id), firstKickoff: first, games, kind: "main" });
    }
  }
  return { slates, skipped };
}

/** Prop lines for the slate's first kickoff group captured after the pregame capture opens. */
async function freshLineCount(slate: GradableSlate, now: Date): Promise<number> {
  const r = rows(await db.execute(sql`SELECT count(*)::int AS n FROM prop_odds_history WHERE sport = 'nfl'
    AND commence_time BETWEEN ${slate.firstKickoff}::timestamptz - interval '5 minutes' AND ${slate.firstKickoff}::timestamptz + interval '5 minutes'
    AND captured_at >= ${slate.firstKickoff}::timestamptz - make_interval(mins => ${FRESH_LINES_MINUTES})
    AND captured_at <= ${now.toISOString()}::timestamptz`))[0];
  return Number(r?.n ?? 0);
}

/**
 * Freeze one slate's inputs once. Returns the freeze id, "waiting" while the
 * pregame lines have not landed and the deadline has not passed, or null with
 * an event saying why not.
 */
export async function freezeSlate(slate: GradableSlate, secret: string, now = new Date()): Promise<string | "waiting" | null> {
  const existing = rows(await db.execute(sql`SELECT freeze_id FROM nfl_dfs_ghost_freezes WHERE design_version = ${GHOST_DESIGN_VERSION} AND draft_group_id = ${slate.draftGroupId}`));
  if (existing.length) return String(existing[0].freeze_id);
  const minutes = (Date.parse(slate.firstKickoff) - now.getTime()) / 60_000;
  if (minutes > FREEZE_FROM_MINUTES || minutes < FREEZE_UNTIL_MINUTES) return null;
  let timing: FreezeTiming;
  let freshLines = 0;
  try {
    freshLines = await freshLineCount(slate, now);
    timing = freezeTiming(minutes, freshLines > 0);
  } catch (error) {
    // A failed freshness read must not block the freeze past its deadline.
    timing = minutes > FRESH_LINES_DEADLINE_MINUTES ? "wait" : "freeze_stale";
    await event({ draftGroupId: slate.draftGroupId, stage: "freeze_lines", ok: false, detail: error instanceof Error ? error.message : String(error) });
  }
  if (timing === "wait") {
    await event({ draftGroupId: slate.draftGroupId, stage: "freeze_wait", ok: true,
      detail: `waiting for pregame prop lines (none captured since T-${FRESH_LINES_MINUTES}); freezes regardless at T-${FRESH_LINES_DEADLINE_MINUTES}, now T-${Math.round(minutes)}` });
    return "waiting";
  }
  const linesNote = timing === "freeze"
    ? `lines: pregame capture (${freshLines} prop rows since T-${FRESH_LINES_MINUTES})`
    : `lines: STALE, no pregame prop capture by T-${FRESH_LINES_DEADLINE_MINUTES}; froze on older lines`;
  try {
    // Newest snapshot at freeze time (statuses change in the last hours).
    const snap = rows(await db.execute(sql`SELECT snapshot_id, captured_at FROM nfl_dfs_dk_pool_snapshots WHERE draft_group_id = ${slate.draftGroupId}
      ORDER BY captured_at DESC LIMIT 1`))[0];
    const poolRows = rows(await db.execute(sql`SELECT dk_pid, name, position, team, opponent, salary, status, is_disabled, ppg
      FROM nfl_dfs_dk_pool_player_status WHERE snapshot_id = ${snap.snapshot_id}::uuid`))
      .map(r => ({ ...r, dk_pid: Number(r.dk_pid), salary: r.salary == null ? null : Number(r.salary), ppg: r.ppg == null ? null : Number(r.ppg) })) as PoolRow[];
    const digest = sha256(canonicalAuditJson({ version: SYSTEM_SLATE_VERSION, snapshot: snap.snapshot_id, poolRows, games: slate.games }));
    const house = houseSettings(slate.format, { favoriteTeam: null, underdogTeam: null });
    const frozen = await ghostFreezeSlate(secret, { format: slate.format, poolRows, games: slate.games,
      fileName: `system:dk-pool:${slate.draftGroupId}:${snap.snapshot_id}`, digest, house });
    const infos = [...new Set(slate.games.map(g => dkGameInfo(g.away, g.home, g.kickoff)))].sort();
    const ownerSignature = sha256(`${slate.format}|${infos.join("|")}`);
    const inputDigest = sha256(canonicalAuditJson({ inputSnapshot: frozen.inputSnapshot, house: frozen.resolvedHouse }));
    const freezeId = randomUUID();
    const notes = [...frozen.warnings, linesNote, `projections ${frozen.modelVersion} as of ${frozen.modelAsOf}`, `ownership ${frozen.ownership.capability}, leverage ${frozen.ownership.leverage ? "on" : "off"}`,
      `design ${ghostDesign(slate.format).digest}`, `extra-arms ${GHOST_EXTRA_ARMS_VERSION} ${extraArms(slate.format).digest}`,
      `house ${GHOST_HOUSE_VERSION}`, `optimizer ${NFL_OPTIMIZER_VERSION}`];
    if (!frozen.projectionRunId) throw new Error("The system slate resolved no projection run");
    const inserted = rows(await db.execute(sql`INSERT INTO nfl_dfs_ghost_freezes (freeze_id, design_version, draft_group_id, pool_snapshot_id, upload_id, projection_run_id, format, season, week,
        game_ids, teams, owner_signature, first_kickoff, deployed_sha, input_snapshot, house_settings, input_digest, notes)
      VALUES (${freezeId}::uuid, ${GHOST_DESIGN_VERSION}, ${slate.draftGroupId}, ${snap.snapshot_id}::uuid, ${frozen.uploadId}::uuid, ${frozen.projectionRunId}::uuid, ${slate.format},
        ${slate.games[0].season}, ${slate.games[0].week}, ${JSON.stringify(slate.games.map(g => g.gameId))}::jsonb,
        ${JSON.stringify([...new Set(slate.games.flatMap(g => [g.away, g.home]))].sort())}::jsonb, ${ownerSignature}, ${slate.firstKickoff}::timestamptz,
        ${process.env.VERCEL_GIT_COMMIT_SHA ?? null}, ${JSON.stringify(frozen.inputSnapshot)}::jsonb, ${JSON.stringify(frozen.resolvedHouse)}::jsonb,
        ${inputDigest}, ${JSON.stringify(notes)}::jsonb)
      ON CONFLICT (design_version, draft_group_id) DO NOTHING RETURNING freeze_id`));
    const id = inserted[0]?.freeze_id ? String(inserted[0].freeze_id)
      : String(rows(await db.execute(sql`SELECT freeze_id FROM nfl_dfs_ghost_freezes WHERE design_version = ${GHOST_DESIGN_VERSION} AND draft_group_id = ${slate.draftGroupId}`))[0].freeze_id);
    await event({ draftGroupId: slate.draftGroupId, freezeId: id, stage: "freeze", ok: true,
      detail: `${slate.format} ${slate.kind}, ${frozen.inputSnapshot.length} players, kickoff ${slate.firstKickoff}; ${linesNote}` });
    return id;
  } catch (error) {
    await event({ draftGroupId: slate.draftGroupId, stage: "freeze", ok: false, detail: error instanceof Error ? error.message : String(error) });
    return null;
  }
}

/** The extra-arms digest a freeze was taken under, or null for a freeze that predates the extra arms. */
const recordedExtraArms = (notes: string[]) => notes.find(n => n.startsWith("extra-arms "))?.split(" ")[2] ?? null;

/**
 * Arms still to build: every design arm, plus the extra arms when the freeze
 * recorded the deployed extra-arms digest (a freeze taken before the pair
 * existed, or under another pair, gets the design arms only; the pair's
 * cohort starts at the first freeze that recorded it).
 */
export async function missingArms(freezeId: string): Promise<number[]> {
  const f = rows(await db.execute(sql`SELECT format, notes FROM nfl_dfs_ghost_freezes WHERE freeze_id = ${freezeId}::uuid`))[0];
  const have = new Set(rows(await db.execute(sql`SELECT arm FROM nfl_dfs_ghost_arms WHERE freeze_id = ${freezeId}::uuid`)).map(r => Number(r.arm)));
  const format = f.format as GhostFormat;
  const extra = recordedExtraArms((f.notes as string[]) ?? []) === extraArms(format).digest;
  return (extra ? allArms(format) : ghostDesign(format).arms).map(a => a.arm).filter(a => !have.has(a));
}

const lockOpen = (kickoff: string, marginMs: number) => Date.now() < Date.parse(kickoff) - marginMs;

/** Build and save one arm. Idempotent: an arm already saved is skipped. */
export async function buildAndSaveArm(freezeId: string, arm: number): Promise<{ status: string; built: number; ms?: number; skipped?: string }> {
  await ensureNflGhostTables();
  const f = rows(await db.execute(sql`SELECT freeze_id, format, upload_id, projection_run_id, first_kickoff, input_snapshot, house_settings, notes, draft_group_id
    FROM nfl_dfs_ghost_freezes WHERE freeze_id = ${freezeId}::uuid`))[0];
  if (!f) throw new Error(`No freeze ${freezeId}`);
  const kickoff = new Date(f.first_kickoff).toISOString();
  if (rows(await db.execute(sql`SELECT 1 FROM nfl_dfs_ghost_arms WHERE freeze_id = ${freezeId}::uuid AND arm = ${arm}`)).length) return { status: "exists", built: 0, skipped: "already saved" };
  if (!lockOpen(kickoff, BUILD_STOP_MINUTES * 60_000)) {
    await event({ draftGroupId: Number(f.draft_group_id), freezeId, arm, stage: "arm", ok: false, detail: "lock window: too close to first kickoff to start a build" });
    return { status: "late", built: 0, skipped: "lock window" };
  }
  const format = f.format as GhostFormat;
  const design = ghostDesign(format);
  const recordedDesign = (f.notes as string[]).find(n => n.startsWith("design "))?.slice(7);
  if (recordedDesign !== design.digest) throw new Error("The deployed design differs from the one this slate was frozen under; refusing (a changed design is a new version)");
  const extra = extraArms(format);
  const spec = allArms(format).find(a => a.arm === arm);
  if (!spec) throw new Error(`Arm ${arm} is not in the design`);
  if (spec.extra && recordedExtraArms((f.notes as string[]) ?? []) !== extra.digest)
    throw new Error("This slate was not frozen under the deployed extra-arms pair; refusing (a changed pair is a new version)");
  const house = f.house_settings as NflOptimizerSettings;
  const started = Date.now();
  let result: ReturnType<typeof optimizeNflLineups> | null = null, error: string | null = null;
  let built: ReturnType<typeof buildArm> | null = null;
  try {
    built = buildArm(format, f.input_snapshot as Row[], house, spec.levels);
    result = optimizeNflLineups(built.players, built.settings);
  } catch (e) { error = e instanceof Error ? e.message : String(e); }
  const ms = Date.now() - started;
  const lineups = result?.lineups ?? [];
  const completion = buildCompletion({ ...(result ?? { warnings: [], sourceCoverage: { requested: 0, direct: 0, fallback: 0, excluded: 0 }, lineups: [] }),
    requestedLineups: house.nLineups, generatedLineups: lineups.length });
  const status = `research_${error ? "failed" : completion.status}`;
  const researchStudy = { version: GHOST_DESIGN_VERSION, freezeId, arm, house: spec.house, levels: spec.levels, designDigest: design.digest,
    ...(spec.extra ? { extraArms: GHOST_EXTRA_ARMS_VERSION, extraArmsDigest: extra.digest } : {}),
    houseVersion: GHOST_HOUSE_VERSION, draftGroupId: Number(f.draft_group_id), ruleExcludedIds: built?.ruleExcludedIds ?? [],
    completion: error ? "failed" : completion.status, issues: completion.issues, error, buildMs: ms,
    deployedSha: process.env.VERCEL_GIT_COMMIT_SHA ?? null };
  const settings = { ...(built?.settings ?? house), researchStudy, runEvidence: {
    eligibility: result?.eligibility ?? [], exposureReport: result?.exposureReport ?? [], duplication: result?.duplication ?? [],
    warnings: [...(result?.warnings ?? []), ...(built?.notes ?? []), ...(error ? [error] : [])], archetypePlan: result?.archetypePlan ?? [] } };
  // The frozen input lives once on the freeze row; the run records its digest, not a 5 MB copy.
  const buildInfo = { commitSha: process.env.VERCEL_GIT_COMMIT_SHA ?? null, settingsSchemaVersion: GHOST_DESIGN_VERSION, optimizerVersion: NFL_OPTIMIZER_VERSION };
  const inputDigest = sha256(canonicalAuditJson({ settings, freezeId, optimizerVersion: NFL_OPTIMIZER_VERSION, buildInfo }));
  if (!lockOpen(kickoff, SAVE_STOP_SECONDS * 1000)) {
    await event({ draftGroupId: Number(f.draft_group_id), freezeId, arm, stage: "arm", ok: false, detail: `built in ${ms} ms but the lock window closed before saving` });
    return { status: "late", built: lineups.length, ms, skipped: "lock window at save" };
  }
  const runId = randomUUID();
  const writes = [
    db.execute(sql`INSERT INTO nfl_dfs_optimizer_runs (run_id, upload_id, projection_run_id, optimizer_version, mode, projection_source, settings,
        input_snapshot, input_digest, build_info, requested_lineups, generated_lineups, status, failure_reason, created_at)
      SELECT ${runId}::uuid, ${f.upload_id}::uuid, ${f.projection_run_id}::uuid, ${NFL_OPTIMIZER_VERSION}, 'gpp', 'our', ${JSON.stringify(settings)}::jsonb,
        '[]'::jsonb, ${inputDigest}, ${JSON.stringify(buildInfo)}::jsonb, ${house.nLineups}, ${lineups.length}, ${status},
        ${error ?? (completion.issues.join(" ") || null)}, clock_timestamp()
      FROM nfl_dfs_slate_uploads u WHERE u.upload_id = ${f.upload_id}::uuid`),
    ...lineups.map(l => db.execute(sql`INSERT INTO nfl_dfs_lineups (run_id, lineup_number, slots, player_ids, total_salary, projected_fpts, floor_fpts,
        ceiling_fpts, projected_ownership, stack_summary)
      VALUES (${runId}::uuid, ${l.lineupNumber}, ${JSON.stringify(l.slots.map(s => ({ slot: s.slot, dkPlayerId: s.player.dkPlayerId,
        captainDkPlayerId: s.player.captainDkPlayerId, name: s.player.name, team: s.player.team, salary: s.salary, projection: s.projection,
        source: s.projectionSource, multiplier: s.multiplier })))}::jsonb, ${JSON.stringify(l.playerIds)}::jsonb, ${l.totalSalary}, ${l.projectedFpts},
        ${l.floorFpts}, ${l.ceilingFpts}, ${l.projectedOwnership}, ${JSON.stringify({ ...l.stackSummary, archetype: l.archetype ?? null })}::jsonb)`)),
    // Unique (freeze_id, arm): a concurrent duplicate fails this whole batch, never half of it.
    db.execute(sql`INSERT INTO nfl_dfs_ghost_arms (freeze_id, arm, run_id, levels, status, requested, built, build_ms)
      VALUES (${freezeId}::uuid, ${arm}, ${runId}::uuid, ${JSON.stringify(spec.levels)}::jsonb, ${status}, ${house.nLineups}, ${lineups.length}, ${ms})`),
  ];
  try {
    await db.batch(writes as unknown as Parameters<typeof db.batch>[0]);
  } catch (e) {
    if (rows(await db.execute(sql`SELECT 1 FROM nfl_dfs_ghost_arms WHERE freeze_id = ${freezeId}::uuid AND arm = ${arm}`)).length)
      return { status: "exists", built: 0, skipped: "saved concurrently" };
    throw e;
  }
  await event({ draftGroupId: Number(f.draft_group_id), freezeId, arm, stage: "arm", ok: !error,
    detail: `${status}: ${lineups.length}/${house.nLineups} lineups in ${ms} ms${error ? `; ${error}` : ""}` });
  return { status, built: lineups.length, ms };
}

/**
 * One cron tick: freeze what is due, then build every missing arm of every
 * open freeze. Arms fan out to the arm route (one function call each) when a
 * production URL is known; a call that fails falls back to an in-process
 * build while time remains.
 */
export async function runGhostTick(secret: string, budgetMs = 270_000): Promise<Row> {
  const t0 = Date.now();
  await ensureNflGhostTables();
  const now = new Date();
  const { slates, skipped } = await gradableSlates(now);
  const frozen: string[] = [];
  const freezeFailures: number[] = [];
  for (const s of slates) {
    const id = await freezeSlate(s, secret, now);
    if (id === "waiting") continue;
    if (id) frozen.push(id);
    else {
      const minutes = (Date.parse(s.firstKickoff) - now.getTime()) / 60_000;
      if (minutes <= FREEZE_FROM_MINUTES && minutes >= FREEZE_UNTIL_MINUTES) freezeFailures.push(s.draftGroupId);
    }
  }
  const open = rows(await db.execute(sql`SELECT freeze_id, first_kickoff FROM nfl_dfs_ghost_freezes WHERE design_version = ${GHOST_DESIGN_VERSION}
    AND first_kickoff > clock_timestamp() + make_interval(mins => ${BUILD_STOP_MINUTES})`));
  const base = process.env.VERCEL_PROJECT_PRODUCTION_URL ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}` : null;
  const outcomes: Row[] = [];
  const local: { freezeId: string; arm: number }[] = [];
  const calls: Promise<void>[] = [];
  for (const o of open) {
    const freezeId = String(o.freeze_id);
    for (const arm of await missingArms(freezeId)) {
      if (!base) { local.push({ freezeId, arm }); continue; }
      calls.push((async () => {
        try {
          const res = await fetch(`${base}/api/nfl-ghost/arm`, { method: "POST", headers: { authorization: `Bearer ${secret}`, "content-type": "application/json" },
            body: JSON.stringify({ freezeId, arm }), signal: AbortSignal.timeout(Math.max(10_000, budgetMs - (Date.now() - t0) - 5_000)) });
          if (!res.ok) throw new Error(`arm route ${res.status}`);
          outcomes.push({ freezeId, arm, ...(await res.json()) });
        } catch (error) {
          outcomes.push({ freezeId, arm, status: "fanout_failed", detail: error instanceof Error ? error.message : String(error) });
          local.push({ freezeId, arm });
        }
      })());
    }
  }
  await Promise.all(calls);
  for (const { freezeId, arm } of local) {
    if (Date.now() - t0 > budgetMs - 30_000) { outcomes.push({ freezeId, arm, status: "deferred", detail: "time budget; next tick" }); continue; }
    try { outcomes.push({ freezeId, arm, ...(await buildAndSaveArm(freezeId, arm)) }); }
    catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      await event({ freezeId, arm, stage: "arm", ok: false, detail });
      outcomes.push({ freezeId, arm, status: "error", detail });
    }
  }
  // Visible failure (the heartbeat goes red): a due slate that could not freeze,
  // or a frozen slate still missing arms inside 30 minutes of its kickoff.
  const lateMissing: string[] = [];
  for (const o of open) {
    if (Date.parse(new Date(o.first_kickoff).toISOString()) - Date.now() < 30 * 60_000 && (await missingArms(String(o.freeze_id))).length)
      lateMissing.push(String(o.freeze_id));
  }
  return { gradable: slates.map(s => ({ draftGroupId: s.draftGroupId, format: s.format, kind: s.kind, firstKickoff: s.firstKickoff })),
    skipped, frozen, freezeFailures, lateMissing, openFreezes: open.length, arms: outcomes.length,
    built: outcomes.filter(o => String(o.status).startsWith("research_")).length,
    // An arm the optimizer cannot build (research_failed) is a result, not a pipeline failure.
    failed: outcomes.filter(o => o.status === "error").length + freezeFailures.length + lateMissing.length,
    ms: Date.now() - t0 };
}
