"use server";

import { createHash, randomUUID } from "node:crypto";
import { and, desc, eq } from "drizzle-orm";
import { db } from "@/db";
import { ensureNflDfsTables } from "@/db/ensure-schema";
import {
  nflDfsLineups,
  nflDfsOptimizerRuns,
  nflDfsPlayerProjections,
  nflDfsProjectionRuns,
  nflDfsSlatePlayers,
  nflDfsSlateUploads,
} from "@/db/schema";
import { parseNflDkSalaryCsv } from "@/lib/nfl-dfs/dk-salary-csv";
import { getNflRosterEvidence, getNflInjuryCoverage, type InjuryCoverage } from "@/db/nfl-dfs-availability";
import { resolveGameAvailability, type Availability } from "@/lib/nfl-dfs/availability";
import { previewAbsence } from "@/lib/nfl-dfs/absence-preview";
import type { PlayerContext } from "@/lib/nfl-dfs/player-context";
import { benchmarkPool, type Competitor, type ImportEvidence, type BenchmarkSnapshot, benchmarkTeam } from '@/lib/nfl-dfs/competitor-benchmark';
import { saveNflBenchmark, readNflBenchmarks } from '@/db/nfl-dfs-benchmark';
import { redistributeInjuryTargets } from '@/lib/nfl-dfs/injury-redistribution';
import { getCalibratedSnapshots } from "@/db/nfl-dfs-calibrated";
import { readCalibratedProjection, type CalibrationSnapshot } from "@/lib/nfl-dfs/calibrated-projection";
import {
  NFL_OPTIMIZER_VERSION,
  optimizeNflLineups,
  type NflGeneratedLineup,
  type NflOptimizerPlayer,
  type NflOptimizerSettings,
} from "./nfl-optimizer";

export type NflWorkspacePlayer = NflOptimizerPlayer & {
  ffPlayerId: number | null;
  identityMethod: string;
  modelConfidence: number | null;
  historyGames: number | null;
  dkStatus: string | null;
  availability?: Availability;
  gameInfo: string | null;
};

export type NflWorkspaceSlate = {
  injuryCoverage?: InjuryCoverage | null;
  uploadId: string;
  projectionRunId: string | null;
  modelVersion: string | null;
  modelAsOf: string | null;
  format: "classic" | "showdown";
  games: string[];
  teams: string[];
  warnings: string[];
  players: NflWorkspacePlayer[];
  fileName: string;
};

export type NflComparisonSource = "fantasypros" | "linestar" | "custom";

export async function loadNflBenchmarks(uploadId:string) { return readNflBenchmarks(uploadId); }

export async function freezeNflBenchmark(uploadId:string,source:Competitor) {
  const slate=await workspaceSlate(uploadId);
  if(slate.format!=='classic')return {ok:false as const,error:'The first benchmark supports Classic slates only.'};
  if(!slate.modelAsOf||!slate.projectionRunId)return {ok:false as const,error:'Load a slate linked to our model first.'};
  const raw=await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId,uploadId));
  const upload=(await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId,uploadId)).limit(1))[0];
  // Use values from the same query as their import evidence, avoiding mixed upload revisions.
  const capturedAt=new Date().toISOString();
  const evidence=Object.fromEntries(raw.map(p=>[String(p.dkPlayerId),p.comparisonEvidence as Partial<Record<Competitor,ImportEvidence>>]));
  const players=slate.players.map(p=>{const row=raw.find(r=>r.dkPlayerId===p.dkPlayerId)!;return {...p,fantasyprosProj:row.fantasyprosProj,linestarProj:row.linestarProj};});
  try {
    const pool=benchmarkPool(players,evidence,source,Date.parse(capturedAt),slate.modelAsOf);
    if(!pool.rows.length)return {ok:false as const,error:'No paired pregame players. Import current competitor projections and refresh forecasts.'};
    const settings:NflOptimizerSettings={format:'classic',mode:'gpp',projectionSource:'custom',allowDkFallback:false,nLineups:5,minSalary:45000,maxExposure:1,minUnique:1,stackPassCatchers:1,bringBack:false,randomness:0,lockedPlayerIds:[],excludedPlayerIds:[],minExposureByPlayer:{},maxExposureByPlayer:{}};
    const lineups:BenchmarkSnapshot['lineups']=[],lineupWarnings:string[]=[];
    for(const variant of ['our','competitor'] as const) {
      const inputs=pool.rows.map(r=>({...r.player,customProj:variant==='our'?r.player.ourProj:r.competitor,linestarOwnPct:null}));
      const result=optimizeNflLineups(inputs,settings);
      lineups.push(...result.lineups.map(l=>({source:variant,slots:l.slots.map(s=>({id:s.player.dkPlayerId,multiplier:s.multiplier}))})));
      if(result.lineups.length!==5)lineupWarnings.push(`${variant}: ${result.lineups.length}/5 legal lineups; insufficient paired pool or constraints.`);
    }
    if(lineups.filter(l=>l.source==='our').length!==lineups.filter(l=>l.source==='competitor').length) {
      lineups.length=0;lineupWarnings.push('Unequal portfolio sizes: lineup comparison withheld. Player comparison retained.');
    }
    const snapshot:BenchmarkSnapshot={version:'nfl-competitor-benchmark-v1',capturedAt,source,uploadId,modelAsOf:slate.modelAsOf,projectionRunId:slate.projectionRunId,optimizerVersion:NFL_OPTIMIZER_VERSION,...pool,settings,lineups,lineupWarnings,salaryDigest:upload.fileDigest,sourcePublicationTime:'unknown'};
    const digest=sha256(JSON.stringify(snapshot));await saveNflBenchmark(digest,snapshot);
    return {ok:true as const,digest,paired:pool.rows.length};
  } catch(error) {return {ok:false as const,error:error instanceof Error?error.message:'Benchmark could not be saved.'};}
}

export async function previewNflTargetRedistribution(uploadId:string,team:string) {
  const slate=await workspaceSlate(uploadId);
  if(!slate.teams.includes(team)||!slate.projectionRunId)return {ok:false as const,error:'Choose a team in a model-linked salary slate.'};
  const run=(await db.select().from(nflDfsProjectionRuns).where(eq(nflDfsProjectionRuns.runId,slate.projectionRunId)).limit(1))[0];
  const [{default:context},{default:historical},roster]=await Promise.all([import('@/data/nfl-team-context.json'),import('@/data/nfl-player-context-2025.json'),getNflRosterEvidence(run.season,run.week)]);
  const selected=context.teams.find(t=>benchmarkTeam(t.team)===benchmarkTeam(team));const now=Date.now();
  if(!selected||context.season!==run.season||Date.parse(context.as_of)>now||now-Date.parse(context.as_of)>72*3600000)return {ok:false as const,error:'Refresh the full-roster team-context snapshot.'};
  const members=selected.players.map(p=>{const e=roster.get(Number(p.id));return {...p,availability:resolveGameAvailability(e,team,p.position,now,run.week,e?.kickoff??null)};});
  const qb=members.filter(p=>p.position==='QB'&&p.availability.role==='Expected starter · QB1'&&p.availability.fresh&&!p.availability.blockedReason);
  const history=historical as unknown as PlayerContext;
  const games=Object.entries(history.games).filter(([,g])=>benchmarkTeam(g.team)===benchmarkTeam(team)).sort((a,b)=>b[1].week-a[1].week).slice(0,4);
  const qbs=games.map(([key])=>history.rows.filter(r=>r.gameKey===key&&(r.attempts??0)>0).sort((a,b)=>(b.attempts??0)-(a.attempts??0))[0]?.playerId);
  const historicalQb=qbs.length===4&&qbs.every(q=>q&&q===qbs[0])?qbs[0]!:null;
  const profile=selected.profiles.all;
  if(!profile)return {ok:false as const,error:'Historical team passing budget unavailable.'};
  const targets=profile.plays_per_game*(1-profile.designed_run_rate)*(1-profile.scramble_rate-profile.sack_rate)*profile.target_rate;
  try {
    const result=redistributeInjuryTargets(members,targets,qb.length===1?qb[0].identity:null,historicalQb,now);
    return {ok:true as const,result:{...result,team,evaluatedAt:new Date(now).toISOString(),rosterDigest:context.roster_digest,recipeDigest:context.recipe_digest,coaching: selected.coaching,continuity:selected.continuity,priorWindow:selected.prior_role_window}};
  }catch(error){return {ok:false as const,error:error instanceof Error?error.message:'Target scenario unavailable.'};}
}

export async function previewNflAbsence(uploadId: string, receiverId: number, teammateId: number) {
  const slate = await workspaceSlate(uploadId); // Re-read official evidence; never trust browser flags.
  const receiver = slate.players.find(p=>p.dkPlayerId===receiverId);
  const teammate = slate.players.find(p=>p.dkPlayerId===teammateId);
  if (!receiver || !teammate) return {ok:false as const,error:'Both players must belong to this saved salary slate.'};
  const { default: history } = await import('@/data/nfl-player-context-2025.json');
  try {
    const result = previewAbsence(history as unknown as PlayerContext, receiver, teammate, Date.now());
    return {ok:true as const,result:{...result,uploadId,projectionRunId:slate.projectionRunId,digest:sha256(JSON.stringify(result))}};
  } catch (error) {
    return {ok:false as const,error:error instanceof Error ? error.message : 'Scenario evidence is unavailable.'};
  }
}
export type NflComparisonRow = { name: string; team?: string | null; projection?: number | null; ownership?: number | null };

function normalizeName(value: string): string {
  return value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, "").replace(/[^a-z0-9]+/g, "");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function numeric(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function latestProjectionRun() {
  const rows = await db.select({
    runId: nflDfsProjectionRuns.runId,
    modelVersion: nflDfsProjectionRuns.modelVersion,
    asOfAt: nflDfsProjectionRuns.asOfAt,
    season: nflDfsProjectionRuns.season,
    week: nflDfsProjectionRuns.week,
  }).from(nflDfsProjectionRuns).orderBy(desc(nflDfsProjectionRuns.season), desc(nflDfsProjectionRuns.week), desc(nflDfsProjectionRuns.asOfAt)).limit(1);
  return rows[0] ?? null;
}

async function workspaceSlate(uploadId: string): Promise<NflWorkspaceSlate> {
  const uploads = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  const upload = uploads[0];
  if (!upload) throw new Error("NFL slate upload was not found.");
  const run = upload.projectionRunId
    ? (await db.select().from(nflDfsProjectionRuns).where(eq(nflDfsProjectionRuns.runId, upload.projectionRunId)).limit(1))[0] ?? null
    : null;
  const rows = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  let snapshots: CalibrationSnapshot[] = [];
  let calibrationWarning: string | null = null;
  if (run?.week) {
    try { snapshots = await getCalibratedSnapshots(run.season, run.week); }
    catch { calibrationWarning = "Calibrated forecasts could not be loaded; historical projections remain available."; }
  }
  const byPlayer = new Map(snapshots.map(s => [s.playerId, s]));
  const roster = run ? await getNflRosterEvidence(run.season, run.week) : new Map();
  const injuryCoverage = run ? await getNflInjuryCoverage(run.season,run.week) : null;
  const now = Date.now();
  const availability = (row: typeof rows[number]) => resolveGameAvailability(roster.get(row.ffPlayerId ?? -1), row.team, row.position, now, run?.week ?? null, roster.get(row.ffPlayerId ?? -1)?.kickoff ?? null);
  return {
    injuryCoverage,
    uploadId,
    projectionRunId: upload.projectionRunId,
    modelVersion: run?.modelVersion ?? null,
    modelAsOf: run?.asOfAt?.toISOString() ?? null,
    format: upload.format as "classic" | "showdown",
    games: upload.games as string[],
    teams: upload.teams as string[],
    warnings: [...upload.warnings as string[], ...(calibrationWarning ? [calibrationWarning] : [])],
    fileName: upload.fileName,
    players: rows.map((row) => ({
      id: row.id,
      dkPlayerId: row.dkPlayerId,
      captainDkPlayerId: row.captainDkPlayerId,
      ffPlayerId: row.ffPlayerId,
      name: row.name,
      position: row.position as NflWorkspacePlayer["position"],
      team: row.team,
      opponent: row.opponent,
      gameKey: row.gameKey,
      gameInfo: row.gameInfo,
      salary: row.salary,
      captainSalary: row.captainSalary,
      avgFptsDk: numeric(row.avgFptsDk),
      dkStatus: row.dkStatus,
      isOut: row.isOut || Boolean(availability(row).blockedReason),
      availability: availability(row),
      identityMethod: row.identityMethod,
      projectionStatus: row.projectionStatus,
      ourProj: numeric(row.ourProj),
      floorFpts: numeric(row.floorFpts),
      ceilingFpts: numeric(row.ceilingFpts),
      boomRate: numeric(row.boomRate),
      modelConfidence: numeric(row.modelConfidence),
      historyGames: row.historyGames,
      fantasyprosProj: numeric(row.fantasyprosProj),
      linestarProj: numeric(row.linestarProj),
      linestarOwnPct: numeric(row.linestarOwnPct),
      customProj: numeric(row.customProj),
      ...(() => {
        const candidate = readCalibratedProjection(byPlayer.get(row.ffPlayerId ?? -1), row, run?.season ?? 0, run?.week ?? 0, now);
        return { calibrated: candidate.projection, calibrationReason: candidate.reason };
      })(),
    })),
  };
}

export async function loadNflSalaryCsv(formData: FormData): Promise<NflWorkspaceSlate> {
  await ensureNflDfsTables();
  const file = formData.get("file");
  if (!(file instanceof File)) throw new Error("Select a DraftKings NFL salary CSV.");
  const content = await file.text();
  const slate = parseNflDkSalaryCsv(content);
  const digest = sha256(content);
  const run = await latestProjectionRun();
  const projectionRows = run
    ? await db.select().from(nflDfsPlayerProjections).where(eq(nflDfsPlayerProjections.runId, run.runId))
    : [];
  const byIdentity = new Map<string, typeof projectionRows>();
  for (const projection of projectionRows) {
    const keys = [
      `${projection.normalizedName}|${projection.position}|${projection.team ?? ""}`,
      `${projection.normalizedName}|${projection.position}|`,
      projection.position === "DST" ? `dst|DST|${projection.team ?? ""}` : "",
    ].filter(Boolean);
    for (const key of keys) byIdentity.set(key, [...(byIdentity.get(key) ?? []), projection]);
  }
  const signature = sha256(`${slate.format}|${slate.games.join("|")}`);
  const existing = await db.select({ uploadId: nflDfsSlateUploads.uploadId })
    .from(nflDfsSlateUploads)
    .where(run ? and(eq(nflDfsSlateUploads.fileDigest, digest), eq(nflDfsSlateUploads.projectionRunId, run.runId)) : eq(nflDfsSlateUploads.fileDigest, digest))
    .orderBy(desc(nflDfsSlateUploads.createdAt)).limit(1);
  const uploadId = existing[0]?.uploadId ?? randomUUID();
  if (!existing[0]) {
    await db.insert(nflDfsSlateUploads).values({
      uploadId,
      slateSignature: signature,
      fileName: file.name,
      fileDigest: digest,
      format: slate.format,
      games: slate.games,
      teams: slate.teams,
      warnings: slate.warnings,
      playerCount: slate.players.length,
      projectionRunId: run?.runId ?? null,
    });
  }
  for (const player of slate.players) {
    const normalized = normalizeName(player.name);
    const teamKey = `${normalized}|${player.position}|${player.teamAbbrev}`;
    const broadKey = `${normalized}|${player.position}|`;
    const dstKey = `dst|DST|${player.teamAbbrev}`;
    let matches = byIdentity.get(player.position === "DST" ? dstKey : teamKey) ?? [];
    let identityMethod = player.position === "DST" ? "exact_name_position_team" : "exact_name_position_team";
    if (matches.length !== 1 && player.position !== "DST") {
      matches = byIdentity.get(broadKey) ?? [];
      identityMethod = "exact_name_position";
    }
    const projection = matches.length === 1 ? matches[0] : null;
    if (!projection) identityMethod = matches.length > 1 ? "ambiguous" : "unmatched";
    const values = {
      uploadId,
      dkPlayerId: player.dkPlayerId,
      captainDkPlayerId: player.captain?.dkPlayerId ?? null,
      ffPlayerId: projection?.playerId ?? null,
      name: player.name,
      normalizedName: normalized,
      position: player.position,
      rosterPositions: player.rosterPositions,
      team: player.teamAbbrev,
      opponent: player.opponent,
      gameKey: player.gameKey,
      gameInfo: player.gameInfo,
      salary: player.salary,
      captainSalary: player.captain?.salary ?? null,
      avgFptsDk: player.avgFptsDk,
      dkStatus: player.status,
      isOut: player.isOut,
      identityMethod,
      projectionStatus: projection?.projectionStatus ?? "unmatched",
      ourProj: projection?.modelProjFpts ?? null,
      floorFpts: projection?.floorFpts ?? null,
      medianFpts: projection?.medianFpts ?? null,
      ceilingFpts: projection?.ceilingFpts ?? null,
      boomRate: projection?.boomRate ?? null,
      modelConfidence: projection?.confidence ?? null,
      historyGames: projection?.historyGames ?? null,
      updatedAt: new Date(),
    };
    await db.insert(nflDfsSlatePlayers).values(values).onConflictDoUpdate({
      target: [nflDfsSlatePlayers.uploadId, nflDfsSlatePlayers.dkPlayerId],
      set: {
        captainDkPlayerId: values.captainDkPlayerId,
        ffPlayerId: values.ffPlayerId,
        name: values.name,
        normalizedName: values.normalizedName,
        position: values.position,
        rosterPositions: values.rosterPositions,
        team: values.team,
        opponent: values.opponent,
        gameKey: values.gameKey,
        gameInfo: values.gameInfo,
        salary: values.salary,
        captainSalary: values.captainSalary,
        avgFptsDk: values.avgFptsDk,
        dkStatus: values.dkStatus,
        isOut: values.isOut,
        identityMethod: values.identityMethod,
        projectionStatus: values.projectionStatus,
        ourProj: values.ourProj,
        floorFpts: values.floorFpts,
        medianFpts: values.medianFpts,
        ceilingFpts: values.ceilingFpts,
        boomRate: values.boomRate,
        modelConfidence: values.modelConfidence,
        historyGames: values.historyGames,
        updatedAt: values.updatedAt,
      },
    });
  }
  return workspaceSlate(uploadId);
}

/** Resume an existing salary snapshot while reading the latest qualified candidates. */
export async function loadLatestNflSlate(): Promise<NflWorkspaceSlate | null> {
  const rows = await db.select({ uploadId: nflDfsSlateUploads.uploadId }).from(nflDfsSlateUploads).orderBy(desc(nflDfsSlateUploads.createdAt)).limit(1);
  return rows[0] ? workspaceSlate(rows[0].uploadId) : null;
}

export async function applyNflComparison(
  uploadId: string,
  source: NflComparisonSource,
  rows: NflComparisonRow[],
  fileName: string,
): Promise<{ slate: NflWorkspaceSlate; matched: number; unmatched: string[] }> {
  await ensureNflDfsTables();
  const players = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  const unmatched: string[] = [];
  let matched = 0;
  const evidence = { fileName, importedAt: new Date().toISOString(), rowCount: rows.length, digest: sha256(JSON.stringify(rows)) };
  for (const incoming of rows) {
    const name = normalizeName(incoming.name);
    const team = incoming.team?.trim().toUpperCase() ?? "";
    const candidates = players.filter((player) => player.normalizedName === name && (!team || player.team === team));
    if (candidates.length !== 1) { unmatched.push(incoming.name); continue; }
    const player = candidates[0];
    const projection = numeric(incoming.projection);
    const ownership = numeric(incoming.ownership);
    const currentEvidence = (player.comparisonEvidence ?? {}) as Record<string, unknown>;
    await db.update(nflDfsSlatePlayers).set({
      ...(source === "fantasypros" ? { fantasyprosProj: projection } : {}),
      ...(source === "linestar" ? { linestarProj: projection, linestarOwnPct: ownership } : {}),
      ...(source === "custom" ? { customProj: projection } : {}),
      comparisonEvidence: { ...currentEvidence, [source]: evidence },
      updatedAt: new Date(),
    }).where(eq(nflDfsSlatePlayers.id, player.id));
    matched++;
  }
  return { slate: await workspaceSlate(uploadId), matched, unmatched };
}

export async function runNflOptimizer(
  uploadId: string,
  settings: NflOptimizerSettings,
): Promise<{ runId: string; slate: NflWorkspaceSlate; result: { lineups: NflGeneratedLineup[]; warnings: string[]; sourceCoverage: { requested: number; direct: number; fallback: number; excluded: number } } }> {
  await ensureNflDfsTables();
  const slate = await workspaceSlate(uploadId);
  const result = optimizeNflLineups(slate.players, settings);
  if (result.lineups.some(l => l.slots.some(s => s.projectionSource === "calibrated" && Date.parse(s.player.calibrated!.kickoff) <= Date.now()))) throw new Error("A calibrated player's game started during optimization. Refresh the slate before regenerating.");
  const runId = randomUUID();
  const inputSnapshot = slate.players.map((player) => ({
    dkPlayerId: player.dkPlayerId, name: player.name, team: player.team, position: player.position,
    salary: player.salary, captainSalary: player.captainSalary, status: player.dkStatus,
    ourProj: player.ourProj, floor: player.floorFpts, ceiling: player.ceilingFpts,
    dkAvg: player.avgFptsDk, fantasypros: player.fantasyprosProj,
    linestar: player.linestarProj, ownership: player.linestarOwnPct, custom: player.customProj,
    availability: player.availability, isOut: player.isOut,
    injuryCoverageSnapshotId: slate.injuryCoverage?.snapshotId ?? null,
    calibrated: player.calibrated ?? null, calibrationReason: player.calibrationReason,
  }));
  const inputDigest = sha256(JSON.stringify({ settings, inputSnapshot, optimizerVersion: NFL_OPTIMIZER_VERSION }));
  const status = result.lineups.length === settings.nLineups ? "complete" : result.lineups.length ? "partial" : "failed";
  await db.insert(nflDfsOptimizerRuns).values({
    runId, uploadId, projectionRunId: slate.projectionRunId,
    optimizerVersion: NFL_OPTIMIZER_VERSION, mode: settings.mode,
    projectionSource: settings.projectionSource, settings, inputSnapshot, inputDigest,
    requestedLineups: settings.nLineups, generatedLineups: result.lineups.length,
    status, failureReason: status === "complete" ? null : result.warnings.join(" "),
  });
  if (result.lineups.length) await db.insert(nflDfsLineups).values(result.lineups.map((lineup) => ({
    runId,
    lineupNumber: lineup.lineupNumber,
    slots: lineup.slots.map((entry) => ({ slot: entry.slot, dkPlayerId: entry.player.dkPlayerId, captainDkPlayerId: entry.player.captainDkPlayerId, name: entry.player.name, team: entry.player.team, salary: entry.salary, projection: entry.projection, source: entry.projectionSource })),
    playerIds: lineup.playerIds,
    totalSalary: lineup.totalSalary,
    projectedFpts: lineup.projectedFpts,
    floorFpts: lineup.floorFpts,
    ceilingFpts: lineup.ceilingFpts,
    projectedOwnership: lineup.projectedOwnership,
    stackSummary: lineup.stackSummary,
  })));
  return { runId, slate, result };
}
