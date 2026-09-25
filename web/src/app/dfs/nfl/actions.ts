"use server";

import { resolveSlateWeek, type SlateGame, type ScheduledGame } from "@/lib/nfl-dfs/slate-week";
import { createHash, randomUUID } from "node:crypto";
import { restoreSavedLineups, savedSlateLabel } from '@/lib/nfl-dfs/saved-workspace';
import { and, desc, eq, or, sql } from "drizzle-orm";
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
import { matchNflIdentity, resolveNflRosterIdentity, assertUniqueNflSalaryIdentities } from "@/lib/nfl-dfs/identity";
import { getNflIdentityRoster } from "@/db/nfl-identity";
import { parseNflDkSalaryCsv, type NflDkSlate } from "@/lib/nfl-dfs/dk-salary-csv";
import { getNflRosterEvidence, getNflInjuryCoverage, type InjuryCoverage } from "@/db/nfl-dfs-availability";
import { resolveGameAvailability, applyTeamQbContext, identifyTeamQb1s, ROSTER_FRESH_MS, type Availability } from "@/lib/nfl-dfs/availability";
import { previewAbsence } from "@/lib/nfl-dfs/absence-preview";
import type { PlayerContext } from "@/lib/nfl-dfs/player-context";
import { benchmarkPool, type Competitor, type ImportEvidence, type BenchmarkSnapshot, benchmarkTeam } from '@/lib/nfl-dfs/competitor-benchmark';
import { saveNflBenchmark, readNflBenchmarks } from '@/db/nfl-dfs-benchmark';
import { redistributeInjuryTargets } from '@/lib/nfl-dfs/injury-redistribution';
import { availabilityNote, storedSlateProjection, type ModelAvailabilityNote } from '@/lib/nfl-dfs/out-projection';
import { redistributeOutOpportunity, inheritanceNote, paidByDonor, type RedistributionRow, type InheritedFrom } from '@/lib/nfl-dfs/opportunity-redistribution';
import { resolveOpportunityProjection, type ProjectionScenario } from '@/lib/nfl-dfs/resolved-projection';
import { staleRunWarning } from '@/lib/nfl-dfs/stale-run';
import { buildLiveStatusOverlay, EMPTY_LIVE_OVERLAY, isLiveOutStatus, type LiveStatusOverlay } from '@/lib/nfl-dfs/live-dk-status';
import { getLiveDkPool } from '@/db/nfl-dfs-live-pool';
import { chunkRows, assertSlateFullyPersisted, incompleteSlateWarning, isSlateComplete } from '@/lib/nfl-dfs/slate-persist';
import { selectedWorkload,validateWorkloadPositions } from "@/lib/nfl-dfs/workload-selection";
import { prepareProjectionAudits, validateSituations, type SituationTeam } from '@/lib/nfl-dfs/projection-audit';
import { loadSituationContext } from '@/lib/nfl-dfs/situation-context';
import { canonicalAuditJson } from '@/lib/nfl-dfs/audit-json';
import { auditSlate, normalizeName as normalizeFieldName, type FieldAudit } from '@/lib/nfl-dfs/field-audit';
import type { HistorySlate } from '@/lib/nfl-dfs/results-history';
import { estimateRank, projectionError, scoreLineups, summarizeSet, type PositionError, type ScoreCurve, type ScoredLineup, type SetSummary } from '@/lib/nfl-dfs/slate-results';
import { parseDkGameInfoKickoff } from '@/lib/nfl-dfs/workspace-stage';
import { readWorkloadProjection, workloadPoolEligible, type WorkloadReport } from "@/lib/nfl-dfs/workload-projection";
import { getCalibratedSnapshots } from "@/db/nfl-dfs-calibrated";
import { readCalibratedProjection, readPositionWorkloadProjection, type CalibrationSnapshot } from "@/lib/nfl-dfs/calibrated-projection";
import { nflBuildInfo } from "@/lib/nfl-dfs/build-info";
import { assessOwnership, type OwnershipAssessment } from "@/lib/nfl-dfs/ownership-capability";
import {
  NFL_OPTIMIZER_VERSION,
  optimizeNflLineups,
  resolveProjectionAudit,
  type NflOptimizerResult,
  type NflOptimizerPlayer,
  type NflOptimizerSettings,
} from "./nfl-optimizer";

export type NflWorkspacePlayer = NflOptimizerPlayer & {
  ffPlayerId: number | null;
  workloadEligible?: boolean;
  identityMethod: string;
  identityEvidence?: unknown;
  modelConfidence: number | null;
  historyGames: number | null;
  dkStatus: string | null;
  availability?: Availability;
  gameInfo: string | null;
  /**
   * Opportunity this player picked up from a ruled-out teammate, one entry
   * per pool. `null` when he inherited nothing, which is the common case.
   * `ourProj`, `floorFpts` and `ceilingFpts` on this row already include it;
   * `projectionBeforeInheritance` is what they were without it, so a
   * breakdown can show the step rather than assert it.
   */
  inherited?: InheritedFrom[] | null;
  inheritedNote?: string | null;
  projectionBeforeInheritance?: number | null;
  projectionScenario?: ProjectionScenario;
  statMeans?: Record<string, number>;
  medianFpts?: number | null;
};

/**
 * What DraftKings' own live pool says about this slate right now, compared
 * against the salary file that was uploaded. Serialisable: the overlay's Map
 * does not cross the server boundary, only the summary a person can act on.
 */
export type NflLiveDkStatus = {
  applied: boolean;
  reason: string;
  draftGroupId: number | null;
  /** When DraftKings' pool last CHANGED. */
  capturedAt: string | null;
  /** When we last LOOKED, changed or not. The two are different questions. */
  lastPolledAt: string | null;
  matched: number;
  ambiguousNames: string[];
  changes: { name: string; team: string | null; from: string | null; to: string | null }[];
};

export type NflWorkspaceSlate = {
  situationTeams?: SituationTeam[];
  injuryCoverage?: InjuryCoverage | null;
  liveDkStatus?: NflLiveDkStatus;
  uploadId: string;
  projectionRunId: string | null;
  modelVersion: string | null;
  modelAsOf: string | null;
  refreshAvailable?: boolean;
  refreshMessage?: string | null;
  /** ISO time of the slate's first kickoff; drives which workspace step opens. */
  firstKickoff?: string | null;
  format: "classic" | "showdown";
  games: string[];
  teams: string[];
  /**
   * Showdown only: the Vegas favorite/underdog resolved from this game's
   * moneyline in nfl_season_games (market consensus first, quoted line as
   * fallback). Null when unknown — game-script archetypes then fold into
   * Standard ceiling rather than guessing.
   */
  favoriteTeam?: string | null;
  underdogTeam?: string | null;
  warnings: string[];
  players: NflWorkspacePlayer[];
  fileName: string;
  /**
   * What happened to the opportunity of every player ruled out on this
   * slate. Reported whether or not it could be placed -- an unplaceable
   * pool is a fact about the slate, not a failure to hide.
   */
  redistribution?: {
    version: string;
    recipients: number;
    pools?: { team: string; pool: string; offered: number; assigned: number; unassigned: number }[];
    withheld?: { team: string; pool: string; unit: string; reason: string;
      donors: { key: number; name: string; historicalUnits: number }[] }[];
    unresolved: { team: string; pool: string; pooled: number; from: string[]; reason: string }[];
    donorsWithoutOpportunity: { team: string; name: string; position: string; reason: string }[];
  };
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

async function latestProjectionRun(players: readonly SlateGame[]) {
  const dates = players.flatMap(p => [...(p.gameInfo?.matchAll(/\b\d{2}\/\d{2}\/(\d{4})\b/g) ?? [])].map(m => Number(m[1])));
  if (!dates.length) throw new Error('Salary Game Info must include a game date.');
  const schedule = await db.execute(sql`SELECT g.season,g.week,g.kickoff,
    h.abbreviation AS "homeTeam", a.abbreviation AS "awayTeam"
    FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id
    JOIN nfl_teams a ON a.team_id=g.away_team_id
    WHERE g.game_type='REG' AND g.season BETWEEN ${Math.min(...dates)-1} AND ${Math.max(...dates)}`);
  const target = resolveSlateWeek(players, schedule.rows as unknown as ScheduledGame[]);
  const rows = await db.select({
    runId: nflDfsProjectionRuns.runId,
    modelVersion: nflDfsProjectionRuns.modelVersion,
    asOfAt: nflDfsProjectionRuns.asOfAt,
    season: nflDfsProjectionRuns.season,
    week: nflDfsProjectionRuns.week,
  }).from(nflDfsProjectionRuns).where(and(eq(nflDfsProjectionRuns.season, target.season), eq(nflDfsProjectionRuns.week, target.week)))
    .orderBy(desc(nflDfsProjectionRuns.asOfAt)).limit(1);
  return rows[0] ?? null;
}

async function workspaceSlate(uploadId: string): Promise<NflWorkspaceSlate> {
  const uploads = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  const upload = uploads[0];
  if (!upload) throw new Error("NFL slate upload was not found.");
  const run = upload.projectionRunId
    ? (await db.select().from(nflDfsProjectionRuns).where(eq(nflDfsProjectionRuns.runId, upload.projectionRunId)).limit(1))[0] ?? null
    : null;
  // Season-aware observed-history input: completed games per team this season,
  // so the optimizer can cap the >=2-game requirement for early-season rookies
  // who have played every game that exists for them (observedHistoryRequirement).
  // An unknown season yields null per player, which keeps the flat requirement.
  const seasonKnown = run?.season != null;
  const completedByTeam = new Map<string, number>();
  if (seasonKnown) {
    const played = await db.execute(sql`SELECT t.abbreviation AS team, COUNT(*)::int AS played FROM (
        SELECT home_team_id AS team_id FROM nfl_season_games WHERE season=${run!.season} AND game_type='REG' AND completed
        UNION ALL
        SELECT away_team_id FROM nfl_season_games WHERE season=${run!.season} AND game_type='REG' AND completed
      ) g JOIN nfl_teams t ON t.team_id=g.team_id GROUP BY t.abbreviation`);
    for (const r of played.rows) completedByTeam.set(String(r.team), Number(r.played));
  }
  // Showdown game-script context: resolve the Vegas favorite from this game's
  // own moneyline. Source order is by FRESHNESS: nfl_matchups (refreshed
  // daily by refresh_nfl_vegas), then nfl_season_games.market_* (the twice-
  // weekly survivor refresh -- which sat frozen at 2026-09-08 for two weeks
  // when that job kept dying on the schema lock, so it cannot be first), then
  // nflverse's quoted line. Unknown stays null — the balanced plan folds
  // game-script archetypes into Standard ceiling rather than inventing a
  // favorite.
  let favoriteTeam: string | null = null;
  let underdogTeam: string | null = null;
  const slateTeams = (upload.teams as string[]) ?? [];
  if (upload.format === "showdown" && seasonKnown && slateTeams.length === 2) {
    const game = await db.execute(sql`SELECT h.abbreviation AS home, a.abbreviation AS away,
        COALESCE(m.home_ml, g.market_home_ml, g.quoted_home_ml) AS home_ml,
        COALESCE(m.away_ml, g.market_away_ml, g.quoted_away_ml) AS away_ml
      FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
      LEFT JOIN nfl_matchups m ON m.id = g.matchup_id AND m.home_ml IS NOT NULL AND m.away_ml IS NOT NULL
      WHERE g.season=${run!.season} AND g.game_type='REG' AND NOT g.completed
        AND h.abbreviation IN (${slateTeams[0]}, ${slateTeams[1]}) AND a.abbreviation IN (${slateTeams[0]}, ${slateTeams[1]})
      ORDER BY g.kickoff ASC LIMIT 1`);
    const g = game.rows[0] as { home: string; away: string; home_ml: number | null; away_ml: number | null } | undefined;
    // A more negative (or less positive) American moneyline is the favorite.
    if (g && g.home_ml != null && g.away_ml != null && Number(g.home_ml) !== Number(g.away_ml)) {
      favoriteTeam = Number(g.home_ml) < Number(g.away_ml) ? g.home : g.away;
      underdogTeam = favoriteTeam === g.home ? g.away : g.home;
    }
  }
  const storedRows = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));

  // DraftKings' own live pool, laid over the saved slate.
  //
  // A salary file records what DraftKings listed the moment it was downloaded.
  // Upload Thursday's slate on Wednesday, finalise lineups Thursday evening,
  // and that column is a day stale -- which is most of the window in which a
  // player gets ruled out. `ingest/nfl_dfs_dk_pool.py` appends observations of
  // DraftKings' own player pool; this prefers whichever is newer. `storedRows`
  // is never written back: that row is the record of what the workspace showed
  // when a lineup was built.
  let liveStatus: LiveStatusOverlay = EMPTY_LIVE_OVERLAY;
  let liveLastPolledAt: string | null = null;
  try {
    const live = await getLiveDkPool(upload.format, (upload.teams as string[]) ?? []);
    liveLastPolledAt = live.lastPolledAt ? live.lastPolledAt.toISOString() : null;
    liveStatus = buildLiveStatusOverlay(
      storedRows.map((row) => ({ normalizedName: row.normalizedName, salary: row.salary, dkStatus: row.dkStatus })),
      upload.format,
      (upload.teams as string[]) ?? [],
      live.pool,
      upload.createdAt,
    );
  } catch (error) {
    // A status feed that cannot be read must not take the slate down with it.
    liveStatus = { ...EMPTY_LIVE_OVERLAY, reason: error instanceof Error ? error.message : "Live DraftKings status is unavailable." };
  }
  const rows = liveStatus.applied
    ? storedRows.map((row) => {
        if (!liveStatus.statuses.has(row.normalizedName)) return row;
        const status = liveStatus.statuses.get(row.normalizedName) ?? null;
        // Newer evidence can rule a player OUT. It deliberately cannot clear
        // one the stored slate already ruled out: `isOut` is also set by our
        // own availability feed and by a manual ruling, and DraftKings dropping
        // a tag is not grounds to overturn either of those.
        return { ...row, dkStatus: status, isOut: row.isOut || isLiveOutStatus(status) };
      })
    : storedRows;
  // A slate written before the batched write could stop part-way and still
  // leave a header row claiming the full pool. Say so rather than serving a
  // short pool as though it were the slate.
  const incompleteWarning = incompleteSlateWarning(upload.playerCount, rows.length, upload.fileName);
  let snapshots: CalibrationSnapshot[] = [];
  // A saved slate keeps its original run on purpose, so it stays reproducible.
  // The cost is that a model fix can ship and the slate silently keeps the old
  // one -- which is exactly how the `attempts`/`carries` fix sat unused and
  // left the pass and rush redistribution pools with no input at all.
  // Both keys must be known: comparing across weeks would flag every slate.
  let newestRun: Awaited<ReturnType<typeof latestProjectionRun>> | null = null;
  let refreshMessage: string | null = null;
  try { newestRun = await latestProjectionRun(rows); }
  catch (error) { refreshMessage = error instanceof Error ? error.message : 'Could not check projection freshness.'; }
  const staleWarning = staleRunWarning(run, newestRun);
  const refreshAvailable = Boolean(newestRun && newestRun.runId !== upload.projectionRunId);
  let calibrationWarning: string | null = null;
  if (run?.week) {
    try { snapshots = await getCalibratedSnapshots(run.season, run.week); }
    catch { calibrationWarning = "Calibrated forecasts could not be loaded; historical projections remain available."; }
  }
  const byPlayer = new Map(snapshots.map(s => [s.playerId, s]));
  const roster = run ? await getNflRosterEvidence(run.season, run.week) : new Map();
  const injuryCoverage = run ? await getNflInjuryCoverage(run.season,run.week) : null;
  const {default:workloadReport}=await import('@/data/nfl-volume-share-report.json');
  const identities=run ? await db.execute(sql`SELECT id, gsis_id FROM ff_players WHERE season=${run.season} AND gsis_id IS NOT NULL`) : {rows:[]};
  const identityMap=new Map(identities.rows.map(r=>[Number(r.id),String(r.gsis_id)]));
  const now = Date.now();
  const situations=run?.week?await loadSituationContext(run.season,run.week,roster,now):null;
  const baseAvailability = (row: typeof rows[number]) => resolveGameAvailability(roster.get(row.ffPlayerId ?? -1), row.team, row.position, now, run?.week ?? null, roster.get(row.ffPlayerId ?? -1)?.kickoff ?? null);
  // A QB with no depth number is not blocked on his own evidence, but once his
  // team has an identified QB1 he is a backup by construction (see availability.ts).
  const teamQb1s = identifyTeamQb1s(rows.map((row) => ({ team: row.team, position: row.position, name: row.name, availability: baseAvailability(row) })));
  const availability = (row: typeof rows[number]) => applyTeamQbContext(baseAvailability(row), row.position, teamQb1s.get(row.team) ?? teamQb1s.get(({ LA: 'LAR', WAS: 'WSH', AZ: 'ARI', JAC: 'JAX' } as Record<string, string>)[row.team] ?? row.team));

  // The depth chart is what blocks backup quarterbacks, and its feed has now
  // twice gone silently stale for days because an unrelated step in the same
  // single-transaction refresh aborted and rolled the roster write back. Stale
  // evidence still blocks, but it can no longer clear anyone, so the pool
  // quietly narrows instead of visibly breaking. Say so on the slate.
  const rosterCapturedAt = [...roster.values()]
    .map((entry) => Date.parse(entry.fetchedAt))
    .filter((value) => Number.isFinite(value) && value <= now)
    .sort((a, b) => b - a)[0] ?? null;
  const rosterStaleWarning = rosterCapturedAt === null
    ? (roster.size ? "Roster evidence carries no usable capture time; depth-chart blocks cannot be trusted." : null)
    : now - rosterCapturedAt > ROSTER_FRESH_MS
      ? `Roster and depth-chart evidence is ${Math.floor((now - rosterCapturedAt) / 864e5)} days old (captured ${new Date(rosterCapturedAt).toISOString().slice(0, 10)}). Listed backups are still blocked, but nobody can be cleared and injury status is unverified. Re-run the Fantasy Football refresh workflow.`
      : null;

  // A ruled-out player's work does not vanish -- it goes to his teammates.
  // The OUT flag is known here and only here (DK's Status column), while the
  // stat line needed to move opportunity lives on the immutable projection
  // row, so the two are joined at read time. Neither table is rewritten.
  const outFlag = (row: typeof rows[number]) => row.isOut || row.projectionStatus === "out" || Boolean(availability(row).blockedReason);
  const projectionStats = upload.projectionRunId
    ? await db.select({
        playerId: nflDfsPlayerProjections.playerId,
        statMeans: nflDfsPlayerProjections.statMeans,
        sourceEvidence: nflDfsPlayerProjections.sourceEvidence,
      }).from(nflDfsPlayerProjections).where(eq(nflDfsPlayerProjections.runId, upload.projectionRunId))
    : [];
  const statsByPlayer = new Map(projectionStats.map(r => [Number(r.playerId), (r.statMeans ?? {}) as Record<string, number>]));
  const notesByPlayer = new Map(projectionStats.map(r => [Number(r.playerId),
    (r.sourceEvidence as { availability?: ModelAvailabilityNote & { slate_transfer_allowed?: boolean; points_before?: number } })?.availability]));
  const redistribution = redistributeOutOpportunity(
    rows.flatMap((row): RedistributionRow[] => {
      const stats = statsByPlayer.get(row.ffPlayerId ?? -1);
      if (!stats || !row.team) return [];
      return [{
        key: row.dkPlayerId,
        name: row.name,
        position: row.position,
        team: row.team,
        historyGames: row.historyGames,
        depthOrder: availability(row).fresh ? Number((roster.get(row.ffPlayerId ?? -1)?.sleeper as { depth_chart_order?: number })?.depth_chart_order) || null : null,
        canDonate: (row.isOut || row.projectionStatus === 'out' || ['OUT','IR','PUP','NFI','SUSPENDED','INACTIVE'].includes(availability(row).status))
          && notesByPlayer.get(row.ffPlayerId ?? -1)?.slate_transfer_allowed !== false,
        isOut: outFlag(row),
        projectionStatus: row.projectionStatus,
        statMeans: stats,
        ourProj: numeric(row.ourProj),
        floorFpts: numeric(row.floorFpts),
        ceilingFpts: numeric(row.ceilingFpts),
      }];
    }),
  );
  for (const row of rows) {
    const note = notesByPlayer.get(row.ffPlayerId ?? -1) as { rule?: string; from_player?: string;
      offered_opportunity?: number; assigned_opportunity?: number; unassigned_opportunity?: number } | undefined;
    if (note?.rule !== 'inherits' || note.offered_opportunity == null || note.assigned_opportunity == null || note.unassigned_opportunity == null) continue;
    redistribution.pools.push({team:row.team, pool:'pass', offered:note.offered_opportunity,
      assigned:note.assigned_opportunity, unassigned:note.unassigned_opportunity});
    if (note.unassigned_opportunity > 0) redistribution.unresolved.push({team:row.team,pool:'pass',pooled:note.unassigned_opportunity,
      from:[note.from_player ?? 'Upstream donor'],reason:'Pipeline QB transfer reached its workload cap.'});
  }
  const inheritedBy = new Map(redistribution.applied.map(r => [r.key, r]));

  return {
    redistribution: {
      version: redistribution.version,
      unresolved: redistribution.unresolved,
      donorsWithoutOpportunity: redistribution.donorsWithoutOpportunity,
      recipients: redistribution.applied.length,
      pools: redistribution.pools,
      withheld: redistribution.withheld,
    },
    situationTeams:situations?.teams??[],
    injuryCoverage,
    firstKickoff: (() => {
      const times = rows.map((row) => Date.parse(availability(row).kickoff ?? parseDkGameInfoKickoff(row.gameInfo) ?? ""))
        .filter(Number.isFinite);
      return times.length ? new Date(Math.min(...times)).toISOString() : null;
    })(),
    liveDkStatus: {
      applied: liveStatus.applied,
      reason: liveStatus.reason,
      draftGroupId: liveStatus.draftGroupId,
      capturedAt: liveStatus.capturedAt,
      lastPolledAt: liveLastPolledAt,
      matched: liveStatus.matched,
      ambiguousNames: liveStatus.ambiguousNames,
      changes: liveStatus.changes,
    },
    uploadId,
    projectionRunId: upload.projectionRunId,
    modelVersion: run?.modelVersion ?? null,
    modelAsOf: run?.asOfAt?.toISOString() ?? null,
    refreshAvailable, refreshMessage,
    favoriteTeam, underdogTeam,
    format: upload.format as "classic" | "showdown",
    games: upload.games as string[],
    teams: upload.teams as string[],
    warnings: [...upload.warnings as string[], ...(incompleteWarning ? [incompleteWarning] : []), ...(staleWarning ? [staleWarning] : []), ...(rosterStaleWarning ? [rosterStaleWarning] : []), ...(refreshMessage ? [refreshMessage] : []), ...(calibrationWarning ? [calibrationWarning] : [])],
    fileName: upload.fileName,
    players: rows.map((row) => ({
      id: row.id,
      dkPlayerId: row.dkPlayerId,
      captainDkPlayerId: row.captainDkPlayerId,
      rosterPositions: row.rosterPositions as string[],
      ffPlayerId: row.ffPlayerId,
      situationEvidence:situations?.rates.get(`${benchmarkTeam(row.team)}:${identityMap.get(row.ffPlayerId??-1)}:${row.position}`)??{team:null,rates:null,ratesDigest:null,ratesAsOf:null,reason:situations?.failure??'No model-linked situation evidence.'},
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
      // Flat copy for the optimizer, which gates the Showdown Captain slot on
      // it. A QUESTIONABLE player is NOT blocked from the pool -- he usually
      // plays -- but he may not take the 1.5x multiplier. See
      // `captainBlockedByAvailability`.
      availabilityStatus: availability(row).status,
      workloadEligible:workloadPoolEligible({...row,availability:availability(row)},now),
      identityMethod: row.identityMethod,
      identityEvidence: row.identityEvidence,
      // A player who is not playing projects zero, not his healthy number.
      // DK's own Status column is the trigger here: the model's availability
      // feed runs off separate observations that are routinely empty, which is
      // how a ruled-out Nico Collins reached the pool carrying 17.4 points.
      // Order matters: inherit first, then zero. A player cannot be both a
      // recipient and ruled out -- `redistributeOutOpportunity` only ever
      // pays available players -- but applying the ruling last means the
      // zero is final under every path.
      ...resolveOpportunityProjection({
        projectionStatus: row.projectionStatus,
        ourProj: numeric(row.ourProj),
        floorFpts: numeric(row.floorFpts),
        medianFpts: numeric(row.medianFpts),
        ceilingFpts: numeric(row.ceilingFpts),
        boomRate: numeric(row.boomRate),
        statMeans: statsByPlayer.get(row.ffPlayerId ?? -1) ?? {},
      }, inheritedBy.get(row.dkPlayerId), notesByPlayer.get(row.ffPlayerId ?? -1), outFlag(row)),
      inherited: inheritedBy.get(row.dkPlayerId)?.inherited ?? null,
      inheritedNote: inheritedBy.has(row.dkPlayerId)
        ? inheritanceNote(inheritedBy.get(row.dkPlayerId)!.inherited) : null,
      projectionBeforeInheritance: inheritedBy.get(row.dkPlayerId)?.pointsBefore ??
        (notesByPlayer.get(row.ffPlayerId ?? -1)?.rule === 'inherits' && notesByPlayer.get(row.ffPlayerId ?? -1)?.applied === true
          ? numeric(notesByPlayer.get(row.ffPlayerId ?? -1)?.points_before) : null),
      modelConfidence: numeric(row.modelConfidence),
      historyGames: row.historyGames,
      // Week-1 teams legitimately read 0 (requirement floors at 1 game); null
      // only when the season itself is unresolved.
      teamSeasonGames: seasonKnown ? completedByTeam.get(row.team) ?? 0 : null,
      fantasyprosProj: numeric(row.fantasyprosProj),
      linestarProj: numeric(row.linestarProj),
      linestarOwnPct: numeric(row.linestarOwnPct),
      customProj: numeric(row.customProj),
      ...(() => {
        const candidate = readCalibratedProjection(byPlayer.get(row.ffPlayerId ?? -1), row, run?.season ?? 0, run?.week ?? 0, now);
        const workload=readWorkloadProjection(workloadReport as WorkloadReport,{identity:identityMap.get(row.ffPlayerId??-1)??null,position:row.position,team:row.team,gameInfo:row.gameInfo,isOut:row.isOut,availability:availability(row)},run?.season??0,run?.week??0,now);
        const positionCandidate=readPositionWorkloadProjection(byPlayer.get(row.ffPlayerId??-1),row,run?.season??0,run?.week??0,now);
        const eligiblePosition=workloadPoolEligible({...row,availability:availability(row)},now)&&positionCandidate.projection&&Date.parse(positionCandidate.projection.kickoff)===Date.parse(availability(row).kickoff??'');
        return { positionWorkload:eligiblePosition?positionCandidate.projection:null,positionWorkloadReason:positionCandidate.projection&&!eligiblePosition?'Current roster, starting role or schedule evidence is unresolved.':positionCandidate.reason, calibrated: candidate.projection, calibrationReason: candidate.reason, workload:workload.projection,workloadReason:workload.reason };
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
  return persistSalarySlate(slate, digest, file.name);
}

async function persistSalarySlate(slate: NflDkSlate, digest: string, fileName: string,
  comparisonRows: (typeof nflDfsSlatePlayers.$inferSelect)[] = []): Promise<NflWorkspaceSlate> {
  const run = await latestProjectionRun(slate.players);
  if (!run) throw new Error('No projection snapshot exists for this slate game week. Refresh projections before uploading.');
  const projectionRows = run
    ? await db.select().from(nflDfsPlayerProjections).where(eq(nflDfsPlayerProjections.runId, run.runId))
    : [];
  const identityCandidates = projectionRows.map(row => ({...row, name: row.playerName, gsisId: row.playerGsisId}));
  const identityRoster = run ? await getNflIdentityRoster(run.season) : {available:false,candidates:[]};
  const identityDecisions = slate.players.map(player=>{
    const incoming={name:player.name,position:player.position,team:player.teamAbbrev};
    const permanent=identityRoster.available&&player.position!=='DST'?resolveNflRosterIdentity(incoming,identityRoster.candidates):null;
    const decision=permanent&&!permanent.gsisId?{match:null,method:permanent.method}
      :matchNflIdentity({...incoming,gsisId:permanent?.gsisId},identityCandidates);
    return {decision,permanent};
  });
  assertUniqueNflSalaryIdentities(identityDecisions.map(({decision},i)=>({name:slate.players[i].name,
    gsisId:decision.match?.gsisId,localPlayerId:decision.match?.playerId})));
  const signature = sha256(`${slate.format}|${[...new Set(slate.players.map(p => p.gameInfo))].sort().join("|")}`);
  const existing = await db.select({ uploadId: nflDfsSlateUploads.uploadId })
    .from(nflDfsSlateUploads)
    .where(run ? and(eq(nflDfsSlateUploads.fileDigest, digest), eq(nflDfsSlateUploads.projectionRunId, run.runId)) : eq(nflDfsSlateUploads.fileDigest, digest))
    .orderBy(desc(nflDfsSlateUploads.createdAt)).limit(1);
  if (existing[0]) {
    const [stored] = await db.select({n:sql<number>`count(*)::int`}).from(nflDfsSlatePlayers)
      .where(eq(nflDfsSlatePlayers.uploadId,existing[0].uploadId));
    if (isSlateComplete(slate.players.length,stored?.n ?? 0)) return workspaceSlate(existing[0].uploadId);
  }
  const uploadId = existing[0]?.uploadId ?? randomUUID();
  // Built, NOT awaited: the header row goes into the same transaction as the
  // players below. Committing it on its own is what let a failed player write
  // leave a slate that still looked whole -- see `slate-persist.ts`.
  const headerWrite = existing[0] ? null : db.insert(nflDfsSlateUploads).values({
      uploadId,
      slateSignature: signature,
      fileName,
      fileDigest: digest,
      format: slate.format,
      games: slate.games,
      teams: slate.teams,
      warnings: slate.warnings,
      playerCount: slate.players.length,
      projectionRunId: run?.runId ?? null,
    });
  const playerRows = slate.players.map((player, playerIndex) => {
    const normalized = normalizeName(player.name);
    const {decision,permanent}=identityDecisions[playerIndex];
    const projection = decision.match;
    const identityMethod = decision.method;
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
      identityEvidence: {version:'nfl-identity-registry-v1', method:identityMethod, gsisId:permanent?.gsisId ?? null,
        projectionRunId:run?.runId ?? null, projectionRowId:projection?.id ?? null,
        registryAvailable:identityRoster.available,
        roster:identityRoster.candidates.filter(p=>Boolean(permanent?.gsisId&&p.gsisId===permanent.gsisId)
          || [p.name,...p.aliases].some(name=>normalizeName(name)===normalized)),
        salaryEntry:{name:player.name,team:player.teamAbbrev,position:player.position,dkRosterEntryId:player.dkPlayerId}},
      // A DK-flagged OUT player is stored at ZERO, not at his model number.
      // Until 2026-09-22 only the web read layer zeroed him, so the stored
      // row (and every report card reading it) carried a projection for a
      // player who was never going to play. The projection run row keeps the
      // original number; identityEvidence.projectionRowId points at it.
      ...storedSlateProjection(projection, player.isOut),
      modelConfidence: projection?.confidence ?? null,
      historyGames: projection?.historyGames ?? null,
      ...(() => {
        const old = comparisonRows.find(r => r.dkPlayerId === player.dkPlayerId);
        return old ? { fantasyprosProj: old.fantasyprosProj, linestarProj: old.linestarProj,
          linestarOwnPct: old.linestarOwnPct, customProj: old.customProj, comparisonEvidence: old.comparisonEvidence } : {};
      })(),
      updatedAt: new Date(),
    };
    return values;
  });

  // ONE transaction for the whole slate. `drizzle-orm/neon-http` has no
  // interactive transactions and every `await db.insert(...)` is its own HTTPS
  // request, so the previous per-player loop was ~670 independent commits that
  // could stop anywhere. `db.batch` sends these statements together and the
  // server runs them in a single transaction: the header and every player land
  // together, or nothing does.
  const excluded = (column: { name: string }) => sql.raw(`excluded."${column.name}"`);
  const playerWrites = chunkRows(playerRows).map((rows) =>
    db.insert(nflDfsSlatePlayers).values(rows).onConflictDoUpdate({
      target: [nflDfsSlatePlayers.uploadId, nflDfsSlatePlayers.dkPlayerId],
      set: {
        captainDkPlayerId: excluded(nflDfsSlatePlayers.captainDkPlayerId),
        ffPlayerId: excluded(nflDfsSlatePlayers.ffPlayerId),
        name: excluded(nflDfsSlatePlayers.name),
        normalizedName: excluded(nflDfsSlatePlayers.normalizedName),
        position: excluded(nflDfsSlatePlayers.position),
        rosterPositions: excluded(nflDfsSlatePlayers.rosterPositions),
        team: excluded(nflDfsSlatePlayers.team),
        opponent: excluded(nflDfsSlatePlayers.opponent),
        gameKey: excluded(nflDfsSlatePlayers.gameKey),
        gameInfo: excluded(nflDfsSlatePlayers.gameInfo),
        salary: excluded(nflDfsSlatePlayers.salary),
        captainSalary: excluded(nflDfsSlatePlayers.captainSalary),
        avgFptsDk: excluded(nflDfsSlatePlayers.avgFptsDk),
        dkStatus: excluded(nflDfsSlatePlayers.dkStatus),
        isOut: excluded(nflDfsSlatePlayers.isOut),
        identityMethod: excluded(nflDfsSlatePlayers.identityMethod),
        identityEvidence: excluded(nflDfsSlatePlayers.identityEvidence),
        projectionStatus: excluded(nflDfsSlatePlayers.projectionStatus),
        ourProj: excluded(nflDfsSlatePlayers.ourProj),
        floorFpts: excluded(nflDfsSlatePlayers.floorFpts),
        medianFpts: excluded(nflDfsSlatePlayers.medianFpts),
        ceilingFpts: excluded(nflDfsSlatePlayers.ceilingFpts),
        boomRate: excluded(nflDfsSlatePlayers.boomRate),
        modelConfidence: excluded(nflDfsSlatePlayers.modelConfidence),
        historyGames: excluded(nflDfsSlatePlayers.historyGames),
        updatedAt: excluded(nflDfsSlatePlayers.updatedAt),
      },
    }),
  );
  const writes = [...(headerWrite ? [headerWrite] : []), ...playerWrites];
  if (writes.length) await db.batch(writes as [(typeof writes)[number], ...(typeof writes)[number][]]);

  // Trust, then verify. The transaction is a claim made by the driver; the row
  // count is the fact. A slate that half-saved becomes a loud error here rather
  // than a believable short player pool in the workspace.
  const [stored] = await db.select({ n: sql<number>`count(*)::int` })
    .from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  assertSlateFullyPersisted(slate.players.length, stored?.n ?? 0, fileName);

  return workspaceSlate(uploadId);
}

/** Clone salaries onto a compatible newer run; old slate and lineup snapshots stay immutable. */
export async function refreshNflSlateProjections(uploadId: string): Promise<NflWorkspaceSlate> {
  await ensureNflDfsTables();
  const [upload] = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  if (!upload) throw new Error('Saved salary slate not found.');
  const rows = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  assertSlateFullyPersisted(upload.playerCount, rows.length, upload.fileName);
  const slate: NflDkSlate = { format: upload.format as NflDkSlate['format'], games: upload.games as string[],
    teams: upload.teams as string[], warnings: upload.warnings as string[],
    players: rows.map(r => ({ dkPlayerId:r.dkPlayerId, name:r.name, position:r.position as NflDkSlate['players'][number]['position'],
      rosterPositions:r.rosterPositions as string[], teamAbbrev:r.team, opponent:r.opponent, homeAway:null,
      gameKey:r.gameKey, gameInfo:r.gameInfo, salary:r.salary, avgFptsDk:numeric(r.avgFptsDk), status:r.dkStatus, isOut:r.isOut,
      captain:r.captainDkPlayerId != null && r.captainSalary != null ? {dkPlayerId:r.captainDkPlayerId,salary:r.captainSalary}:null })) };
  return persistSalarySlate(slate, upload.fileDigest, upload.fileName, rows);
}

/** Resume an existing salary snapshot while reading the latest qualified candidates. */
export async function loadLatestNflSlate(): Promise<NflWorkspaceSlate | null> {
  const rows = await db.select({ uploadId: nflDfsSlateUploads.uploadId }).from(nflDfsSlateUploads).orderBy(desc(nflDfsSlateUploads.createdAt)).limit(1);
  return rows[0] ? workspaceSlate(rows[0].uploadId) : null;
}

export async function listSavedNflSlates() {
  const rows = await db.select({ uploadId: nflDfsSlateUploads.uploadId, signature: nflDfsSlateUploads.slateSignature,
    format: nflDfsSlateUploads.format, games: nflDfsSlateUploads.games, claimed: nflDfsSlateUploads.playerCount,
    stored: sql<number>`(select count(*)::int from nfl_dfs_slate_players p where p.upload_id = nfl_dfs_slate_uploads.upload_id)`,
    gameInfo: sql<string | null>`(select min(game_info) from nfl_dfs_slate_players p where p.upload_id = nfl_dfs_slate_uploads.upload_id)`,
  }).from(nflDfsSlateUploads).orderBy(desc(nflDfsSlateUploads.createdAt));
  const seen = new Set<string>();
  return rows
    // An incomplete slate is filtered BEFORE the newest-per-label dedupe, so a
    // half-written upload cannot shadow the complete one it sits next to. That
    // is exactly what happened on 2026-09-19: a 10-player row hid a 670-player
    // row for the same file, and the workspace served the 10.
    .filter(row => isSlateComplete(row.claimed, row.stored))
    .map(row => ({ uploadId: row.uploadId, label: savedSlateLabel(row.format, row.gameInfo, row.games as string[]) }))
    .filter(row => { if (seen.has(row.label)) return false; seen.add(row.label); return true; });
}

export async function loadSavedNflWorkspace(uploadId: string) {
  if (!/^[0-9a-f-]{36}$/.test(uploadId)) throw new Error(`Invalid saved slate: ${JSON.stringify(uploadId)}`);
  const slate = await workspaceSlate(uploadId);
  return { slate, runs: await slateRuns(uploadId) };
}

/** Saved lineup sets for a slate (including re-uploads of the same slate), newest first. */
async function slateRuns(uploadId: string) {
  const uploads = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  if (!uploads[0]) return [];
  const runs = await db.select({ runId: nflDfsOptimizerRuns.runId, createdAt: nflDfsOptimizerRuns.createdAt,
    count: nflDfsOptimizerRuns.generatedLineups, mode: nflDfsOptimizerRuns.mode, source: nflDfsOptimizerRuns.projectionSource,
  }).from(nflDfsOptimizerRuns).innerJoin(nflDfsSlateUploads, eq(nflDfsOptimizerRuns.uploadId, nflDfsSlateUploads.uploadId))
    .where(and(or(eq(nflDfsSlateUploads.slateSignature, uploads[0].slateSignature), eq(nflDfsSlateUploads.fileDigest, uploads[0].fileDigest)), sql`${nflDfsOptimizerRuns.status} in ('complete','partial')`,
      sql`${nflDfsOptimizerRuns.generatedLineups} > 0`)).orderBy(desc(nflDfsOptimizerRuns.createdAt)).limit(100);
  return runs.map(run => ({ ...run, createdAt: run.createdAt.toISOString() }));
}

export async function loadSavedNflLineups(uploadId: string, runId: string) {
  const { run, lineups } = await readNflOptimizerAudit(runId);
  const selected = (await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1))[0];
  const original = (await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, run.uploadId)).limit(1))[0];
  if (!selected || !original || (selected.slateSignature !== original.slateSignature && selected.fileDigest !== original.fileDigest)) throw new Error('This lineup set belongs to a different slate.');
  if (lineups.length !== run.generatedLineups) throw new Error('Saved lineup set is incomplete.');
  return { runId, settings: run.settings as NflOptimizerSettings,
    lineups: restoreSavedLineups(run.inputSnapshot, lineups.sort((a, b) => a.lineupNumber - b.lineupNumber)) };
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
): Promise<{ runId: string; slate: NflWorkspaceSlate; result: NflOptimizerResult; ownership?: OwnershipAssessment }> {
  await ensureNflDfsTables();
  const slate = await workspaceSlate(uploadId);
  if(settings.format!==slate.format)throw new Error("Optimizer format must match the saved salary slate.");
  return saveOptimizerResult(slate,settings);
}

export async function compareNflWorkload(uploadId:string, settings:NflOptimizerSettings) {
  await ensureNflDfsTables();
  validateWorkloadPositions(settings.workloadPositions);
  const slate=await workspaceSlate(uploadId);
  // One server-read cohort, both sources, identical controls and deterministic search.
  if(!Number.isInteger(settings.nLineups)||settings.nLineups<1||settings.nLineups>150)throw new Error('Lineup count must be 1–150.');
  const now=Date.now();
  const common=slate.players.filter(p=>(p.ourProj!==null&&p.ourProj>0||settings.allowDkFallback&&p.avgFptsDk!==null&&p.avgFptsDk>0)&&workloadPoolEligible(p,now));
  const pairedSlate={...slate,players:common};
  const paired={...settings,format:slate.format,randomness:0,nLineups:Math.min(5,settings.nLineups)};
  if(!common.some(p=>selectedWorkload(p,settings.workloadPositions)&&!p.isOut))throw new Error('No eligible pregame workload forecasts for comparison.');
  const baseline=await saveOptimizerResult(pairedSlate,{...paired,projectionSource:'our'});
  const candidate=await saveOptimizerResult(pairedSlate,{...paired,projectionSource:'workload'});
  return {baseline,candidate,settings:paired,excludedFromComparison:slate.players.length-common.length,comparedAt:new Date().toISOString(),note:'Identical frozen player inputs and settings; up to five lineups, randomness zero. Generated scores are predictions, not measured performance.'};
}

async function saveOptimizerResult(slate:NflWorkspaceSlate,settings:NflOptimizerSettings) {
  if (slate.players.some(p=>p.identityMethod==='exact_name_position')) {
    throw new Error('This salary upload contains legacy player matches without team verification. Reload its salary CSV before optimizing.');
  }
  const uploadId=slate.uploadId;
  const now=Date.now();
  validateSituations(settings.situations,slate.teams);
  const prepared= settings.projectionSource==='workload'?prepareProjectionAudits(slate.players,slate.situationTeams??[],settings.workloadPositions,settings.situations,now):slate.players;
  const eligible= settings.projectionSource==='workload' ? prepared.filter(p=>workloadPoolEligible(p,now)) : prepared;
  // Phase 2: the SERVER authoritatively resolves ownership capability from the
  // actual feed — the client can never claim "validated". Missing ownership
  // stays null. LineStar supplies a single combined percentage, not slot-level
  // ownership, so the feed is DECLARED heuristic: capability caps at
  // heuristic_uncalibrated, and leverage runs only through the user's explicit
  // opt-in (labeled "Uncalibrated estimate" everywhere). Validated-only
  // features (duplication model, contrarian-captain thresholds) stay off.
  const ownershipAssessment = assessOwnership(
    eligible.filter(p=>!p.isOut).map(p=>({playerId:p.dkPlayerId, medianProjection: p.medianFpts ?? p.ourProj ?? null})),
    eligible.filter(p=>p.linestarOwnPct!=null).map(p=>({playerId:p.dkPlayerId, flexPct:(p.linestarOwnPct as number)/100, captainPct:null, source:'linestar', asOf:slate.modelAsOf})),
    { heuristic: true, optIntoHeuristic: settings.useHeuristicOwnershipLeverage ?? true, format: slate.format },
  );
  const resolvedSettings: NflOptimizerSettings = { ...settings,
    ownershipCapability: ownershipAssessment.capability,
    ownershipLeverageEnabled: ownershipAssessment.features.leverage,
    // Game-script context: an explicit user choice wins (with ITS underdog,
    // never a mixed pairing); otherwise the SERVER supplies the Vegas favorite
    // resolved from this game's own moneyline, so balanced-mode archetypes
    // need no manual team input.
    favoriteTeam: settings.favoriteTeam ?? slate.favoriteTeam ?? null,
    underdogTeam: settings.favoriteTeam ? settings.underdogTeam ?? null : slate.underdogTeam ?? null };
  const result = optimizeNflLineups(eligible, resolvedSettings);
  if(eligible.length!==slate.players.length)result.warnings.push(`${slate.players.length-eligible.length} players excluded: workload optimization requires an unstarted, matching salary game.`);
  if (result.lineups.some(l => l.slots.some(s => s.projectionSource === "calibrated" && Date.parse(s.player.calibrated!.kickoff) <= Date.now()))) throw new Error("A calibrated player's game started during optimization. Refresh the slate before regenerating.");
  if (result.lineups.some(l => l.slots.some(s => s.projectionSource === "workload" && (Date.parse(selectedWorkload(s.player,settings.workloadPositions)!.kickoff) <= Date.now() || Date.now()-Date.parse(selectedWorkload(s.player,settings.workloadPositions)!.capturedAt)>72*3600000)))) throw new Error("A workload forecast expired during optimization. Refresh forecasts before regenerating.");
  if(settings.projectionSource==='workload'&&result.lineups.some(l=>l.slots.some(s=>!workloadPoolEligible(slate.players.find(p=>p.dkPlayerId===s.player.dkPlayerId)!,Date.now()))))throw new Error('Roster or kickoff evidence expired during optimization. Refresh the slate.');
  const runId = randomUUID();
  const inputSnapshot = slate.players.map((player) => ({
    dkPlayerId: player.dkPlayerId, ffPlayerId: player.ffPlayerId, identityMethod: player.identityMethod, identityEvidence:player.identityEvidence, gameInfo: player.gameInfo, name: player.name, team: player.team, position: player.position,
    id:player.id,captainDkPlayerId:player.captainDkPlayerId,opponent:player.opponent,gameKey:player.gameKey,boomRate:player.boomRate,projectionStatus:player.projectionStatus,
    salary: player.salary, captainSalary: player.captainSalary, rosterPositions: player.rosterPositions, status: player.dkStatus,
    ourProj: player.ourProj, floor: player.floorFpts, ceiling: player.ceilingFpts,
    projectionScenario: player.projectionScenario, redistributionVersion: slate.redistribution?.version,
    statMeans: player.statMeans,
    dkAvg: player.avgFptsDk, fantasypros: player.fantasyprosProj,
    linestar: player.linestarProj, ownership: player.linestarOwnPct, custom: player.customProj,
    availability: player.availability, isOut: player.isOut,
    injuryCoverageSnapshotId: slate.injuryCoverage?.snapshotId ?? null,
    calibrated: player.calibrated ?? null, calibrationReason: player.calibrationReason,
    workloadEligible:player.workloadEligible,
    workload: player.workload ?? null, workloadReason: player.workloadReason,
    positionWorkload:player.positionWorkload??null,positionWorkloadReason:player.positionWorkloadReason,
    situationEvidence:player.situationEvidence,
    situationTeamEvidence:slate.players.find(p=>p.team===player.team)?.dkPlayerId===player.dkPlayerId?slate.situationTeams?.find(t=>t.team===benchmarkTeam(player.team)):undefined,
    projectionAudit:resolveProjectionAudit(prepared.find(p=>p.dkPlayerId===player.dkPlayerId)!,settings),
    baselineSource:{runId:slate.projectionRunId,modelVersion:slate.modelVersion,asOf:slate.modelAsOf},
  }));
  const buildInfo = nflBuildInfo();
  // Phase 2 (P2-AC3): persist the resolved capability and an ownership
  // disclosure so a projection-only export can never later claim leverage.
  const persistedSettings = { ...resolvedSettings, ownershipDisclosure: {
    capability: ownershipAssessment.capability, source: ownershipAssessment.source, asOf: ownershipAssessment.asOf,
    coverage: ownershipAssessment.coverage, captainTotal: ownershipAssessment.captainTotal, flexTotal: ownershipAssessment.flexTotal,
    errors: ownershipAssessment.errors,
  } };
  // Build identity is part of what makes a run reproducible, so it is inside the
  // digest: two runs with different code cannot share an input digest.
  const inputDigest = sha256(canonicalAuditJson({ settings: persistedSettings, inputSnapshot, optimizerVersion: NFL_OPTIMIZER_VERSION, buildInfo }));
  const status = result.lineups.length === settings.nLineups ? "complete" : result.lineups.length ? "partial" : "failed";
  try {
  await db.insert(nflDfsOptimizerRuns).values({
    runId, uploadId, projectionRunId: slate.projectionRunId,
    optimizerVersion: NFL_OPTIMIZER_VERSION, mode: settings.mode,
    projectionSource: settings.projectionSource, settings: persistedSettings, inputSnapshot, inputDigest, buildInfo,
    requestedLineups: settings.nLineups, generatedLineups: result.lineups.length,
    status, failureReason: status === "complete" ? null : result.warnings.join(" "),
  });
  if (result.lineups.length) await db.insert(nflDfsLineups).values(result.lineups.map((lineup) => ({
    runId,
    lineupNumber: lineup.lineupNumber,
    slots: lineup.slots.map((entry) => ({ slot: entry.slot, dkPlayerId: entry.player.dkPlayerId, captainDkPlayerId: entry.player.captainDkPlayerId, name: entry.player.name, team: entry.player.team, salary: entry.salary, projection: entry.projection, source: entry.projectionSource, multiplier:entry.multiplier, projectionAudit:entry.player.projectionAudit })),
    // Phase 4: archetype label + fades + satisfied beneficiaries persist so the
    // lineup's strategy survives save, reload, export and evaluation (P4-AC4).
    stackSummary: { ...lineup.stackSummary, archetype: lineup.archetype ?? null },
    playerIds: lineup.playerIds,
    totalSalary: lineup.totalSalary,
    projectedFpts: lineup.projectedFpts,
    floorFpts: lineup.floorFpts,
    ceilingFpts: lineup.ceilingFpts,
    projectedOwnership: lineup.projectedOwnership,
  })));
  } catch { throw new Error("Unable to save optimizer results. Refresh the slate and retry; an incomplete run may remain saved."); }
  return { runId, slate, result, ownership: ownershipAssessment };
}

export async function readNflOptimizerAudit(runId:string) {
  if(!/^[0-9a-f-]{36}$/.test(runId))throw new Error('Invalid optimizer run identifier.');
  try {
    const run=(await db.select().from(nflDfsOptimizerRuns).where(eq(nflDfsOptimizerRuns.runId,runId)).limit(1))[0];
    if(!run)throw new Error('Missing run.');
    const lineups=await db.select().from(nflDfsLineups).where(eq(nflDfsLineups.runId,runId));
    return {run,lineups,digestMethod:'SHA-256 of recursively key-sorted JSON {settings,inputSnapshot,optimizerVersion}; applies to v5-audited-situations and later.'};
  }catch{throw new Error('Saved optimizer audit could not be loaded.');}
}

export type NflProjectionExplanation = {
  ok: true;
  player: { name: string; position: string; team: string; opponent: string | null; salary: number | null };
  status: string;                       // historical | position_prior | unavailable | out
  projection: number | null;            // model_proj_fpts — the headline number
  projectionScenario: ProjectionScenario;
  scenarioVersion: string;
  adjustmentUnresolved: string | null;
  /** What the model had before an availability ruling zeroed it. Null unless out. */
  projectionBeforeRuling: number | null;
  /** Opportunity picked up from a ruled-out teammate; null if none. */
  inherited: InheritedFrom[] | null;
  inheritedNote: string | null;
  /** The projection before that inheritance, so the step can be drawn. */
  projectionBeforeInheritance: number | null;
  baseline: number | null;              // recency-weighted historical mean, pre-environment
  floor: number | null;                 // P10 of the 2000 sims
  median: number | null;                // P50
  ceiling: number | null;               // P90
  boomRate: number | null;
  confidence: number | null;
  historyGames: number | null;          // his own games used
  priorGames: number | null;            // position-peer games available
  playerWeight: number | null;          // share of sims drawn from HIS history vs peers
  cutoffSeason: number | null;
  cutoffWeek: number | null;
  teamImpliedTotal: number | null;      // Vegas team total for this game
  environmentFactor: number | null;     // combined team environment multiplier
  yardageFactor: number | null;
  touchdownFactor: number | null;
  draws: number | null;                 // Monte Carlo sample count
  statMeans: Record<string, number>;    // projected mean stat line
  availabilityNote: string | null;      // why zeroed, if out
} | { ok: false; error: string };

/** Decompose WHY a slate player carries his projection: pull the full immutable
 *  projection-run row (baseline, weights, environment factors, sim distribution,
 *  stat line) that the slate table only partially carries. Read-only. */
export async function explainNflPlayerProjection(
  uploadId: string,
  slatePlayerId: number,
): Promise<NflProjectionExplanation> {
  if (!/^[0-9a-f-]{36}$/.test(uploadId)) return { ok: false, error: "Invalid slate." };

  const slateRow = (await db.select().from(nflDfsSlatePlayers)
    .where(and(eq(nflDfsSlatePlayers.uploadId, uploadId), eq(nflDfsSlatePlayers.id, slatePlayerId)))
    .limit(1))[0];
  if (!slateRow) return { ok: false, error: "Player not found in this slate." };

  const upload = (await db.select().from(nflDfsSlateUploads)
    .where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1))[0];
  if (!upload?.projectionRunId) {
    return { ok: false, error: "This slate is not linked to a model projection run, so no explanation is available." };
  }
  if (slateRow.ffPlayerId == null) {
    return { ok: false, error: "This player was never matched to a model identity, so the projection is DK-average only — no model decomposition exists." };
  }

  const proj = (await db.select().from(nflDfsPlayerProjections)
    .where(and(eq(nflDfsPlayerProjections.runId, upload.projectionRunId),
               eq(nflDfsPlayerProjections.playerId, slateRow.ffPlayerId)))
    .limit(1))[0];
  if (!proj) {
    return { ok: false, error: "No model projection row exists for this player in the linked run." };
  }

  const fs = (proj.featureSnapshot ?? {}) as Record<string, unknown>;
  const rawStats = (proj.statMeans ?? {}) as Record<string, unknown>;
  const statMeans: Record<string, number> = {};
  for (const [key, value] of Object.entries(rawStats)) {
    const n = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(n) && Math.abs(n) > 0.01) statMeans[key] = n;
  }
  const num = (v: unknown): number | null => {
    if (v === null || v === undefined) return null;
    const n = typeof v === "number" ? v : Number(v);
    return Number.isFinite(n) ? n : null;
  };
  const source = (proj.sourceEvidence ?? {}) as Record<string, unknown>;
  const availability = (source.availability ?? null) as ModelAvailabilityNote | null;

  // Read the same slate the pool table reads, so the drawer cannot quote a
  // pre-inheritance number for a player the table has already paid. This
  // costs an extra read on a drawer the user opened deliberately; two
  // surfaces disagreeing about one player costs more.
  const slate = await workspaceSlate(uploadId);
  const here = slate.players.find(p => p.dkPlayerId === slateRow.dkPlayerId);
  const outHere = here?.projectionStatus === 'out' || slateRow.isOut || availability?.rule === 'zeroed';
  const estimated = here?.projectionScenario === 'availability_estimate';
  const inherited = here?.inherited ?? null;
  const donors = paidByDonor({
    version: slate.redistribution?.version ?? "",
    applied: slate.players.flatMap(p => p.inherited && p.inherited.length > 0
      ? [{ key: p.dkPlayerId, name: p.name, ourProj: p.ourProj ?? 0, floorFpts: null,
           ceilingFpts: null, statMeans: {}, inherited: p.inherited,
           pointsBefore: p.projectionBeforeInheritance ?? 0, pointsAfter: p.ourProj ?? 0 }]
      : []),
    pools: slate.redistribution?.pools as never ?? [],
    unresolved: slate.redistribution?.unresolved as never ?? [],
    donorsWithoutOpportunity: slate.redistribution?.donorsWithoutOpportunity ?? [],
  }).get(slateRow.name) ?? null;

  return {
    ok: true,
    player: { name: slateRow.name, position: slateRow.position, team: slateRow.team,
              opponent: slateRow.opponent, salary: slateRow.salary },
    projectionScenario: here?.projectionScenario ?? 'baseline_simulation',
    scenarioVersion: slate.redistribution?.version ?? '',
    adjustmentUnresolved: slate.redistribution?.withheld?.some(p => p.team === slateRow.team) && !outHere && !estimated && ['RB','WR','TE'].includes(slateRow.position)
      ? 'Teammate absences are unresolved: a supported team/game budget and incremental role changes are unavailable. This projection retains the baseline workload.' : null,
    // Same ruling the pool table applies, so the two surfaces cannot quote
    // different numbers for one player. `baseline` is deliberately left at
    // its pre-ruling value: the drawer draws it as the step the ruling took
    // away, which is more use than a column of zeroes.
    projectionBeforeRuling: outHere ? num(proj.modelProjFpts) : null,
    // A recipient's headline already includes what he inherited, so the
    // waterfall gets its own step for it rather than burying the gain in
    // "Simulation & DK scoring" -- the same treatment the OUT ruling gets.
    inherited,
    inheritedNote: here?.inheritedNote ?? null,
    projectionBeforeInheritance: here?.projectionBeforeInheritance ?? null,
    ...(outHere
      ? { status: "out", projection: 0, floor: 0, median: 0, ceiling: 0, boomRate: 0,
          baseline: num(proj.baselineFpts) }
      : { status: proj.projectionStatus,
          projection: here?.ourProj ?? num(proj.modelProjFpts),
          baseline: num(proj.baselineFpts),
          floor: estimated ? null : here ? here.floorFpts : num(proj.floorFpts),
          median: estimated ? null : here ? here.medianFpts ?? null : num(proj.medianFpts),
          ceiling: estimated ? null : here ? here.ceilingFpts : num(proj.ceilingFpts),
          boomRate: estimated ? null : here ? here.boomRate : num(proj.boomRate) }),
    confidence: num(proj.confidence),
    historyGames: proj.historyGames,
    priorGames: proj.priorGames,
    playerWeight: num(fs.player_weight),
    cutoffSeason: num(fs.cutoff_season),
    cutoffWeek: num(fs.cutoff_week),
    teamImpliedTotal: num(fs.team_implied_total),
    environmentFactor: num(fs.team_environment_factor),
    yardageFactor: num(fs.yardage_factor),
    touchdownFactor: num(fs.touchdown_factor),
    draws: num(fs.draws),
    statMeans: here?.statMeans ?? statMeans,
    availabilityNote: availabilityNote(availability, { isOut: slateRow.isOut, dkStatus: slateRow.dkStatus },
      slateRow.isOut ? (donors ?? { paidTo: [], units: [] }) : null),
  };
}

/**
 * Import a DraftKings contest standings export and audit this slate against it.
 *
 * The 64 MB file is parsed in the BROWSER (`parseContestExport`); only the
 * ~850-row ownership summary reaches the server, the same shape as the
 * comparison-CSV import. Ownership is persisted because each contest is one
 * more labelled slate and the question it answers -- are the players the field
 * ignores our blind spots or our edges? -- needs many weeks. The audit itself
 * is recomputed on read, so a threshold change never rewrites history.
 *
 * Getting the file is the one step that cannot be automated: DraftKings gates
 * the export behind the account that entered the contest.
 */
export async function importNflContestResults(
  uploadId: string,
  contestId: string,
  parsed: { players: Array<{ name: string; normalizedName: string; draftedPct: number; draftedBySlot: Record<string, number>; fpts: number }>;
            entryCount: number; winningScore: number | null; medianScore: number | null; minScore: number | null;
            format: "classic" | "showdown"; scoreCurve?: Array<[number, number]> },
  fileName: string,
  fileDigest: string,
): Promise<{ audit: FieldAudit; contest: { contestId: string; entryCount: number; winningScore: number | null; medianScore: number | null; format: string }; overlap: number }> {
  await ensureNflDfsTables();
  const id = contestId.trim();
  if (!id) throw new Error("A contest id is required.");
  if (!parsed.players.length) throw new Error("That file carried no player ownership rows.");

  const slate = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  if (!slate.length) throw new Error("That slate has no players.");

  // Guard against attaching a contest to the wrong board: two slates on one
  // day share a date but almost no players, and mis-attributed ownership would
  // poison the audit silently rather than failing.
  const slateNames = new Set(slate.map((p) => normalizeFieldName(p.name)));
  const overlap = parsed.players.filter((p) => slateNames.has(p.normalizedName)).length / parsed.players.length;
  if (overlap < 0.8) {
    throw new Error(`Only ${Math.round(overlap * 100)}% of that contest's players are on this slate. Select the slate the contest was played on.`);
  }

  const upload = await db.select().from(nflDfsSlateUploads).where(eq(nflDfsSlateUploads.uploadId, uploadId)).limit(1);
  const run = upload[0]?.projectionRunId
    ? await db.select().from(nflDfsProjectionRuns).where(eq(nflDfsProjectionRuns.runId, upload[0].projectionRunId)).limit(1)
    : [];

  await db.execute(sql`
    INSERT INTO nfl_dfs_field_contests (contest_id, contest_name, format, season, week, slate_upload_id,
      entry_count, winning_score, median_score, min_score, file_name, file_digest, score_curve)
    VALUES (${id}, ${fileName}, ${parsed.format}, ${run[0]?.season ?? null}, ${run[0]?.week ?? null}, ${uploadId},
      ${parsed.entryCount}, ${parsed.winningScore}, ${parsed.medianScore}, ${parsed.minScore}, ${fileName}, ${fileDigest},
      ${parsed.scoreCurve?.length ? JSON.stringify(parsed.scoreCurve) : null}::jsonb)
    ON CONFLICT (contest_id) DO UPDATE SET slate_upload_id = EXCLUDED.slate_upload_id,
      season = EXCLUDED.season, week = EXCLUDED.week, entry_count = EXCLUDED.entry_count,
      winning_score = EXCLUDED.winning_score, median_score = EXCLUDED.median_score,
      min_score = EXCLUDED.min_score, file_name = EXCLUDED.file_name, file_digest = EXCLUDED.file_digest,
      score_curve = COALESCE(EXCLUDED.score_curve, nfl_dfs_field_contests.score_curve),
      imported_at = NOW()`);

  // Chunked: neon-http commits each awaited statement separately, so a single
  // oversized insert is the thing to avoid, not a lost transaction.
  for (const group of chunkRows(parsed.players, 200)) {
    await db.execute(sql`
      INSERT INTO nfl_dfs_field_ownership (contest_id, player_name, normalized_name, drafted_pct, drafted_by_slot, fpts)
      VALUES ${sql.join(group.map((p) => sql`(${id}, ${p.name}, ${p.normalizedName}, ${p.draftedPct}, ${JSON.stringify(p.draftedBySlot)}::jsonb, ${p.fpts})`), sql`, `)}
      ON CONFLICT (contest_id, normalized_name) DO UPDATE SET
        drafted_pct = EXCLUDED.drafted_pct, drafted_by_slot = EXCLUDED.drafted_by_slot, fpts = EXCLUDED.fpts`);
  }

  return {
    audit: auditForSlate(slate, parsed.players,
                         await scoredNames(run[0]?.season ?? null, run[0]?.week ?? null)),
    contest: { contestId: id, entryCount: parsed.entryCount, winningScore: parsed.winningScore,
               medianScore: parsed.medianScore, format: parsed.format },
    overlap: Number(overlap.toFixed(3)),
  };
}

export type NflSlateResults = {
  contest: { contestId: string; entryCount: number; winningScore: number | null; medianScore: number | null; importedAt: string };
  runId: string | null;
  lineups: Array<ScoredLineup & { rank: number | null; beatShare: number | null; exactRank: boolean }>;
  best: { lineupNumber: number; actual: number; rank: number | null; exactRank: boolean } | null;
  averageActual: number | null;
  averageProjected: number | null;
  positionError: PositionError[];
  rankAvailable: boolean;
  /** Every saved set for this slate, scored the same way, newest first. */
  sets: Array<{ runId: string; createdAt: string; mode: string; source: string; planMode: string | null } & SetSummary>;
  /** Captains in the loaded set. */
  captains: SetSummary["captains"];
};

/** Most sets compared at once; each one is a full audit read. */
const MAX_COMPARED_SETS = 12;

/**
 * How the saved lineup set did, from the most recent contest imported for this
 * slate. Null when no contest has been imported yet -- the Results step then
 * asks for the standings file instead of showing empty numbers.
 */
/**
 * The latest contest imported for a slate, and every saved lineup set for that
 * slate scored against it. Shared by the Results step and Results history so
 * the two can never score the same set differently.
 */
async function scoreSlateContest(uploadId: string, extraRunId: string | null) {
  const contests = await db.execute(sql`
    SELECT contest_id, entry_count, winning_score, median_score, imported_at, score_curve
    FROM nfl_dfs_field_contests WHERE slate_upload_id = ${uploadId}
    ORDER BY imported_at DESC LIMIT 1`);
  const contest = (contests.rows ?? contests)[0] as Record<string, unknown> | undefined;
  if (!contest) return null;
  const owned = await db.execute(sql`
    SELECT normalized_name, fpts, drafted_by_slot FROM nfl_dfs_field_ownership
    WHERE contest_id = ${contest.contest_id as string}`);
  const ownedRows = (owned.rows ?? owned) as Array<Record<string, unknown>>;
  const fptsByName = new Map(ownedRows.filter((r) => r.fpts != null)
    .map((r) => [String(r.normalized_name), Number(r.fpts)] as const));
  // The field's CPT ownership, for grouping captains by how chalky they were.
  const captainPctByName = new Map(ownedRows.flatMap((r) => {
    const slots = (typeof r.drafted_by_slot === "string" ? JSON.parse(r.drafted_by_slot) : r.drafted_by_slot) as Record<string, number> | null;
    return slots?.CPT == null ? [] : [[String(r.normalized_name), Number(slots.CPT)] as const];
  }));
  const entryCount = Number(contest.entry_count);
  const curve = (Array.isArray(contest.score_curve) ? contest.score_curve
    : contest.score_curve ? JSON.parse(String(contest.score_curve)) : []) as ScoreCurve;
  const medianScore = contest.median_score == null ? null : Number(contest.median_score);

  const scoreRun = async (id: string) => {
    const saved = await loadSavedNflLineups(uploadId, id);
    const ranked = scoreLineups(saved.lineups.map((l) => ({
      lineupNumber: l.lineupNumber,
      slots: l.slots.map((s) => ({ slot: s.slot, name: s.player.name, multiplier: s.multiplier, projection: s.projection })),
    })), fptsByName, normalizeFieldName).map((l) => {
      const placed = l.actual == null ? null : estimateRank(l.actual, curve, entryCount);
      return { ...l, rank: placed?.rank ?? null, beatShare: placed?.beatShare ?? null, exactRank: placed?.exact ?? false,
        captainFieldPct: l.captain ? captainPctByName.get(normalizeFieldName(l.captain)) ?? null : null };
    });
    const fingerprint = sha256(saved.lineups
      .map((l) => l.slots.map((s) => `${s.slot}:${s.player.dkPlayerId}`).sort().join(","))
      .sort().join("|"));
    return { ranked, fingerprint, planMode: (saved.settings as { archetypeMode?: string }).archetypeMode ?? null };
  };

  const runs = (await slateRuns(uploadId)).slice(0, MAX_COMPARED_SETS);
  const ids = [...new Set([...runs.map((run) => run.runId), ...(extraRunId ? [extraRunId] : [])])];
  const scoredRuns = new Map(await Promise.all(ids.map(async (id) => [id, await scoreRun(id)] as const)));
  return {
    contest: {
      contestId: String(contest.contest_id), entryCount,
      winningScore: contest.winning_score == null ? null : Number(contest.winning_score),
      medianScore,
      importedAt: new Date(String(contest.imported_at)).toISOString(),
    },
    curve, fptsByName, runs, scoredRuns,
  };
}

export async function readNflSlateResults(uploadId: string, runId: string | null): Promise<NflSlateResults | null> {
  await ensureNflDfsTables();
  const scored = await scoreSlateContest(uploadId, runId);
  if (!scored) return null;
  const { contest, curve, fptsByName, runs, scoredRuns } = scored;

  const slatePlayers = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  const positionError = projectionError(slatePlayers.map((p) => ({
    name: p.name, position: p.position, ourProj: numeric(p.ourProj), isOut: p.isOut,
  })), fptsByName, normalizeFieldName);

  const lineups = runId ? scoredRuns.get(runId)!.ranked : [];
  const current = summarizeSet(lineups, contest.medianScore);
  return {
    contest,
    runId,
    lineups,
    best: current.best,
    averageActual: current.averageActual,
    averageProjected: current.averageProjected,
    positionError,
    rankAvailable: curve.length > 0,
    sets: runs.map((run) => ({ runId: run.runId, createdAt: run.createdAt, mode: run.mode, source: run.source,
      planMode: scoredRuns.get(run.runId)!.planMode, ...summarizeSet(scoredRuns.get(run.runId)!.ranked, contest.medianScore) })),
    captains: current.captains,
  };
}

/**
 * Every slate with an imported contest, each saved lineup set scored against
 * it, newest slate first. The input to the Results history page.
 */
export async function readNflResultsHistory(): Promise<HistorySlate[]> {
  await ensureNflDfsTables();
  const contestSlates = await db.execute(sql`
    SELECT DISTINCT ON (c.slate_upload_id) c.slate_upload_id, c.format, c.imported_at, u.games,
      (SELECT min(game_info) FROM nfl_dfs_slate_players p WHERE p.upload_id = c.slate_upload_id) AS game_info,
      (SELECT array_agg(DISTINCT game_info) FROM nfl_dfs_slate_players p WHERE p.upload_id = c.slate_upload_id) AS game_infos
    FROM nfl_dfs_field_contests c
    JOIN nfl_dfs_slate_uploads u ON u.upload_id = c.slate_upload_id
    WHERE c.slate_upload_id IS NOT NULL
    ORDER BY c.slate_upload_id, c.imported_at DESC`);
  const rows = (contestSlates.rows ?? contestSlates) as Array<Record<string, unknown>>;
  const slates = await Promise.all(rows.map(async (row): Promise<HistorySlate | null> => {
    const uploadId = String(row.slate_upload_id);
    const scored = await scoreSlateContest(uploadId, null);
    if (!scored) return null;
    const format = row.format === "showdown" ? "showdown" : "classic";
    const games = (Array.isArray(row.games) ? row.games : typeof row.games === "string" ? JSON.parse(row.games) : []) as string[];
    return {
      uploadId, format,
      label: savedSlateLabel(format, row.game_info == null ? null : String(row.game_info), games),
      startsAt: (() => {
        const kicks = ((row.game_infos ?? []) as Array<string | null>).map((info) => Date.parse(info ? parseDkGameInfoKickoff(info) ?? "" : ""))
          .filter(Number.isFinite);
        return kicks.length ? new Date(Math.min(...kicks)).toISOString() : null;
      })(),
      contestId: scored.contest.contestId, entryCount: scored.contest.entryCount,
      medianScore: scored.contest.medianScore, winningScore: scored.contest.winningScore,
      sets: scored.runs.map((run) => {
        const result = scored.scoredRuns.get(run.runId)!;
        return { uploadId, runId: run.runId, createdAt: run.createdAt, planKey: result.planMode ?? "standard",
          source: run.source, mode: run.mode, fingerprint: result.fingerprint, lineups: result.ranked };
      }),
    };
  }));
  return slates.filter((slate): slate is HistorySlate => slate != null)
    .sort((a, b) => (b.startsAt ?? "").localeCompare(a.startsAt ?? ""));
}

/** The most recent contest imported for this slate, if any. */
export async function readNflFieldAudit(uploadId: string): Promise<
  { audit: FieldAudit; contest: { contestId: string; entryCount: number; winningScore: number | null; medianScore: number | null; format: string } } | null
> {
  await ensureNflDfsTables();
  const contests = await db.execute(sql`
    SELECT contest_id, entry_count, winning_score, median_score, format, season, week
    FROM nfl_dfs_field_contests WHERE slate_upload_id = ${uploadId}
    ORDER BY imported_at DESC LIMIT 1`);
  const contest = (contests.rows ?? contests)[0] as Record<string, unknown> | undefined;
  if (!contest) return null;
  const owned = await db.execute(sql`
    SELECT player_name, normalized_name, drafted_pct, fpts
    FROM nfl_dfs_field_ownership WHERE contest_id = ${contest.contest_id as string}`);
  const slate = await db.select().from(nflDfsSlatePlayers).where(eq(nflDfsSlatePlayers.uploadId, uploadId));
  const players = ((owned.rows ?? owned) as Array<Record<string, unknown>>).map((r) => ({
    name: String(r.player_name), normalizedName: String(r.normalized_name),
    draftedPct: Number(r.drafted_pct), draftedBySlot: {},
    fpts: r.fpts === null ? 0 : Number(r.fpts),
  }));
  return {
    audit: auditForSlate(slate, players, await scoredNames(
      contest.season === null || contest.season === undefined ? null : Number(contest.season),
      contest.week === null || contest.week === undefined ? null : Number(contest.week))),
    contest: {
      contestId: String(contest.contest_id), entryCount: Number(contest.entry_count),
      winningScore: contest.winning_score === null ? null : Number(contest.winning_score),
      medianScore: contest.median_score === null ? null : Number(contest.median_score),
      format: String(contest.format),
    },
  };
}

/**
 * Normalized names with a scored line that week -- who actually took the field.
 *
 * From `nfl_dfs_player_week_results`, NOT the player stat feed: a defense has
 * no row in the feed, so "no stat row" would read as "did not play" for every
 * DST. DraftKings also lists a defense by nickname ("Falcons") where
 * `nfl_teams.name` is the full name, so both spellings are emitted -- without
 * that, every defense was mis-classified.
 *
 * Returns null when the week is unknown, and the audit declines to split
 * rather than guessing.
 */
async function scoredNames(season: number | null, week: number | null): Promise<Set<string> | null> {
  if (season === null || week === null) return null;
  const result = await db.execute(sql`
    SELECT f.canonical_name AS name FROM nfl_dfs_player_week_results r
    JOIN ff_players f ON f.id = r.player_id
    WHERE r.season = ${season} AND r.week = ${week} AND r.scoring_status = 'exact'
    UNION
    SELECT t.name FROM nfl_dfs_player_week_results r
    JOIN nfl_teams t ON t.abbreviation = r.team
    WHERE r.season = ${season} AND r.week = ${week} AND r.position = 'DST'
      AND r.scoring_status = 'exact'`);
  const names = new Set<string>();
  for (const row of ((result.rows ?? result) as Array<Record<string, unknown>>)) {
    const full = String(row.name ?? "").trim();
    if (!full) continue;
    names.add(normalizeFieldName(full));
    const last = full.split(/\s+/).pop() ?? "";
    if (last) names.add(normalizeFieldName(last));
  }
  return names;
}

function auditForSlate(
  slate: Array<{ dkPlayerId: number; name: string; position: string; salary: number; ourProj: number | null; isOut: boolean }>,
  players: Array<{ normalizedName: string; draftedPct: number; fpts: number }>,
  played: Set<string> | null,
): FieldAudit {
  return auditSlate(
    slate.map((p) => ({ dkPlayerId: Number(p.dkPlayerId), name: p.name, position: p.position,
                        salary: p.salary, ourProj: p.ourProj === null ? null : Number(p.ourProj), isOut: p.isOut })),
    new Map(players.map((p) => [p.normalizedName, {
      draftedPct: p.draftedPct, fpts: p.fpts,
      played: played === null ? null : played.has(p.normalizedName),
    }])),
  );
}
