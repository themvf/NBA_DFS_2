"use server";

import { createHash, randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ensureCfbDfsTables } from "@/db/cfb-dfs-schema";
import { parseCfbSalaryCsv, type CfbPosition, type CfbSlatePlayer } from "@/lib/cfb-dfs/salary-csv";
import {
  CFB_PROJECTION_VERSION, normalizeName, projectCfbPlayers, resolveCfbTeams, teamSeasonKey, type HistoryRow, type TeamResolution,
} from "@/lib/cfb-dfs/projection";
import {
  cfbUploadCsv, CFB_OPTIMIZER_VERSION, DEFAULT_CFB_SETTINGS, optimizeCfbLineups,
  type CfbLineup, type CfbOptimizerSettings, type CfbPoolPlayer,
} from "@/lib/cfb-dfs/optimizer";
import {
  cfbProjectionError, parseCfbContestStandings, scoreCfbLineups, summarizeCfbSet,
  type CfbSetResult, type PositionMiss, type ScoreCurve, type ScoredCfbLineup,
} from "@/lib/cfb-dfs/results";

type Row = Record<string, unknown>;
const rowsOf = (result: unknown) => ((result as { rows?: Row[] }).rows ?? (result as Row[])) as Row[];
const num = (value: unknown) => (value == null ? null : Number(value));

export interface CfbSlateSummary {
  uploadId: string; fileName: string; label: string; firstKickoff: string | null;
  playerCount: number; createdAt: string; games: Array<{ game: string; kickoff: string | null }>;
}
export interface CfbPlayerRow extends CfbSlatePlayer {
  proj: number | null; rate: number | null; env: number | null;
  games2026: number | null; games2025: number | null; matchMethod: string | null;
}
export interface CfbRunSummary { runId: string; createdAt: string; lineupCount: number; stoppedEarly: string | null; settings: CfbOptimizerSettings }
export interface CfbWorkspace {
  slate: CfbSlateSummary & { projectionVersion: string | null; projectedAt: string | null; teamMap: Record<string, TeamResolution & { implied: number | null }>;
    /** True once the first game has kicked off: results are the next thing to do. */
    started: boolean };
  players: CfbPlayerRow[];
  runs: CfbRunSummary[];
}

/** College seasons span New Year: a January bowl belongs to the previous season. */
function seasonOf(kickoff: string | null): number {
  const d = kickoff ? new Date(kickoff) : new Date();
  return d.getUTCMonth() <= 1 ? d.getUTCFullYear() - 1 : d.getUTCFullYear();
}

function slateLabel(games: Array<{ game: string; kickoff: string | null }>, firstKickoff: string | null): string {
  const day = firstKickoff ? new Date(firstKickoff).toLocaleDateString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric" }) : "Undated";
  return `${day} · ${games.length} games · ${games.map((g) => g.game).join(", ")}`;
}

export async function listCfbSlates(): Promise<CfbSlateSummary[]> {
  await ensureCfbDfsTables();
  const result = await db.execute(sql`SELECT upload_id, file_name, games, first_kickoff, player_count, created_at
    FROM cfb_dfs_slates ORDER BY first_kickoff DESC NULLS LAST, created_at DESC LIMIT 60`);
  return rowsOf(result).map((r) => {
    const games = r.games as CfbSlateSummary["games"];
    const firstKickoff = r.first_kickoff ? new Date(String(r.first_kickoff)).toISOString() : null;
    return { uploadId: String(r.upload_id), fileName: String(r.file_name), games, firstKickoff,
      playerCount: Number(r.player_count), createdAt: new Date(String(r.created_at)).toISOString(), label: slateLabel(games, firstKickoff) };
  });
}

/** Upload a DK CFB Classic salary file. Re-uploading the same file returns the existing slate. */
export async function uploadCfbSlate(formData: FormData): Promise<{ uploadId: string; reused: boolean }> {
  await ensureCfbDfsTables();
  const file = formData.get("file");
  if (!(file instanceof File)) throw new Error("Choose a DraftKings CFB salary CSV.");
  const text = await file.text();
  const parsed = parseCfbSalaryCsv(text);
  const digest = createHash("sha256").update(text).digest("hex");
  const existing = rowsOf(await db.execute(sql`SELECT upload_id FROM cfb_dfs_slates WHERE file_digest = ${digest} LIMIT 1`))[0];
  if (existing) {
    await projectCfbSlate(String(existing.upload_id));
    return { uploadId: String(existing.upload_id), reused: true };
  }
  const uploadId = randomUUID();
  const values = parsed.players.map((p) => sql`(${uploadId}, ${p.dkId}, ${p.name}, ${p.position}, ${p.team}, ${p.opponent}, ${p.game},
    ${p.kickoff}, ${p.salary}, ${p.dkAvg}, ${p.status})`);
  await db.execute(sql`INSERT INTO cfb_dfs_slate_players (upload_id, dk_id, name, position, team, opponent, game, kickoff, salary, dk_avg, status)
    VALUES ${sql.join(values, sql`, `)} ON CONFLICT (upload_id, dk_id) DO NOTHING`);
  const teamMap = await projectCfbSlate(uploadId, parsed.players);
  // Parent row last: the slate exists only once its players and projections do.
  await db.execute(sql`INSERT INTO cfb_dfs_slates (upload_id, file_name, file_digest, games, first_kickoff, player_count,
      team_map, projection_version, projected_at)
    VALUES (${uploadId}, ${file.name}, ${digest}, ${JSON.stringify(parsed.games)}::jsonb, ${parsed.firstKickoff}, ${parsed.players.length},
      ${JSON.stringify(teamMap)}::jsonb, ${CFB_PROJECTION_VERSION}, NOW())`);
  return { uploadId, reused: false };
}

type TeamMap = Record<string, TeamResolution & { implied: number | null }>;

async function slatePlayers(uploadId: string): Promise<CfbPlayerRow[]> {
  const result = await db.execute(sql`SELECT * FROM cfb_dfs_slate_players WHERE upload_id = ${uploadId} ORDER BY salary DESC, dk_id`);
  return rowsOf(result).map((r) => ({
    dkId: Number(r.dk_id), name: String(r.name), position: String(r.position) as CfbPosition,
    rosterPositions: [], salary: Number(r.salary), team: String(r.team), opponent: String(r.opponent), game: String(r.game),
    kickoff: r.kickoff ? new Date(String(r.kickoff)).toISOString() : null, dkAvg: Number(r.dk_avg), status: String(r.status ?? ""),
    proj: num(r.proj), rate: num(r.rate), env: num(r.env), games2026: num(r.games_2026), games2025: num(r.games_2025),
    matchMethod: r.match_method == null ? null : String(r.match_method),
  }));
}

/**
 * Compute and store projections for a slate from the box-score history
 * (cfb_player_game_stats) and tonight's lines (cfb_matchups).
 */
export async function projectCfbSlate(uploadId: string, known?: CfbSlatePlayer[]): Promise<TeamMap> {
  await ensureCfbDfsTables();
  const players = known ?? await slatePlayers(uploadId);
  if (!players.length) throw new Error("That slate has no players.");
  const kicks = players.map((p) => p.kickoff).filter((k): k is string => k != null).sort();
  const season = seasonOf(kicks[0] ?? null);

  const history: HistoryRow[] = rowsOf(await db.execute(sql`SELECT cfbd_player_id, player_name, team, season, dk_points
    FROM cfb_player_game_stats WHERE season IN (${season}, ${season - 1})`)).map((r) => ({
      cfbdPlayerId: String(r.cfbd_player_id), playerName: String(r.player_name), team: String(r.team),
      season: Number(r.season), dkPoints: Number(r.dk_points) }));
  if (!history.some((r) => r.season === season)) throw new Error(`No ${season} box scores are loaded yet; run the CFB Player Game Stats workflow.`);
  const teamGames = new Map(rowsOf(await db.execute(sql`SELECT team, season, count(DISTINCT cfbd_game_id) AS games
    FROM cfb_player_game_stats WHERE season IN (${season}, ${season - 1}) GROUP BY team, season`))
    .map((r) => [teamSeasonKey(String(r.team), Number(r.season)), Number(r.games)] as const));
  const teamPpg = new Map(rowsOf(await db.execute(sql`SELECT t.name AS team,
      avg(CASE WHEN m.home_team_id = t.team_id THEN m.home_score ELSE m.away_score END) AS ppg
    FROM cfb_matchups m JOIN cfb_teams t ON t.team_id IN (m.home_team_id, m.away_team_id)
    WHERE m.season = ${season} AND m.completed GROUP BY t.name`))
    .filter((r) => r.ppg != null).map((r) => [String(r.team), Number(r.ppg)] as const));

  const resolved = resolveCfbTeams(players, history, season);
  const nameOf = (code: string) => resolved.get(code)?.team ?? code;

  // Implied totals: the matchup between the two resolved teams nearest the DK kickoff.
  const lines = rowsOf(await db.execute(sql`SELECT a.name AS away, h.name AS home, m.away_implied, m.home_implied, m.commence_time
    FROM cfb_matchups m JOIN cfb_teams h ON h.team_id = m.home_team_id JOIN cfb_teams a ON a.team_id = m.away_team_id
    WHERE m.season = ${season} AND m.commence_time BETWEEN ${kicks[0] ?? new Date().toISOString()}::timestamptz - INTERVAL '1 day'
      AND ${kicks.at(-1) ?? new Date().toISOString()}::timestamptz + INTERVAL '1 day'`));
  const implied = new Map<string, number>();
  for (const game of new Set(players.map((p) => p.game))) {
    const [away, home] = game.split("@");
    const kickoff = players.find((p) => p.game === game)?.kickoff;
    const candidates = lines.filter((l) => new Set([String(l.away), String(l.home)]).has(nameOf(away)) && new Set([String(l.away), String(l.home)]).has(nameOf(home)))
      .sort((a, b) => Math.abs(Date.parse(String(a.commence_time)) - Date.parse(kickoff ?? "")) - Math.abs(Date.parse(String(b.commence_time)) - Date.parse(kickoff ?? "")));
    const line = candidates[0];
    if (!line) continue;
    for (const code of [away, home]) {
      const value = String(line.home) === nameOf(code) ? line.home_implied : line.away_implied;
      if (value != null) implied.set(code, Number(value));
    }
  }

  const projections = projectCfbPlayers(players, { season, history, teamGames, teamPpg, implied, teamName: nameOf });
  const values = projections.map((p) => sql`(${p.dkId}::bigint, ${p.proj}::float8, ${p.rate}::float8, ${p.env}::float8, ${p.games2026}::int,
    ${p.games2025}::int, ${p.cfbdPlayerId}::text, ${p.match}::text)`);
  await db.execute(sql`UPDATE cfb_dfs_slate_players s SET proj = v.proj, rate = v.rate, env = v.env, games_2026 = v.g26,
      games_2025 = v.g25, cfbd_player_id = v.cid, match_method = v.method
    FROM (VALUES ${sql.join(values, sql`, `)}) AS v (dk_id, proj, rate, env, g26, g25, cid, method)
    WHERE s.upload_id = ${uploadId} AND s.dk_id = v.dk_id`);
  const teamMap: TeamMap = Object.fromEntries([...resolved].map(([code, r]) => [code, { ...r, implied: implied.get(code) ?? null }]));
  await db.execute(sql`UPDATE cfb_dfs_slates SET team_map = ${JSON.stringify(teamMap)}::jsonb,
    projection_version = ${CFB_PROJECTION_VERSION}, projected_at = NOW() WHERE upload_id = ${uploadId}`);
  return teamMap;
}

export async function loadCfbWorkspace(uploadId: string): Promise<CfbWorkspace> {
  await ensureCfbDfsTables();
  if (!/^[0-9a-f-]{36}$/.test(uploadId)) throw new Error("Invalid slate.");
  const slate = rowsOf(await db.execute(sql`SELECT * FROM cfb_dfs_slates WHERE upload_id = ${uploadId}`))[0];
  if (!slate) throw new Error("That slate does not exist.");
  const games = slate.games as CfbSlateSummary["games"];
  const firstKickoff = slate.first_kickoff ? new Date(String(slate.first_kickoff)).toISOString() : null;
  const runs = rowsOf(await db.execute(sql`SELECT run_id, created_at, lineup_count, stopped_early, settings
    FROM cfb_dfs_lineup_runs WHERE upload_id = ${uploadId} ORDER BY created_at DESC LIMIT 30`)).map((r) => ({
      runId: String(r.run_id), createdAt: new Date(String(r.created_at)).toISOString(), lineupCount: Number(r.lineup_count),
      stoppedEarly: r.stopped_early == null ? null : String(r.stopped_early), settings: r.settings as CfbOptimizerSettings }));
  return {
    slate: { uploadId, fileName: String(slate.file_name), games, firstKickoff, playerCount: Number(slate.player_count),
      createdAt: new Date(String(slate.created_at)).toISOString(), label: slateLabel(games, firstKickoff),
      projectionVersion: slate.projection_version == null ? null : String(slate.projection_version),
      projectedAt: slate.projected_at ? new Date(String(slate.projected_at)).toISOString() : null,
      teamMap: (slate.team_map ?? {}) as TeamMap, started: firstKickoff != null && Date.parse(firstKickoff) <= Date.now() },
    players: await slatePlayers(uploadId),
    runs,
  };
}

export async function refreshCfbProjections(uploadId: string): Promise<CfbWorkspace> {
  await projectCfbSlate(uploadId);
  return loadCfbWorkspace(uploadId);
}

/** Build and save a lineup set. Lineups are written first, the run row last. */
export async function generateCfbLineups(uploadId: string, input: Partial<CfbOptimizerSettings>): Promise<{ runId: string; lineups: CfbLineup[]; stoppedEarly: string | null }> {
  await ensureCfbDfsTables();
  const settings: CfbOptimizerSettings = { ...DEFAULT_CFB_SETTINGS, ...input };
  if (!Number.isInteger(settings.nLineups) || settings.nLineups < 1 || settings.nLineups > 150) throw new Error("Lineups must be 1-150.");
  if (settings.minUnique < 1 || settings.minUnique > 7) throw new Error("Minimum unique must be 1-7.");
  const players = await slatePlayers(uploadId);
  const pool: CfbPoolPlayer[] = players.map((p) => ({ dkId: p.dkId, name: p.name, position: p.position, team: p.team,
    game: p.game, salary: p.salary, proj: p.proj ?? 0 }));
  const result = optimizeCfbLineups(pool, settings);
  if (!result.lineups.length) throw new Error(result.stoppedEarly ?? "No legal lineup could be built.");
  const runId = randomUUID();
  const values = result.lineups.map((l) => sql`(${runId}, ${l.lineupNumber},
    ${JSON.stringify(l.slots.map((s) => ({ slot: s.slot, dkId: s.player.dkId })))}::jsonb, ${l.salary}, ${l.projection})`);
  await db.execute(sql`INSERT INTO cfb_dfs_lineups (run_id, lineup_number, slots, salary, projection) VALUES ${sql.join(values, sql`, `)}`);
  await db.execute(sql`INSERT INTO cfb_dfs_lineup_runs (run_id, upload_id, settings, projection_version, optimizer_version, lineup_count, stopped_early)
    VALUES (${runId}, ${uploadId}, ${JSON.stringify(settings)}::jsonb, ${CFB_PROJECTION_VERSION}, ${CFB_OPTIMIZER_VERSION},
      ${result.lineups.length}, ${result.stoppedEarly})`);
  return { runId, lineups: result.lineups, stoppedEarly: result.stoppedEarly };
}

/** A saved run's lineups, rebuilt from the slate's players. */
export async function loadCfbRun(uploadId: string, runId: string): Promise<CfbLineup[]> {
  await ensureCfbDfsTables();
  const run = rowsOf(await db.execute(sql`SELECT run_id FROM cfb_dfs_lineup_runs WHERE run_id = ${runId} AND upload_id = ${uploadId}`))[0];
  if (!run) throw new Error("That lineup set does not belong to this slate.");
  const byId = new Map((await slatePlayers(uploadId)).map((p) => [p.dkId, p]));
  const rows = rowsOf(await db.execute(sql`SELECT lineup_number, slots, salary, projection FROM cfb_dfs_lineups
    WHERE run_id = ${runId} ORDER BY lineup_number`));
  return rows.map((r) => ({
    lineupNumber: Number(r.lineup_number), salary: Number(r.salary), projection: Number(r.projection),
    slots: (r.slots as Array<{ slot: CfbLineup["slots"][number]["slot"]; dkId: number }>).map((s) => {
      const p = byId.get(Number(s.dkId));
      if (!p) throw new Error(`Player ${s.dkId} is missing from the slate.`);
      return { slot: s.slot, player: { dkId: p.dkId, name: p.name, position: p.position, team: p.team, game: p.game, salary: p.salary, proj: p.proj ?? 0 } };
    }),
  }));
}

export async function exportCfbRun(uploadId: string, runId: string): Promise<string> {
  return cfbUploadCsv(await loadCfbRun(uploadId, runId));
}

export interface CfbResults {
  contest: { contestId: string; entryCount: number; winningScore: number | null; medianScore: number | null; importedAt: string; fileName: string };
  sets: Array<{ runId: string; createdAt: string; rules: string } & CfbSetResult>;
  runId: string | null;
  lineups: ScoredCfbLineup[];
  positionError: PositionMiss[];
  /** For the selected set: our exposure against the field's, with what each player scored. */
  exposureVsField: Array<{ name: string; position: string; ours: number; field: number | null; fpts: number | null }>;
  /** Most-drafted players in the field, and whether we had them. */
  fieldChalk: Array<{ name: string; field: number; fpts: number; ours: number }>;
}

/** Import a DraftKings contest standings CSV for this slate. Re-importing the same contest replaces it. */
export async function importCfbContest(uploadId: string, formData: FormData): Promise<{ contestId: string; entries: number }> {
  await ensureCfbDfsTables();
  const file = formData.get("file");
  if (!(file instanceof File)) throw new Error("Choose the contest-standings CSV (unzip DraftKings' download first).");
  if (/\.zip$/i.test(file.name)) throw new Error("That is the .zip DraftKings downloads. Unzip it and upload the .csv inside.");
  const text = await file.text();
  const parsed = parseCfbContestStandings(text);
  const digest = createHash("sha256").update(text).digest("hex");
  const contestId = file.name.match(/contest-standings-(\d+)/i)?.[1] ?? `file-${digest.slice(0, 12)}`;
  const values = parsed.players.map((p) => sql`(${contestId}, ${p.key}, ${p.name}, ${p.draftedPct}, ${JSON.stringify(p.draftedBySlot)}::jsonb, ${p.fpts})`);
  await db.execute(sql`INSERT INTO cfb_dfs_contest_players (contest_id, player_key, name, drafted_pct, drafted_by_slot, fpts)
    VALUES ${sql.join(values, sql`, `)} ON CONFLICT (contest_id, player_key) DO UPDATE SET
      name = EXCLUDED.name, drafted_pct = EXCLUDED.drafted_pct, drafted_by_slot = EXCLUDED.drafted_by_slot, fpts = EXCLUDED.fpts`);
  // Contest row last: results exist only once every player row does.
  await db.execute(sql`INSERT INTO cfb_dfs_contests (contest_id, upload_id, file_name, file_digest, entry_count, winning_score, median_score, score_curve)
    VALUES (${contestId}, ${uploadId}, ${file.name}, ${digest}, ${parsed.entryCount}, ${parsed.winningScore}, ${parsed.medianScore},
      ${JSON.stringify(parsed.scoreCurve)}::jsonb)
    ON CONFLICT (contest_id) DO UPDATE SET upload_id = EXCLUDED.upload_id, file_name = EXCLUDED.file_name, file_digest = EXCLUDED.file_digest,
      entry_count = EXCLUDED.entry_count, winning_score = EXCLUDED.winning_score, median_score = EXCLUDED.median_score,
      score_curve = EXCLUDED.score_curve, imported_at = NOW()`);
  return { contestId, entries: parsed.entryCount };
}

/** Grade every saved lineup set and the projections against the latest contest imported for this slate. */
export async function readCfbResults(uploadId: string, runId: string | null): Promise<CfbResults | null> {
  await ensureCfbDfsTables();
  const contest = rowsOf(await db.execute(sql`SELECT * FROM cfb_dfs_contests WHERE upload_id = ${uploadId}
    ORDER BY imported_at DESC LIMIT 1`))[0];
  if (!contest) return null;
  const contestId = String(contest.contest_id);
  const field = rowsOf(await db.execute(sql`SELECT player_key, name, drafted_pct, fpts FROM cfb_dfs_contest_players WHERE contest_id = ${contestId}`));
  const fptsByKey = new Map(field.map((r) => [String(r.player_key), Number(r.fpts)] as const));
  const draftedByKey = new Map(field.map((r) => [String(r.player_key), Number(r.drafted_pct)] as const));
  const curve = (contest.score_curve ?? []) as ScoreCurve;
  const entryCount = Number(contest.entry_count);
  const medianScore = contest.median_score == null ? null : Number(contest.median_score);

  const players = await slatePlayers(uploadId);
  const runs = rowsOf(await db.execute(sql`SELECT run_id, created_at, settings FROM cfb_dfs_lineup_runs
    WHERE upload_id = ${uploadId} ORDER BY created_at DESC LIMIT 12`));
  const scoredByRun = new Map(await Promise.all(runs.map(async (r) => {
    const lineups = await loadCfbRun(uploadId, String(r.run_id));
    return [String(r.run_id), { lineups, scored: scoreCfbLineups(lineups, fptsByKey, curve, entryCount) }] as const;
  })));
  const selected = runId && scoredByRun.has(runId) ? runId : runs[0] ? String(runs[0].run_id) : null;
  const chosen = selected ? scoredByRun.get(selected)! : null;


  const exposure = new Map<string, { name: string; position: string; n: number }>();
  for (const l of chosen?.lineups ?? []) for (const s of l.slots) {
    const k = normalizeName(s.player.name);
    exposure.set(k, { name: s.player.name, position: s.player.position, n: (exposure.get(k)?.n ?? 0) + 1 });
  }
  const total = chosen?.lineups.length ?? 0;
  const oursPct = (k: string) => (total ? Math.round(((exposure.get(k)?.n ?? 0) / total) * 1000) / 10 : 0);

  return {
    contest: { contestId, entryCount, winningScore: contest.winning_score == null ? null : Number(contest.winning_score), medianScore,
      importedAt: new Date(String(contest.imported_at)).toISOString(), fileName: String(contest.file_name) },
    sets: runs.map((r) => {
      const st = r.settings as { requireTwoQbs?: boolean; stackQb?: boolean; bringBack?: boolean; maxExposure?: number };
      const rules = [st.requireTwoQbs ? "2 QBs" : null, st.stackQb ? "stacks" : null, st.bringBack ? "bring-backs" : null].filter(Boolean).join(", ") || "no rules";
      return { runId: String(r.run_id), createdAt: new Date(String(r.created_at)).toISOString(),
        rules: `${rules} · max ${Math.round((st.maxExposure ?? 0.7) * 100)}%`, ...summarizeCfbSet(scoredByRun.get(String(r.run_id))!.scored, medianScore) };
    }),
    runId: selected,
    lineups: chosen?.scored ?? [],
    positionError: cfbProjectionError(players.map((p) => ({ name: p.name, position: p.position, proj: p.proj })), fptsByKey),
    exposureVsField: [...exposure].map(([k, e]) => ({ name: e.name, position: e.position, ours: oursPct(k),
      field: draftedByKey.get(k) ?? null, fpts: fptsByKey.get(k) ?? null })).sort((a, b) => b.ours - a.ours),
    fieldChalk: field.map((r) => ({ name: String(r.name), field: Number(r.drafted_pct), fpts: Number(r.fpts), ours: oursPct(String(r.player_key)) }))
      .sort((a, b) => b.field - a.field).slice(0, 15),
  };
}
