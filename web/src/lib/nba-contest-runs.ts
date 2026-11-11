// Saved runs of "Build entries for your contest" (/nba/contest-trios). Every Optimize
// click is stored so the entries the owner actually submits exist somewhere: the
// tracked all-games entries are a registered test of rules he does not play. Pure:
// the request check the save route applies, and the box-score reading the page shows.
// The scoring rule is the contest grade's (research/nba_wta_trios.py load_actuals).

import type { OptimizerEntry, OptimizerResult } from "./nba-contest-optimizer";
import { playerKey, teamKey } from "./nba-top-scorers";

export const CONTEST_RUN_VERSION = "nba-contest-run-v1";

export const RUN_LIMITS = { maxBytes: 60_000, maxGames: 20, maxPool: 40, maxEntries: 20, minField: 20, maxField: 100_000 } as const;

export type RunPoolPlayer = { key: string; player: string; eventId: string; team: string | null; line: number; books: number };
export type RunEntry = { rank: number; players: string[]; lineSum: number; standaloneShare: number; marginalShare: number; portfolioShare: number };

/** What the browser sends: the settings and the result it showed. Games are named by event id only. */
export type RunRequest = {
  date: string;
  eventIds: string[];
  fieldSize: number;
  entriesRequested: number;
  linesCapturedAt: string | null;
  recalVersion: string;
  sims: number;
  winningScore: { p25: number; median: number; p75: number };
  pool: RunPoolPlayer[];
  entries: RunEntry[];
  baseline: RunEntry[];
};

/** A game as the server knows it (from the stored lines), never as the browser describes it. */
export type RunGame = { eventId: string; homeTeam: string; awayTeam: string; commenceTime: string };

export type RunPayload = Omit<RunRequest, "date" | "eventIds"> & { version: string; gameDate: string; games: RunGame[] };

const entryOf = (e: OptimizerEntry): RunEntry => ({
  rank: e.rank, players: e.players, lineSum: e.lineSum,
  standaloneShare: e.standaloneShare, marginalShare: e.marginalShare, portfolioShare: e.portfolioShare,
});

/** The request for a finished run. The ladder curves stay in the browser: the stored lines already hold them. */
export function runRequest(date: string, eventIds: string[], entriesRequested: number, linesCapturedAt: string | null,
  recalVersion: string, run: OptimizerResult): RunRequest {
  return {
    date, eventIds: [...eventIds].sort(), fieldSize: run.fieldSize, entriesRequested, linesCapturedAt, recalVersion, sims: run.sims,
    winningScore: run.winningScore,
    pool: run.pool.map((p) => ({ key: p.key, player: p.player, eventId: p.eventId, team: p.team, line: p.line, books: p.books })),
    entries: run.entries.map(entryOf), baseline: run.baseline.map(entryOf),
  };
}

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const isStr = (v: unknown, max = 200): v is string => typeof v === "string" && v.length > 0 && v.length <= max;
const isShare = (v: unknown) => isNum(v) && v >= 0 && v <= 1;

function checkEntries(value: unknown, keys: Set<string>, label: string): string | null {
  if (!Array.isArray(value) || value.length < 1 || value.length > RUN_LIMITS.maxEntries) return `${label}: 1 to ${RUN_LIMITS.maxEntries} entries`;
  const seen = new Set<string>();
  for (const [i, raw] of value.entries()) {
    const e = raw as Partial<RunEntry> | null;
    if (!e || typeof e !== "object") return `${label} ${i + 1}: not an entry`;
    if (e.rank !== i + 1) return `${label} ${i + 1}: rank out of order`;
    if (!Array.isArray(e.players) || e.players.length !== 3 || new Set(e.players).size !== 3) return `${label} ${i + 1}: needs three different players`;
    if (!e.players.every((k) => typeof k === "string" && keys.has(k))) return `${label} ${i + 1}: a player is not in the pool`;
    const id = [...e.players].sort().join("|");
    if (seen.has(id)) return `${label} ${i + 1}: the same trio twice`;
    seen.add(id);
    if (!isNum(e.lineSum) || !isShare(e.standaloneShare) || !isShare(e.marginalShare) || !isShare(e.portfolioShare)) return `${label} ${i + 1}: bad numbers`;
  }
  return null;
}

/** The request as a typed value, or the reason it is refused. Nothing here trusts a tip time from the browser. */
export function checkRunRequest(body: unknown): { ok: true; value: RunRequest } | { ok: false; error: string } {
  const no = (error: string) => ({ ok: false as const, error });
  const b = body as Partial<RunRequest> | null;
  if (!b || typeof b !== "object" || Array.isArray(b)) return no("not a run");
  if (typeof b.date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(b.date)) return no("bad date");
  if (!Array.isArray(b.eventIds) || b.eventIds.length < 1 || b.eventIds.length > RUN_LIMITS.maxGames
    || !b.eventIds.every((id) => isStr(id, 80)) || new Set(b.eventIds).size !== b.eventIds.length) return no("bad games");
  if (!isNum(b.fieldSize) || !Number.isInteger(b.fieldSize) || b.fieldSize < RUN_LIMITS.minField || b.fieldSize > RUN_LIMITS.maxField) return no("bad field size");
  if (!isNum(b.entriesRequested) || !Number.isInteger(b.entriesRequested) || b.entriesRequested < 1 || b.entriesRequested > RUN_LIMITS.maxEntries) return no("bad entry count");
  if (b.linesCapturedAt !== null && (!isStr(b.linesCapturedAt, 40) || Number.isNaN(Date.parse(b.linesCapturedAt)))) return no("bad capture time");
  if (!isStr(b.recalVersion, 60) || !isNum(b.sims) || b.sims < 1) return no("bad simulation settings");
  const w = b.winningScore;
  if (!w || !isNum(w.p25) || !isNum(w.median) || !isNum(w.p75)) return no("bad winning score");
  if (!Array.isArray(b.pool) || b.pool.length < 3 || b.pool.length > RUN_LIMITS.maxPool) return no("bad pool");
  const games = new Set(b.eventIds), keys = new Set<string>();
  for (const raw of b.pool) {
    const p = raw as Partial<RunPoolPlayer> | null;
    if (!p || !isStr(p.key, 80) || !isStr(p.player, 80) || !isStr(p.eventId, 80) || !isNum(p.line) || !isNum(p.books)
      || !(p.team === null || isStr(p.team, 80))) return no("bad pool player");
    if (!games.has(p.eventId)) return no(`${p.player} is not in a chosen game`);
    if (p.key !== playerKey(p.player)) return no(`${p.player}: key does not match the name`);
    if (keys.has(p.key)) return no(`${p.player} is in the pool twice`);
    keys.add(p.key);
  }
  const bad = checkEntries(b.entries, keys, "entry") ?? checkEntries(b.baseline, keys, "baseline");
  if (bad) return no(bad);
  if (b.entries!.length > b.entriesRequested) return no("more entries than requested");
  return {
    ok: true,
    value: {
      date: b.date, eventIds: [...b.eventIds].sort(), fieldSize: b.fieldSize, entriesRequested: b.entriesRequested,
      linesCapturedAt: b.linesCapturedAt ?? null, recalVersion: b.recalVersion, sims: b.sims,
      winningScore: { p25: w.p25, median: w.median, p75: w.p75 },
      pool: (b.pool as RunPoolPlayer[]).map((p) => ({ key: p.key, player: p.player, eventId: p.eventId, team: p.team, line: p.line, books: p.books })),
      entries: (b.entries as RunEntry[]).map((e) => ({ rank: e.rank, players: [...e.players], lineSum: e.lineSum,
        standaloneShare: e.standaloneShare, marginalShare: e.marginalShare, portfolioShare: e.portfolioShare })),
      baseline: (b.baseline as RunEntry[]).map((e) => ({ rank: e.rank, players: [...e.players], lineSum: e.lineSum,
        standaloneShare: e.standaloneShare, marginalShare: e.marginalShare, portfolioShare: e.portfolioShare })),
    },
  };
}

/** The stored payload: the checked request plus the games as the server's own lines describe them. */
export function runPayload(request: RunRequest, games: RunGame[]): RunPayload {
  return {
    version: CONTEST_RUN_VERSION, gameDate: request.date, games: [...games].sort((a, b) => a.eventId.localeCompare(b.eventId)),
    fieldSize: request.fieldSize, entriesRequested: request.entriesRequested, linesCapturedAt: request.linesCapturedAt,
    recalVersion: request.recalVersion, sims: request.sims, winningScore: request.winningScore,
    pool: request.pool, entries: request.entries, baseline: request.baseline,
  };
}

export type BoxGame = { homeTeam: string; awayTeam: string; players: { playerKey: string; played: boolean; points: number | null }[] };

export type PlayerResult = { key: string; points: number | null; dnp: boolean };
export type EntryResult = { players: PlayerResult[]; total: number | null };
export type RunResult = { entries: EntryResult[]; baseline: EntryResult[]; best: number | null; baselineBest: number | null; graded: boolean };

/**
 * Actual points for a saved run from the final box scores. A player with no row in his
 * finished game, or who did not play, scores 0 (the contest's rule); a game that is not
 * final, matches two box scores, or lists him twice leaves him unknown, never a guess.
 */
export function scoreRun(payload: RunPayload, finals: BoxGame[]): RunResult {
  const gameOf = new Map(payload.games.map((g) => [g.eventId, g]));
  const result = new Map<string, PlayerResult>();
  for (const p of payload.pool) {
    const g = gameOf.get(p.eventId);
    const hits = g ? finals.filter((f) => teamKey(f.homeTeam) === teamKey(g.homeTeam) && teamKey(f.awayTeam) === teamKey(g.awayTeam)) : [];
    if (hits.length !== 1) { result.set(p.key, { key: p.key, points: null, dnp: false }); continue; }
    const rows = hits[0].players.filter((x) => x.playerKey === p.key);
    if (rows.length > 1) result.set(p.key, { key: p.key, points: null, dnp: false });
    else if (rows.length === 0 || !rows[0].played) result.set(p.key, { key: p.key, points: 0, dnp: true });
    else result.set(p.key, { key: p.key, points: rows[0].points ?? 0, dnp: false });
  }
  const score = (entries: RunEntry[]): EntryResult[] => entries.map((e) => {
    const players = e.players.map((k) => result.get(k) ?? { key: k, points: null, dnp: false });
    return { players, total: players.every((x) => x.points != null) ? players.reduce((a, x) => a + (x.points ?? 0), 0) : null };
  });
  const best = (rows: EntryResult[]) => (rows.length > 0 && rows.every((r) => r.total != null) ? Math.max(...rows.map((r) => r.total as number)) : null);
  const entries = score(payload.entries), baseline = score(payload.baseline);
  return { entries, baseline, best: best(entries), baselineBest: best(baseline), graded: best(entries) != null };
}
