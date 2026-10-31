/**
 * CFB DFS results: DraftKings' contest standings file, read and used to grade
 * the saved lineup sets and the projections. Separate from the NFL results
 * code by design.
 *
 * The standings file has two blocks. Left: one row per entry (Rank, EntryId,
 * EntryName, TimeRemaining, Points, Lineup). Right (columns 7-10): one row per
 * player PER ROSTER SLOT he was drafted in (a QB drafted at QB and at S-FLEX
 * appears twice), with % drafted in that slot and his fantasy points. Only
 * players at least one entry drafted appear, so a player missing from the
 * block has an unknown score here, never a zero.
 *
 * Showdown: a player appears once at UTIL with his base score and once at CPT
 * with 1.5x that. Base always comes from the UTIL (or FLEX) row; the CPT row is
 * used, divided by 1.5, only when nobody drafted him at UTIL. Reading whichever
 * row comes last would inflate every captained player by 50% (the NFL parser
 * documents the same trap).
 */
import { splitCsvLine } from "./salary-csv";
import { normalizeName } from "./projection";
import { optimizeCfbLineups } from "./optimizer";
import type { CfbMode } from "./salary-csv";
import { DEFAULT_CFB_SETTINGS, type CfbLineup, type CfbPoolPlayer } from "./settings";

export const CFB_RESULTS_VERSION = "cfb-dfs-results-v2";
/** A Captain scores 1.5x his base points in DraftKings Showdown. */
export const CFB_CAPTAIN_MULTIPLIER = 1.5;
/** [rank, score], rank ascending. Exact for the top 100 ranks, then every half-percent. */
export type ScoreCurve = Array<[number, number]>;

export interface ContestPlayer { name: string; key: string; draftedPct: number; draftedBySlot: Record<string, number>; fpts: number }
export interface ParsedCfbContest {
  format: CfbMode; entryCount: number; winningScore: number | null; medianScore: number | null;
  scoreCurve: ScoreCurve; players: ContestPlayer[];
}

export function buildScoreCurve(scores: readonly number[]): ScoreCurve {
  const sorted = [...scores].filter(Number.isFinite).sort((a, b) => b - a);
  const n = sorted.length;
  if (!n) return [];
  const ranks = new Set<number>();
  for (let r = 1; r <= Math.min(100, n); r += 1) ranks.add(r);
  const step = Math.max(1, Math.round(n * 0.005));
  for (let r = 100; r <= n; r += step) ranks.add(r);
  ranks.add(n);
  return [...ranks].sort((a, b) => a - b).map((r) => [r, sorted[r - 1]]);
}

/** Where a score would have finished: 1 + entries that scored strictly more. */
export function estimateRank(score: number, curve: ScoreCurve, entryCount: number) {
  if (!curve.length || !Number.isFinite(score) || entryCount <= 0) return null;
  let above = -1;
  for (let i = 0; i < curve.length; i += 1) { if (curve[i][1] > score) above = i; else break; }
  if (above === -1) return { rank: 1, beatShare: 1, exact: true };
  const [r0, s0] = curve[above];
  let higher = r0, exact = true;
  if (above < curve.length - 1) {
    const [r1, s1] = curve[above + 1];
    exact = r1 - r0 === 1;
    const frac = s0 === s1 ? 0 : (s0 - score) / (s0 - s1);
    higher = exact ? r0 : Math.round(r0 + frac * (r1 - 1 - r0));
  }
  return { rank: higher + 1, beatShare: Math.max(0, 1 - higher / entryCount), exact };
}

export function parseCfbContestStandings(text: string): ParsedCfbContest {
  const lines = text.replace(/^﻿/, "").split(/\r?\n/);
  const header = splitCsvLine(lines[0] ?? "").map((h) => h.trim().toUpperCase());
  if (header[0] !== "RANK" || header[4] !== "POINTS" || header[7] !== "PLAYER" || header[10] !== "FPTS") {
    throw new Error("This is not a DraftKings contest standings file (expected Rank … Points … Player, Roster Position, %Drafted, FPTS).");
  }
  const scores: number[] = [];
  const byKey = new Map<string, ContestPlayer>();
  const fptsBySlot = new Map<string, Record<string, number>>();
  for (const line of lines.slice(1)) {
    if (!line.trim()) continue;
    const row = splitCsvLine(line);
    if (row[0]?.trim()) {
      const points = Number(row[4]);
      if (Number.isFinite(points)) scores.push(points);
    }
    const name = row[7]?.trim();
    if (!name) continue;
    const slot = (row[8] ?? "").trim().toUpperCase();
    const fpts = Number(row[10]);
    const drafted = Number(String(row[9] ?? "").replace("%", ""));
    if (!Number.isFinite(fpts) || !Number.isFinite(drafted)) continue;
    const key = normalizeName(name);
    const player = byKey.get(key) ?? { name, key, draftedPct: 0, draftedBySlot: {}, fpts };
    player.draftedBySlot[slot] = drafted;
    player.draftedPct = Math.round(Object.values(player.draftedBySlot).reduce((a, b) => a + b, 0) * 100) / 100;
    fptsBySlot.set(key, { ...fptsBySlot.get(key), [slot]: fpts });
    byKey.set(key, player);
  }
  if (!scores.length) throw new Error("No contest entries found in this file.");
  if (!byKey.size) throw new Error("No player rows found in this file.");
  const showdown = [...fptsBySlot.values()].some((s) => "CPT" in s);
  for (const [key, player] of byKey) {
    const s = fptsBySlot.get(key)!;
    const base = s.UTIL ?? s.FLEX ?? (s.CPT != null && Object.keys(s).length === 1 ? s.CPT / CFB_CAPTAIN_MULTIPLIER : Object.values(s)[0]);
    player.fpts = Math.round(base * 100) / 100;
  }
  scores.sort((a, b) => b - a);
  return { format: showdown ? "showdown" : "classic", entryCount: scores.length, winningScore: scores[0], medianScore: scores[Math.floor(scores.length / 2)],
    scoreCurve: buildScoreCurve(scores), players: [...byKey.values()] };
}

export interface ScoredCfbLineup {
  lineupNumber: number; projected: number; actual: number | null; missing: string[];
  rank: number | null; beatShare: number | null; exactRank: boolean; qbs: string[];
  /** Showdown only: the Captain, whose points count 1.5x. */
  captain: string | null;
}

/** Actual DK points per lineup; any player without a known score leaves the lineup unknown. */
export function scoreCfbLineups(lineups: readonly CfbLineup[], fptsByKey: ReadonlyMap<string, number>,
  curve: ScoreCurve, entryCount: number): ScoredCfbLineup[] {
  return lineups.map((l) => {
    let actual = 0;
    const missing: string[] = [];
    for (const s of l.slots) {
      const fpts = fptsByKey.get(normalizeName(s.player.name));
      if (fpts == null) missing.push(s.player.name); else actual += s.slot === "CPT" ? fpts * CFB_CAPTAIN_MULTIPLIER : fpts;
    }
    const total = missing.length ? null : Math.round(actual * 100) / 100;
    const placed = total == null ? null : estimateRank(total, curve, entryCount);
    return { lineupNumber: l.lineupNumber, projected: l.projection, actual: total, missing,
      rank: placed?.rank ?? null, beatShare: placed?.beatShare ?? null, exactRank: placed?.exact ?? false,
      qbs: l.slots.filter((s) => s.player.position === "QB").map((s) => s.player.name),
      captain: l.slots.find((s) => s.slot === "CPT")?.player.name ?? null };
  });
}

export interface CfbSetResult {
  lineups: number; scored: number; best: ScoredCfbLineup | null;
  averageActual: number | null; averageProjected: number | null; aboveMedian: number; topFifth: number; ranked: number;
}

export function summarizeCfbSet(scored: readonly ScoredCfbLineup[], medianScore: number | null): CfbSetResult {
  const known = scored.filter((l) => l.actual != null);
  const mean = (xs: number[]) => (xs.length ? Math.round((xs.reduce((a, b) => a + b, 0) / xs.length) * 100) / 100 : null);
  return {
    lineups: scored.length, scored: known.length,
    best: [...known].sort((a, b) => (b.actual ?? 0) - (a.actual ?? 0))[0] ?? null,
    averageActual: mean(known.map((l) => l.actual!)), averageProjected: mean(scored.map((l) => l.projected)),
    aboveMedian: known.filter((l) => medianScore != null && l.actual! > medianScore).length,
    topFifth: known.filter((l) => l.beatShare != null && l.beatShare >= 0.8).length,
    ranked: known.filter((l) => l.beatShare != null).length,
  };
}

export interface PositionMiss { position: string; n: number; mae: number; bias: number }

/**
 * Projection vs what DraftKings paid, by position, over players we projected
 * above zero who appear in the standings block. Bias = actual - projected.
 * Players nobody drafted are absent from the block and cannot be graded here.
 */
export function cfbProjectionError(players: ReadonlyArray<{ name: string; position: string; proj: number | null }>,
  fptsByKey: ReadonlyMap<string, number>): PositionMiss[] {
  const groups = new Map<string, Array<[number, number]>>();
  for (const p of players) {
    if (!p.proj || p.proj <= 0) continue;
    const actual = fptsByKey.get(normalizeName(p.name));
    if (actual == null) continue;
    groups.set(p.position, [...(groups.get(p.position) ?? []), [p.proj, actual]]);
  }
  const summarize = (position: string, rows: Array<[number, number]>): PositionMiss => ({
    position, n: rows.length,
    mae: Math.round((rows.reduce((a, [p, f]) => a + Math.abs(f - p), 0) / rows.length) * 100) / 100,
    bias: Math.round((rows.reduce((a, [p, f]) => a + (f - p), 0) / rows.length) * 100) / 100,
  });
  const out = [...groups].map(([pos, rows]) => summarize(pos, rows)).sort((a, b) => a.position.localeCompare(b.position));
  const all = [...groups.values()].flat();
  if (all.length) out.push(summarize("All", all));
  return out;
}

export interface CfbPerfectLineup {
  lineup: CfbLineup; actual: number; rank: number | null; exactRank: boolean;
  /** Slate players with no score in the file (nobody drafted them), left out of the search. */
  unscored: number;
}

/**
 * The best lineup that could have been entered, in hindsight: the same solver
 * and roster rules as the builder, run on what each player actually scored.
 * Players missing from the standings block have no known score and are left
 * out, so this is the best among players someone drafted.
 */
export function cfbPerfectLineup(players: readonly CfbPoolPlayer[], fptsByKey: ReadonlyMap<string, number>, mode: CfbMode,
  curve: ScoreCurve, entryCount: number): CfbPerfectLineup | null {
  const known = players.filter((p) => fptsByKey.has(normalizeName(p.name)));
  const pool = known.map((p) => ({ ...p, proj: fptsByKey.get(normalizeName(p.name))! }));
  const result = optimizeCfbLineups(pool, { ...DEFAULT_CFB_SETTINGS, mode, nLineups: 1, randomness: 0, minSalary: 0, maxExposure: 1,
    maxExposureById: {}, lockedIds: [], excludedIds: [], requireTwoQbs: false, stackQb: false, bringBack: false });
  const lineup = result.lineups[0];
  if (!lineup) return null;
  const placed = estimateRank(lineup.projection, curve, entryCount);
  return { lineup, actual: lineup.projection, rank: placed?.rank ?? null, exactRank: placed?.exact ?? false, unscored: players.length - known.length };
}

export interface CfbCaptainRow { name: string; ours: number; field: number | null; fpts: number | null; captainPoints: number | null }

/**
 * Showdown Captains: how often we captained each player in the loaded set, the
 * field's Captain %, and what a Captain of his paid. Field Captains we never
 * used are included so the misses show.
 */
export function cfbCaptainTable(lineups: readonly CfbLineup[], field: ReadonlyArray<{ key: string; name: string; cpt: number | null; fpts: number }>): CfbCaptainRow[] {
  const ours = new Map<string, { name: string; n: number }>();
  for (const l of lineups) for (const s of l.slots) if (s.slot === "CPT") {
    const k = normalizeName(s.player.name);
    ours.set(k, { name: s.player.name, n: (ours.get(k)?.n ?? 0) + 1 });
  }
  const byKey = new Map(field.map((f) => [f.key, f] as const));
  const keys = new Set([...ours.keys(), ...[...field].filter((f) => (f.cpt ?? 0) > 0).sort((a, b) => (b.cpt ?? 0) - (a.cpt ?? 0)).slice(0, 10).map((f) => f.key)]);
  const total = lineups.length;
  return [...keys].map((k) => {
    const f = byKey.get(k);
    return { name: ours.get(k)?.name ?? f?.name ?? k, ours: total ? Math.round(((ours.get(k)?.n ?? 0) / total) * 1000) / 10 : 0,
      field: f?.cpt ?? null, fpts: f?.fpts ?? null, captainPoints: f ? Math.round(f.fpts * CFB_CAPTAIN_MULTIPLIER * 100) / 100 : null };
  }).sort((a, b) => (b.captainPoints ?? -1) - (a.captainPoints ?? -1));
}
