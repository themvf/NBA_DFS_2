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
 */
import { splitCsvLine } from "./salary-csv";
import { normalizeName } from "./projection";
import type { CfbLineup } from "./settings";

export const CFB_RESULTS_VERSION = "cfb-dfs-results-v1";
/** [rank, score], rank ascending. Exact for the top 100 ranks, then every half-percent. */
export type ScoreCurve = Array<[number, number]>;

export interface ContestPlayer { name: string; key: string; draftedPct: number; draftedBySlot: Record<string, number>; fpts: number }
export interface ParsedCfbContest {
  entryCount: number; winningScore: number | null; medianScore: number | null;
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
    if (slot === "CPT") throw new Error("This is a Showdown contest. The CFB page grades Classic contests only.");
    const fpts = Number(row[10]);
    const drafted = Number(String(row[9] ?? "").replace("%", ""));
    if (!Number.isFinite(fpts) || !Number.isFinite(drafted)) continue;
    const key = normalizeName(name);
    const player = byKey.get(key) ?? { name, key, draftedPct: 0, draftedBySlot: {}, fpts };
    player.draftedBySlot[slot] = drafted;
    player.draftedPct = Math.round(Object.values(player.draftedBySlot).reduce((a, b) => a + b, 0) * 100) / 100;
    byKey.set(key, player);
  }
  if (!scores.length) throw new Error("No contest entries found in this file.");
  if (!byKey.size) throw new Error("No player rows found in this file.");
  scores.sort((a, b) => b - a);
  return { entryCount: scores.length, winningScore: scores[0], medianScore: scores[Math.floor(scores.length / 2)],
    scoreCurve: buildScoreCurve(scores), players: [...byKey.values()] };
}

export interface ScoredCfbLineup {
  lineupNumber: number; projected: number; actual: number | null; missing: string[];
  rank: number | null; beatShare: number | null; exactRank: boolean; qbs: string[];
}

/** Actual DK points per lineup; any player without a known score leaves the lineup unknown. */
export function scoreCfbLineups(lineups: readonly CfbLineup[], fptsByKey: ReadonlyMap<string, number>,
  curve: ScoreCurve, entryCount: number): ScoredCfbLineup[] {
  return lineups.map((l) => {
    let actual = 0;
    const missing: string[] = [];
    for (const s of l.slots) {
      const fpts = fptsByKey.get(normalizeName(s.player.name));
      if (fpts == null) missing.push(s.player.name); else actual += fpts;
    }
    const total = missing.length ? null : Math.round(actual * 100) / 100;
    const placed = total == null ? null : estimateRank(total, curve, entryCount);
    return { lineupNumber: l.lineupNumber, projected: l.projection, actual: total, missing,
      rank: placed?.rank ?? null, beatShare: placed?.beatShare ?? null, exactRank: placed?.exact ?? false,
      qbs: l.slots.filter((s) => s.player.position === "QB").map((s) => s.player.name) };
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
