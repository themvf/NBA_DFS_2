/**
 * Results across slates.
 *
 * One slate describes one game. The questions worth answering -- does one
 * portfolio plan beat another, do chalky captains beat contrarian ones -- only
 * start to have answers when slates accumulate, so this rolls every scored
 * lineup set into running totals. Every rate is reported as a count over its
 * denominator, never as a bare percentage, and every total says how many
 * slates it rests on.
 */
import type { RankedLineup } from "./slate-results";

export interface HistoryLineup extends RankedLineup {
  /** The captain's CPT ownership in the field, in percent; null on Classic or when unknown. */
  captainFieldPct: number | null;
}

export interface HistorySet {
  uploadId: string;
  runId: string;
  createdAt: string;
  planKey: string;
  source: string;
  mode: string;
  /** Identity of the lineups themselves, to spot a set built twice. */
  fingerprint: string;
  lineups: HistoryLineup[];
}

export interface HistorySlate {
  uploadId: string;
  label: string;
  /** ISO kickoff of the slate's first game, from DraftKings' Game Info; null when unparseable. */
  startsAt: string | null;
  format: "classic" | "showdown";
  contestId: string;
  entryCount: number;
  medianScore: number | null;
  winningScore: number | null;
  sets: HistorySet[];
}

export interface Tally {
  slates: number;
  sets: number;
  lineups: number;
  aboveMedian: number;
  /** Out of `ranked`: lineups with a known rank. */
  topFifth: number;
  ranked: number;
  /** Mean of (lineup score - contest median). */
  averageMargin: number | null;
}

/** Stated priors, not fitted: roughly the top three captains on a typical Showdown. */
export const CHALK_CAPTAIN_PCT = 20;
export const CONTRARIAN_CAPTAIN_PCT = 8;

export function captainBucket(pct: number | null): string {
  if (pct == null) return "Unknown";
  if (pct >= CHALK_CAPTAIN_PCT) return `Chalk (${CHALK_CAPTAIN_PCT}%+ CPT owned)`;
  if (pct >= CONTRARIAN_CAPTAIN_PCT) return `Middle (${CONTRARIAN_CAPTAIN_PCT}–${CHALK_CAPTAIN_PCT}%)`;
  return `Contrarian (under ${CONTRARIAN_CAPTAIN_PCT}%)`;
}

/**
 * Drop a set whose lineups exactly repeat another set on the same slate, keeping
 * the newest. Building the same thing twice is one decision, not two results --
 * on 2026-09-24 two identical Balanced sets would otherwise have counted double.
 */
export function distinctSets(sets: readonly HistorySet[]): { kept: HistorySet[]; duplicates: number } {
  const seen = new Set<string>();
  const kept: HistorySet[] = [];
  for (const set of [...sets].sort((a, b) => b.createdAt.localeCompare(a.createdAt))) {
    const key = `${set.uploadId}|${set.fingerprint}`;
    if (seen.has(key)) continue;
    seen.add(key);
    kept.push(set);
  }
  return { kept, duplicates: sets.length - kept.length };
}

function tally(rows: ReadonlyArray<{ slate: HistorySlate; set: HistorySet; lineup: HistoryLineup }>): Tally {
  const scored = rows.filter((r) => r.lineup.actual != null);
  const margins = scored.filter((r) => r.slate.medianScore != null).map((r) => r.lineup.actual! - r.slate.medianScore!);
  return {
    slates: new Set(rows.map((r) => r.slate.uploadId)).size,
    sets: new Set(rows.map((r) => r.set.runId)).size,
    lineups: scored.length,
    aboveMedian: scored.filter((r) => r.slate.medianScore != null && r.lineup.actual! > r.slate.medianScore).length,
    topFifth: scored.filter((r) => r.lineup.beatShare != null && r.lineup.beatShare >= 0.8).length,
    ranked: scored.filter((r) => r.lineup.beatShare != null).length,
    averageMargin: margins.length ? Math.round((margins.reduce((a, b) => a + b, 0) / margins.length) * 10) / 10 : null,
  };
}

function rowsOf(slates: readonly HistorySlate[]) {
  return slates.flatMap((slate) => slate.sets.flatMap((set) => set.lineups.map((lineup) => ({ slate, set, lineup }))));
}

function groupTally<K extends string>(slates: readonly HistorySlate[], key: (row: ReturnType<typeof rowsOf>[number]) => K | null) {
  const groups = new Map<K, ReturnType<typeof rowsOf>>();
  for (const row of rowsOf(slates)) {
    const k = key(row);
    if (k == null) continue;
    groups.set(k, [...(groups.get(k) ?? []), row]);
  }
  return [...groups].map(([group, rows]) => ({ group, ...tally(rows) })).sort((a, b) => b.lineups - a.lineups);
}

export interface ResultsHistory {
  overall: Tally;
  byPlan: Array<{ group: string } & Tally>;
  byCaptain: Array<{ group: string } & Tally>;
  duplicatesDropped: number;
  /** Sets saved at or after the slate's first kickoff: built knowing results. */
  builtAfterStart: number;
}

/**
 * A set saved after the first game kicked off could not have been entered, and
 * may have been built knowing how the games went. It is shown, never counted.
 * An unknown start time counts the set, rather than hiding it.
 */
export function builtAfterStart(set: HistorySet, slate: HistorySlate): boolean {
  return slate.startsAt != null && Date.parse(set.createdAt) >= Date.parse(slate.startsAt);
}

/** Plans only differ on Showdown; a Classic set is recorded under its format. */
export function planGroup(format: HistorySlate["format"], planKey: string): string {
  return format === "classic" ? "Classic" : planKey || "standard";
}

export function summarizeHistory(input: readonly HistorySlate[]): ResultsHistory {
  let duplicatesDropped = 0, afterStart = 0;
  const slates = input.map((slate) => {
    const pregame = slate.sets.filter((set) => !builtAfterStart(set, slate));
    afterStart += slate.sets.length - pregame.length;
    const { kept, duplicates } = distinctSets(pregame);
    duplicatesDropped += duplicates;
    return { ...slate, sets: kept };
  });
  return {
    overall: tally(rowsOf(slates)),
    byPlan: groupTally(slates, (r) => planGroup(r.slate.format, r.set.planKey)),
    byCaptain: groupTally(slates, (r) => (r.slate.format === "showdown" && r.lineup.actual != null ? captainBucket(r.lineup.captainFieldPct) : null)),
    duplicatesDropped,
    builtAfterStart: afterStart,
  };
}
