/**
 * CFB DFS lineup types and defaults. No solver here: this file is safe for the
 * browser bundle, the solver (optimizer.ts) runs on the server only.
 */
import type { CfbMode, CfbPosition } from "./salary-csv";

export const CFB_SALARY_CAP = 50_000;
export const CFB_ROSTER_SIZE = 8;
export const CFB_SLOTS = ["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"] as const;
export const CFB_SHOWDOWN_SLOTS = ["CPT", "FLEX", "FLEX", "FLEX", "FLEX", "FLEX"] as const;
export const CFB_OPTIMIZER_VERSION = "cfb-dk-modes-ilp-v2";

export interface CfbPoolPlayer {
  dkId: number;
  captainId?: number;
  captainSalary?: number;
  name: string;
  position: CfbPosition;
  team: string;
  game: string;
  salary: number;
  proj: number;
}

/** A frozen ownership forecast: [total %, Captain % or null, UTIL % or null] per DraftKings id. */
export interface CfbOwnershipSnapshot { version: string; inputsDigest: string; byId: Record<string, [number, number | null, number | null]> }

export interface CfbOptimizerSettings {
  mode?: CfbMode;
  /** User-entered base point estimates, preserved with the run. */
  projectionById?: Record<string, number>;
  /** The field-ownership estimate the run was built beside, frozen for grading. Written by the server. */
  ownership?: CfbOwnershipSnapshot;
  nLineups: number;
  /** 0-1: the most lineups any one player may appear in, as a share. */
  maxExposure: number;
  minUnique: number;
  /** Per-lineup multiplicative noise, standard deviation on the log scale. */
  randomness: number;
  minSalary: number;
  lockedIds: number[];
  excludedIds: number[];
  /** Per-player maximum exposure in percent, overriding the global cap. */
  maxExposureById: Record<string, number>;
  seed: number;
  /** Exactly two QBs in every lineup (the SUPER FLEX goes to a QB). */
  requireTwoQbs: boolean;
  /** Most lineups (percent) a Questionable player may appear in unless given his own cap. */
  questionableCapPct: number;
  /** Every QB comes with at least one teammate WR or RB. */
  stackQb: boolean;
  /** Every QB also comes with at least one WR or RB from the team he faces. */
  bringBack: boolean;
}

export const DEFAULT_CFB_SETTINGS: CfbOptimizerSettings = {
  nLineups: 20, maxExposure: 0.7, minUnique: 2, randomness: 0.18, minSalary: 45_000,
  lockedIds: [], excludedIds: [], maxExposureById: {}, seed: 20260925,
  requireTwoQbs: false, stackQb: false, bringBack: false, questionableCapPct: 25,
};

export interface CfbLineupSlot { slot: (typeof CFB_SLOTS)[number] | "CPT"; player: CfbPoolPlayer }
export interface CfbLineup { lineupNumber: number; slots: CfbLineupSlot[]; salary: number; projection: number }
export interface CfbOptimizerResult { lineups: CfbLineup[]; stoppedEarly: string | null; version: string }

/** Every rule a DK CFB Classic lineup must satisfy; empty when legal. */
export function cfbLineupProblems(players: CfbPoolPlayer[]): string[] {
  const problems: string[] = [];
  if (players.length !== CFB_ROSTER_SIZE) problems.push(`${players.length} players, not 8`);
  if (new Set(players.map((p) => p.dkId)).size !== players.length) problems.push("a player appears twice");
  const salary = players.reduce((a, p) => a + p.salary, 0);
  if (salary > CFB_SALARY_CAP) problems.push(`salary ${salary} over the cap`);
  const count = (pos: CfbPosition) => players.filter((p) => p.position === pos).length;
  if (count("QB") < 1 || count("QB") > 2) problems.push(`${count("QB")} QBs`);
  if (count("RB") < 2) problems.push(`${count("RB")} RBs`);
  if (count("WR") < 3) problems.push(`${count("WR")} WRs`);
  if (new Set(players.map((p) => p.game)).size < 2) problems.push("only one game");
  return problems;
}

/** Validate stored slot assignments before exporting either contest format. */
export function cfbAssignedLineupProblems(lineup: CfbLineup): string[] {
  const showdown = lineup.slots.some((s) => s.slot === "CPT");
  const expected = showdown ? CFB_SHOWDOWN_SLOTS : CFB_SLOTS;
  const players = lineup.slots.map((s) => s.player);
  const problems = showdown ? [] : cfbLineupProblems(players);
  if (lineup.slots.map((s) => s.slot).join() !== expected.join()) problems.push("incorrect slot order");
  if (showdown) {
    if (new Set(players.map((p) => p.dkId)).size !== 6) problems.push("six distinct players required");
    if (new Set(players.map((p) => p.game)).size !== 1) problems.push("exactly one game required");
    if (new Set(players.map((p) => p.team)).size !== 2) problems.push("both teams required");
    const c = players[0];
    if (!c?.captainId || c.captainSalary !== c.salary * 1.5) problems.push("invalid Captain draftable");
    const salary = players.reduce((sum, p, i) => sum + (i === 0 ? p.captainSalary ?? Infinity : p.salary), 0);
    if (salary > CFB_SALARY_CAP || salary !== lineup.salary) problems.push("invalid salary total");
  }
  return problems;
}
export const cfbSlotId = (s: CfbLineupSlot) => s.slot === "CPT" ? s.player.captainId : s.player.dkId;
