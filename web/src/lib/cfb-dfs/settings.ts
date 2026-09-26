/**
 * CFB DFS lineup types and defaults. No solver here: this file is safe for the
 * browser bundle, the solver (optimizer.ts) runs on the server only.
 */
import type { CfbPosition } from "./salary-csv";

export const CFB_SALARY_CAP = 50_000;
export const CFB_ROSTER_SIZE = 8;
export const CFB_SLOTS = ["QB", "RB", "RB", "WR", "WR", "WR", "FLEX", "S-FLEX"] as const;
export const CFB_OPTIMIZER_VERSION = "cfb-dk-classic-ilp-v1";

export interface CfbPoolPlayer {
  dkId: number;
  name: string;
  position: CfbPosition;
  team: string;
  game: string;
  salary: number;
  proj: number;
}

export interface CfbOptimizerSettings {
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

export interface CfbLineupSlot { slot: (typeof CFB_SLOTS)[number]; player: CfbPoolPlayer }
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
