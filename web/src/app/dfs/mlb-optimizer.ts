import "server-only";

/**
 * DraftKings MLB lineup optimizer using Integer Linear Programming.
 *
 * Classic lineup structure (10 players, $50k salary cap):
 *   P  P  C  1B  2B  3B  SS  OF  OF  OF
 *
 * Key MLB-specific constraints vs NBA:
 *   - Batter stack: min N batters from the same team (default 4)
 *   - Pitcher anti-correlation: if SP from team X is in the lineup,
 *     at most 1 batter from X's opposing lineup can be included.
 *     Rationale: pitcher ER and opposing batter FPTS are negatively
 *     correlated — stacking against your pitcher destroys lineup ceiling.
 *   - Bring-back: mirrors NBA bring-back (if 4+ from team A, add ≥1 from team B)
 *
 * Position eligibility: players can be multi-eligible (e.g. "2B/SS", "1B/3B").
 * The ILP enforces count-level constraints; backtracking handles slot assignment.
 */

import type { DkPlayerRow } from "@/db/queries";
import { parseCsvLine, stringifyCsvLine } from "./csv";

// eslint-disable-next-line @typescript-eslint/no-require-imports
const solver = require("javascript-lp-solver") as {
  Solve: (model: SolverModel) => SolverResult;
};

type SolverModel = {
  optimize: string;
  opType: "max" | "min";
  constraints: Record<string, { min?: number; max?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, number>;
};

type SolverResult = Record<string, number> & { feasible: boolean; result: number };

export type MlbOptimizerPlayer = Pick<
  DkPlayerRow,
  | "id"
  | "dkPlayerId"
  | "name"
  | "teamAbbrev"
  | "teamId"
  | "matchupId"
  | "eligiblePositions"
  | "salary"
  | "ourProj"
  | "ourLeverage"
  | "linestarProj"
  | "projOwnPct"
  | "isOut"
  | "gameInfo"
  | "teamLogo"
  | "teamName"
> & {
  homeTeamId: number | null;
};

export type MlbLineupSlot = "P1" | "P2" | "C" | "1B" | "2B" | "3B" | "SS" | "OF1" | "OF2" | "OF3";

export type MlbGeneratedLineup = {
  players: MlbOptimizerPlayer[];
  slots: Record<MlbLineupSlot, MlbOptimizerPlayer>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
};

export type MlbOptimizerSettings = {
  mode: "cash" | "gpp";
  nLineups: number;
  minStack: number;          // min batters from same team (default 4)
  maxExposure: number;
  bringBackThreshold: number; // if ≥ N batters from team A, add ≥1 from team B
  antiCorrMax: number;        // max batters facing your own SP (default 1)
};

const MLB_SALARY_CAP = 50000;
const MLB_ROSTER_SIZE = 10;

/** True if the position string marks a pitcher slot. */
function isPitcher(pos: string): boolean {
  return pos.includes("SP") || pos.includes("RP");
}

export function optimizeMlbLineups(
  pool: MlbOptimizerPlayer[],
  settings: MlbOptimizerSettings,
): MlbGeneratedLineup[] {
  const { mode, nLineups, minStack, maxExposure, bringBackThreshold, antiCorrMax } = settings;

  const eligible = pool.filter(
    (p) => !p.isOut && p.ourProj != null && p.ourProj > 0 && p.salary > 0,
  );
  if (eligible.length < MLB_ROSTER_SIZE) return [];

  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const probe = (ms: number, bb: number, ac: number) =>
    !!solveMlbLineup(eligible, mode, ms, nLineups, freshCount(), [], bb, ac);

  let effectiveMinStack    = minStack;
  let effectiveBringBack   = bringBackThreshold;
  let effectiveAntiCorr    = antiCorrMax;
  let relaxed = false;

  // Progressively relax constraints until feasible
  if (!probe(effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveBringBack = 0;
    relaxed = true;
  }
  if (!probe(effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveAntiCorr = MLB_ROSTER_SIZE; // disable anti-corr limit
    relaxed = true;
  }
  if (!probe(effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    effectiveMinStack = 0;
    relaxed = true;
  }
  if (relaxed && !probe(effectiveMinStack, effectiveBringBack, effectiveAntiCorr)) {
    return [];
  }

  const effectiveMinChanges = mode === "gpp" && eligible.length >= 60 && !relaxed ? 3 : 2;

  const exposureCount = new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const lineups: MlbGeneratedLineup[] = [];
  const previousLineupSets: Set<number>[] = [];

  for (let i = 0; i < nLineups; i++) {
    const maxExp = Math.ceil(nLineups * maxExposure);
    let lineup = solveMlbLineup(
      eligible, mode, effectiveMinStack, maxExp,
      exposureCount, previousLineupSets,
      effectiveBringBack, effectiveAntiCorr, effectiveMinChanges,
    );
    if (!lineup && effectiveMinChanges > 1) {
      lineup = solveMlbLineup(
        eligible, mode, effectiveMinStack, maxExp,
        exposureCount, previousLineupSets,
        effectiveBringBack, effectiveAntiCorr, 1,
      );
    }
    if (!lineup) break;

    lineups.push(lineup);
    previousLineupSets.push(new Set(lineup.players.map((p) => p.id)));
    for (const p of lineup.players) {
      exposureCount.set(p.id, (exposureCount.get(p.id) ?? 0) + 1);
    }
  }

  return lineups;
}

function solveMlbLineup(
  pool: MlbOptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  maxExposureCount: number,
  exposureCount: Map<number, number>,
  previousLineupSets: Set<number>[],
  bringBackThreshold = 4,
  antiCorrMax = 1,
  minChanges = 3,
): MlbGeneratedLineup | null {
  // Group by matchupId for stacking and bring-back
  const gamePlayers = new Map<number, MlbOptimizerPlayer[]>();
  if (minStack > 0 || bringBackThreshold >= 2) {
    for (const p of pool) {
      if (p.matchupId == null) continue;
      if (!gamePlayers.has(p.matchupId)) gamePlayers.set(p.matchupId, []);
      gamePlayers.get(p.matchupId)!.push(p);
    }
  }

  // Group by teamId for batter stack and anti-correlation
  const teamPlayers = new Map<number | null, MlbOptimizerPlayer[]>();
  for (const p of pool) {
    if (!teamPlayers.has(p.teamId)) teamPlayers.set(p.teamId, []);
    teamPlayers.get(p.teamId)!.push(p);
  }

  const stackableGames = Array.from(gamePlayers.entries())
    .filter(([, players]) => players.length >= minStack)
    .map(([mid]) => mid);

  const bringBackGames: number[] = [];
  if (mode === "gpp" && bringBackThreshold >= 2) {
    for (const [mid, players] of gamePlayers) {
      const teams = new Set(players.map((p) => p.teamId).filter(Boolean));
      if (teams.size === 2) bringBackGames.push(mid);
    }
  }

  // Anti-correlation: for each matchup, for each team that has pitchers,
  // limit batters from the opposing team to antiCorrMax.
  // Formulation: Σ pitcher_from_A + Σ batter_from_B ≤ 1 + antiCorrMax
  // (relaxed: if no pitcher from A, constraint is trivially satisfied)
  // Encoded as: pitcher_from_A_count - batter_from_B_count ≥ -(antiCorrMax)
  //             → batter_from_B_count ≤ antiCorrMax + pitcher_from_A_indicator * big_M
  // Simpler big-M: use the constraint that for each game:
  //   home_pitchers_in_lineup × big_M + away_batter_count ≤ antiCorrMax + big_M
  // But big-M isn't clean with LP solver. Instead, encode with binary helper:
  //   For each matchup: anti_ac_home_{mid} + anti_ac_away_{mid} handled via
  //   per-player contributions to matchup-level constraints.
  //
  // Practical clean approach: for each matchup m (home A vs away B):
  //   Constraint anti_ac_a_{m}: Σ(home pitchers) × (MLB_ROSTER_SIZE - antiCorrMax) + Σ(away batters) ≤ MLB_ROSTER_SIZE
  //   → if home pitcher included, away batters must stay ≤ antiCorrMax
  //   Similarly for anti_ac_b_{m}.
  const antiCorrGames: number[] = [];
  if (antiCorrMax < MLB_ROSTER_SIZE) {
    for (const mid of gamePlayers.keys()) {
      antiCorrGames.push(mid);
    }
  }

  const stackableSet  = new Set(stackableGames);
  const bringBackSet  = new Set(bringBackGames);
  const antiCorrSet   = new Set(antiCorrGames);
  const bringBackMax  = bringBackThreshold - 1;

  const constraints: SolverModel["constraints"] = {
    salary:    { max: MLB_SALARY_CAP },
    total:     { equal: MLB_ROSTER_SIZE },
    // MLB position slot requirements
    p_count:   { min: 2 },   // 2 P slots (SP/RP)
    c_count:   { min: 1 },
    b1_count:  { min: 1 },
    b2_count:  { min: 1 },
    b3_count:  { min: 1 },
    ss_count:  { min: 1 },
    of_count:  { min: 3 },   // 3 OF slots
    // Batter stack: at least one team provides ≥ minStack batters
    ...(stackableGames.length > 0 ? { stack_count: { min: 1 } } : {}),
  };

  for (const mid of bringBackGames) {
    constraints[`bb_home_${mid}`] = { max: bringBackMax };
    constraints[`bb_away_${mid}`] = { max: bringBackMax };
  }

  // Anti-correlation constraints per matchup:
  // home_pitchers * (ROSTER - antiMax) + away_batters ≤ ROSTER
  // away_pitchers * (ROSTER - antiMax) + home_batters ≤ ROSTER
  const antiCorrCoeff = MLB_ROSTER_SIZE - antiCorrMax;
  for (const mid of antiCorrGames) {
    constraints[`ac_home_${mid}`] = { max: MLB_ROSTER_SIZE };
    constraints[`ac_away_${mid}`] = { max: MLB_ROSTER_SIZE };
  }

  for (const p of pool) {
    if ((exposureCount.get(p.id) ?? 0) >= maxExposureCount) {
      constraints[`excl_${p.id}`] = { max: 0 };
    }
  }

  const DIV_WINDOW = 5;
  const divWindow = previousLineupSets.slice(-DIV_WINDOW);
  for (let i = 0; i < divWindow.length; i++) {
    constraints[`div_${i}`] = { max: MLB_ROSTER_SIZE - minChanges };
  }

  for (const mid of stackableGames) {
    constraints[`game_${mid}`] = { min: 0 };
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};

  for (const p of pool) {
    const key = `p_${p.id}`;
    const score = (mode === "gpp" ? p.ourLeverage : p.ourProj) ?? 0;
    const pos = p.eligiblePositions;
    const pitcher = isPitcher(pos);
    const batter  = !pitcher;

    const entry: Record<string, number> = {
      score,
      salary: p.salary,
      total:  1,
    };

    // Position contributions
    if (pitcher)           entry.p_count  = 1;
    if (pos.includes("C") && batter) entry.c_count  = 1;
    if (pos.includes("1B"))          entry.b1_count = 1;
    if (pos.includes("2B"))          entry.b2_count = 1;
    if (pos.includes("3B"))          entry.b3_count = 1;
    if (pos.includes("SS"))          entry.ss_count = 1;
    if (pos.includes("OF"))          entry.of_count = 1;

    // Batter stack helper
    if (p.matchupId != null && stackableSet.has(p.matchupId) && batter) {
      entry[`game_${p.matchupId}`] = 1;
    }

    // Bring-back (same as NBA, based on matchupId + home/away side)
    if (p.matchupId != null && bringBackSet.has(p.matchupId) && p.teamId != null) {
      const isHome = p.teamId === p.homeTeamId;
      entry[`bb_home_${p.matchupId}`] = isHome ? 1 : -1;
      entry[`bb_away_${p.matchupId}`] = isHome ? -1 : 1;
    }

    // Anti-correlation:
    // Home pitchers contribute antiCorrCoeff to ac_home_{mid}
    // Away batters contribute 1 to ac_home_{mid}  (opposing the home pitcher)
    // Away pitchers contribute antiCorrCoeff to ac_away_{mid}
    // Home batters contribute 1 to ac_away_{mid}  (opposing the away pitcher)
    if (p.matchupId != null && antiCorrSet.has(p.matchupId) && p.teamId != null) {
      const isHome = p.teamId === p.homeTeamId;
      if (pitcher && isHome)  entry[`ac_home_${p.matchupId}`] = antiCorrCoeff;
      if (batter  && !isHome) entry[`ac_home_${p.matchupId}`] = 1;
      if (pitcher && !isHome) entry[`ac_away_${p.matchupId}`] = antiCorrCoeff;
      if (batter  && isHome)  entry[`ac_away_${p.matchupId}`] = 1;
    }

    // Diversity + exposure
    for (let i = 0; i < divWindow.length; i++) {
      if (divWindow[i].has(p.id)) entry[`div_${i}`] = 1;
    }
    if ((exposureCount.get(p.id) ?? 0) >= maxExposureCount) {
      entry[`excl_${p.id}`] = 1;
    }

    variables[key] = entry;
    binaries[key]  = 1;
  }

  // Stack helper variables (z_game = 1 iff team from game has ≥ minStack batters)
  for (const mid of stackableGames) {
    const key = `z_game_${mid}`;
    variables[key] = {
      stack_count: 1,
      [`game_${mid}`]: -minStack,
    };
    binaries[key] = 1;
  }

  const model: SolverModel = {
    optimize: "score",
    opType: "max",
    constraints,
    variables,
    binaries,
  };

  const result = solver.Solve(model);
  if (!result.feasible) return null;

  const selected = pool.filter((p) => (result[`p_${p.id}`] ?? 0) >= 0.5);
  if (selected.length !== MLB_ROSTER_SIZE) return null;

  const slots = assignMlbPositions(selected);
  if (!slots) return null;

  const totalSalary   = selected.reduce((s, p) => s + p.salary, 0);
  const projFpts      = selected.reduce((s, p) => s + (p.ourProj ?? 0), 0);
  const leverageScore = selected.reduce((s, p) => s + (p.ourLeverage ?? 0), 0);

  return { players: selected, slots, totalSalary, projFpts, leverageScore };
}

/**
 * Assign 10 MLB players to P1/P2/C/1B/2B/3B/SS/OF1/OF2/OF3 slots.
 * Backtracking guarantees a valid assignment if one exists.
 */
function assignMlbPositions(
  players: MlbOptimizerPlayer[],
): Record<MlbLineupSlot, MlbOptimizerPlayer> | null {
  const slots: MlbLineupSlot[] = ["P1", "P2", "C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"];

  const canFill = (slot: MlbLineupSlot, pos: string): boolean => {
    switch (slot) {
      case "P1":
      case "P2":  return isPitcher(pos);
      case "C":   return pos.includes("C") && !isPitcher(pos);
      case "1B":  return pos.includes("1B");
      case "2B":  return pos.includes("2B");
      case "3B":  return pos.includes("3B");
      case "SS":  return pos.includes("SS");
      case "OF1":
      case "OF2":
      case "OF3": return pos.includes("OF");
    }
  };

  const assignment: (MlbOptimizerPlayer | null)[] = new Array(10).fill(null);
  const used = new Set<number>();

  const solve = (slotIdx: number): boolean => {
    if (slotIdx === 10) return true;
    const slot = slots[slotIdx];
    for (const p of players) {
      if (used.has(p.id)) continue;
      if (!canFill(slot, p.eligiblePositions)) continue;
      assignment[slotIdx] = p;
      used.add(p.id);
      if (solve(slotIdx + 1)) return true;
      used.delete(p.id);
      assignment[slotIdx] = null;
    }
    return false;
  };

  if (!solve(0)) return null;

  const result = {} as Record<MlbLineupSlot, MlbOptimizerPlayer>;
  for (let i = 0; i < 10; i++) result[slots[i]] = assignment[i]!;
  return result;
}

/**
 * Build DK MLB multi-entry upload CSV.
 * Header: Entry ID,Contest Name,Contest ID,Entry Fee,P,P,C,1B,2B,3B,SS,OF,OF,OF
 * Player cell: "Name (dkPlayerId)"
 */
export function buildMlbMultiEntryCSV(
  lineups: MlbGeneratedLineup[],
  entryRows: string[],
): string {
  if (lineups.length === 0) return "";
  if (entryRows.length < 2) {
    throw new Error("Entry template must include a header row and at least one entry row.");
  }
  if (entryRows.length - 1 < lineups.length) {
    throw new Error(`Entry template has ${entryRows.length - 1} entries for ${lineups.length} lineups.`);
  }

  const slotOrder: MlbLineupSlot[] = ["P1", "P2", "C", "1B", "2B", "3B", "SS", "OF1", "OF2", "OF3"];
  const rows = [stringifyCsvLine(parseCsvLine(entryRows[0]))];

  for (let i = 0; i < lineups.length; i++) {
    const lineup = lineups[i];
    const cols = parseCsvLine(entryRows[i + 1]);
    if (cols.length < 4 + slotOrder.length) {
      throw new Error(`Entry row ${i + 2} is missing required DraftKings columns.`);
    }
    for (let j = 0; j < 10; j++) {
      const p = lineup.slots[slotOrder[j]];
      cols[4 + j] = p ? `${p.name} (${p.dkPlayerId})` : "";
    }
    rows.push(stringifyCsvLine(cols));
  }

  return rows.join("\n");
}
