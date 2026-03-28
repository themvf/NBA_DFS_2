import "server-only";

/**
 * DraftKings NBA lineup optimizer using Integer Linear Programming.
 *
 * Lineup structure: PG / SG / SF / PF / C / G / F / UTIL (8 players, $50k cap)
 *   - PG slot: player with "PG" in eligible_positions
 *   - SG slot: player with "SG" in eligible_positions
 *   - SF slot: player with "SF" in eligible_positions
 *   - PF slot: player with "PF" in eligible_positions
 *   - C slot:  player with "C" in eligible_positions
 *   - G slot:  player with "G" in eligible_positions (PG or SG flex)
 *   - F slot:  player with "F" in eligible_positions (SF or PF flex)
 *   - UTIL:    any player
 *
 * Uses javascript-lp-solver for binary ILP.
 */

import type { DkPlayerRow } from "@/db/queries";

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

export type OptimizerPlayer = Pick<
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
  /** Home team ID for the player's matchup — used for bring-back enforcement. */
  homeTeamId: number | null;
};

export type LineupSlot = "PG" | "SG" | "SF" | "PF" | "C" | "G" | "F" | "UTIL";

export type GeneratedLineup = {
  players: OptimizerPlayer[];
  slots: Record<LineupSlot, OptimizerPlayer>;
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
};

export type OptimizerSettings = {
  mode: "cash" | "gpp";
  nLineups: number;
  minStack: number;
  maxExposure: number;
  /**
   * Bring-back threshold (GPP only).
   * If a team contributes ≥ this many players to a lineup, the optimizer
   * must include ≥ 1 player from their opponent in that matchup.
   * 0 = disabled; 3 = standard GPP construction (3+1 or 4+1 stacks).
   */
  bringBackThreshold: number;
};

const SALARY_CAP = 50000;
const ROSTER_SIZE = 8;

export function optimizeLineups(
  pool: OptimizerPlayer[],
  settings: OptimizerSettings
): GeneratedLineup[] {
  const { mode, nLineups, minStack, maxExposure, bringBackThreshold } = settings;

  const eligible = pool.filter((p) => {
    if (p.isOut) return false;
    // Use ourProj for eligibility in both modes: any player who can score is
    // a valid lineup member (serves as a salary filler in GPP if leverage < 0).
    // Excluding negative-leverage players from the ILP entirely can make the
    // salary-cap constraint infeasible when all positive-leverage players are
    // high-salary. The objective function naturally minimises their usage.
    return p.ourProj != null && p.ourProj > 0 && p.salary > 0;
  });

  if (eligible.length < ROSTER_SIZE) return [];

  // Probe for feasibility — run a single test solve (no history, no exposure cap)
  // to discover which constraints the current pool can satisfy. Relax progressively:
  //   1st try: full constraints (stack + bring-back)
  //   2nd try: no bring-back  (bring-back requires an opponent player that may be expensive)
  //   3rd try: no stack either (matchupId missing or pool too thin per game)
  const freshCount = () => new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const probe = (ms: number, bb: number) =>
    !!solveOneLineup(eligible, mode, ms, nLineups, freshCount(), [], bb);

  let effectiveMinStack  = minStack;
  let effectiveBringBack = bringBackThreshold;
  let relaxed = false;

  if (!probe(effectiveMinStack, effectiveBringBack)) {
    effectiveBringBack = 0;           // disable bring-back
    relaxed = true;
  }
  if (!probe(effectiveMinStack, effectiveBringBack)) {
    effectiveMinStack  = 0;           // disable stacking too
    effectiveBringBack = 0;           // bring-back needs gamePlayers, empty when minStack=0
    relaxed = true;
  }
  // Third probe: verify even the first lineup (no history, no exposure caps)
  // is feasible after full relaxation. Does NOT guarantee subsequent lineups
  // succeed — exposure caps and diversity can still exhaust the pool mid-run.
  if (relaxed && !probe(effectiveMinStack, effectiveBringBack)) {
    return [];                        // genuinely infeasible — bail early
  }

  // Adaptive diversity: small or thin pools can't sustain 3-player diff between
  // every pair of lineups — drop to 2 when eligible < 55 or constraints were relaxed.
  const effectiveMinChanges = mode === "gpp" && eligible.length >= 55 && !relaxed ? 3 : 2;

  const exposureCount = new Map<number, number>(eligible.map((p) => [p.id, 0]));
  const lineups: GeneratedLineup[] = [];
  const previousLineupSets: Set<number>[] = [];

  for (let i = 0; i < nLineups; i++) {
    const maxExp = Math.ceil(nLineups * maxExposure);
    let lineup = solveOneLineup(
      eligible, mode, effectiveMinStack, maxExp,
      exposureCount, previousLineupSets, effectiveBringBack, effectiveMinChanges,
    );
    // If still infeasible, relax diversity to 1 (just prevent exact duplicates)
    if (!lineup && effectiveMinChanges > 1) {
      lineup = solveOneLineup(
        eligible, mode, effectiveMinStack, maxExp,
        exposureCount, previousLineupSets, effectiveBringBack, 1,
      );
    }
    if (!lineup) break;

    lineups.push(lineup);
    const lineupSet = new Set(lineup.players.map((p) => p.id));
    previousLineupSets.push(lineupSet);
    for (const p of lineup.players) {
      exposureCount.set(p.id, (exposureCount.get(p.id) ?? 0) + 1);
    }
  }

  return lineups;
}

function solveOneLineup(
  pool: OptimizerPlayer[],
  mode: "cash" | "gpp",
  minStack: number,
  maxExposureCount: number,
  exposureCount: Map<number, number>,
  previousLineupSets: Set<number>[],
  bringBackThreshold = 3,
  minChanges = mode === "gpp" ? 3 : 2,
): GeneratedLineup | null {
  // Group by matchupId for game stacking.
  // When minStack <= 0 stacking is effectively disabled — skip entirely to avoid
  // polluting the model with trivial helper variables that bloat the B&B tree.
  const gamePlayers = new Map<number, OptimizerPlayer[]>();
  if (minStack > 0) {
    for (const p of pool) {
      if (p.matchupId == null) continue;
      if (!gamePlayers.has(p.matchupId)) gamePlayers.set(p.matchupId, []);
      gamePlayers.get(p.matchupId)!.push(p);
    }
  }
  const stackableGames = Array.from(gamePlayers.entries())
    .filter(([, players]) => players.length >= minStack)
    .map(([mid]) => mid);

  // Bring-back: matchups where both teams have players in the pool
  // (threshold 0 = disabled; only applies in GPP mode)
  const bringBackGames: number[] = [];
  if (mode === "gpp" && bringBackThreshold >= 2) {
    for (const [mid, players] of gamePlayers) {
      const teams = new Set(players.map((p) => p.teamId).filter(Boolean));
      if (teams.size === 2) bringBackGames.push(mid);
    }
  }

  const stackableSet = new Set(stackableGames);
  const bringBackSet = new Set(bringBackGames);

  const constraints: SolverModel["constraints"] = {
    salary:    { max: SALARY_CAP },
    total:     { equal: ROSTER_SIZE },
    // NBA position slot requirements
    pg_count:  { min: 1 },   // PG slot
    sg_count:  { min: 1 },   // SG slot
    sf_count:  { min: 1 },   // SF slot
    pf_count:  { min: 1 },   // PF slot
    c_count:   { min: 1 },   // C slot
    g_count:   { min: 3 },   // PG + SG + G slots (3 G-eligible players needed)
    f_count:   { min: 3 },   // SF + PF + F slots (3 F-eligible players needed)
    // Stack constraint only applies when games with enough players exist.
    // If matchupId is null for all players (no schedule data), omitting this
    // prevents the solver from being immediately infeasible.
    ...(stackableGames.length > 0 ? { stack_count: { min: 1 } } : {}),
  };

  // Bring-back constraints: if team A has ≥ threshold players in the lineup,
  // team B (their opponent) must have ≥ 1.
  //
  // Formulated as two symmetric max-constraints per matchup:
  //   home_net = Σ home_players - Σ away_players  → home_net ≤ threshold - 1
  //   away_net = Σ away_players - Σ home_players  → away_net ≤ threshold - 1
  //
  // Example (threshold=3): 3 home + 0 away → home_net=3 > 2 → infeasible.
  // Optimizer must include ≥1 away player (home_net drops to 2 → feasible).
  const bringBackMax = bringBackThreshold - 1;
  for (const mid of bringBackGames) {
    constraints[`bb_home_${mid}`] = { max: bringBackMax };
    constraints[`bb_away_${mid}`] = { max: bringBackMax };
  }

  for (const p of pool) {
    const used = exposureCount.get(p.id) ?? 0;
    if (used >= maxExposureCount) {
      constraints[`excl_player_${p.id}`] = { max: 0 };
    }
  }

  // Only enforce diversity against the last DIV_WINDOW lineups.
  // Applying it to ALL prior lineups causes the ILP to become infeasible
  // after ~5-7 lineups on a small pool (each new lineup must simultaneously
  // differ from every previous one).
  const DIV_WINDOW = 5;
  const divWindow = previousLineupSets.slice(-DIV_WINDOW);
  for (let i = 0; i < divWindow.length; i++) {
    constraints[`div_${i}`] = { max: ROSTER_SIZE - minChanges };
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
    const entry: Record<string, number> = {
      score,
      salary: p.salary,
      total: 1,
    };

    // Position contributions
    if (pos.includes("PG")) entry.pg_count = 1;
    if (pos.includes("SG")) entry.sg_count = 1;
    if (pos.includes("SF")) entry.sf_count = 1;
    if (pos.includes("PF")) entry.pf_count = 1;
    if (pos.includes("C"))  entry.c_count  = 1;
    if (pos.includes("G"))  entry.g_count  = 1;  // PG or SG eligible → G flex
    if (pos.includes("F"))  entry.f_count  = 1;  // SF or PF eligible → F flex

    // Game stack
    if (p.matchupId != null && stackableSet.has(p.matchupId)) {
      entry[`game_${p.matchupId}`] = 1;
    }

    // Bring-back: home team players contribute +1 to home_net, −1 to away_net.
    // Away team players are the mirror image.
    if (p.matchupId != null && bringBackSet.has(p.matchupId) && p.teamId != null) {
      const isHome = p.teamId === p.homeTeamId;
      entry[`bb_home_${p.matchupId}`] = isHome ? 1 : -1;
      entry[`bb_away_${p.matchupId}`] = isHome ? -1 : 1;
    }

    // Diversity (rolling window only)
    for (let i = 0; i < divWindow.length; i++) {
      if (divWindow[i].has(p.id)) {
        entry[`div_${i}`] = 1;
      }
    }

    // Exposure cap
    if ((exposureCount.get(p.id) ?? 0) >= maxExposureCount) {
      entry[`excl_player_${p.id}`] = 1;
    }

    variables[key] = entry;
    binaries[key] = 1;
  }

  // Stack helper variables
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
  if (selected.length !== ROSTER_SIZE) return null;

  const slots = assignPositions(selected);
  if (!slots) return null;

  const totalSalary = selected.reduce((s, p) => s + p.salary, 0);
  const projFpts = selected.reduce((s, p) => s + (p.ourProj ?? 0), 0);
  const leverageScore = selected.reduce((s, p) => s + (p.ourLeverage ?? 0), 0);

  return { players: selected, slots, totalSalary, projFpts, leverageScore };
}

/**
 * Assign 8 NBA players to PG/SG/SF/PF/C/G/F/UTIL slots.
 * Greedy: pure-position players first, flex players fill remaining slots.
 */
function assignPositions(
  players: OptimizerPlayer[]
): Record<LineupSlot, OptimizerPlayer> | null {
  const assigned = new Map<LineupSlot, OptimizerPlayer>();
  const unassigned = [...players];

  const fill = (slots: LineupSlot[], filter: (p: OptimizerPlayer) => boolean) => {
    for (const slot of slots) {
      if (assigned.has(slot)) continue;
      const idx = unassigned.findIndex(filter);
      if (idx >= 0) {
        assigned.set(slot, unassigned.splice(idx, 1)[0]);
      }
    }
  };

  // C-eligible (includes PF/C) — assign before PF step consumes them
  fill(["C"], (p) => p.eligiblePositions.includes("C") && !p.eligiblePositions.includes("G"));
  // Pure PG (no SG)
  fill(["PG"], (p) => p.eligiblePositions.includes("PG") && !p.eligiblePositions.includes("SG"));
  // Pure SG (no PG)
  fill(["SG"], (p) => p.eligiblePositions.includes("SG") && !p.eligiblePositions.includes("PG"));
  // Pure SF (no PF)
  fill(["SF"], (p) => p.eligiblePositions.includes("SF") && !p.eligiblePositions.includes("PF"));
  // Pure PF (no SF)
  fill(["PF"], (p) => p.eligiblePositions.includes("PF") && !p.eligiblePositions.includes("SF"));
  // Any C-eligible fills remaining C slot
  fill(["C"], (p) => p.eligiblePositions.includes("C"));
  // G-eligible fills PG, SG, then G
  fill(["PG", "SG", "G"], (p) => p.eligiblePositions.includes("G"));
  // F-eligible fills SF, PF, then F
  fill(["SF", "PF", "F"], (p) => p.eligiblePositions.includes("F"));
  // UTIL: anyone
  fill(["UTIL"], () => true);

  if (assigned.size !== 8) return null;
  return Object.fromEntries(assigned) as Record<LineupSlot, OptimizerPlayer>;
}

/**
 * Build DK NBA multi-entry upload CSV.
 * Header: Entry ID,Contest Name,Contest ID,Entry Fee,PG,SG,SF,PF,C,G,F,UTIL
 * Player cell format: "Name (dkPlayerId)"
 */
export function buildMultiEntryCSV(
  lineups: GeneratedLineup[],
  entryRows: string[]
): string {
  if (lineups.length === 0 || entryRows.length < 2) return "";

  const header = entryRows[0];
  const rows = [header];

  for (let i = 0; i < lineups.length; i++) {
    const lineup = lineups[i];
    const entryLine = entryRows[i + 1] ?? entryRows[1];
    const cols = entryLine.split(",");
    const slotOrder: LineupSlot[] = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"];
    for (let j = 0; j < 8; j++) {
      const player = lineup.slots[slotOrder[j]];
      cols[4 + j] = player ? `${player.name} (${player.dkPlayerId})` : "";
    }
    rows.push(cols.join(","));
  }

  return rows.join("\n");
}
