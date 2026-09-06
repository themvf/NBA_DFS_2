import "server-only";
import {selectedWorkload,validateWorkloadPositions,WORKLOAD_POSITIONS,type WorkloadPositions} from "@/lib/nfl-dfs/workload-selection";
import type { WorkloadProjection } from "@/lib/nfl-dfs/workload-projection";
import type { CalibratedProjection } from "@/lib/nfl-dfs/calibrated-projection";

export const NFL_OPTIMIZER_VERSION = "nfl-dfs-ilp-v4-position-workload";

export type NflProjectionSource = "our" | "workload" | "calibrated" | "dk_avg" | "fantasypros" | "linestar" | "custom";
export type NflOptimizerMode = "cash" | "gpp";
export type NflSlateFormat = "classic" | "showdown";

export type NflOptimizerPlayer = {
  id: number;
  dkPlayerId: number;
  captainDkPlayerId: number | null;
  name: string;
  position: "QB" | "RB" | "WR" | "TE" | "K" | "DST";
  team: string;
  opponent: string | null;
  gameKey: string | null;
  salary: number;
  captainSalary: number | null;
  isOut: boolean;
  projectionStatus: string;
  ourProj: number | null;
  floorFpts: number | null;
  ceilingFpts: number | null;
  boomRate: number | null;
  avgFptsDk: number | null;
  fantasyprosProj: number | null;
  linestarProj: number | null;
  linestarOwnPct: number | null;
  customProj: number | null;
  workload?: WorkloadProjection | null;
  workloadReason?: string;
  positionWorkload?: CalibratedProjection | null;
  positionWorkloadReason?: string;
  calibrated?: CalibratedProjection | null;
  calibrationReason?: string;
};

export type NflOptimizerSettings = {
  format: NflSlateFormat;
  mode: NflOptimizerMode;
  projectionSource: NflProjectionSource;
  allowDkFallback: boolean;
  workloadPositions?: WorkloadPositions;
  nLineups: number;
  minSalary: number;
  maxExposure: number;
  minUnique: number;
  stackPassCatchers: 0 | 1 | 2;
  bringBack: boolean;
  randomness: number;
  lockedPlayerIds: number[];
  excludedPlayerIds: number[];
  minExposureByPlayer: Record<string, number>;
  maxExposureByPlayer: Record<string, number>;
};

export type NflLineupSlot = {
  slot: string;
  player: NflOptimizerPlayer;
  salary: number;
  multiplier: number;
  projection: number;
  projectionSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback";
};

export type NflGeneratedLineup = {
  lineupNumber: number;
  slots: NflLineupSlot[];
  playerIds: number[];
  totalSalary: number;
  projectedFpts: number;
  floorFpts: number;
  ceilingFpts: number;
  projectedOwnership: number | null;
  stackSummary: { quarterback: string | null; passCatchers: string[]; bringBack: string | null };
};

export type NflOptimizerResult = {
  lineups: NflGeneratedLineup[];
  warnings: string[];
  sourceCoverage: { requested: number; direct: number; fallback: number; excluded: number };
};

type ResolvedPlayer = NflOptimizerPlayer & {
  projection: number;
  resolvedSource: NflProjectionSource | "dk_avg_fallback" | "our_fallback";
};

type SolverModel = {
  optimize: "score";
  opType: "max";
  constraints: Record<string, { max?: number; min?: number; equal?: number }>;
  variables: Record<string, Record<string, number>>;
  binaries: Record<string, 1>;
};
type SolverResult = Record<string, number | boolean> & { feasible?: boolean; result?: number };

const CLASSIC_SLOTS = ["QB", "RB1", "RB2", "WR1", "WR2", "WR3", "TE", "FLEX", "DST"] as const;
const SHOWDOWN_SLOTS = ["CPT", "FLEX1", "FLEX2", "FLEX3", "FLEX4", "FLEX5"] as const;

function finite(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

function projectionFor(player: NflOptimizerPlayer, settings: NflOptimizerSettings): { value: number; source: ResolvedPlayer["resolvedSource"] } | null {
  if (settings.projectionSource === "workload") {
    const candidate=selectedWorkload(player,settings.workloadPositions);
    if (candidate && finite(candidate.mean) !== null && candidate.mean > 0) return { value: candidate.mean, source: "workload" };
    if (finite(player.ourProj) !== null && player.ourProj! > 0) return { value: player.ourProj!, source: "our_fallback" };
  }
  if (settings.projectionSource === "calibrated") {
    if (player.calibrated && finite(player.calibrated.mean) !== null && player.calibrated.mean > 0) return { value: player.calibrated.mean, source: "calibrated" };
    if (finite(player.ourProj) !== null && player.ourProj! > 0) return { value: player.ourProj!, source: "our_fallback" };
  }
  const direct = settings.projectionSource === "our" ? finite(player.ourProj)
    : settings.projectionSource === "calibrated" || settings.projectionSource === "workload" ? null
    : settings.projectionSource === "dk_avg" ? finite(player.avgFptsDk)
    : settings.projectionSource === "fantasypros" ? finite(player.fantasyprosProj)
    : settings.projectionSource === "linestar" ? finite(player.linestarProj)
    : finite(player.customProj);
  if (direct != null && direct > 0) return { value: direct, source: settings.projectionSource };
  const fallback = finite(player.avgFptsDk);
  return settings.allowDkFallback && fallback != null && fallback > 0
    ? { value: fallback, source: "dk_avg_fallback" }
    : null;
}

function safe(value: string): string {
  return value.replace(/[^a-zA-Z0-9]/g, "_");
}

function jitter(seed: number, lineup: number, playerId: number): number {
  let value = (seed ^ (lineup * 2654435761) ^ (playerId * 1597334677)) >>> 0;
  value = Math.imul(value ^ (value >>> 16), 2246822507) >>> 0;
  return (value / 4294967295) * 2 - 1;
}

function objective(player: ResolvedPlayer, settings: NflOptimizerSettings, lineupNumber: number): number {
  const historical = player.resolvedSource === "our" || player.resolvedSource === "our_fallback";
  const base = settings.mode === "cash"
    ? (player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p10 : player.resolvedSource === "calibrated" ? player.calibrated!.p10 : historical ? finite(player.floorFpts) : null) ?? player.projection * 0.74
    : (player.resolvedSource === "workload" ? selectedWorkload(player,settings.workloadPositions)!.p90 : player.resolvedSource === "calibrated" ? player.calibrated!.p90 : historical ? finite(player.ceilingFpts) : null) ?? player.projection * 1.28;
  const ownershipPenalty = settings.mode === "gpp" ? (finite(player.linestarOwnPct) ?? 0) * 0.025 : 0;
  const workload=player.resolvedSource === "workload"?selectedWorkload(player,settings.workloadPositions):null;
  const boomBonus = settings.mode === "gpp" ? (workload && "boom" in workload ? workload.boom : player.resolvedSource === "calibrated" ? player.calibrated!.boom : historical ? finite(player.boomRate) ?? 0 : 0) * 2 : 0;
  return base + boomBonus - ownershipPenalty + jitter(20260902, lineupNumber, player.dkPlayerId) * settings.randomness * player.projection;
}

function validateSettings(settings: NflOptimizerSettings): void {
  if(settings.projectionSource === "workload")validateWorkloadPositions(settings.workloadPositions);
  if (!["our", "workload", "calibrated", "dk_avg", "fantasypros", "linestar", "custom"].includes(settings.projectionSource)) throw new Error("Unknown projection source.");
  if (!Number.isInteger(settings.nLineups) || settings.nLineups < 1 || settings.nLineups > 150) throw new Error("Lineup count must be between 1 and 150.");
  if (settings.minSalary < 0 || settings.minSalary > 50000) throw new Error("Minimum salary must be between $0 and $50,000.");
  if (settings.maxExposure <= 0 || settings.maxExposure > 1) throw new Error("Maximum exposure must be greater than 0 and at most 100%.");
  const rosterSize = settings.format === "classic" ? 9 : 6;
  if (settings.minUnique < 1 || settings.minUnique > rosterSize) throw new Error(`Minimum unique players must be 1-${rosterSize}.`);
}

function buildOne(
  pool: ResolvedPlayer[],
  settings: NflOptimizerSettings,
  lineupNumber: number,
  previous: NflGeneratedLineup[],
  exposureCounts: Map<number, number>,
  forcedIds: Set<number>,
): NflGeneratedLineup | null {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const solver = require("javascript-lp-solver") as { Solve: (model: SolverModel) => SolverResult };
  const slots = settings.format === "classic" ? [...CLASSIC_SLOTS] : [...SHOWDOWN_SLOTS];
  const rosterSize = slots.length;
  const maxCount = (player: ResolvedPlayer) => {
    const override = settings.maxExposureByPlayer[String(player.dkPlayerId)];
    return override == null
      ? Math.max(1, Math.floor(settings.maxExposure * settings.nLineups + 1e-9))
      : Math.max(0, Math.floor(override * settings.nLineups + 1e-9));
  };
  const available = pool.filter((player) => (exposureCounts.get(player.dkPlayerId) ?? 0) < maxCount(player));
  const constraints: SolverModel["constraints"] = { salary: { max: 50000, min: settings.minSalary } };
  if (settings.format === "classic") {
    constraints.roster = { equal: 9 };
    constraints.qb = { equal: 1 };
    constraints.rb = { min: 2, max: 3 };
    constraints.wr = { min: 3, max: 4 };
    constraints.te = { min: 1, max: 2 };
    constraints.dst = { equal: 1 };
    constraints.flex = { equal: 7 };
  } else {
    constraints.cpt = { equal: 1 };
    constraints.flex = { equal: 5 };
  }
  for (const player of available) constraints[`player_${player.dkPlayerId}`] = { max: 1 };
  for (const game of new Set(available.map((player) => player.gameKey).filter(Boolean))) {
    constraints[`game_${safe(game!)}`] = { max: settings.format === "classic" ? 8 : 6 };
  }
  if (settings.format === "showdown") {
    for (const team of new Set(available.map((player) => player.team))) constraints[`team_${safe(team)}`] = { max: 5 };
  }
  for (const playerId of forcedIds) constraints[`force_${playerId}`] = { equal: 1 };
  previous.forEach((lineup, index) => { constraints[`prior_${index}`] = { max: rosterSize - settings.minUnique }; });

  if (settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0) {
    for (const quarterback of available.filter((player) => player.position === "QB")) {
      constraints[`stack_${quarterback.dkPlayerId}`] = { min: 0 };
      if (settings.bringBack) constraints[`bring_${quarterback.dkPlayerId}`] = { min: 0 };
      constraints[`anti_${quarterback.dkPlayerId}`] = { max: 1 };
    }
  }

  const variables: SolverModel["variables"] = {};
  const binaries: SolverModel["binaries"] = {};
  for (const player of available) {
    const purchaseTypes = settings.format === "classic" ? ["CLASSIC"] : ["CPT", "FLEX"];
    for (const purchaseType of purchaseTypes) {
      if (purchaseType === "CPT" && (player.captainDkPlayerId == null || player.captainSalary == null)) continue;
      const key = `${purchaseType === "CLASSIC" ? "x" : purchaseType === "CPT" ? "c" : "f"}_${player.dkPlayerId}`;
      const slot = purchaseType === "CPT" ? "CPT" : purchaseType === "FLEX" ? "FLEX" : "CLASSIC";
      const multiplier = slot === "CPT" ? 1.5 : 1;
      const salary = slot === "CPT" ? player.captainSalary! : player.salary;
      const variable: Record<string, number> = {
        score: objective(player, settings, lineupNumber) * multiplier,
        salary,
        [`player_${player.dkPlayerId}`]: 1,
      };
      if (settings.format === "classic") {
        variable.roster = 1;
        variable[player.position.toLowerCase()] = 1;
        if (["RB", "WR", "TE"].includes(player.position)) variable.flex = 1;
      } else {
        variable[slot === "CPT" ? "cpt" : "flex"] = 1;
      }
      if (settings.format === "showdown") variable[`team_${safe(player.team)}`] = 1;
      if (player.gameKey) variable[`game_${safe(player.gameKey)}`] = 1;
      if (forcedIds.has(player.dkPlayerId)) variable[`force_${player.dkPlayerId}`] = 1;
      previous.forEach((lineup, index) => { if (lineup.playerIds.includes(player.dkPlayerId)) variable[`prior_${index}`] = 1; });
      if (settings.format === "classic" && settings.mode === "gpp" && settings.stackPassCatchers > 0) {
        for (const quarterback of available.filter((candidate) => candidate.position === "QB")) {
          if (player.dkPlayerId === quarterback.dkPlayerId) {
            variable[`stack_${quarterback.dkPlayerId}`] = -settings.stackPassCatchers;
            if (settings.bringBack) variable[`bring_${quarterback.dkPlayerId}`] = -1;
            variable[`anti_${quarterback.dkPlayerId}`] = 1;
          }
          if (["WR", "TE"].includes(player.position) && player.team === quarterback.team) variable[`stack_${quarterback.dkPlayerId}`] = 1;
          if (settings.bringBack && ["RB", "WR", "TE"].includes(player.position) && player.team === quarterback.opponent) variable[`bring_${quarterback.dkPlayerId}`] = 1;
          if (player.position === "DST" && player.team === quarterback.opponent) variable[`anti_${quarterback.dkPlayerId}`] = 1;
        }
      }
      variables[key] = variable;
      binaries[key] = 1;
    }
  }
  const solved = solver.Solve({ optimize: "score", opType: "max", constraints, variables, binaries });
  if (solved.feasible === false) return null;
  const purchases: { player: ResolvedPlayer; slot: "CPT" | "FLEX" | "CLASSIC" }[] = [];
  for (const [key, raw] of Object.entries(solved)) {
    if (!/^[xcf]_/.test(key) || typeof raw !== "number" || raw < 0.5) continue;
    const match = key.match(/^([xcf])_(\d+)$/);
    if (!match) continue;
    const player = available.find((candidate) => candidate.dkPlayerId === Number(match[2]));
    if (!player) continue;
    const slot = match[1] === "c" ? "CPT" : match[1] === "f" ? "FLEX" : "CLASSIC";
    purchases.push({ player, slot });
  }
  const chosen: NflLineupSlot[] = [];
  if (settings.format === "showdown") {
    let flexIndex = 0;
    for (const purchase of purchases) {
      const slot = purchase.slot === "CPT" ? "CPT" : `FLEX${++flexIndex}`;
      const multiplier = purchase.slot === "CPT" ? 1.5 : 1;
      chosen.push({ slot, player: purchase.player, salary: purchase.slot === "CPT" ? purchase.player.captainSalary! : purchase.player.salary, multiplier, projection: purchase.player.projection * multiplier, projectionSource: purchase.player.resolvedSource });
    }
  } else {
    const byPosition = (position: NflOptimizerPlayer["position"]) => purchases.filter((entry) => entry.player.position === position).map((entry) => entry.player);
    const qb = byPosition("QB"), rb = byPosition("RB"), wr = byPosition("WR"), te = byPosition("TE"), dst = byPosition("DST");
    const assigned = new Set<number>();
    const assign = (slot: string, player: ResolvedPlayer) => { assigned.add(player.dkPlayerId); chosen.push({ slot, player, salary: player.salary, multiplier: 1, projection: player.projection, projectionSource: player.resolvedSource }); };
    assign("QB", qb[0]); assign("RB1", rb[0]); assign("RB2", rb[1]); assign("WR1", wr[0]); assign("WR2", wr[1]); assign("WR3", wr[2]); assign("TE", te[0]); assign("DST", dst[0]);
    const flex = purchases.map((entry) => entry.player).find((player) => !assigned.has(player.dkPlayerId));
    if (!flex) return null;
    assign("FLEX", flex);
  }
  const slotOrder = new Map<string, number>(slots.map((slot, index) => [slot, index]));
  chosen.sort((a, b) => (slotOrder.get(a.slot) ?? 99) - (slotOrder.get(b.slot) ?? 99));
  if (chosen.length !== rosterSize) return null;
  const qb = chosen.find((entry) => entry.player.position === "QB")?.player ?? null;
  const passCatchers = qb ? chosen.filter((entry) => ["WR", "TE"].includes(entry.player.position) && entry.player.team === qb.team).map((entry) => entry.player.name) : [];
  const bringBack = qb ? chosen.find((entry) => ["RB", "WR", "TE"].includes(entry.player.position) && entry.player.team === qb.opponent)?.player.name ?? null : null;
  return {
    lineupNumber,
    slots: chosen,
    playerIds: chosen.map((entry) => entry.player.dkPlayerId),
    totalSalary: chosen.reduce((sum, entry) => sum + entry.salary, 0),
    projectedFpts: chosen.reduce((sum, entry) => sum + entry.projection, 0),
    floorFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p10 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p10 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.floorFpts ?? entry.projection / entry.multiplier * .74 : entry.projection / entry.multiplier * .74) * entry.multiplier, 0),
    ceilingFpts: chosen.reduce((sum, entry) => sum + (entry.projectionSource === "workload" ? selectedWorkload(entry.player,settings.workloadPositions)!.p90 : entry.projectionSource === "calibrated" ? entry.player.calibrated!.p90 : entry.projectionSource === "our" || entry.projectionSource === "our_fallback" ? entry.player.ceilingFpts ?? entry.projection / entry.multiplier * 1.28 : entry.projection / entry.multiplier * 1.28) * entry.multiplier, 0),
    projectedOwnership: chosen.some((entry) => entry.player.linestarOwnPct != null)
      ? chosen.reduce((sum, entry) => sum + (entry.player.linestarOwnPct ?? 0), 0)
      : null,
    stackSummary: { quarterback: qb?.name ?? null, passCatchers, bringBack },
  };
}

export function optimizeNflLineups(players: NflOptimizerPlayer[], settings: NflOptimizerSettings): NflOptimizerResult {
  validateSettings(settings);
  const excluded = new Set(settings.excludedPlayerIds);
  const coverage = { requested: players.length, direct: 0, fallback: 0, excluded: 0 };
  const pool: ResolvedPlayer[] = [];
  for (const player of players) {
    if (player.isOut || excluded.has(player.dkPlayerId)) { coverage.excluded++; continue; }
    const resolved = projectionFor(player, settings);
    if (!resolved) { coverage.excluded++; continue; }
    if (resolved.source === "dk_avg_fallback" || resolved.source === "our_fallback") coverage.fallback++; else coverage.direct++;
    pool.push({ ...player, projection: resolved.value, resolvedSource: resolved.source });
  }
  for (const [rawId, target] of Object.entries(settings.minExposureByPlayer)) {
    if (target > 0 && !pool.some((player) => player.dkPlayerId === Number(rawId))) {
      const named = players.find((player) => player.dkPlayerId === Number(rawId));
      throw new Error(`${named?.name ?? `Player ${rawId}`} has a target exposure but is unavailable in the selected projection source.`);
    }
  }
  const warnings: string[] = [];
  const dkFallback = pool.filter(p => p.resolvedSource === "dk_avg_fallback").length;
  const ourFallback = pool.filter(p => p.resolvedSource === "our_fallback").length;
  if (dkFallback) warnings.push(`${dkFallback} players used DK Avg fallback.`);
  if (ourFallback) warnings.push(`${ourFallback} players retained historical baseline projections.`);
  if (settings.projectionSource === "calibrated") {
    if (!coverage.direct) throw new Error("No qualified pregame calibrated projections are available. Refresh forecasts or choose the historical model.");
    warnings.push("Calibrated QB/DST is experimental; forward validation is pending.");
  }
  if (settings.projectionSource === "workload") {
    if (!coverage.direct) throw new Error("No eligible pregame forecasts for the enabled workload positions. Refresh the workload snapshot or select the historical source.");
    const counts=WORKLOAD_POSITIONS.map(pos=>`${pos}: ${pool.filter(p=>p.resolvedSource==='workload'&&p.position===pos).length}`).join(', ');
    warnings.push(`Workload coverage — ${counts}. Other players retain disclosed fallback. RB/WR/TE candidate ranges worsened historical interval scores. No injury redistribution; WR has no invented boom bonus.`);
  }
  warnings.push("Lineup floor/ceiling sums are player-level search heuristics, not lineup P10/P90. Use Scenario Lab for joint distributions.");
  const exposureCounts = new Map<number, number>();
  const lineups: NflGeneratedLineup[] = [];
  const locked = new Set(settings.lockedPlayerIds);
  for (let lineupNumber = 1; lineupNumber <= settings.nLineups; lineupNumber++) {
    const remaining = settings.nLineups - lineupNumber + 1;
    const forced = new Set(locked);
    for (const player of pool) {
      const minPct = settings.minExposureByPlayer[String(player.dkPlayerId)] ?? 0;
      const target = Math.ceil(minPct * settings.nLineups - 1e-9);
      const current = exposureCounts.get(player.dkPlayerId) ?? 0;
      if (target - current >= remaining) forced.add(player.dkPlayerId);
    }
    const lineup = buildOne(pool, settings, lineupNumber, lineups, exposureCounts, forced);
    if (!lineup) {
      warnings.push(`Stopped after ${lineups.length} lineup(s): remaining exposure, uniqueness, salary, or stacking constraints are infeasible.`);
      break;
    }
    lineups.push(lineup);
    lineup.playerIds.forEach((id) => exposureCounts.set(id, (exposureCounts.get(id) ?? 0) + 1));
  }
  for (const player of pool) {
    const target = Math.ceil((settings.minExposureByPlayer[String(player.dkPlayerId)] ?? 0) * settings.nLineups - 1e-9);
    const actual = exposureCounts.get(player.dkPlayerId) ?? 0;
    if (actual < target) warnings.push(`${player.name} minimum exposure missed (${actual}/${target}); constraints were infeasible.`);
  }
  return { lineups, warnings, sourceCoverage: coverage };
}
